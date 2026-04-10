/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use crate::Result;
use crate::error::CoreError;
use crate::file_group::log_file::content::Decoder;
use crate::file_group::log_file::log_block::{
    BlockMetadataKey, BlockMetadataType, BlockType, LogBlock, LogBlockContentLocation,
};
use crate::file_group::log_file::log_format::{LogFormatVersion, MAGIC};
use crate::file_group::reader::reader_context::ReaderContext;
use crate::storage::Storage;
use crate::storage::reader::StorageReader;
use crate::timeline::selector::InstantRange;
use std::collections::HashMap;
use std::io::SeekFrom;
use std::io::{self, Read, Seek};
use std::sync::Arc;

#[derive(Debug)]
pub struct LogFileReader<R: Read + Seek> {
    reader_context: Arc<ReaderContext>,
    reader: R,
    timezone: String,
}

impl LogFileReader<StorageReader> {
    pub async fn new(
        reader_context: Arc<ReaderContext>,
        storage: Arc<Storage>,
        relative_path: &str,
    ) -> Result<Self> {
        let reader = storage.get_storage_reader(relative_path).await?;
        let timezone = reader_context.timezone();
        Ok(Self {
            reader_context,
            reader,
            timezone,
        })
    }

    pub fn read_all_blocks(&mut self, instant_range: &InstantRange) -> Result<Vec<LogBlock>> {
        let mut blocks = Vec::new();
        while let Some(block) = self.read_next_block(instant_range)? {
            if block.skipped {
                continue;
            }
            blocks.push(block);
        }
        Ok(blocks)
    }

    /// Read all blocks from the log file without instant-range filtering.
    ///
    /// Unlike [`read_all_blocks`], this method returns ALL blocks including those
    /// that would normally be skipped by instant-range filtering. This is needed
    /// by [`BaseHoodieLogRecordReader::scan_internal`] which implements its own
    /// 4-gate filtering algorithm.
    pub fn read_all_blocks_unfiltered(&mut self) -> Result<Vec<LogBlock>> {
        let wide_range = InstantRange::up_to("99991231235959999", &self.timezone);
        let mut blocks = Vec::new();
        while let Some(block) = self.read_next_block(&wide_range)? {
            // Include all blocks (even skipped ones) since the caller
            // handles filtering via the 4-gate algorithm.
            blocks.push(block);
        }
        Ok(blocks)
    }

    /// Read all blocks as metadata-only (no content decoding).
    ///
    /// Mirrors Java's Pass 1 behavior: reads block headers and records
    /// `content_location` for later lazy inflate, but seeks past the
    /// content bytes without decoding them. Only one block's content is
    /// in memory at a time during Pass 3.
    ///
    /// Each block receives a clone of the underlying file `Bytes` (O(1) ref-count
    /// increment), mirroring Java's `inputStreamSupplier` on `HoodieLogBlock`.
    /// This enables the buffer to call `inflate_from_bytes()` synchronously
    /// during Pass 3 without re-downloading the file.
    ///
    /// # Arguments
    /// * `log_file_path` - The relative path to the log file (stored in `LogBlockContentLocation`)
    pub fn read_all_blocks_metadata_only(
        &mut self,
        log_file_path: &str,
    ) -> Result<Vec<LogBlock>> {
        let file_bytes = self.reader.get_ref_bytes().clone();
        let mut blocks = Vec::new();
        while let Some(mut block) = self.read_next_block_metadata_only(log_file_path)? {
            block.set_source_bytes(file_bytes.clone());
            blocks.push(block);
        }
        Ok(blocks)
    }
}

impl<R: Read + Seek> LogFileReader<R> {
    /// Read [`MAGIC`] from the log file.
    ///
    /// Returns `Ok(true)` if the magic bytes are read successfully.
    ///
    /// Returns `Ok(false)` if the end of the file is reached.
    ///
    /// Returns an error if the magic bytes are invalid or an I/O error occurs.
    fn read_magic(&mut self) -> Result<bool> {
        let mut magic = [0u8; 6];
        match self.reader.read_exact(&mut magic) {
            Ok(_) => {
                if magic != MAGIC {
                    return Err(CoreError::LogFormatError("Invalid magic".to_string()));
                }
                Ok(true)
            }
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(false),
            Err(e) => Err(CoreError::ReadLogFileError(e)),
        }
    }

    /// Read 8 bytes for the log block's length excluding the magic.
    fn read_block_length(&mut self) -> Result<u64> {
        let mut size_buf = [0u8; 8];
        self.reader.read_exact(&mut size_buf)?;
        Ok(u64::from_be_bytes(size_buf))
    }

    fn create_corrupted_block_if_needed(
        &mut self,
        _curent_pos: u64,
        _block_length: Option<u64>,
    ) -> Option<LogBlock> {
        // TODO: support creating corrupted block
        None
    }

    fn read_block_length_or_corrupted_block(
        &mut self,
        start_pos: u64,
    ) -> Result<(u64, Option<LogBlock>)> {
        match self.read_block_length() {
            Ok(length) => {
                if let Some(block) = self.create_corrupted_block_if_needed(start_pos, Some(length))
                {
                    Ok((0, Some(block)))
                } else {
                    Ok((length, None))
                }
            }
            Err(e) => Err(e),
        }
    }

    /// Read 4 bytes for [`LogFormatVersion`].
    fn read_log_format_version(&mut self) -> Result<LogFormatVersion> {
        let mut version_buf = [0u8; 4];
        self.reader.read_exact(&mut version_buf)?;
        LogFormatVersion::try_from(version_buf)
    }

    /// Read 4 bytes for [`BlockType`].
    fn read_block_type(&mut self, format_version: &LogFormatVersion) -> Result<BlockType> {
        if !format_version.has_block_type() {
            return Err(CoreError::LogFormatError(
                "Block type is not supported".to_string(),
            ));
        }
        let mut type_buf = [0u8; 4];
        self.reader.read_exact(&mut type_buf)?;
        BlockType::try_from(type_buf)
    }

    /// Read block metadata (header or footer).
    ///
    /// Format:
    /// 1. 4 bytes: number of entries
    /// 2. For each entry:
    ///    - 4 bytes: key ordinal (see [`BlockMetadataKey`])
    ///    - 4 bytes: value length
    ///    - N bytes: value string
    fn read_block_metadata(
        &mut self,
        metadata_type: BlockMetadataType,
        format_version: &LogFormatVersion,
    ) -> Result<HashMap<BlockMetadataKey, String>> {
        match metadata_type {
            BlockMetadataType::Header if !format_version.has_header() => {
                return Ok(HashMap::new());
            }
            BlockMetadataType::Footer if !format_version.has_footer() => {
                return Ok(HashMap::new());
            }
            _ => {}
        }
        let mut num_entries_buf = [0u8; 4];
        self.reader.read_exact(&mut num_entries_buf)?;
        let num_entries = u32::from_be_bytes(num_entries_buf);
        let mut metadata: HashMap<BlockMetadataKey, String> =
            HashMap::with_capacity(num_entries as usize);
        for _ in 0..num_entries {
            let mut key_buf = [0u8; 4];
            self.reader.read_exact(&mut key_buf)?;
            let key = BlockMetadataKey::try_from(key_buf)?;
            let mut value_len_buf = [0u8; 4];
            self.reader.read_exact(&mut value_len_buf)?;
            let value_len = u32::from_be_bytes(value_len_buf);
            let mut value_buf = vec![0u8; value_len as usize];
            self.reader.read_exact(&mut value_buf)?;
            let value =
                String::from_utf8(value_buf).map_err(|e| CoreError::Utf8Error(e.utf8_error()))?;
            metadata.insert(key, value);
        }
        Ok(metadata)
    }

    /// Read 8 bytes for the total length of the log block.
    fn read_total_block_length(
        &mut self,
        format_version: &LogFormatVersion,
    ) -> Result<Option<u64>> {
        if !format_version.has_total_log_block_length() {
            return Ok(None);
        }
        let mut size_buf = [0u8; 8];
        self.reader.read_exact(&mut size_buf)?;
        Ok(Some(u64::from_be_bytes(size_buf)))
    }

    /// Read 8 bytes for the content length, or fall back to `block_length` for V0.
    ///
    /// Mirrors Java's `HoodieLogFileReader.readBlock()` step 5:
    /// ```java
    /// int contentLength = nextBlockVersion.getVersion() != DEFAULT_VERSION
    ///     ? (int) inputStream.readLong() : blockSize;
    /// ```
    fn read_content_length(
        &mut self,
        format_version: &LogFormatVersion,
        block_length: u64,
    ) -> Result<u64> {
        if format_version.has_content_length() {
            let mut buf = [0u8; 8];
            self.reader.read_exact(&mut buf)?;
            Ok(u64::from_be_bytes(buf))
        } else {
            Ok(block_length)
        }
    }

    fn should_skip_block(
        &self,
        header: &HashMap<BlockMetadataKey, String>,
        instant_range: &InstantRange,
    ) -> Result<bool> {
        let instant_time =
            header
                .get(&BlockMetadataKey::InstantTime)
                .ok_or(CoreError::LogFormatError(
                    "Instant time not found".to_string(),
                ))?;
        instant_range.not_in_range(instant_time, &self.timezone)
    }

    fn read_next_block(&mut self, instant_range: &InstantRange) -> Result<Option<LogBlock>> {
        if !self.read_magic()? {
            return Ok(None);
        }

        let curr_pos = self
            .reader
            .stream_position()
            .map_err(CoreError::ReadLogFileError)?;

        let (block_length, _) = self.read_block_length_or_corrupted_block(curr_pos)?;
        let format_version = self.read_log_format_version()?;
        let block_type = self.read_block_type(&format_version)?;
        let header = self.read_block_metadata(BlockMetadataType::Header, &format_version)?;
        // If block is out of the requested range, fast skip its payload without decoding
        if self.should_skip_block(&header, instant_range)? {
            // block_length excludes the magic; we consumed 8 bytes of length already.
            // Jump to the end of this block (absolute seek from start of file):
            // end_pos = curr_pos (right after magic) + 8 (length field) + block_length
            let target = curr_pos
                .checked_add(8)
                .and_then(|v| v.checked_add(block_length))
                .ok_or_else(|| CoreError::LogFormatError("Block length overflow".to_string()))?;
            self.reader
                .seek(SeekFrom::Start(target))
                .map_err(CoreError::ReadLogFileError)?;

            return Ok(Some(LogBlock::new_skipped(
                format_version,
                block_type,
                header,
            )));
        }

        // Read content length before decoding, matching Java step 5→6
        let content_length = self.read_content_length(&format_version, block_length)?;

        let decoder = Decoder::new(self.reader_context.clone());
        let content = decoder.decode_content(
            self.reader.by_ref(),
            content_length,
            &block_type,
            &header,
        )?;
        let footer = self.read_block_metadata(BlockMetadataType::Footer, &format_version)?;
        let _ = self.read_total_block_length(&format_version)?;

        Ok(Some(LogBlock::new(
            format_version,
            block_type,
            header,
            content,
            footer,
        )))
    }

    /// Read the next block as metadata-only (no content decoding).
    ///
    /// Mirrors Java's `HoodieLogFileReader.readBlock()` with `shouldReadLazily=true`:
    /// reads the block header, records the content position in a `LogBlockContentLocation`,
    /// then seeks past the content + footer without decoding them.
    ///
    /// The returned `LogBlock` has `content = Empty` and `content_location = Some(...)`.
    /// Call `LogBlock::inflate()` later to load and decode the content on demand.
    fn read_next_block_metadata_only(
        &mut self,
        log_file_path: &str,
    ) -> Result<Option<LogBlock>> {
        if !self.read_magic()? {
            return Ok(None);
        }

        let curr_pos = self
            .reader
            .stream_position()
            .map_err(CoreError::ReadLogFileError)?;

        let (block_length, _) = self.read_block_length_or_corrupted_block(curr_pos)?;
        let format_version = self.read_log_format_version()?;
        let block_type = self.read_block_type(&format_version)?;
        let header = self.read_block_metadata(BlockMetadataType::Header, &format_version)?;

        // Read the content length field (8 bytes for V1+), matching Java step 5:
        //   int contentLength = nextBlockVersion.getVersion() != DEFAULT_VERSION
        //       ? (int) inputStream.readLong() : blockSize;
        let content_length = self.read_content_length(&format_version, block_length)?;

        // Record where the content starts — AFTER the content_length field,
        // matching Java step 6: contentPosition = inputStream.getPos()
        let content_position = self
            .reader
            .stream_position()
            .map_err(CoreError::ReadLogFileError)?;

        // Seek past the rest of the block (content + footer + total_block_length)
        let block_end = curr_pos
            .checked_add(8)
            .and_then(|v| v.checked_add(block_length))
            .ok_or_else(|| CoreError::LogFormatError("Block length overflow".to_string()))?;
        self.reader
            .seek(SeekFrom::Start(block_end))
            .map_err(CoreError::ReadLogFileError)?;

        let content_loc = LogBlockContentLocation {
            log_file_path: log_file_path.to_string(),
            content_position,
            content_length,
            block_length,
        };

        Ok(Some(LogBlock::new_lazy(
            format_version,
            block_type,
            header,
            content_loc,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::table::HudiTableConfig;
    use crate::file_group::log_file::log_block::CommandBlock;
    use crate::storage::util::parse_uri;
    use apache_avro::schema::Schema as AvroSchema;
    use std::fs::canonicalize;
    use std::path::PathBuf;

    fn get_valid_log_avro_data() -> (String, String) {
        let dir = PathBuf::from("tests/data/log_files/valid_log_avro_data");
        (
            canonicalize(dir).unwrap().to_str().unwrap().to_string(),
            ".ff32ab89-5ad0-4968-83b4-89a34c95d32f-0_20250316025816068.log.1_0-54-122".to_string(),
        )
    }

    fn get_valid_log_parquet_data() -> (String, String) {
        let dir = PathBuf::from("tests/data/log_files/valid_log_parquet_data");
        (
            canonicalize(dir).unwrap().to_str().unwrap().to_string(),
            ".ee2ace10-7667-40f5-9848-0a144b5ea064-0_20250113230302428.log.1_0-188-387".to_string(),
        )
    }

    fn get_valid_log_delete() -> (String, String) {
        let dir = PathBuf::from("tests/data/log_files/valid_log_delete");
        (
            canonicalize(dir).unwrap().to_str().unwrap().to_string(),
            ".6d3d1d6e-2298-4080-a0c1-494877d6f40a-0_20250618054711154.log.1_0-26-85".to_string(),
        )
    }

    fn get_valid_log_rollback() -> (String, String) {
        let dir = PathBuf::from("tests/data/log_files/valid_log_rollback");
        (
            canonicalize(dir).unwrap().to_str().unwrap().to_string(),
            ".0712b9f9-d2d5-4cae-bcf4-8fd7146af503-0_20250126040823628.log.2_1-0-1".to_string(),
        )
    }

    async fn create_log_file_reader(
        dir: &str,
        file_name: &str,
    ) -> Result<LogFileReader<StorageReader>> {
        let dir_url = parse_uri(dir)?;
        let storage = Storage::new_with_base_url(dir_url)?;
        LogFileReader::new(Arc::new(ReaderContext::empty()), storage, file_name).await
    }

    #[tokio::test]
    async fn test_read_log_file_with_avro_data_block() -> Result<()> {
        let (dir, file_name) = get_valid_log_avro_data();
        let mut reader = create_log_file_reader(&dir, &file_name).await?;
        let instant_range = InstantRange::up_to("20250316025828811", "utc");
        let blocks = reader.read_all_blocks(&instant_range)?;
        assert_eq!(blocks.len(), 1);

        let block = &blocks[0];
        assert_eq!(block.format_version, LogFormatVersion::V1);
        assert_eq!(block.block_type, BlockType::AvroData);
        assert_eq!(block.header.len(), 2);
        assert_eq!(block.instant_time()?, "20250316025828811");
        assert!(block.target_instant_time().is_err());
        assert!(block.schema().is_ok());
        assert!(block.command_block_type().is_err());

        let batches = block.record_batches().unwrap().data_batches.as_slice();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);

        assert!(block.footer.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_read_log_file_with_parquet_data_block() -> Result<()> {
        let (dir, file_name) = get_valid_log_parquet_data();
        let mut reader = create_log_file_reader(&dir, &file_name).await?;
        let instant_range = InstantRange::up_to("20250113230424191", "utc");
        let blocks = reader.read_all_blocks(&instant_range)?;
        assert_eq!(blocks.len(), 1);

        let block = &blocks[0];
        assert_eq!(block.format_version, LogFormatVersion::V1);
        assert_eq!(block.block_type, BlockType::ParquetData);
        assert_eq!(block.header.len(), 2);
        assert_eq!(block.instant_time()?, "20250113230424191");
        assert!(block.target_instant_time().is_err());
        assert!(block.schema().is_ok());
        assert!(block.command_block_type().is_err());

        let batches = block.record_batches().unwrap();
        assert_eq!(batches.num_data_batches(), 1);
        assert_eq!(batches.num_data_rows(), 1);

        assert!(block.footer.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_read_log_file_with_delete_block() -> Result<()> {
        let (dir, file_name) = get_valid_log_delete();
        let mut reader = create_log_file_reader(&dir, &file_name).await?;
        let instant_range = InstantRange::up_to("20250618054714114", "utc");
        let blocks = reader.read_all_blocks(&instant_range)?;
        assert_eq!(blocks.len(), 1, "Expected one delete block");

        let block = &blocks[0];
        assert_eq!(
            block.format_version,
            LogFormatVersion::V1,
            "Expected V1 format version"
        );
        assert_eq!(
            block.block_type,
            BlockType::Delete,
            "Expected Delete block type"
        );
        assert!(!block.is_data_block());
        assert!(block.is_delete_block());
        assert!(!block.is_rollback_block());

        // check header
        assert_eq!(block.header.len(), 2);
        assert_eq!(block.instant_time()?, "20250618054714114");
        assert!(
            block.target_instant_time().is_err(),
            "Target instant time should not be available for delete block"
        );
        let schema = block.schema()?;
        let schema = AvroSchema::parse_str(schema)?;
        assert_eq!(
            schema.name().unwrap().to_string(),
            "hoodie.v6_trips_8i3d.v6_trips_8i3d_record"
        );
        assert!(
            block.command_block_type().is_err(),
            "Command block type should not be available for delete block"
        );

        // Check record batches
        let batches = block.record_batches().unwrap();
        assert_eq!(batches.num_data_batches(), 0);
        assert_eq!(batches.num_delete_batches(), 1);
        assert_eq!(batches.num_data_rows(), 0);
        assert_eq!(batches.num_delete_rows(), 3);

        // Check footer
        assert!(block.footer.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_read_log_file_with_rollback_block() -> Result<()> {
        let (dir, file_name) = get_valid_log_rollback();
        let mut reader = create_log_file_reader(&dir, &file_name).await?;
        let instant_range = InstantRange::up_to("20250126040936578", "utc");
        let blocks = reader.read_all_blocks(&instant_range)?;
        assert_eq!(blocks.len(), 1, "Expected one rollback block");

        let block = &blocks[0];
        assert_eq!(
            block.format_version,
            LogFormatVersion::V1,
            "Expected V1 format version"
        );
        assert_eq!(
            block.block_type,
            BlockType::Command,
            "Expected Command block type for rollback"
        );
        assert!(!block.is_data_block());
        assert!(!block.is_delete_block());
        assert!(block.is_rollback_block());

        // check header
        assert_eq!(
            block.header.len(),
            3,
            "Expected 3 header entries for rollback block"
        );
        assert_eq!(block.instant_time()?, "20250126040936578");
        assert_eq!(block.target_instant_time()?, "20250126040826878");
        assert_eq!(
            block.schema().unwrap_err().to_string(),
            "Schema not found",
            "Schema should not be available for rollback block"
        );
        assert_eq!(block.command_block_type()?, CommandBlock::Rollback);

        // Command blocks have no record content
        assert!(block.record_batches().is_none());

        // Check footer
        assert!(block.footer.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_skip_out_of_range_block_fast_path() -> Result<()> {
        // use a file with a single data block
        let (dir, file_name) = get_valid_log_parquet_data();
        let mut reader = create_log_file_reader(&dir, &file_name).await?;

        // choose an end timestamp earlier than the block's instant time so it should be skipped
        let instant_range = InstantRange::up_to("20200101000000000", "utc");

        // call the internal reader to inspect the skipped block
        let maybe_block = reader.read_next_block(&instant_range)?;
        assert!(maybe_block.is_some(), "Expected a block to be read");
        let block = maybe_block.unwrap();
        assert!(block.skipped, "Block should be marked as skipped");
        // Skipped blocks have empty content
        assert!(block.record_batches().is_none());

        // next call should hit EOF
        let next = reader.read_next_block(&instant_range)?;
        assert!(
            next.is_none(),
            "Should reach EOF after skipping the only block"
        );

        Ok(())
    }
}
