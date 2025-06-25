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

use crate::config::table::HudiTableConfig;
use crate::config::HudiConfigs;
use crate::error::CoreError;
use crate::file_group::log_file::content::Decoder;
use crate::file_group::log_file::log_block::{
    BlockMetadataKey, BlockMetadataType, BlockType, LogBlock,
};
use crate::file_group::log_file::log_format::{LogFormatVersion, MAGIC};
use crate::storage::reader::StorageReader;
use crate::storage::Storage;
use crate::timeline::selector::InstantRange;
use crate::Result;
use bytes::BytesMut;
use std::collections::HashMap;
use std::io::{self, Read, Seek};
use std::sync::Arc;

pub const DEFAULT_BUFFER_SIZE: usize = 16 * 1024 * 1024;

#[allow(dead_code)]
#[derive(Debug)]
pub struct LogFileReader<R: Read + Seek> {
    hudi_configs: Arc<HudiConfigs>,
    storage: Arc<Storage>,
    reader: R,
    buffer: BytesMut,
    timezone: String,
}

impl LogFileReader<StorageReader> {
    pub async fn new(
        hudi_configs: Arc<HudiConfigs>,
        storage: Arc<Storage>,
        relative_path: &str,
    ) -> Result<Self> {
        let reader = storage.get_storage_reader(relative_path).await?;
        let timezone = hudi_configs
            .get_or_default(HudiTableConfig::TimelineTimezone)
            .to::<String>();
        Ok(Self {
            hudi_configs,
            storage,
            reader,
            buffer: BytesMut::with_capacity(DEFAULT_BUFFER_SIZE),
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

    /// First, read 4 bytes for the number of entries in the metadata section (header or footer).
    ///
    /// Then for each entry,
    /// 1. Read 4 bytes for the key
    /// 2. Read 4 bytes for the value's length
    /// 3. Read the bytes of the length for the value
    ///
    /// See also [`BlockMetadataKey`].
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
        let mut skipped = false;
        if self.should_skip_block(&header, instant_range)? {
            skipped = true;
            // TODO skip reading block
        }

        let decoder = Decoder::new(self.hudi_configs.clone());
        let record_batches = decoder.decode_content(
            self.reader.by_ref(),
            &format_version,
            block_length,
            &block_type,
            &header,
        )?;
        let footer = self.read_block_metadata(BlockMetadataType::Footer, &format_version)?;
        let _ = self.read_total_block_length(&format_version)?;

        Ok(Some(LogBlock {
            format_version,
            block_type,
            header,
            record_batches,
            footer,
            skipped,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
        let hudi_configs = Arc::new(HudiConfigs::new([(HudiTableConfig::PrecombineField, "ts")]));
        let storage = Storage::new_with_base_url(dir_url)?;
        LogFileReader::new(hudi_configs, storage, file_name).await
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

        let batches = block.record_batches.data_batches.as_slice();
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

        let batches = &block.record_batches;
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
        assert_eq!(block.record_batches.num_data_batches(), 0);
        assert_eq!(block.record_batches.num_delete_batches(), 1);
        assert_eq!(block.record_batches.num_data_rows(), 0);
        assert_eq!(block.record_batches.num_delete_rows(), 3);

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

        // Check record batches
        assert_eq!(block.record_batches.num_data_batches(), 0);
        assert_eq!(block.record_batches.num_delete_batches(), 0);
        assert_eq!(block.record_batches.num_data_rows(), 0);
        assert_eq!(block.record_batches.num_delete_rows(), 0);

        // Check footer
        assert!(block.footer.is_empty());

        Ok(())
    }
}
