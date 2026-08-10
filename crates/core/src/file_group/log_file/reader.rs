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
use crate::config::HudiConfigs;
use crate::config::table::HudiTableConfig;
use crate::error::CoreError;
use crate::file_group::log_file::content::Decoder;
use crate::file_group::log_file::log_block::{
    BlockMetadataKey, BlockMetadataType, BlockType, DeferredContent, LogBlock, LogBlockContent,
    LogBlockContentLocation,
};
use crate::file_group::log_file::log_format::{LogFormatVersion, MAGIC};
use crate::storage::reader::LogBlockFetcher;
use crate::storage::reader::StorageReader;
use crate::storage::{RowFilterBuilder, Storage};
use crate::timeline::selector::InstantRange;
use std::collections::HashMap;
use std::io::SeekFrom;
use std::io::{self, Read, Seek};
use std::sync::Arc;

/// Read until `buf` is full or the stream ends, retrying on interrupts.
///
/// `Read::read` may return fewer bytes than asked for without being at EOF, so
/// a single call cannot decide whether the scan window is exhausted.
fn read_up_to<R: Read>(reader: &mut R, buf: &mut [u8]) -> Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        match reader.read(&mut buf[filled..]) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(CoreError::ReadLogFileError(e)),
        }
    }
    Ok(filled)
}

/// First offset of `needle` within `haystack`.
fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

pub struct LogFileReader<R: Read + Seek> {
    hudi_configs: Arc<HudiConfigs>,
    reader: R,
    timezone: String,
    /// Predicate to push into parquet log blocks. Unset by default, so a caller
    /// that has not decided whether pushing is sound reads every row.
    row_filter: Option<RowFilterBuilder>,
    /// Schema Avro blocks are resolved up to. Unset by default, so a caller that
    /// has none reads each block at the schema it was written with.
    reader_schema_json: Option<String>,
}

// `row_filter` holds a closure, which has no `Debug`. Report whether one is set
// rather than dropping the derive from the whole struct.
impl<R: Read + Seek> std::fmt::Debug for LogFileReader<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogFileReader")
            .field("hudi_configs", &self.hudi_configs)
            .field("timezone", &self.timezone)
            .field("row_filter", &self.row_filter.is_some())
            .field("reader_schema_json", &self.reader_schema_json.is_some())
            .finish_non_exhaustive()
    }
}

impl LogFileReader<StorageReader> {
    pub async fn new(
        hudi_configs: Arc<HudiConfigs>,
        storage: Arc<Storage>,
        relative_path: &str,
    ) -> Result<Self> {
        let reader = storage.get_storage_reader(relative_path).await?;
        let timezone: String = hudi_configs
            .get_or_default(HudiTableConfig::TimelineTimezone)
            .into();
        Ok(Self {
            hudi_configs,
            reader,
            timezone,
            row_filter: None,
            reader_schema_json: None,
        })
    }

    /// Open a reader that fetches bounded windows rather than the whole file.
    ///
    /// Pairs with [`Self::read_all_blocks_metadata_only`]: the scan walks
    /// headers out of the window and each admitted block reads its own content
    /// later, so neither step holds the file.
    pub async fn new_streaming(
        hudi_configs: Arc<HudiConfigs>,
        storage: Arc<Storage>,
        relative_path: &str,
    ) -> Result<Self> {
        let reader = storage.get_streaming_storage_reader(relative_path).await?;
        let timezone: String = hudi_configs
            .get_or_default(HudiTableConfig::TimelineTimezone)
            .into();
        Ok(Self {
            hudi_configs,
            reader,
            timezone,
            row_filter: None,
            reader_schema_json: None,
        })
    }

    /// Walk every block reading only its header, recording where the content
    /// sits so an admitted block can read it later.
    ///
    /// `instant_range` is not applied here — the caller decides what to admit
    /// from the headers, which is the point of not decoding yet.
    pub fn read_all_blocks_metadata_only(&mut self) -> Result<Vec<LogBlock>> {
        let fetcher = self.reader.block_fetcher();
        let mut blocks = Vec::new();
        while let Some(block) = self.read_next_block_metadata_only(&fetcher)? {
            blocks.push(block);
        }
        Ok(blocks)
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

    /// Push a predicate into this file's parquet log blocks.
    ///
    /// Left unset, no predicate is pushed and every row is read. Whether pushing
    /// is sound is the caller's decision — see [`Decoder::with_row_filter`].
    pub fn with_row_filter(mut self, row_filter: Option<RowFilterBuilder>) -> Self {
        self.row_filter = row_filter;
        self
    }

    /// Resolve Avro blocks up to this schema. See
    /// [`Decoder::with_reader_schema`].
    pub fn with_reader_schema(mut self, reader_schema_json: Option<String>) -> Self {
        self.reader_schema_json = reader_schema_json;
        self
    }

    /// Window used when scanning for the next MAGIC after a corrupt block.
    const BLOCK_SCAN_READ_BUFFER_SIZE: usize = 1024 * 1024;

    /// Total length of the stream, restoring the original position.
    fn stream_len(&mut self) -> Result<u64> {
        let cur = self
            .reader
            .stream_position()
            .map_err(CoreError::ReadLogFileError)?;
        let end = self
            .reader
            .seek(SeekFrom::End(0))
            .map_err(CoreError::ReadLogFileError)?;
        self.reader
            .seek(SeekFrom::Start(cur))
            .map_err(CoreError::ReadLogFileError)?;
        Ok(end)
    }

    /// Whether the next bytes are a MAGIC marker, treating end-of-file as one.
    fn next_is_magic_or_eof(&mut self) -> Result<bool> {
        let mut magic = [0u8; 6];
        match self.reader.read_exact(&mut magic) {
            Ok(_) => Ok(magic == MAGIC),
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(true),
            Err(e) => Err(CoreError::ReadLogFileError(e)),
        }
    }

    /// Whether the block starting at `magic_pos` is corrupt, given the
    /// `block_length` already read from just after the magic.
    ///
    /// A well-formed block records its total size twice — once in the header
    /// length field and once in a trailing reverse pointer — and is followed by
    /// either another block or the end of the file. Three checks, and every
    /// offset is computed with checked arithmetic so a garbage length reports
    /// corruption rather than panicking or allocating against it:
    ///
    /// 1. the trailing pointer has to lie inside the file
    /// 2. the size it records has to agree with the header
    /// 3. what follows the block has to be a MAGIC marker or the end
    ///
    /// The reader is left just after the length field either way, which is
    /// where the caller expects to continue from.
    fn is_block_corrupted(&mut self, magic_pos: u64, block_length: u64) -> Result<bool> {
        let after_length = magic_pos
            .checked_add(MAGIC.len() as u64)
            .and_then(|v| v.checked_add(8));
        let Some(after_length) = after_length else {
            return Ok(true);
        };
        // The trailing long sits 8 bytes before the block ends.
        let trailing_pos = after_length
            .checked_add(block_length)
            .and_then(|v| v.checked_sub(8));
        let Some(trailing_pos) = trailing_pos else {
            return Ok(true);
        };

        let stream_len = self.stream_len()?;

        if trailing_pos
            .checked_add(8)
            .map(|e| e > stream_len)
            .unwrap_or(true)
        {
            self.reader
                .seek(SeekFrom::Start(after_length))
                .map_err(CoreError::ReadLogFileError)?;
            return Ok(true);
        }

        self.reader
            .seek(SeekFrom::Start(trailing_pos))
            .map_err(CoreError::ReadLogFileError)?;
        let mut buf = [0u8; 8];
        self.reader
            .read_exact(&mut buf)
            .map_err(CoreError::ReadLogFileError)?;
        let trailing = u64::from_be_bytes(buf);

        // The trailing value counts the magic; the header length does not.
        let corrupt = match trailing.checked_sub(MAGIC.len() as u64) {
            Some(size_from_footer) => size_from_footer != block_length,
            None => true,
        };

        let block_end = after_length
            .checked_add(block_length)
            .ok_or_else(|| CoreError::LogFormatError("Block length overflow".to_string()))?;

        let result = if corrupt {
            true
        } else {
            self.reader
                .seek(SeekFrom::Start(block_end))
                .map_err(CoreError::ReadLogFileError)?;
            !self.next_is_magic_or_eof()?
        };

        self.reader
            .seek(SeekFrom::Start(after_length))
            .map_err(CoreError::ReadLogFileError)?;
        Ok(result)
    }

    /// Offset of the next MAGIC strictly after the magic at `from_pos`, or the
    /// end of the file. The scan starts `MAGIC.len()` bytes in, so `from_pos`
    /// itself is never a candidate — a caller recovering from a bad block at
    /// `from_pos` would otherwise be handed it straight back.
    ///
    /// Windows overlap by `MAGIC.len() - 1` so a marker straddling a window
    /// boundary is still found.
    fn scan_for_next_block_offset(&mut self, from_pos: u64) -> Result<u64> {
        let stream_len = self.stream_len()?;
        let mut pos = from_pos.saturating_add(MAGIC.len() as u64);
        if pos >= stream_len {
            return Ok(stream_len);
        }
        self.reader
            .seek(SeekFrom::Start(pos))
            .map_err(CoreError::ReadLogFileError)?;
        let mut buf = vec![0u8; Self::BLOCK_SCAN_READ_BUFFER_SIZE];
        loop {
            let n = read_up_to(&mut self.reader, &mut buf)?;
            if n == 0 {
                return Ok(stream_len);
            }
            if let Some(idx) = find_subslice(&buf[..n], MAGIC) {
                return Ok(pos + idx as u64);
            }
            if n < buf.len() {
                return Ok(stream_len);
            }
            let advance = (n - (MAGIC.len() - 1)) as u64;
            pos += advance;
            self.reader
                .seek(SeekFrom::Start(pos))
                .map_err(CoreError::ReadLogFileError)?;
        }
    }

    /// Synthesize a corrupt block and leave the reader at the recovery offset,
    /// so one bad block costs its own span rather than the rest of the file.
    fn create_corrupted_block(&mut self, magic_pos: u64) -> Result<LogBlock> {
        let next_offset = self.scan_for_next_block_offset(magic_pos)?;
        log::warn!(
            "Found corrupt log block at offset {magic_pos}; next available block at {next_offset}"
        );
        self.reader
            .seek(SeekFrom::Start(next_offset))
            .map_err(CoreError::ReadLogFileError)?;
        Ok(LogBlock::new(
            LogFormatVersion::V1,
            BlockType::Corrupted,
            HashMap::new(),
            LogBlockContent::Empty,
            HashMap::new(),
        ))
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

    /// Read one block's header, recording where its content sits and seeking
    /// past it without decoding.
    ///
    /// Corruption is still detected — a corrupt block cannot be trusted to say
    /// where the next one starts, so the check has to happen during the sweep
    /// rather than being deferred with the content.
    fn read_next_block_metadata_only(
        &mut self,
        fetcher: &LogBlockFetcher,
    ) -> Result<Option<LogBlock>> {
        let magic_pos = self
            .reader
            .stream_position()
            .map_err(CoreError::ReadLogFileError)?;
        if !self.read_magic()? {
            return Ok(None);
        }

        let block_length = self.read_block_length()?;
        if self.is_block_corrupted(magic_pos, block_length)? {
            return Ok(Some(self.create_corrupted_block(magic_pos)?));
        }

        let format_version = self.read_log_format_version()?;
        let block_type = self.read_block_type(&format_version)?;
        let header = self.read_block_metadata(BlockMetadataType::Header, &format_version)?;

        // The range starts at the content-length field, not after it, because
        // decoding reads that field itself — inflate hands the same bytes to the
        // same decoder the eager path uses.
        let content_position = self
            .reader
            .stream_position()
            .map_err(CoreError::ReadLogFileError)?;
        let payload_length = if format_version.has_content_length() {
            let mut buf = [0u8; 8];
            self.reader
                .read_exact(&mut buf)
                .map_err(CoreError::ReadLogFileError)?;
            u64::from_be_bytes(buf)
        } else {
            block_length
        };
        let content_length = if format_version.has_content_length() {
            // `payload_length` comes straight from the file and nothing has
            // validated it — `is_block_corrupted` checks the outer block span,
            // not this inner field — so a corrupt value must not wrap here any
            // more than in the `checked_add` below.
            payload_length.checked_add(8).ok_or_else(|| {
                CoreError::LogFormatError(format!(
                    "Content length overflow: payload length {payload_length}"
                ))
            })?
        } else {
            payload_length
        };

        // Skip the content; the footer and trailing length follow it.
        let after_content = content_position
            .checked_add(content_length)
            .ok_or_else(|| CoreError::LogFormatError("Content length overflow".to_string()))?;
        self.reader
            .seek(SeekFrom::Start(after_content))
            .map_err(CoreError::ReadLogFileError)?;
        let footer = self.read_block_metadata(BlockMetadataType::Footer, &format_version)?;
        let _ = self.read_total_block_length(&format_version)?;

        let mut block = LogBlock::new(
            format_version,
            block_type,
            header,
            LogBlockContent::Empty,
            footer,
        );
        block.deferred_content = Some(DeferredContent {
            location: LogBlockContentLocation {
                content_position,
                content_length,
            },
            fetcher: fetcher.clone(),
        });
        Ok(Some(block))
    }

    fn read_next_block(&mut self, instant_range: &InstantRange) -> Result<Option<LogBlock>> {
        // The magic's own offset — where a corrupt block's span starts.
        let magic_pos = self
            .reader
            .stream_position()
            .map_err(CoreError::ReadLogFileError)?;
        if !self.read_magic()? {
            return Ok(None);
        }

        let curr_pos = self
            .reader
            .stream_position()
            .map_err(CoreError::ReadLogFileError)?;

        let block_length = self.read_block_length()?;
        // Validate before parsing the body: a corrupt or truncated block must
        // yield a corrupt marker and resume at the next block, rather than
        // failing the whole file on one bad span.
        if self.is_block_corrupted(magic_pos, block_length)? {
            return Ok(Some(self.create_corrupted_block(magic_pos)?));
        }
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

        let decoder = Decoder::new(self.hudi_configs.clone())
            .with_row_filter(self.row_filter.clone())
            .with_reader_schema(self.reader_schema_json.clone());
        let content = decoder.decode_content(
            self.reader.by_ref(),
            &format_version,
            block_length,
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
        let hudi_configs = Arc::new(HudiConfigs::new([(HudiTableConfig::OrderingFields, "ts")]));
        let storage = Storage::new_with_base_url(dir_url)?;
        LogFileReader::new(hudi_configs, storage, file_name).await
    }

    /// A block whose recorded length disagrees with its trailing reverse
    /// pointer is corrupt. Both sizes are written by the same writer, so a
    /// mismatch means the span cannot be trusted.
    /// Sweeping headers and inflating afterwards has to produce exactly what
    /// reading the file eagerly produces. This is the property the whole lazy
    /// path rests on: if it holds, nothing downstream can tell which way the
    /// blocks were read.
    async fn assert_lazy_matches_eager(dir: &str, file_name: &str) -> Result<()> {
        let dir_url = parse_uri(dir)?;
        let hudi_configs = Arc::new(HudiConfigs::new([(HudiTableConfig::OrderingFields, "ts")]));
        let storage = Storage::new_with_base_url(dir_url)?;

        let mut eager =
            LogFileReader::new(hudi_configs.clone(), storage.clone(), file_name).await?;
        let eager_blocks =
            eager.read_all_blocks(&InstantRange::up_to("99991231235959999", "utc"))?;

        let mut lazy =
            LogFileReader::new_streaming(hudi_configs.clone(), storage, file_name).await?;
        let mut lazy_blocks = lazy.read_all_blocks_metadata_only()?;

        assert_eq!(
            lazy_blocks.len(),
            eager_blocks.len(),
            "the sweep must find the same blocks"
        );

        let decoder = Decoder::new(hudi_configs);
        for (lazy_block, eager_block) in lazy_blocks.iter_mut().zip(eager_blocks.iter()) {
            assert_eq!(lazy_block.block_type, eager_block.block_type);
            assert_eq!(lazy_block.header, eager_block.header);
            lazy_block.load_content(&decoder)?;
            assert_eq!(
                lazy_block.content.as_records().map(|b| b.num_data_rows()),
                eager_block.content.as_records().map(|b| b.num_data_rows()),
                "inflated content must match the eager read for a {:?} block",
                eager_block.block_type
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_lazy_sweep_matches_eager_for_avro_blocks() -> Result<()> {
        let (dir, file_name) = get_valid_log_avro_data();
        assert_lazy_matches_eager(&dir, &file_name).await
    }

    #[tokio::test]
    async fn test_lazy_sweep_matches_eager_for_parquet_blocks() -> Result<()> {
        let (dir, file_name) = get_valid_log_parquet_data();
        assert_lazy_matches_eager(&dir, &file_name).await
    }

    #[tokio::test]
    async fn test_lazy_sweep_matches_eager_for_delete_blocks() -> Result<()> {
        let (dir, file_name) = get_valid_log_delete();
        assert_lazy_matches_eager(&dir, &file_name).await
    }

    #[tokio::test]
    async fn test_corrupt_block_detected_when_trailing_length_disagrees() -> Result<()> {
        let (dir, file_name) = get_valid_log_avro_data();
        let mut reader = create_log_file_reader(&dir, &file_name).await?;

        let magic_pos = 0;
        let real_length = {
            reader.read_magic()?;
            reader.read_block_length()?
        };
        assert!(
            !reader.is_block_corrupted(magic_pos, real_length)?,
            "a well-formed block must not be reported corrupt"
        );
        assert!(
            reader.is_block_corrupted(magic_pos, real_length + 1)?,
            "a length disagreeing with the trailing pointer must be reported corrupt"
        );
        Ok(())
    }

    /// A length pointing past the end of the file is corrupt, and must be
    /// decided by arithmetic rather than by trying to read there.
    #[tokio::test]
    async fn test_corrupt_block_detected_when_length_runs_past_eof() -> Result<()> {
        let (dir, file_name) = get_valid_log_avro_data();
        let mut reader = create_log_file_reader(&dir, &file_name).await?;

        assert!(
            reader.is_block_corrupted(0, u64::MAX)?,
            "overflow is corrupt"
        );
        assert!(
            reader.is_block_corrupted(0, 1 << 40)?,
            "a length past EOF is corrupt"
        );
        Ok(())
    }

    /// The streaming reader walks every block's header without decoding its
    /// content, and records where the content sits so an admitted block can
    /// fetch its own bytes later.
    ///
    /// This is what lets a scan walk a log file it could not hold: the assertion
    /// is that headers come back while content does not, since a metadata-only
    /// pass that quietly decoded would defeat the whole point.
    #[tokio::test]
    async fn test_read_all_blocks_metadata_only_defers_content() -> Result<()> {
        let (dir, file_name) = get_valid_log_avro_data();
        let dir_url = parse_uri(&dir)?;
        let hudi_configs = Arc::new(HudiConfigs::new([(HudiTableConfig::OrderingFields, "ts")]));
        let storage = Storage::new_with_base_url(dir_url)?;
        let mut reader = LogFileReader::new_streaming(hudi_configs, storage, &file_name).await?;

        let blocks = reader.read_all_blocks_metadata_only()?;
        assert!(!blocks.is_empty(), "the fixture has blocks to walk");
        for block in &blocks {
            assert!(
                block.content.is_empty(),
                "a metadata-only pass must not decode content"
            );
            assert!(
                block.deferred_content.is_some(),
                "each block must record where to fetch its content from"
            );
            assert!(
                block.header.contains_key(&BlockMetadataKey::InstantTime),
                "the header is what a metadata-only pass is for"
            );
        }
        Ok(())
    }

    /// A block deferred by the metadata-only pass reads back the same records
    /// the eager path produces. Deferring must change when the bytes are read,
    /// not what they decode to.
    #[tokio::test]
    async fn test_load_content_matches_the_eager_read() -> Result<()> {
        let (dir, file_name) = get_valid_log_avro_data();

        let mut eager_reader = create_log_file_reader(&dir, &file_name).await?;
        let eager =
            eager_reader.read_all_blocks(&InstantRange::up_to("29991231235959999", "utc"))?;

        let dir_url = parse_uri(&dir)?;
        let hudi_configs = Arc::new(HudiConfigs::new([(HudiTableConfig::OrderingFields, "ts")]));
        let storage = Storage::new_with_base_url(dir_url)?;
        let mut lazy_reader =
            LogFileReader::new_streaming(hudi_configs.clone(), storage, &file_name).await?;
        let mut lazy = lazy_reader.read_all_blocks_metadata_only()?;

        let decoder = Decoder::new(hudi_configs);
        for block in lazy.iter_mut() {
            block.load_content(&decoder)?;
        }

        let rows = |bs: &[LogBlock]| -> usize {
            bs.iter()
                .map(|b| match &b.content {
                    LogBlockContent::Records(r) => r.num_data_rows(),
                    _ => 0,
                })
                .sum()
        };
        assert!(rows(&eager) > 0, "the fixture must decode to something");
        assert_eq!(
            rows(&lazy),
            rows(&eager),
            "deferring content must not change what it decodes to"
        );
        Ok(())
    }

    /// Loading content on a block that already has it is a no-op, and on a block
    /// that never recorded where its content sits it is an error rather than a
    /// silent empty read.
    #[tokio::test]
    async fn test_load_content_without_a_deferred_location_is_an_error() -> Result<()> {
        let (dir, file_name) = get_valid_log_avro_data();
        let mut reader = create_log_file_reader(&dir, &file_name).await?;
        let mut blocks =
            reader.read_all_blocks(&InstantRange::up_to("29991231235959999", "utc"))?;
        let decoder = Decoder::new(Arc::new(HudiConfigs::new([(
            HudiTableConfig::OrderingFields,
            "ts",
        )])));

        // Already decoded by the eager read: nothing to do.
        blocks[0].load_content(&decoder)?;

        // A block with content cleared and no deferred location cannot say where
        // to read from.
        blocks[0].content = LogBlockContent::Empty;
        blocks[0].deferred_content = None;
        let err = blocks[0]
            .load_content(&decoder)
            .expect_err("a block with nowhere to read from must be an error");
        assert!(
            err.to_string().contains("headers-only"),
            "the error must say why it cannot load, got: {err}"
        );
        Ok(())
    }

    /// A block outside the scan's window is skipped by its instant time, and a
    /// block whose header carries none is an error — guessing would admit or
    /// drop it silently.
    #[tokio::test]
    async fn test_should_skip_block_uses_the_instant_time_header() -> Result<()> {
        let (dir, file_name) = get_valid_log_avro_data();
        let reader = create_log_file_reader(&dir, &file_name).await?;

        let in_window = HashMap::from([(
            BlockMetadataKey::InstantTime,
            "20250316025816068".to_string(),
        )]);
        assert!(
            !reader
                .should_skip_block(&in_window, &InstantRange::up_to("20250316025816068", "utc"))?,
            "a block at the window's inclusive end must be read"
        );
        assert!(
            reader
                .should_skip_block(&in_window, &InstantRange::up_to("20250316025816067", "utc"))?,
            "a block past the window's end must be skipped"
        );

        let err = reader
            .should_skip_block(
                &HashMap::new(),
                &InstantRange::up_to("20250316025816068", "utc"),
            )
            .expect_err("a header with no instant time must be an error");
        assert!(err.to_string().contains("Instant time"), "got: {err}");
        Ok(())
    }

    /// Recovery lands on a later magic marker, or the end of the file when
    /// there is none — so one bad block costs its own span, not the rest.
    ///
    /// The fixture is a valid multi-block file, so the scan lands on the *next*
    /// block's magic rather than EOF; what is pinned is that either answer
    /// stays inside the file. An earlier name claimed EOF specifically, which
    /// this fixture cannot produce.
    #[tokio::test]
    async fn test_scan_for_next_block_offset_stays_within_file_bounds() -> Result<()> {
        let (dir, file_name) = get_valid_log_avro_data();
        let mut reader = create_log_file_reader(&dir, &file_name).await?;

        let len = reader.stream_len()?;
        let offset = reader.scan_for_next_block_offset(0)?;
        assert!(
            offset <= len,
            "recovery offset {offset} must lie within the file ({len})"
        );
        Ok(())
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
