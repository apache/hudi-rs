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
use std::io::{self, Cursor};
use std::sync::Arc;

/// First offset of `needle` within `haystack`.
fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

pub struct LogFileReader {
    hudi_configs: Arc<HudiConfigs>,
    reader: StorageReader,
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
impl std::fmt::Debug for LogFileReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogFileReader")
            .field("hudi_configs", &self.hudi_configs)
            .field("timezone", &self.timezone)
            .field("row_filter", &self.row_filter.is_some())
            .field("reader_schema_json", &self.reader_schema_json.is_some())
            .finish_non_exhaustive()
    }
}

impl LogFileReader {
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
    /// Walk with no residency bound.
    ///
    /// For callers that are measuring or exercising the walk itself rather than a
    /// scan. Production goes through [`Self::read_all_blocks_metadata_only`] with a
    /// budget shared across every log file, which is what keeps peak residency at
    /// one window.
    pub async fn read_all_blocks_metadata_only_unbounded(&mut self) -> Result<Vec<LogBlock>> {
        let mut budget = u64::MAX;
        self.read_all_blocks_metadata_only(&mut budget).await
    }

    pub async fn read_all_blocks_metadata_only(
        &mut self,
        resident_budget: &mut u64,
    ) -> Result<Vec<LogBlock>> {
        let fetcher = self.reader.block_fetcher();
        let mut blocks = Vec::new();
        while let Some(block) = self
            .read_next_block_metadata_only(&fetcher, resident_budget)
            .await?
        {
            blocks.push(block);
        }
        Ok(blocks)
    }

    pub async fn read_all_blocks(&mut self, instant_range: &InstantRange) -> Result<Vec<LogBlock>> {
        let mut blocks = Vec::new();
        while let Some(block) = self.read_next_block(instant_range).await? {
            if block.skipped {
                continue;
            }
            blocks.push(block);
        }
        Ok(blocks)
    }

    /// Read [`MAGIC`] from the log file.
    ///
    /// Returns `Ok(true)` if the magic bytes are read successfully.
    ///
    /// Returns `Ok(false)` if the end of the file is reached.
    ///
    /// Returns an error if the magic bytes are invalid or an I/O error occurs.
    async fn read_magic(&mut self) -> Result<bool> {
        let mut magic = [0u8; 6];
        match self.reader.read_exact(&mut magic).await {
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
    async fn read_block_length(&mut self) -> Result<u64> {
        let mut size_buf = [0u8; 8];
        self.reader.read_exact(&mut size_buf).await?;
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
    const BLOCK_SCAN_READ_BUFFER_SIZE: u64 = 1024 * 1024;

    /// Whether the next bytes are a MAGIC marker, treating end-of-file as one.
    async fn next_is_magic_or_eof(&mut self) -> Result<bool> {
        let mut magic = [0u8; 6];
        match self.reader.read_exact(&mut magic).await {
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
    async fn is_block_corrupted(&mut self, magic_pos: u64, block_length: u64) -> Result<bool> {
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

        let stream_len = self.reader.file_len();

        if trailing_pos
            .checked_add(8)
            .map(|e| e > stream_len)
            .unwrap_or(true)
        {
            self.reader.seek_to(after_length);
            return Ok(true);
        }

        self.reader.seek_to(trailing_pos);
        let mut buf = [0u8; 8];
        self.reader
            .read_exact(&mut buf)
            .await
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
            self.reader.seek_to(block_end);
            !self.next_is_magic_or_eof().await?
        };

        self.reader.seek_to(after_length);
        Ok(result)
    }

    /// Offset of the next MAGIC strictly after the magic at `from_pos`, or the
    /// end of the file. The scan starts `MAGIC.len()` bytes in, so `from_pos`
    /// itself is never a candidate — a caller recovering from a bad block at
    /// `from_pos` would otherwise be handed it straight back.
    ///
    /// Windows overlap by `MAGIC.len() - 1` so a marker straddling a window
    /// boundary is still found.
    async fn scan_for_next_block_offset(&mut self, from_pos: u64) -> Result<u64> {
        let stream_len = self.reader.file_len();
        let mut pos = from_pos.saturating_add(MAGIC.len() as u64);
        loop {
            let Some(remaining) = stream_len.checked_sub(pos) else {
                return Ok(stream_len);
            };
            let window = remaining.min(Self::BLOCK_SCAN_READ_BUFFER_SIZE);
            if window == 0 {
                return Ok(stream_len);
            }
            self.reader.seek_to(pos);
            let buf = self
                .reader
                .read_bytes(window)
                .await
                .map_err(CoreError::ReadLogFileError)?;
            if let Some(idx) = find_subslice(&buf, MAGIC) {
                return Ok(pos + idx as u64);
            }
            // A short window means the file ended inside it, so there is no
            // later marker to find. This is also what makes the loop terminate:
            // past it the window is a full buffer, so the advance below is
            // positive.
            if window < Self::BLOCK_SCAN_READ_BUFFER_SIZE {
                return Ok(stream_len);
            }
            pos += window - (MAGIC.len() as u64 - 1);
        }
    }

    /// Synthesize a corrupt block and leave the reader at the recovery offset,
    /// so one bad block costs its own span rather than the rest of the file.
    async fn create_corrupted_block(&mut self, magic_pos: u64) -> Result<LogBlock> {
        let next_offset = self.scan_for_next_block_offset(magic_pos).await?;
        log::warn!(
            "Found corrupt log block at offset {magic_pos}; next available block at {next_offset}"
        );
        self.reader.seek_to(next_offset);
        Ok(LogBlock::new(
            LogFormatVersion::V1,
            BlockType::Corrupted,
            HashMap::new(),
            LogBlockContent::Empty,
            HashMap::new(),
        ))
    }

    /// Read 4 bytes for [`LogFormatVersion`].
    async fn read_log_format_version(&mut self) -> Result<LogFormatVersion> {
        let mut version_buf = [0u8; 4];
        self.reader.read_exact(&mut version_buf).await?;
        LogFormatVersion::try_from(version_buf)
    }

    /// Read 4 bytes for [`BlockType`].
    async fn read_block_type(&mut self, format_version: &LogFormatVersion) -> Result<BlockType> {
        if !format_version.has_block_type() {
            return Err(CoreError::LogFormatError(
                "Block type is not supported".to_string(),
            ));
        }
        let mut type_buf = [0u8; 4];
        self.reader.read_exact(&mut type_buf).await?;
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
    async fn read_block_metadata(
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
        self.reader.read_exact(&mut num_entries_buf).await?;
        let num_entries = u32::from_be_bytes(num_entries_buf);
        let mut metadata: HashMap<BlockMetadataKey, String> =
            HashMap::with_capacity(num_entries as usize);
        for _ in 0..num_entries {
            let mut key_buf = [0u8; 4];
            self.reader.read_exact(&mut key_buf).await?;
            let key = BlockMetadataKey::try_from(key_buf)?;
            let mut value_len_buf = [0u8; 4];
            self.reader.read_exact(&mut value_len_buf).await?;
            let value_len = u32::from_be_bytes(value_len_buf);
            let value_buf = self.reader.read_bytes(value_len as u64).await?;
            let value = String::from_utf8(value_buf.to_vec())
                .map_err(|e| CoreError::Utf8Error(e.utf8_error()))?;
            metadata.insert(key, value);
        }
        Ok(metadata)
    }

    /// Read 8 bytes for the total length of the log block.
    async fn read_total_block_length(
        &mut self,
        format_version: &LogFormatVersion,
    ) -> Result<Option<u64>> {
        if !format_version.has_total_log_block_length() {
            return Ok(None);
        }
        let mut size_buf = [0u8; 8];
        self.reader.read_exact(&mut size_buf).await?;
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

    /// End of the block whose magic sits at `magic_pos`, which is also where
    /// the next block's magic sits. `block_length` excludes the magic and its
    /// own 8-byte field, so both are added back.
    fn block_end(magic_pos: u64, block_length: u64) -> Result<u64> {
        magic_pos
            .checked_add(MAGIC.len() as u64)
            .and_then(|v| v.checked_add(8))
            .and_then(|v| v.checked_add(block_length))
            .ok_or_else(|| CoreError::LogFormatError("Block length overflow".to_string()))
    }

    /// Read one block's header, recording where its content sits and seeking
    /// past it without decoding.
    ///
    /// Corruption is still detected — a corrupt block cannot be trusted to say
    /// where the next one starts, so the check has to happen during the sweep
    /// rather than being deferred with the content.
    async fn read_next_block_metadata_only(
        &mut self,
        fetcher: &LogBlockFetcher,
        resident_budget: &mut u64,
    ) -> Result<Option<LogBlock>> {
        let magic_pos = self.reader.position();
        if !self.read_magic().await? {
            return Ok(None);
        }

        let block_length = self.read_block_length().await?;
        if self.is_block_corrupted(magic_pos, block_length).await? {
            return Ok(Some(self.create_corrupted_block(magic_pos).await?));
        }

        let format_version = self.read_log_format_version().await?;
        let block_type = self.read_block_type(&format_version).await?;
        let header = self
            .read_block_metadata(BlockMetadataType::Header, &format_version)
            .await?;

        // The range starts at the content-length field, not after it, because
        // decoding reads that field itself — inflate hands the same bytes to the
        // same decoder the eager path uses.
        let content_position = self.reader.position();
        let payload_length = if format_version.has_content_length() {
            let mut buf = [0u8; 8];
            self.reader
                .read_exact(&mut buf)
                .await
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

        // Take the content when it is already in hand, and skip past it otherwise.
        //
        // Deferring is what lets a scan walk a large file without holding it, and
        // it is why a block the gates discard costs no bytes. It buys nothing when
        // the bytes have already been transferred: a log file smaller than one
        // fetch window arrives whole on the first fill, so deferring its blocks
        // would re-request bytes this reader is already holding, one extra round
        // trip per file. The slice is zero-copy, sharing the window's allocation.
        let after_content = content_position
            .checked_add(content_length)
            .ok_or_else(|| CoreError::LogFormatError("Content length overflow".to_string()))?;
        let affordable = *resident_budget >= content_length;
        let resident_content =
            if affordable && self.reader.has_resident(content_position, after_content) {
                self.reader.seek_to(content_position);
                let window_slice = self
                    .reader
                    .read_bytes(content_length)
                    .await
                    .map_err(CoreError::ReadLogFileError)?;
                *resident_budget -= content_length;
                // Copied, not sliced. `Bytes` slices share their allocation, so keeping
                // a slice would pin the whole fetch window: one retained block would
                // hold `hoodie.memory.dfs.buffer.max.size` rather than its own content,
                // and a scan accumulates blocks from every log file before the gates
                // run. Copying makes residency the block's own length, which the budget
                // above then bounds.
                Some(bytes::Bytes::copy_from_slice(&window_slice))
            } else {
                None
            };
        self.reader.seek_to(after_content);
        let footer = self
            .read_block_metadata(BlockMetadataType::Footer, &format_version)
            .await?;
        let _ = self.read_total_block_length(&format_version).await?;

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
        // Bytes already in hand short-circuit the fetch, not the checks: the
        // location above is what the decode validates the byte count against.
        block.resident_content = resident_content;
        Ok(Some(block))
    }

    async fn read_next_block(&mut self, instant_range: &InstantRange) -> Result<Option<LogBlock>> {
        // The magic's own offset — where a corrupt block's span starts.
        let magic_pos = self.reader.position();
        if !self.read_magic().await? {
            return Ok(None);
        }

        let block_length = self.read_block_length().await?;
        // Validate before parsing the body: a corrupt or truncated block must
        // yield a corrupt marker and resume at the next block, rather than
        // failing the whole file on one bad span.
        if self.is_block_corrupted(magic_pos, block_length).await? {
            return Ok(Some(self.create_corrupted_block(magic_pos).await?));
        }
        let block_end = Self::block_end(magic_pos, block_length)?;
        let format_version = self.read_log_format_version().await?;
        let block_type = self.read_block_type(&format_version).await?;
        let header = self
            .read_block_metadata(BlockMetadataType::Header, &format_version)
            .await?;
        // If block is out of the requested range, fast skip its payload without decoding
        if self.should_skip_block(&header, instant_range)? {
            self.reader.seek_to(block_end);
            return Ok(Some(LogBlock::new_skipped(
                format_version,
                block_type,
                header,
            )));
        }

        let decoder = Decoder::new(self.hudi_configs.clone())
            .with_row_filter(self.row_filter.clone())
            .with_reader_schema(self.reader_schema_json.clone());
        // Decode out of the block's own remaining bytes rather than straight off
        // the reader: the reader is async and the decoders are not. The span
        // ends where the block does, so a content length that lies runs out of
        // bytes instead of reading into the next block.
        let content_start = self.reader.position();
        let remaining = block_end.checked_sub(content_start).ok_or_else(|| {
            CoreError::LogFormatError(format!(
                "the block at offset {magic_pos} declares {block_length} bytes, fewer than its \
                 own header occupies"
            ))
        })?;
        let mut content_bytes = Cursor::new(self.reader.read_bytes(remaining).await?);
        let content = decoder.decode_content(
            &mut content_bytes,
            &format_version,
            block_length,
            &block_type,
            &header,
        )?;
        // What follows the content is at whatever offset the decoder stopped at,
        // which is not a function of the block length: a decoder is capped at
        // the content length but need not consume all of it.
        self.reader
            .seek_to(content_start + content_bytes.position());
        let footer = self
            .read_block_metadata(BlockMetadataType::Footer, &format_version)
            .await?;
        let _ = self.read_total_block_length(&format_version).await?;

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

    /// The walk keeps at most the budget it was given, whatever the file holds.
    ///
    /// Without a bound, a block's kept bytes are memory the header walk exists to
    /// avoid: a scan accumulates blocks from every log file before the gates run,
    /// so residency would sum across the slice rather than staying at one window.
    #[tokio::test]
    async fn the_walk_keeps_no_more_than_its_residency_budget() -> Result<()> {
        let (dir, file_name) = get_valid_log_avro_data();
        let base_url = parse_uri(&dir)?;
        let configs = Arc::new(HudiConfigs::new([
            (HudiTableConfig::BasePath.as_ref(), base_url.as_str()),
            (HudiTableConfig::OrderingFields.as_ref(), "ts"),
        ]));

        let walk = async |budget: u64| -> Result<(usize, u64, u64)> {
            let storage =
                Storage::new(Arc::new(std::collections::HashMap::new()), configs.clone())?;
            let mut reader =
                LogFileReader::new_streaming(configs.clone(), storage, &file_name).await?;
            let mut left = budget;
            let blocks = reader.read_all_blocks_metadata_only(&mut left).await?;
            let kept: u64 = blocks
                .iter()
                .filter_map(|b| b.resident_content.as_ref().map(|c| c.len() as u64))
                .sum();
            Ok((blocks.len(), kept, budget - left))
        };

        // Generous: the file is far inside one window, so every block is kept.
        let (blocks, kept, charged) = walk(u64::MAX).await?;
        assert!(blocks > 0, "the fixture must hold blocks");
        assert!(kept > 0, "a generous budget must let the walk keep content");
        assert_eq!(kept, charged, "what is kept must be what is charged");

        // Zero: nothing may be kept, and the deferred path takes over.
        let (_, kept_none, charged_none) = walk(0).await?;
        assert_eq!(
            (kept_none, charged_none),
            (0, 0),
            "a zero budget must leave the walk holding nothing"
        );

        // One byte under what the first block needs: still nothing kept, which is
        // what makes the budget a bound rather than a suggestion.
        let (_, kept_short, _) = walk(kept - 1).await?;
        assert!(
            kept_short < kept,
            "a budget below the total must keep less than everything: {kept_short} against {kept}"
        );
        Ok(())
    }

    /// A log file inside one fetch window costs one request, not two.
    ///
    /// Asserted by request count rather than by the records returned, because both
    /// paths return the same records over the same file: that is the point. The
    /// counts come from a store that wraps the real one and counts what passes
    /// through.
    #[tokio::test]
    async fn a_log_file_inside_one_window_is_read_in_a_single_request() -> Result<()> {
        use crate::storage::counting::CountingObjectStore;

        let (dir, file_name) = get_valid_log_avro_data();
        let base_url = parse_uri(&dir)?;

        async fn walk(
            base_url: url::Url,
            file_name: &str,
            window: &str,
        ) -> Result<(usize, usize, usize, usize)> {
            let configs = Arc::new(HudiConfigs::new([
                (HudiTableConfig::BasePath.as_ref(), base_url.as_str()),
                (HudiTableConfig::OrderingFields.as_ref(), "ts"),
                (crate::storage::reader::CONFIG_DFS_BUFFER_MAX_SIZE, window),
            ]));
            let (store, counts) =
                CountingObjectStore::new(Arc::new(object_store::local::LocalFileSystem::new()));
            let storage = Storage::new_with_object_store(base_url, store, configs.clone());
            let mut reader = LogFileReader::new_streaming(configs, storage, file_name).await?;
            let blocks = reader.read_all_blocks_metadata_only_unbounded().await?;
            let resident = blocks
                .iter()
                .filter(|b| b.resident_content.is_some())
                .count();
            Ok((blocks.len(), resident, counts.gets(), counts.heads()))
        }

        // Inside one window: the walk holds every block's content, so nothing is
        // left to fetch and the whole file cost one request.
        let (blocks, resident, gets, heads) =
            walk(base_url.clone(), &file_name, "16777216").await?;
        assert!(blocks > 0, "the fixture must hold blocks");
        assert_eq!(
            resident, blocks,
            "every block in a file inside one window must hold its own content"
        );
        assert_eq!(
            (gets, heads),
            (1, 1),
            "one window fill and the size lookup, and nothing else"
        );

        // Smaller than the file: the walk cannot hold the content, so blocks defer
        // and each one's bytes cost a request of its own later. Unchanged behaviour.
        let (blocks_small, resident_small, gets_small, _) =
            walk(base_url, &file_name, "64").await?;
        assert_eq!(blocks_small, blocks, "the same file holds the same blocks");
        assert_eq!(
            resident_small, 0,
            "a window smaller than the file must leave every block deferred"
        );
        assert!(
            gets_small > gets,
            "the windowed walk issues more requests than the single fill, got \
             {gets_small} against {gets}"
        );
        Ok(())
    }

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

    async fn create_log_file_reader(dir: &str, file_name: &str) -> Result<LogFileReader> {
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
        let eager_blocks = eager
            .read_all_blocks(&InstantRange::up_to("99991231235959999", "utc"))
            .await?;

        let mut lazy =
            LogFileReader::new_streaming(hudi_configs.clone(), storage, file_name).await?;
        let mut lazy_blocks = lazy.read_all_blocks_metadata_only_unbounded().await?;

        assert_eq!(
            lazy_blocks.len(),
            eager_blocks.len(),
            "the sweep must find the same blocks"
        );

        let decoder = Decoder::new(hudi_configs);
        for (lazy_block, eager_block) in lazy_blocks.iter_mut().zip(eager_blocks.iter()) {
            assert_eq!(lazy_block.block_type, eager_block.block_type);
            assert_eq!(lazy_block.header, eager_block.header);
            lazy_block.load_content(&decoder).await?;
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
            reader.read_magic().await?;
            reader.read_block_length().await?
        };
        assert!(
            !reader.is_block_corrupted(magic_pos, real_length).await?,
            "a well-formed block must not be reported corrupt"
        );
        assert!(
            reader
                .is_block_corrupted(magic_pos, real_length + 1)
                .await?,
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
            reader.is_block_corrupted(0, u64::MAX).await?,
            "overflow is corrupt"
        );
        assert!(
            reader.is_block_corrupted(0, 1 << 40).await?,
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

        let blocks = reader.read_all_blocks_metadata_only_unbounded().await?;
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

    /// One batched read returns exactly what the same ranges return one at a
    /// time. This is the property the Pass-3 prefetch rests on: fewer round
    /// trips must not mean different bytes.
    #[tokio::test]
    async fn test_read_contents_matches_reading_each_range_alone() -> Result<()> {
        // No shipped fixture holds more than one block, and a log file is just a
        // sequence of self-contained blocks, so two of them concatenated is a
        // valid two-block file and the smallest thing that can batch.
        let (dir, file_name) = get_valid_log_avro_data();
        let one = std::fs::read(PathBuf::from(&dir).join(&file_name)).unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let mut doubled = one.clone();
        doubled.extend_from_slice(&one);
        std::fs::write(tmp.path().join(&file_name), &doubled).unwrap();

        let hudi_configs = Arc::new(HudiConfigs::new([(HudiTableConfig::OrderingFields, "ts")]));
        let storage = Storage::new_with_base_url(parse_uri(tmp.path().to_str().unwrap())?)?;
        let mut reader = LogFileReader::new_streaming(hudi_configs, storage, &file_name).await?;
        let blocks = reader.read_all_blocks_metadata_only_unbounded().await?;

        let deferred: Vec<_> = blocks
            .iter()
            .filter_map(|b| b.deferred_content.as_ref())
            .collect();
        assert_eq!(
            deferred.len(),
            2,
            "the doubled file must walk as two blocks for this to be a batch"
        );

        let fetcher = &deferred[0].fetcher;
        let ranges: Vec<std::ops::Range<u64>> = deferred
            .iter()
            .map(|d| {
                d.location.content_position..d.location.content_position + d.location.content_length
            })
            .collect();
        let batched = fetcher.read_contents(&ranges).await?;
        assert_eq!(batched.len(), ranges.len(), "one buffer per range");
        for (i, d) in deferred.iter().enumerate() {
            let alone = d
                .fetcher
                .read_content(d.location.content_position, d.location.content_length)
                .await?;
            assert_eq!(
                batched[i], alone,
                "batched range {i} must be byte-identical to reading it alone"
            );
        }
        Ok(())
    }

    /// A block whose content runs past the end of the file must say so. The
    /// ranged read comes back CLAMPED rather than refused, so without a length
    /// check the decoder is handed a short buffer and fails somewhere inside the
    /// block format instead. Also pins that a loaded block releases what it
    /// needed to fetch itself.
    #[tokio::test]
    async fn test_load_content_reports_a_block_running_past_the_file_end() -> Result<()> {
        let (dir, file_name) = get_valid_log_avro_data();
        let tmp = tempfile::tempdir().unwrap();
        let copied = tmp.path().join(&file_name);
        std::fs::copy(PathBuf::from(&dir).join(&file_name), &copied).unwrap();

        // A window of one byte forces every block to defer its content, which is
        // the path where a ranged read can come back short. Left at the default,
        // the walk would hold the whole file and decode from bytes it read before
        // the truncation below, so the case would never arise.
        //
        // The window is read from the STORAGE's configs, not the reader's, so it
        // has to be set on the storage or the setting is silently ignored.
        let base_url = parse_uri(tmp.path().to_str().unwrap())?;
        let hudi_configs = Arc::new(HudiConfigs::new([
            (HudiTableConfig::OrderingFields.as_ref(), "ts"),
            (HudiTableConfig::BasePath.as_ref(), base_url.as_str()),
            (crate::storage::reader::CONFIG_DFS_BUFFER_MAX_SIZE, "1"),
        ]));
        let storage = Storage::new(
            Arc::new(std::collections::HashMap::new()),
            hudi_configs.clone(),
        )?;
        let mut reader =
            LogFileReader::new_streaming(hudi_configs.clone(), storage, &file_name).await?;
        let mut blocks = reader.read_all_blocks_metadata_only_unbounded().await?;

        let block = blocks.last_mut().expect("the fixture has a block to walk");
        assert!(
            block.resident_content.is_none(),
            "the tiny window must have forced deferral, or this tests nothing"
        );
        let location = block
            .deferred_content
            .as_ref()
            .expect("a metadata-only block records where its content is")
            .location
            .clone();

        // Cut the file one byte short of this block's content so the ranged read
        // returns less than it asked for.
        let end = location.content_position + location.content_length;
        std::fs::OpenOptions::new()
            .write(true)
            .open(&copied)
            .unwrap()
            .set_len(end - 1)
            .unwrap();

        let decoder = Decoder::new(hudi_configs);
        let err = block
            .load_content(&decoder)
            .await
            .expect_err("content running past the file end must be an error");
        assert!(
            err.to_string().contains("truncated or corrupt block"),
            "the error must name the truncation rather than fail inside the decoder, got: {err}"
        );
        Ok(())
    }

    /// Loading is a no-op the second time. A block that decodes to no content —
    /// a command block does — looks unloaded afterwards, so releasing its
    /// location on the way out would make the second call fail instead.
    #[tokio::test]
    async fn test_load_content_is_idempotent_for_every_block_type() -> Result<()> {
        let (dir, file_name) = get_valid_log_rollback();
        let hudi_configs = Arc::new(HudiConfigs::new([(HudiTableConfig::OrderingFields, "ts")]));
        let storage = Storage::new_with_base_url(parse_uri(&dir)?)?;
        let mut reader =
            LogFileReader::new_streaming(hudi_configs.clone(), storage, &file_name).await?;
        let mut blocks = reader.read_all_blocks_metadata_only_unbounded().await?;
        assert!(
            blocks.iter().any(|b| b.block_type == BlockType::Command),
            "this fixture is chosen for its command block, which decodes to no content"
        );

        let decoder = Decoder::new(hudi_configs);
        for block in blocks.iter_mut() {
            block.load_content(&decoder).await?;
            block
                .load_content(&decoder)
                .await
                .expect("loading an already-loaded block must be a no-op, whatever it decoded to");
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
        let eager = eager_reader
            .read_all_blocks(&InstantRange::up_to("29991231235959999", "utc"))
            .await?;

        let dir_url = parse_uri(&dir)?;
        let hudi_configs = Arc::new(HudiConfigs::new([(HudiTableConfig::OrderingFields, "ts")]));
        let storage = Storage::new_with_base_url(dir_url)?;
        let mut lazy_reader =
            LogFileReader::new_streaming(hudi_configs.clone(), storage, &file_name).await?;
        let mut lazy = lazy_reader
            .read_all_blocks_metadata_only_unbounded()
            .await?;

        let decoder = Decoder::new(hudi_configs);
        for block in lazy.iter_mut() {
            block.load_content(&decoder).await?;
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
        let mut blocks = reader
            .read_all_blocks(&InstantRange::up_to("29991231235959999", "utc"))
            .await?;
        let decoder = Decoder::new(Arc::new(HudiConfigs::new([(
            HudiTableConfig::OrderingFields,
            "ts",
        )])));

        // Already decoded by the eager read: nothing to do.
        blocks[0].load_content(&decoder).await?;
        assert!(
            blocks[0].deferred_content.is_none(),
            "an eagerly read block has nothing deferred to release"
        );

        // A block with content cleared and no deferred location cannot say where
        // to read from.
        blocks[0].content = LogBlockContent::Empty;
        blocks[0].deferred_content = None;
        let err = blocks[0]
            .load_content(&decoder)
            .await
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
    /// stays inside the file.
    #[tokio::test]
    async fn test_scan_for_next_block_offset_stays_within_file_bounds() -> Result<()> {
        let (dir, file_name) = get_valid_log_avro_data();
        let mut reader = create_log_file_reader(&dir, &file_name).await?;

        let len = reader.reader.file_len();
        let offset = reader.scan_for_next_block_offset(0).await?;
        assert!(
            offset <= len,
            "recovery offset {offset} must lie within the file ({len})"
        );
        Ok(())
    }

    /// Options carrying a base path, and a streaming window when one is asked
    /// for. `Storage::new_with_base_url` builds its own configs, so the window
    /// knob has to be set on the storage the reader is opened from.
    fn storage_with_window(dir: &str, window: Option<u64>) -> Result<Arc<Storage>> {
        let mut options = HashMap::new();
        options.insert(
            HudiTableConfig::BasePath.as_ref().to_string(),
            parse_uri(dir)?.as_str().to_string(),
        );
        options.insert(
            HudiTableConfig::OrderingFields.as_ref().to_string(),
            "ts".to_string(),
        );
        if let Some(window) = window {
            options.insert(
                crate::storage::reader::CONFIG_DFS_BUFFER_MAX_SIZE.to_string(),
                window.to_string(),
            );
        }
        Ok(Storage::new(
            Arc::new(HashMap::new()),
            Arc::new(HudiConfigs::new(options)),
        )?)
    }

    /// A file whose first block is corrupt and whose second block's MAGIC starts
    /// `straddle_by` bytes before the end of the recovery scan's first 1 MiB
    /// window. Returns the directory, the file name, and where the good block
    /// starts.
    fn corrupt_then_good_file(straddle_by: u64) -> (tempfile::TempDir, String, u64) {
        let (dir, file_name) = get_valid_log_avro_data();
        let good = std::fs::read(PathBuf::from(&dir).join(&file_name)).unwrap();

        let magic_len = MAGIC.len() as u64;
        let scan = LogFileReader::BLOCK_SCAN_READ_BUFFER_SIZE;
        // The scan starts at `magic_len` and reads `scan` bytes, so its first
        // window ends at `magic_len + scan`. Landing the good block's MAGIC
        // `straddle_by` bytes before that end splits the marker across two
        // windows.
        let block_start = magic_len + scan - straddle_by;

        let mut bytes = Vec::with_capacity(block_start as usize + good.len());
        bytes.extend_from_slice(MAGIC);
        // A length no file can hold, so the block is corrupt by arithmetic
        // rather than by a trailing-pointer mismatch. All-ones also cannot
        // contain MAGIC, which the scan would otherwise find.
        bytes.extend_from_slice(&u64::MAX.to_be_bytes());
        bytes.resize(block_start as usize, 0);
        bytes.extend_from_slice(&good);

        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join(&file_name), &bytes).unwrap();
        (tmp, file_name, block_start)
    }

    /// Recovery from a corrupt block scans forward for the next MAGIC over 1 MiB
    /// windows, and consecutive windows overlap by `MAGIC.len() - 1` so a marker
    /// split across a boundary is still found.
    ///
    /// This is the one place the header walk is not a forward parse, so it is
    /// also the case a windowed fetch gets wrong most easily: dropping the
    /// overlap, or refilling a fetch window without it, makes the scan miss the
    /// marker, run to EOF, and swallow every later block into the corrupt span.
    /// Asserted for the eager reader and for streaming readers whose fetch
    /// window is smaller than, larger than, and unrelated to the scan window.
    async fn assert_corrupt_recovery_finds_a_straddling_magic(window: Option<u64>) -> Result<()> {
        let scan = LogFileReader::BLOCK_SCAN_READ_BUFFER_SIZE;
        let magic_len = MAGIC.len() as u64;
        let (tmp, file_name, block_start) = corrupt_then_good_file(3);
        assert!(
            block_start < magic_len + scan && block_start + magic_len > magic_len + scan,
            "the fixture must split MAGIC across the scan's first window boundary"
        );

        let dir = tmp.path().to_str().unwrap();
        let hudi_configs = Arc::new(HudiConfigs::new([(HudiTableConfig::OrderingFields, "ts")]));

        // The scan itself lands exactly on the good block's magic, not on EOF.
        let storage = storage_with_window(dir, window)?;
        let mut reader = match window {
            None => LogFileReader::new(hudi_configs.clone(), storage.clone(), &file_name).await?,
            Some(_) => {
                LogFileReader::new_streaming(hudi_configs.clone(), storage.clone(), &file_name)
                    .await?
            }
        };
        assert_eq!(
            reader.scan_for_next_block_offset(0).await?,
            block_start,
            "recovery must land on the good block's magic, not run to EOF"
        );

        // ... and the walk therefore reports the corrupt block followed by the
        // good one, rather than one corrupt block covering the whole file.
        let mut eager =
            LogFileReader::new(hudi_configs.clone(), storage.clone(), &file_name).await?;
        let eager_blocks = eager
            .read_all_blocks(&InstantRange::up_to("99991231235959999", "utc"))
            .await?;
        assert_eq!(
            eager_blocks
                .iter()
                .map(|b| b.block_type.clone())
                .collect::<Vec<_>>(),
            vec![BlockType::Corrupted, BlockType::AvroData],
        );
        assert!(
            eager_blocks[1].record_batches().unwrap().num_data_rows() > 0,
            "the block after the corrupt span must still decode"
        );

        let mut lazy =
            LogFileReader::new_streaming(hudi_configs.clone(), storage, &file_name).await?;
        let mut lazy_blocks = lazy.read_all_blocks_metadata_only_unbounded().await?;
        assert_eq!(
            lazy_blocks
                .iter()
                .map(|b| b.block_type.clone())
                .collect::<Vec<_>>(),
            vec![BlockType::Corrupted, BlockType::AvroData],
        );
        let decoder = Decoder::new(hudi_configs);
        lazy_blocks[1].load_content(&decoder).await?;
        assert_eq!(
            lazy_blocks[1]
                .content
                .as_records()
                .map(|b| b.num_data_rows()),
            eager_blocks[1]
                .content
                .as_records()
                .map(|b| b.num_data_rows()),
            "the recovered block must decode the same either way"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_corrupt_recovery_scan_finds_a_magic_split_across_windows() -> Result<()> {
        let scan = LogFileReader::BLOCK_SCAN_READ_BUFFER_SIZE;
        // Eager (whole file resident), a fetch window smaller than the scan
        // window, one larger than it, and one that is not a multiple of it.
        for window in [None, Some(scan / 2), Some(scan + 1024), Some(64 * 1024 + 7)] {
            assert_corrupt_recovery_finds_a_straddling_magic(window).await?;
        }
        Ok(())
    }

    /// Skipping a block has to land exactly on the next block's magic. On a
    /// one-block file it cannot: landing anywhere in the last few bytes reads as
    /// end-of-file either way, so the arithmetic is only pinned when another
    /// block follows. Two copies of a fixture is a valid two-block file, and
    /// both carry the same instant time, so a window below it skips both — the
    /// walk then has to reach the end cleanly rather than find garbage where the
    /// second magic should be.
    #[tokio::test]
    async fn test_skipping_a_block_lands_on_the_next_ones_magic() -> Result<()> {
        let (dir, file_name) = get_valid_log_avro_data();
        let one = std::fs::read(PathBuf::from(&dir).join(&file_name)).unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let mut doubled = one.clone();
        doubled.extend_from_slice(&one);
        std::fs::write(tmp.path().join(&file_name), &doubled).unwrap();

        let hudi_configs = Arc::new(HudiConfigs::new([(HudiTableConfig::OrderingFields, "ts")]));
        let storage = Storage::new_with_base_url(parse_uri(tmp.path().to_str().unwrap())?)?;
        let mut reader = LogFileReader::new(hudi_configs, storage, &file_name).await?;

        // Everything is out of the window, so every block takes the skip path.
        let instant_range = InstantRange::up_to("20200101000000000", "utc");
        let mut skipped = 0;
        while let Some(block) = reader.read_next_block(&instant_range).await? {
            assert!(block.skipped, "a block below the window must be skipped");
            skipped += 1;
        }
        assert_eq!(skipped, 2, "both blocks must be walked and skipped");
        assert_eq!(
            reader.reader.position(),
            doubled.len() as u64,
            "the walk must consume the file exactly, with no bytes left over"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_read_log_file_with_avro_data_block() -> Result<()> {
        let (dir, file_name) = get_valid_log_avro_data();
        let mut reader = create_log_file_reader(&dir, &file_name).await?;
        let instant_range = InstantRange::up_to("20250316025828811", "utc");
        let blocks = reader.read_all_blocks(&instant_range).await?;
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
        let blocks = reader.read_all_blocks(&instant_range).await?;
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
        let blocks = reader.read_all_blocks(&instant_range).await?;
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
        let blocks = reader.read_all_blocks(&instant_range).await?;
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
        let maybe_block = reader.read_next_block(&instant_range).await?;
        assert!(maybe_block.is_some(), "Expected a block to be read");
        let block = maybe_block.unwrap();
        assert!(block.skipped, "Block should be marked as skipped");
        // Skipped blocks have empty content
        assert!(block.record_batches().is_none());

        // next call should hit EOF
        let next = reader.read_next_block(&instant_range).await?;
        assert!(
            next.is_none(),
            "Should reach EOF after skipping the only block"
        );

        Ok(())
    }
}
