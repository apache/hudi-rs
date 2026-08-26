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
//! HFile reader implementation.

use std::collections::BTreeMap;

use crate::hfile::block::{
    BLOCK_HEADER_SIZE, BlockIndexEntry, DataBlock, HFileBlock, read_var_long, var_long_size_on_disk,
};
use crate::hfile::block_type::HFileBlockType;
use crate::hfile::compression::CompressionCodec;
use crate::hfile::error::{HFileError, Result};
use crate::hfile::key::{Key, KeyValue, Utf8Key, compare_keys};
use crate::hfile::proto::InfoProto;
use crate::hfile::record::HFileRecord;
use crate::hfile::trailer::{HFileTrailer, TRAILER_SIZE};
use crate::storage::Storage;
use crate::storage::reader::LogBlockFetcher;
use apache_avro::Schema as AvroSchema;
use prost::Message;
use std::sync::OnceLock;

/// Magic bytes indicating protobuf format in file info block
const PBUF_MAGIC: &[u8; 4] = b"PBUF";

/// Seek result codes for HFile reader
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeekResult {
    /// Lookup key is before the fake first key of a block but >= actual first key
    BeforeBlockFirstKey = -2,
    /// Lookup key is before the first key of the file
    BeforeFileFirstKey = -1,
    /// Exact match found
    Found = 0,
    /// Key not found but within range; cursor points to greatest key < lookup
    InRange = 1,
    /// Key is greater than the last key; EOF reached
    Eof = 2,
}

/// File info key for last key in the file
const FILE_INFO_LAST_KEY: &str = "hfile.LASTKEY";
/// File info key for key-value version
const FILE_INFO_KEY_VALUE_VERSION: &str = "KEY_VALUE_VERSION";
/// File info key for max MVCC timestamp
const FILE_INFO_MAX_MVCC_TS: &str = "MAX_MEMSTORE_TS_KEY";

/// Key-value version indicating MVCC timestamp support
const KEY_VALUE_VERSION_WITH_MVCC_TS: i32 = 1;

/// File info key for Avro schema
const FILE_INFO_SCHEMA: &str = "schema";
/// File info key for min record key
const FILE_INFO_MIN_RECORD_KEY: &str = "minRecordKey";
/// File info key for max record key
const FILE_INFO_MAX_RECORD_KEY: &str = "maxRecordKey";

/// The exclusive upper bound of everything beginning with `prefix`.
///
/// `b"ab"` bounds at `b"ac"`. `None` when the prefix is all `0xFF`, since then
/// nothing sorts above it and the caller must take the tail instead of a range.
fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    while let Some(last) = upper.pop() {
        if last != u8::MAX {
            upper.push(last + 1);
            return Some(upper);
        }
    }
    None
}

/// Where the decoder reads bytes from.
///
/// `Whole` is the file already in memory, which is what an HFile arriving as a
/// log block's content is. `Ranged` holds only the section from
/// `load_on_open_data_offset` to the end of the file, which carries the index,
/// the file info and the trailer; data blocks are fetched when they are read,
/// so peak memory tracks what is being read rather than the file.
enum Source {
    Whole(Vec<u8>),
    Ranged {
        fetcher: LogBlockFetcher,
        /// `[tail_start, file_len)`, resident for the life of the reader.
        tail: bytes::Bytes,
        tail_start: usize,
        file_len: u64,
        /// Bytes per fetch, so index levels are bounded like data blocks are.
        window_budget: u64,
    },
}

impl Source {
    /// Bytes from `offset` to the end of the resident region.
    ///
    /// The load-on-open blocks are self-describing: a caller reads the length
    /// out of the block header rather than being told one, so this hands back
    /// an open-ended slice rather than an exact range.
    fn metadata_at(&self, offset: usize) -> Result<&[u8]> {
        match self {
            Source::Whole(bytes) => bytes.get(offset..).ok_or_else(|| {
                HFileError::InvalidFormat(format!(
                    "offset {offset} is past the end of a {}-byte HFile",
                    bytes.len()
                ))
            }),
            Source::Ranged {
                tail, tail_start, ..
            } => {
                let relative = offset.checked_sub(*tail_start).ok_or_else(|| {
                    HFileError::InvalidFormat(format!(
                        "offset {offset} is below the resident section starting at {tail_start}"
                    ))
                })?;
                tail.get(relative..).ok_or_else(|| {
                    HFileError::InvalidFormat(format!(
                        "offset {offset} is past the resident section of {} bytes",
                        tail.len()
                    ))
                })
            }
        }
    }

    /// An exact range, when it is already resident. A ranged source must fetch
    /// its data blocks instead, which is asynchronous, so it refuses here
    /// rather than reading the wrong bytes.
    fn resident_range(&self, offset: usize, size: usize) -> Result<&[u8]> {
        match self {
            Source::Whole(bytes) => bytes.get(offset..offset + size).ok_or_else(|| {
                HFileError::InvalidFormat(format!(
                    "range {offset}..{} is past the end of a {}-byte HFile",
                    offset + size,
                    bytes.len()
                ))
            }),
            Source::Ranged { .. } => Err(HFileError::InvalidFormat(
                "a ranged HFile reader fetches data blocks asynchronously; \
                 this path needs the file to be resident"
                    .to_string(),
            )),
        }
    }
}

/// HFile reader that supports sequential reads and seeks.
pub struct HFileReader {
    /// Where bytes come from.
    source: Source,
    /// Parsed trailer
    trailer: HFileTrailer,
    /// Compression codec from trailer
    codec: CompressionCodec,
    /// Data block index (first key -> entry)
    data_block_index: BTreeMap<Key, BlockIndexEntry>,
    /// Meta block index (name -> entry)
    meta_block_index: BTreeMap<String, BlockIndexEntry>,
    /// File info map
    file_info: BTreeMap<String, Vec<u8>>,
    /// Last key in the file
    last_key: Option<Key>,
    /// Cached Avro schema (parsed lazily from file info)
    cached_schema: OnceLock<AvroSchema>,
    /// Current cursor position
    cursor: Cursor,
    /// Currently loaded data block
    current_block: Option<DataBlock>,
    /// Current block's index entry
    current_block_entry: Option<BlockIndexEntry>,
}

/// Cursor tracking current position in the file.
#[derive(Debug, Clone, Default)]
struct Cursor {
    /// Absolute offset in file
    offset: usize,
    /// Cached key-value at current position
    cached_kv: Option<KeyValue>,
    /// Whether we've reached EOF
    eof: bool,
    /// Whether seek has been called
    seeked: bool,
}

impl HFileReader {
    /// Create a new HFile reader from raw bytes.
    pub fn new(bytes: Vec<u8>) -> Result<Self> {
        let trailer = HFileTrailer::read(&bytes)?;
        let mut reader = Self::with_source(Source::Whole(bytes), trailer);
        reader.initialize_metadata()?;
        Ok(reader)
    }

    /// A reader with its metadata not yet parsed.
    fn with_source(source: Source, trailer: HFileTrailer) -> Self {
        let codec = trailer.compression_codec;
        Self {
            source,
            trailer,
            codec,
            data_block_index: BTreeMap::new(),
            meta_block_index: BTreeMap::new(),
            file_info: BTreeMap::new(),
            last_key: None,
            cached_schema: OnceLock::new(),
            cursor: Cursor::default(),
            current_block: None,
            current_block_entry: None,
        }
    }

    /// Open an HFile from storage.
    ///
    /// This is an async factory method that reads the file from storage
    /// and creates an HFileReader.
    ///
    /// # Arguments
    /// * `storage` - The storage to read from
    /// * `relative_path` - The relative path to the HFile
    ///
    /// # Example
    /// ```ignore
    /// let reader = HFileReader::open(&storage, "files/data.hfile").await?;
    /// for record in reader.iter()? {
    ///     println!("{:?}", record?);
    /// }
    /// ```
    pub async fn open(storage: &Storage, relative_path: &str) -> Result<Self> {
        let bytes = storage.get_file_data(relative_path).await.map_err(|e| {
            HFileError::InvalidFormat(format!("Failed to read HFile {relative_path}: {e:?}"))
        })?;
        Self::new(bytes.to_vec())
    }

    /// Open the file whole when it is at most `whole_read_max_size` bytes, and in
    /// ranges when it is larger.
    ///
    /// Mirrors `HFileReaderFactory.createInputStream`, which reads the file whole
    /// below `hoodie.metadata.file.cache.max.size.mb` and opens a seekable stream
    /// above it. Both directions matter, measured locally on generated HFiles: a
    /// ranged point lookup is 2.7x slower than a whole read at 8 KB, because a
    /// `head` plus a trailer range plus a load-on-open range are three round trips
    /// where one would do, and 25x faster at 512 MB, because it pays for the blocks
    /// it needs rather than for every byte.
    ///
    /// `known_file_size` skips the size lookup when the caller already has it. When
    /// it does not, the size comes from the streaming reader that the ranged path
    /// would open anyway, so the decision costs no request of its own.
    pub async fn open_sized(
        storage: &Storage,
        relative_path: &str,
        whole_read_max_size: u64,
        known_file_size: Option<u64>,
    ) -> Result<Self> {
        if let Some(size) = known_file_size {
            return if size <= whole_read_max_size {
                Self::open(storage, relative_path).await
            } else {
                Self::open_ranged(storage, relative_path).await
            };
        }
        // Zero means never whole, so the size cannot change the outcome.
        if whole_read_max_size == 0 {
            return Self::open_ranged(storage, relative_path).await;
        }
        let reader = storage
            .get_streaming_storage_reader(relative_path)
            .await
            .map_err(|e| {
                HFileError::InvalidFormat(format!("Failed to open HFile {relative_path}: {e:?}"))
            })?;
        if reader.file_len() <= whole_read_max_size {
            return Self::open(storage, relative_path).await;
        }
        Self::open_ranged_with(storage, relative_path, reader).await
    }

    /// Open an HFile without holding it.
    ///
    /// Two ranged reads before any data is touched: the trailer, which is the
    /// final [`TRAILER_SIZE`] bytes, then the load-on-open section it points at,
    /// which carries the index and the file info. Data blocks are read later,
    /// through [`Self::read_records_batched`].
    pub async fn open_ranged(storage: &Storage, relative_path: &str) -> Result<Self> {
        let reader = storage
            .get_streaming_storage_reader(relative_path)
            .await
            .map_err(|e| {
                HFileError::InvalidFormat(format!("Failed to open HFile {relative_path}: {e:?}"))
            })?;
        Self::open_ranged_with(storage, relative_path, reader).await
    }

    /// Open in ranges from a storage reader already in hand.
    ///
    /// Split out so a caller that needed the file's length to decide between
    /// whole and ranged does not pay a second `head` for it.
    async fn open_ranged_with(
        storage: &Storage,
        relative_path: &str,
        reader: crate::storage::reader::StorageReader,
    ) -> Result<Self> {
        let file_len = reader.file_len();
        let fetcher = reader.block_fetcher();

        let trailer_size = TRAILER_SIZE as u64;
        if file_len < trailer_size {
            return Err(HFileError::InvalidFormat(format!(
                "File too small for HFile trailer: {file_len} bytes, need at least {TRAILER_SIZE}"
            )));
        }

        // The trailer occupies exactly the last TRAILER_SIZE bytes, so it parses
        // from that slice alone.
        let trailer_bytes = fetcher
            .read_content(file_len - trailer_size, trailer_size)
            .await
            .map_err(|e| {
                HFileError::InvalidFormat(format!(
                    "Failed to read the trailer of HFile {relative_path}: {e:?}"
                ))
            })?;
        let trailer = HFileTrailer::read(&trailer_bytes)?;

        let tail_start = trailer.load_on_open_data_offset;
        // A trailer is file data, so its offsets are not trustworthy. Left
        // unchecked, the length below underflows and asks for a nonsense range.
        if tail_start > file_len {
            return Err(HFileError::InvalidFormat(format!(
                "HFile {relative_path} has a load-on-open offset of {tail_start}, \
                 past the end of its {file_len} bytes"
            )));
        }
        let tail = fetcher
            .read_content(tail_start, file_len - tail_start)
            .await
            .map_err(|e| {
                HFileError::InvalidFormat(format!(
                    "Failed to read the load-on-open section of HFile {relative_path}: {e:?}"
                ))
            })?;

        let window_budget = crate::storage::reader::stream_window_size(&storage.hudi_configs)
            .map_err(|e| HFileError::InvalidFormat(format!("{e}")))?;
        let mut reader = Self::with_source(
            Source::Ranged {
                fetcher,
                tail,
                tail_start: tail_start as usize,
                file_len,
                window_budget,
            },
            trailer,
        );
        reader.initialize_metadata_ranged().await?;
        Ok(reader)
    }

    /// Group consecutive blocks into runs that each stay under `budget`.
    ///
    /// A run is read in one request, so this bounds both peak memory and the
    /// number of round trips. A block larger than the budget goes out alone
    /// rather than being split: a block has to be whole to decode.
    pub fn plan_windows(entries: &[BlockIndexEntry], budget: u64) -> Vec<Vec<BlockIndexEntry>> {
        let mut windows: Vec<Vec<BlockIndexEntry>> = Vec::new();
        let mut current: Vec<BlockIndexEntry> = Vec::new();
        let mut current_bytes: u64 = 0;

        for entry in entries {
            let len = entry.size as u64;
            if !current.is_empty() && current_bytes.saturating_add(len) > budget {
                windows.push(std::mem::take(&mut current));
                current_bytes = 0;
            }
            current_bytes = current_bytes.saturating_add(len);
            current.push(entry.clone());
        }
        if !current.is_empty() {
            windows.push(current);
        }
        windows
    }

    /// The window budget a ranged reader was opened with.
    pub fn window_budget(&self) -> Option<u64> {
        match &self.source {
            Source::Ranged { window_budget, .. } => Some(*window_budget),
            Source::Whole(_) => None,
        }
    }

    /// Length of the whole file, known without reading it.
    pub fn file_len(&self) -> u64 {
        match &self.source {
            Source::Ranged { file_len, .. } => *file_len,
            Source::Whole(bytes) => bytes.len() as u64,
        }
    }

    /// The data blocks a key predicate can be satisfied from.
    ///
    /// Over-includes, like the two functions it dispatches to, so the caller must
    /// still filter the records it gets back.
    pub fn blocks_for_predicate(
        &self,
        predicate: &crate::file_group::base_file::reader::KeyPredicate,
    ) -> Vec<BlockIndexEntry> {
        use crate::file_group::base_file::reader::KeyPredicate;
        match predicate {
            KeyPredicate::Keys(keys) => {
                let refs: Vec<&str> = keys.iter().map(String::as_str).collect();
                self.blocks_for_keys(&refs)
            }
            KeyPredicate::Prefixes(prefixes) => {
                // Union the prefixes, deduplicated by offset and back in file
                // order so the result still coalesces.
                let mut selected: BTreeMap<u64, BlockIndexEntry> = BTreeMap::new();
                for prefix in prefixes {
                    for entry in self.blocks_for_prefix(prefix) {
                        selected.insert(entry.offset, entry);
                    }
                }
                selected.into_values().collect()
            }
        }
    }

    /// The data blocks that can contain any of `keys`.
    ///
    /// Pure index arithmetic, no I/O: for each key, the entry with the greatest
    /// index key not exceeding it, which is what `find_block_for_key` does for a
    /// single key. Deduplicated and returned in file order, so the result can go
    /// straight to [`Self::read_records_batched`] and coalesce.
    ///
    /// A block's index key is a *lower bound* on its first real key, since the
    /// writer may shorten it. So this over-includes rather than under-includes: a
    /// selected block may turn out to hold nothing the caller asked for, which
    /// costs one block read. `SeekResult::BeforeBlockFirstKey` is the sync path
    /// naming the same case.
    ///
    /// One case needs the block *before* the selected one as well. Row keys are
    /// not required to be unique, and when a key's copies straddle a block
    /// boundary HBase's midpoint falls back to the right-hand cell, so the
    /// separator equals the key itself. Selecting only the entry at or below the
    /// key would then take the later block and drop the copies in the earlier
    /// one. So when the probe lands exactly on a separator, the preceding block
    /// is taken too — one extra block read on an exact hit, against silently
    /// losing rows.
    pub fn blocks_for_keys(&self, keys: &[&str]) -> Vec<BlockIndexEntry> {
        let mut selected: BTreeMap<u64, BlockIndexEntry> = BTreeMap::new();
        for key in keys {
            // A key too long for the length prefix cannot be compared against
            // the index, so it selects every block rather than none: a wrong
            // answer is worse than a slow one.
            let Some(probe) = Key::from_content(key.as_bytes()) else {
                return self.data_block_entries();
            };
            // No entry at or below the key means every block starts above it, so
            // no block can hold it. Selecting nothing is correct, not a miss.
            let mut at_or_below = self.data_block_index.range(..=probe.clone()).rev();
            if let Some((index_key, entry)) = at_or_below.next() {
                selected.insert(entry.offset, entry.clone());
                // An exact hit on a separator means the key may also end the
                // previous block; see the note above on non-unique row keys.
                if index_key == &probe
                    && let Some((_, previous)) = at_or_below.next()
                {
                    selected.insert(previous.offset, previous.clone());
                }
            }
        }
        selected.into_values().collect()
    }

    /// The data blocks that can contain keys beginning with `prefix`.
    ///
    /// The block holding the prefix's lower bound, plus every block whose index
    /// key sorts below the prefix's exclusive upper bound. Over-includes for the
    /// same reason [`Self::blocks_for_keys`] does.
    pub fn blocks_for_prefix(&self, prefix: &str) -> Vec<BlockIndexEntry> {
        let Some(lower) = Key::from_content(prefix.as_bytes()) else {
            return self.data_block_entries();
        };
        let mut selected: BTreeMap<u64, BlockIndexEntry> = BTreeMap::new();

        // The block the prefix's first possible key falls in. Its index key sorts
        // at or below the prefix, so `range(prefix..)` below would skip it.
        if let Some((_, entry)) = self.data_block_index.range(..=lower.clone()).next_back() {
            selected.insert(entry.offset, entry.clone());
        }

        match prefix_upper_bound(prefix.as_bytes()) {
            // Every block that starts inside the prefix's span.
            Some(upper) => {
                // `upper` is never longer than the prefix, and an over-long
                // prefix already returned above, so this cannot fail.
                let upper = Key::from_content(&upper).unwrap_or_else(|| {
                    unreachable!("an upper bound is never longer than its prefix")
                });
                for (_, entry) in self.data_block_index.range(lower..upper) {
                    selected.insert(entry.offset, entry.clone());
                }
            }
            // The prefix is all 0xFF, so nothing sorts above it: take the tail.
            None => {
                for (_, entry) in self.data_block_index.range(lower..) {
                    selected.insert(entry.offset, entry.clone());
                }
            }
        }
        selected.into_values().collect()
    }

    /// What this reader has actually read from storage, or `None` for a resident
    /// source, which reads nothing after construction.
    ///
    /// Reports what storage returned, not the ranges asked for. A seek is judged
    /// on `bytes` rather than on the rows it returns; see `FetchCounts` for why
    /// `calls` is not a round-trip count.
    pub fn reads(&self) -> Option<&crate::storage::reader::FetchCounts> {
        match &self.source {
            Source::Ranged { fetcher, .. } => Some(fetcher.reads()),
            Source::Whole(_) => None,
        }
    }

    /// The data blocks in key order, each with the range it occupies, so a
    /// caller can decide how many to read at once.
    pub fn data_block_entries(&self) -> Vec<BlockIndexEntry> {
        self.data_block_index.values().cloned().collect()
    }

    /// Decode the records of several data blocks, fetching their ranges in one
    /// request. Peak memory is what `entries` covers, so the caller bounds it by
    /// choosing how many blocks to pass.
    pub async fn read_records_batched(
        &self,
        entries: &[BlockIndexEntry],
    ) -> Result<Vec<HFileRecord>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        let blocks = self.fetch_blocks(entries).await?;
        let mut records = Vec::new();
        for block in blocks {
            if block.block_type() != HFileBlockType::Data {
                return Err(HFileError::UnexpectedBlockType {
                    expected: HFileBlockType::Data.to_string(),
                    actual: block.block_type().to_string(),
                });
            }
            let data_block = DataBlock::from_block(block);
            for kv in data_block.iter() {
                records.push(Self::key_value_to_record(&kv));
            }
        }
        Ok(records)
    }

    /// Initialize metadata by reading index blocks and file info.
    fn initialize_metadata(&mut self) -> Result<()> {
        let offset = self.read_root_index()?;
        if self.trailer.num_data_index_levels > 1 {
            self.load_multi_level_index()?;
        }
        self.read_meta_and_file_info(offset)
    }

    /// As [`Self::initialize_metadata`], but the leaf index blocks of a
    /// multi-level index are fetched rather than sliced. They live in the data
    /// region, not the load-on-open section, so a ranged reader has to go back
    /// to storage for them; every other step reads resident bytes.
    async fn initialize_metadata_ranged(&mut self) -> Result<()> {
        let offset = self.read_root_index()?;
        if self.trailer.num_data_index_levels > 1 {
            self.load_multi_level_index_batched().await?;
        }
        self.read_meta_and_file_info(offset)
    }

    /// Root data index. Returns the offset just past it.
    fn read_root_index(&mut self) -> Result<usize> {
        let start = self.trailer.load_on_open_data_offset as usize;
        let (data_index, offset) = self.read_root_index_block(start)?;
        self.data_block_index = data_index;
        Ok(offset)
    }

    /// Meta index, file info, last key and the MVCC check: the rest of the
    /// load-on-open section, all resident.
    fn read_meta_and_file_info(&mut self, offset: usize) -> Result<()> {
        let (meta_index, offset) = self.read_meta_index_block(offset)?;
        self.meta_block_index = meta_index;
        self.read_file_info_block(offset)?;
        if let Some(last_key_bytes) = self.file_info.get(FILE_INFO_LAST_KEY) {
            self.last_key = Some(Key::from_bytes(last_key_bytes.clone()));
        }
        self.check_mvcc_support()
    }

    /// Check if the file uses MVCC timestamps (not supported).
    fn check_mvcc_support(&self) -> Result<()> {
        if let Some(version_bytes) = self.file_info.get(FILE_INFO_KEY_VALUE_VERSION)
            && version_bytes.len() >= 4
        {
            let version = i32::from_be_bytes([
                version_bytes[0],
                version_bytes[1],
                version_bytes[2],
                version_bytes[3],
            ]);
            if version == KEY_VALUE_VERSION_WITH_MVCC_TS
                && let Some(ts_bytes) = self.file_info.get(FILE_INFO_MAX_MVCC_TS)
                && ts_bytes.len() >= 8
            {
                let max_ts = i64::from_be_bytes([
                    ts_bytes[0],
                    ts_bytes[1],
                    ts_bytes[2],
                    ts_bytes[3],
                    ts_bytes[4],
                    ts_bytes[5],
                    ts_bytes[6],
                    ts_bytes[7],
                ]);
                if max_ts > 0 {
                    return Err(HFileError::UnsupportedMvccTimestamp);
                }
            }
        }
        Ok(())
    }

    /// Read root index block and return the index entries.
    fn read_root_index_block(
        &self,
        start: usize,
    ) -> Result<(BTreeMap<Key, BlockIndexEntry>, usize)> {
        let block = HFileBlock::parse(self.source.metadata_at(start)?, self.codec)?;
        if block.block_type() != HFileBlockType::RootIndex {
            return Err(HFileError::UnexpectedBlockType {
                expected: HFileBlockType::RootIndex.to_string(),
                actual: block.block_type().to_string(),
            });
        }

        let entries = self.parse_root_index_entries(
            &block.data,
            self.trailer.data_index_count as usize,
            false,
        )?;
        let next_offset = start + block.header.on_disk_size_with_header();

        // Convert entries to BTreeMap
        let mut index_map = BTreeMap::new();
        for i in 0..entries.len() {
            let entry = &entries[i];
            let next_key = if i + 1 < entries.len() {
                Some(entries[i + 1].first_key.clone())
            } else {
                None
            };
            index_map.insert(
                entry.first_key.clone(),
                BlockIndexEntry::new(entry.first_key.clone(), next_key, entry.offset, entry.size),
            );
        }

        Ok((index_map, next_offset))
    }

    /// Load multi-level data block index (BFS traversal).
    fn load_multi_level_index(&mut self) -> Result<()> {
        let mut levels_remaining = self.trailer.num_data_index_levels - 1;
        let mut current_entries: Vec<BlockIndexEntry> =
            self.data_block_index.values().cloned().collect();

        while levels_remaining > 0 {
            let mut next_level_entries = Vec::new();

            for entry in &current_entries {
                let block = self.read_block_at(entry.offset as usize, entry.size as usize)?;

                let entries = self.parse_leaf_index_entries(&block.data)?;

                next_level_entries.extend(entries);
            }

            current_entries = next_level_entries;
            levels_remaining -= 1;
        }

        self.data_block_index = Self::index_from_leaf_entries(&current_entries);
        Ok(())
    }

    /// Key-ordered index over leaf entries, each carrying the next entry's
    /// first key so a seek knows where a block's range ends.
    fn index_from_leaf_entries(entries: &[BlockIndexEntry]) -> BTreeMap<Key, BlockIndexEntry> {
        let mut index_map = BTreeMap::new();
        for (i, entry) in entries.iter().enumerate() {
            let next_key = entries.get(i + 1).map(|next| next.first_key.clone());
            index_map.insert(
                entry.first_key.clone(),
                BlockIndexEntry::new(entry.first_key.clone(), next_key, entry.offset, entry.size),
            );
        }
        index_map
    }

    /// The multi-level walk for a ranged source: one batched request per level
    /// instead of one per leaf block, since `read_contents` coalesces ranges
    /// that sit close together.
    async fn load_multi_level_index_batched(&mut self) -> Result<()> {
        let mut levels_remaining = self.trailer.num_data_index_levels - 1;
        let mut current_entries: Vec<BlockIndexEntry> =
            self.data_block_index.values().cloned().collect();

        let budget = match &self.source {
            Source::Ranged { window_budget, .. } => *window_budget,
            Source::Whole(_) => {
                return Err(HFileError::InvalidFormat(
                    "the batched index walk needs a ranged HFile reader".to_string(),
                ));
            }
        };

        while levels_remaining > 0 {
            let mut next_level_entries = Vec::new();
            // A level can hold many leaf blocks, so it is read in runs under the
            // same budget the data blocks use rather than all at once.
            for window in Self::plan_windows(&current_entries, budget) {
                let blocks = self.fetch_blocks(&window).await?;
                for block in &blocks {
                    next_level_entries.extend(self.parse_leaf_index_entries(&block.data)?);
                }
            }
            current_entries = next_level_entries;
            levels_remaining -= 1;
        }

        self.data_block_index = Self::index_from_leaf_entries(&current_entries);
        Ok(())
    }

    /// Fetch and parse several blocks in one request. Ranged sources only.
    async fn fetch_blocks(&self, entries: &[BlockIndexEntry]) -> Result<Vec<HFileBlock>> {
        // Resident: the bytes are already here, so a "fetch" is a slice.
        let Source::Ranged { fetcher, .. } = &self.source else {
            return entries
                .iter()
                .map(|e| {
                    let bytes = self
                        .source
                        .resident_range(e.offset as usize, e.size as usize)?;
                    HFileBlock::parse(bytes, self.codec)
                })
                .collect();
        };
        let ranges: Vec<std::ops::Range<u64>> = entries
            .iter()
            .map(|e| e.offset..e.offset + e.size as u64)
            .collect();
        let fetched = fetcher.read_contents(&ranges).await.map_err(|e| {
            HFileError::InvalidFormat(format!("Failed to read HFile block ranges: {e:?}"))
        })?;
        fetched
            .iter()
            .map(|bytes| HFileBlock::parse(bytes, self.codec))
            .collect()
    }

    /// Parse root index entries from block data.
    fn parse_root_index_entries(
        &self,
        data: &[u8],
        num_entries: usize,
        content_key_only: bool,
    ) -> Result<Vec<BlockIndexEntry>> {
        let mut entries = Vec::with_capacity(num_entries);
        let mut offset = 0;

        for _ in 0..num_entries {
            // Read offset (8 bytes)
            let block_offset = i64::from_be_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
                data[offset + 4],
                data[offset + 5],
                data[offset + 6],
                data[offset + 7],
            ]) as u64;
            offset += 8;

            // Read size (4 bytes)
            let block_size = i32::from_be_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]) as u32;
            offset += 4;

            // Read key length (varint)
            let var_len_size = var_long_size_on_disk(data, offset);
            let (key_length, _) = read_var_long(data, offset);
            offset += var_len_size;

            // Read key bytes
            let key_bytes = data[offset..offset + key_length as usize].to_vec();
            offset += key_length as usize;

            let key = if content_key_only {
                // For meta index: key is just the content
                Key::from_bytes(key_bytes)
            } else {
                // For data index: key has structure (length prefix + content + other info)
                Key::new(&key_bytes, 0, key_bytes.len())
            };

            entries.push(BlockIndexEntry::new(key, None, block_offset, block_size));
        }

        Ok(entries)
    }

    /// Parse leaf index entries from block data.
    fn parse_leaf_index_entries(&self, data: &[u8]) -> Result<Vec<BlockIndexEntry>> {
        let mut entries = Vec::new();
        let mut offset = 0;

        // Read number of entries (4 bytes)
        let num_entries = i32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        offset += 4;

        // Read secondary index (offsets to entries)
        let mut relative_offsets = Vec::with_capacity(num_entries + 1);
        for _ in 0..=num_entries {
            let rel_offset = i32::from_be_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]) as usize;
            relative_offsets.push(rel_offset);
            offset += 4;
        }

        let base_offset = offset;

        // Read entries
        for i in 0..num_entries {
            // Read offset (8 bytes)
            let block_offset = i64::from_be_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
                data[offset + 4],
                data[offset + 5],
                data[offset + 6],
                data[offset + 7],
            ]) as u64;

            // Read size (4 bytes)
            let block_size = i32::from_be_bytes([
                data[offset + 8],
                data[offset + 9],
                data[offset + 10],
                data[offset + 11],
            ]) as u32;

            // Key is from offset+12 to next entry
            let key_start = offset + 12;
            let next_entry_start = base_offset + relative_offsets[i + 1];
            let key_length = next_entry_start - key_start;

            let key_bytes = data[key_start..key_start + key_length].to_vec();
            let key = Key::new(&key_bytes, 0, key_bytes.len());

            entries.push(BlockIndexEntry::new(key, None, block_offset, block_size));
            offset = next_entry_start;
        }

        Ok(entries)
    }

    /// Read meta index block.
    fn read_meta_index_block(
        &self,
        start: usize,
    ) -> Result<(BTreeMap<String, BlockIndexEntry>, usize)> {
        let block = HFileBlock::parse(self.source.metadata_at(start)?, self.codec)?;
        if block.block_type() != HFileBlockType::RootIndex {
            return Err(HFileError::UnexpectedBlockType {
                expected: HFileBlockType::RootIndex.to_string(),
                actual: block.block_type().to_string(),
            });
        }

        let entries = self.parse_root_index_entries(
            &block.data,
            self.trailer.meta_index_count as usize,
            true,
        )?;
        let next_offset = start + block.header.on_disk_size_with_header();

        // Convert to string-keyed map
        let mut index_map = BTreeMap::new();
        for entry in entries {
            let key_str = String::from_utf8_lossy(entry.first_key.content()).to_string();
            index_map.insert(key_str, entry);
        }

        Ok((index_map, next_offset))
    }

    /// Read file info block.
    fn read_file_info_block(&mut self, start: usize) -> Result<()> {
        let block = HFileBlock::parse(self.source.metadata_at(start)?, self.codec)?;
        if block.block_type() != HFileBlockType::FileInfo {
            return Err(HFileError::UnexpectedBlockType {
                expected: HFileBlockType::FileInfo.to_string(),
                actual: block.block_type().to_string(),
            });
        }

        // Check PBUF magic
        if block.data.len() < 4 || &block.data[0..4] != PBUF_MAGIC {
            return Err(HFileError::InvalidFormat(
                "File info block missing PBUF magic".to_string(),
            ));
        }

        // Parse protobuf (length-delimited after magic)
        let proto_data = &block.data[4..];
        let (length, consumed) = read_varint(proto_data);
        let info_proto = InfoProto::decode(&proto_data[consumed..consumed + length as usize])?;

        // Build file info map
        for entry in info_proto.map_entry {
            let key = String::from_utf8_lossy(&entry.first).to_string();
            self.file_info.insert(key, entry.second);
        }

        Ok(())
    }

    /// Read a block at the given offset and size.
    fn read_block_at(&self, offset: usize, size: usize) -> Result<HFileBlock> {
        HFileBlock::parse(self.source.resident_range(offset, size)?, self.codec)
    }

    /// Get the number of key-value entries in the file.
    pub fn num_entries(&self) -> u64 {
        self.trailer.entry_count
    }

    /// Get file info value by key.
    pub fn get_file_info(&self, key: &str) -> Option<&[u8]> {
        self.file_info.get(key).map(|v| v.as_slice())
    }

    /// Get meta block content by name.
    pub fn get_meta_block(&self, name: &str) -> Result<Option<Vec<u8>>> {
        let entry = match self.meta_block_index.get(name) {
            Some(e) => e,
            None => return Ok(None),
        };

        let block = self.read_block_at(entry.offset as usize, entry.size as usize)?;
        if block.block_type() != HFileBlockType::Meta {
            return Err(HFileError::UnexpectedBlockType {
                expected: HFileBlockType::Meta.to_string(),
                actual: block.block_type().to_string(),
            });
        }

        Ok(Some(block.data))
    }

    /// Get the Avro schema embedded in this HFile.
    ///
    /// The schema is cached after the first successful parse.
    /// Returns `None` if no schema is present in the file info.
    /// The Avro schema exactly as the writer stored it.
    ///
    /// Not the parsed schema's canonical form: canonicalising rewrites named-type
    /// references and a decoder built from the result cannot resolve them.
    pub fn avro_schema_json(&self) -> Result<Option<&str>> {
        let Some(bytes) = self.file_info.get(FILE_INFO_SCHEMA) else {
            return Ok(None);
        };
        std::str::from_utf8(bytes)
            .map(Some)
            .map_err(|e| HFileError::InvalidFormat(format!("Invalid UTF-8 in schema: {e}")))
    }

    pub fn get_avro_schema(&self) -> Result<Option<&AvroSchema>> {
        // Check if schema exists in file info
        let schema_bytes = match self.file_info.get(FILE_INFO_SCHEMA) {
            Some(bytes) => bytes,
            None => return Ok(None),
        };

        // Try to get from cache, or parse and cache
        if let Some(schema) = self.cached_schema.get() {
            return Ok(Some(schema));
        }

        // Parse schema from JSON
        let schema_str = std::str::from_utf8(schema_bytes)
            .map_err(|e| HFileError::InvalidFormat(format!("Invalid UTF-8 in schema: {e}")))?;

        let schema = AvroSchema::parse_str(schema_str)
            .map_err(|e| HFileError::InvalidFormat(format!("Invalid Avro schema: {e}")))?;

        // Cache the schema (ignore if already set by another thread)
        let _ = self.cached_schema.set(schema);

        Ok(self.cached_schema.get())
    }

    /// Get the min and max record keys from file info.
    ///
    /// These keys can be used for range pruning - if a lookup key is outside
    /// the [min, max] range, the file can be skipped entirely.
    ///
    /// Returns `None` if min/max keys are not present in file info.
    pub fn read_min_max_record_keys(&self) -> Option<(String, String)> {
        let min_key = self.file_info.get(FILE_INFO_MIN_RECORD_KEY)?;
        let max_key = self.file_info.get(FILE_INFO_MAX_RECORD_KEY)?;

        let min_str = std::str::from_utf8(min_key).ok()?;
        let max_str = std::str::from_utf8(max_key).ok()?;

        Some((min_str.to_string(), max_str.to_string()))
    }

    /// Seek to the beginning of the file.
    pub fn seek_to_first(&mut self) -> Result<bool> {
        if self.trailer.entry_count == 0 {
            self.cursor.eof = true;
            self.cursor.seeked = true;
            return Ok(false);
        }

        // Get first data block
        let first_entry = match self.data_block_index.first_key_value() {
            Some((_, entry)) => entry.clone(),
            None => {
                self.cursor.eof = true;
                self.cursor.seeked = true;
                return Ok(false);
            }
        };

        self.current_block_entry = Some(first_entry.clone());
        self.load_data_block(&first_entry)?;

        self.cursor.offset = first_entry.offset as usize + BLOCK_HEADER_SIZE;
        self.cursor.cached_kv = None;
        self.cursor.eof = false;
        self.cursor.seeked = true;

        Ok(true)
    }

    /// Seek to the given key.
    pub fn seek_to(&mut self, lookup_key: &Utf8Key) -> Result<SeekResult> {
        if !self.cursor.seeked {
            self.seek_to_first()?;
        }

        if self.trailer.entry_count == 0 {
            return Ok(SeekResult::Eof);
        }

        // Get current key-value
        let current_kv = match self.get_key_value()? {
            Some(kv) => kv,
            None => return Ok(SeekResult::Eof),
        };

        let cmp_current = compare_keys(current_kv.key(), lookup_key);

        match cmp_current {
            std::cmp::Ordering::Equal => Ok(SeekResult::Found),
            std::cmp::Ordering::Greater => {
                // Current key > lookup key: backward seek
                // Check if we're at the first key of a block and lookup >= fake first key
                if let Some(entry) = &self.current_block_entry
                    && self.is_at_first_key_of_block()
                    && compare_keys(&entry.first_key, lookup_key) != std::cmp::Ordering::Greater
                {
                    return Ok(SeekResult::BeforeBlockFirstKey);
                }

                // Check if before file's first key
                if self.data_block_index.first_key_value().is_some()
                    && self.is_at_first_key_of_block()
                {
                    return Ok(SeekResult::BeforeFileFirstKey);
                }

                Err(HFileError::BackwardSeekNotSupported)
            }
            std::cmp::Ordering::Less => {
                // Current key < lookup key: forward seek
                self.forward_seek(lookup_key)
            }
        }
    }

    /// Forward seek to find the lookup key.
    fn forward_seek(&mut self, lookup_key: &Utf8Key) -> Result<SeekResult> {
        // Check if we need to jump to a different block
        if let Some(entry) = &self.current_block_entry {
            if let Some(next_key) = &entry.next_block_first_key {
                if compare_keys(next_key, lookup_key) != std::cmp::Ordering::Greater {
                    // Need to find the right block
                    self.find_block_for_key(lookup_key)?;
                }
            } else {
                // Last block - check against last key
                if let Some(last_key) = &self.last_key
                    && compare_keys(last_key, lookup_key) == std::cmp::Ordering::Less
                {
                    self.cursor.eof = true;
                    self.current_block = None;
                    self.current_block_entry = None;
                    return Ok(SeekResult::Eof);
                }
            }
        }

        // Scan within the current block
        self.scan_block_for_key(lookup_key)
    }

    /// Find the block that may contain the lookup key.
    fn find_block_for_key(&mut self, lookup_key: &Utf8Key) -> Result<()> {
        // Binary search using BTreeMap's range
        let lookup_bytes = lookup_key.as_bytes();
        let fake_key = Key::from_bytes(lookup_bytes.to_vec());

        // Find the entry with greatest key <= lookup_key
        let entry = self
            .data_block_index
            .range(..=fake_key)
            .next_back()
            .map(|(_, e)| e.clone());

        if let Some(entry) = entry {
            self.current_block_entry = Some(entry.clone());
            self.load_data_block(&entry)?;
            self.cursor.offset = entry.offset as usize + BLOCK_HEADER_SIZE;
            self.cursor.cached_kv = None;
        }

        Ok(())
    }

    /// Scan within the current block to find the key.
    /// Uses iteration instead of recursion to avoid stack overflow with many blocks.
    fn scan_block_for_key(&mut self, lookup_key: &Utf8Key) -> Result<SeekResult> {
        loop {
            let block = match &self.current_block {
                Some(b) => b,
                None => return Ok(SeekResult::Eof),
            };

            let block_start = self.current_block_entry.as_ref().unwrap().offset as usize;
            let mut offset = self.cursor.offset - block_start - BLOCK_HEADER_SIZE;
            let mut last_offset = offset;
            let mut last_kv = self.cursor.cached_kv.clone();

            while block.is_valid_offset(offset) {
                let kv = block.read_key_value(offset);
                let cmp = compare_keys(kv.key(), lookup_key);

                match cmp {
                    std::cmp::Ordering::Equal => {
                        self.cursor.offset = block_start + BLOCK_HEADER_SIZE + offset;
                        self.cursor.cached_kv = Some(kv);
                        return Ok(SeekResult::Found);
                    }
                    std::cmp::Ordering::Greater => {
                        // Key at offset > lookup key
                        // Set cursor to previous position
                        if let Some(prev_kv) = last_kv {
                            self.cursor.offset = block_start + BLOCK_HEADER_SIZE + last_offset;
                            self.cursor.cached_kv = Some(prev_kv);
                        }
                        if self.is_at_first_key_of_block() {
                            return Ok(SeekResult::BeforeBlockFirstKey);
                        }
                        return Ok(SeekResult::InRange);
                    }
                    std::cmp::Ordering::Less => {
                        last_offset = offset;
                        last_kv = Some(kv.clone());
                        offset += kv.record_size();
                    }
                }
            }

            // Reached end of block - need to check if there are more blocks
            let current_entry = self.current_block_entry.clone().unwrap();
            let next_entry = self.get_next_block_entry(&current_entry);

            match next_entry {
                Some(entry) => {
                    // Move to next block and continue scanning (iterate instead of recurse)
                    self.current_block_entry = Some(entry.clone());
                    self.load_data_block(&entry)?;
                    self.cursor.offset = entry.offset as usize + BLOCK_HEADER_SIZE;
                    self.cursor.cached_kv = None;
                    // Continue the loop to scan the next block
                }
                None => {
                    // No more blocks - this is the last block
                    // Check if lookup key is past the last key in the file
                    if let Some(kv) = last_kv {
                        if compare_keys(kv.key(), lookup_key) == std::cmp::Ordering::Less {
                            // We're past the last key in the file
                            self.cursor.eof = true;
                            self.cursor.cached_kv = None;
                            return Ok(SeekResult::Eof);
                        }
                        // Otherwise, stay at the last key
                        self.cursor.offset = block_start + BLOCK_HEADER_SIZE + last_offset;
                        self.cursor.cached_kv = Some(kv);
                    }
                    return Ok(SeekResult::InRange);
                }
            }
        }
    }

    /// Load a data block.
    fn load_data_block(&mut self, entry: &BlockIndexEntry) -> Result<()> {
        let block = self.read_block_at(entry.offset as usize, entry.size as usize)?;
        if block.block_type() != HFileBlockType::Data {
            return Err(HFileError::UnexpectedBlockType {
                expected: HFileBlockType::Data.to_string(),
                actual: block.block_type().to_string(),
            });
        }
        self.current_block = Some(DataBlock::from_block(block));
        Ok(())
    }

    /// Move to the next key-value pair.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<bool> {
        if !self.cursor.seeked || self.cursor.eof {
            return Ok(false);
        }

        let block = match &self.current_block {
            Some(b) => b,
            None => return Ok(false),
        };

        let block_start = self.current_block_entry.as_ref().unwrap().offset as usize;
        let current_offset = self.cursor.offset - block_start - BLOCK_HEADER_SIZE;

        // Get current key-value to calculate next offset
        let kv = if let Some(cached) = &self.cursor.cached_kv {
            cached.clone()
        } else {
            block.read_key_value(current_offset)
        };

        let next_offset = current_offset + kv.record_size();

        if block.is_valid_offset(next_offset) {
            self.cursor.offset = block_start + BLOCK_HEADER_SIZE + next_offset;
            self.cursor.cached_kv = None;
            return Ok(true);
        }

        // Move to next block
        let current_entry = self.current_block_entry.clone().unwrap();
        let next_entry = self.get_next_block_entry(&current_entry);

        match next_entry {
            Some(entry) => {
                self.current_block_entry = Some(entry.clone());
                self.load_data_block(&entry)?;
                self.cursor.offset = entry.offset as usize + BLOCK_HEADER_SIZE;
                self.cursor.cached_kv = None;
                Ok(true)
            }
            None => {
                self.cursor.eof = true;
                Ok(false)
            }
        }
    }

    /// Get the next block index entry.
    fn get_next_block_entry(&self, current: &BlockIndexEntry) -> Option<BlockIndexEntry> {
        self.data_block_index
            .range((
                std::ops::Bound::Excluded(&current.first_key),
                std::ops::Bound::Unbounded,
            ))
            .next()
            .map(|(_, e)| e.clone())
    }

    /// Get the current key-value pair.
    pub fn get_key_value(&mut self) -> Result<Option<KeyValue>> {
        if !self.cursor.seeked || self.cursor.eof {
            return Ok(None);
        }

        if let Some(cached) = &self.cursor.cached_kv {
            return Ok(Some(cached.clone()));
        }

        let block = match &self.current_block {
            Some(b) => b,
            None => {
                // Need to load the block
                let entry = self.current_block_entry.clone().unwrap();
                self.load_data_block(&entry)?;
                self.current_block.as_ref().unwrap()
            }
        };

        let block_start = self.current_block_entry.as_ref().unwrap().offset as usize;
        let offset = self.cursor.offset - block_start - BLOCK_HEADER_SIZE;

        let kv = block.read_key_value(offset);
        self.cursor.cached_kv = Some(kv.clone());

        Ok(Some(kv))
    }

    /// Check if cursor is at the first key of the current block.
    fn is_at_first_key_of_block(&self) -> bool {
        if let Some(entry) = &self.current_block_entry {
            return self.cursor.offset == entry.offset as usize + BLOCK_HEADER_SIZE;
        }
        false
    }

    /// Check if the reader has been seeked.
    pub fn is_seeked(&self) -> bool {
        self.cursor.seeked
    }

    /// Iterate over all key-value pairs.
    pub fn iter(&mut self) -> Result<HFileIterator<'_>> {
        self.seek_to_first()?;
        Ok(HFileIterator { reader: self })
    }

    // ================== HFileRecord API for metadata table ==================

    /// Convert a KeyValue to an owned HFileRecord.
    ///
    /// This extracts the key content (without length prefix) and value bytes
    /// into an owned struct suitable for metadata table operations.
    fn key_value_to_record(kv: &KeyValue) -> HFileRecord {
        HFileRecord::new(kv.key().content().to_vec(), kv.value().to_vec())
    }

    /// Collect all records from the HFile as owned HFileRecords.
    ///
    /// This is useful for metadata table operations where records need to be
    /// stored and merged with log file records.
    ///
    /// # Example
    /// ```ignore
    /// let records = reader.collect_records()?;
    /// for record in records {
    ///     println!("Key: {}", record.key_as_str().unwrap_or("<binary>"));
    /// }
    /// ```
    pub fn collect_records(&mut self) -> Result<Vec<HFileRecord>> {
        let mut records = Vec::with_capacity(self.trailer.entry_count as usize);
        for result in self.iter()? {
            let kv = result?;
            records.push(Self::key_value_to_record(&kv));
        }
        Ok(records)
    }

    /// Iterate over all records as owned HFileRecords.
    ///
    /// Unlike `iter()` which returns references into file bytes,
    /// this iterator yields owned `HFileRecord` instances.
    pub fn record_iter(&mut self) -> Result<HFileRecordIterator<'_>> {
        self.seek_to_first()?;
        Ok(HFileRecordIterator { reader: self })
    }

    /// Get the current position's record as an owned HFileRecord.
    ///
    /// Returns None if not seeked or at EOF.
    pub fn get_record(&mut self) -> Result<Option<HFileRecord>> {
        match self.get_key_value()? {
            Some(kv) => Ok(Some(Self::key_value_to_record(&kv))),
            None => Ok(None),
        }
    }

    /// Lookup records by keys and return as HFileRecords.
    ///
    /// Keys must be sorted in ascending order. This method efficiently
    /// scans forward through the file to find matching keys.
    ///
    /// Returns a vector of (key, Option<HFileRecord>) tuples where
    /// the Option is Some if the key was found.
    pub fn lookup_records(&mut self, keys: &[&str]) -> Result<Vec<(String, Option<HFileRecord>)>> {
        let mut results = Vec::with_capacity(keys.len());

        if keys.is_empty() {
            return Ok(results);
        }

        self.seek_to_first()?;
        if self.cursor.eof {
            // Empty file - return all as not found
            for key in keys {
                results.push((key.to_string(), None));
            }
            return Ok(results);
        }

        for key in keys {
            let lookup = Utf8Key::new(*key);
            match self.seek_to(&lookup)? {
                SeekResult::Found => {
                    let record = self.get_record()?;
                    results.push((key.to_string(), record));
                }
                _ => {
                    results.push((key.to_string(), None));
                }
            }
        }

        Ok(results)
    }

    /// Collect records matching a key prefix.
    ///
    /// Returns all records where the key starts with the given prefix.
    pub fn collect_records_by_prefix(&mut self, prefix: &str) -> Result<Vec<HFileRecord>> {
        let mut records = Vec::new();
        let prefix_bytes = prefix.as_bytes();

        // Seek to the prefix (or first key >= prefix)
        let lookup = Utf8Key::new(prefix);
        self.seek_to_first()?;

        if self.cursor.eof {
            return Ok(records);
        }

        // Find starting position
        let start_result = self.seek_to(&lookup)?;
        match start_result {
            SeekResult::Eof => return Ok(records),
            SeekResult::Found | SeekResult::InRange | SeekResult::BeforeBlockFirstKey => {
                // We may be at or past a matching key
            }
            SeekResult::BeforeFileFirstKey => {
                // Key is before first key, move to first
                self.seek_to_first()?;
            }
        }

        // Scan and collect records with matching prefix
        loop {
            if self.cursor.eof {
                break;
            }

            match self.get_key_value()? {
                Some(kv) => {
                    let key_content = kv.key().content();
                    if key_content.starts_with(prefix_bytes) {
                        records.push(Self::key_value_to_record(&kv));
                    } else if key_content > prefix_bytes {
                        // Past the prefix range
                        break;
                    }
                }
                None => break,
            }

            if !self.next()? {
                break;
            }
        }

        Ok(records)
    }
}

/// Iterator over all records as owned HFileRecords.
pub struct HFileRecordIterator<'a> {
    reader: &'a mut HFileReader,
}

impl<'a> Iterator for HFileRecordIterator<'a> {
    type Item = Result<HFileRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.reader.cursor.eof {
            return None;
        }

        match self.reader.get_key_value() {
            Ok(Some(kv)) => {
                let record = HFileReader::key_value_to_record(&kv);
                match self.reader.next() {
                    Ok(_) => {}
                    Err(e) => return Some(Err(e)),
                }
                Some(Ok(record))
            }
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

/// Iterator over all key-value pairs in an HFile.
pub struct HFileIterator<'a> {
    reader: &'a mut HFileReader,
}

impl<'a> Iterator for HFileIterator<'a> {
    type Item = Result<KeyValue>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.reader.cursor.eof {
            return None;
        }

        match self.reader.get_key_value() {
            Ok(Some(kv)) => {
                match self.reader.next() {
                    Ok(_) => {}
                    Err(e) => return Some(Err(e)),
                }
                Some(Ok(kv))
            }
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

/// Read a varint from bytes. Returns (value, bytes_consumed).
fn read_varint(bytes: &[u8]) -> (u64, usize) {
    let mut result: u64 = 0;
    let mut shift = 0;
    let mut pos = 0;

    while pos < bytes.len() {
        let b = bytes[pos] as u64;
        pos += 1;
        result |= (b & 0x7F) << shift;
        if b & 0x80 == 0 {
            break;
        }
        shift += 7;
    }

    (result, pos)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn test_data_dir() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("data")
            .join("hfile")
    }

    fn read_test_hfile(filename: &str) -> Vec<u8> {
        let path = test_data_dir().join(filename);
        std::fs::read(&path).unwrap_or_else(|_| panic!("Failed to read test file: {path:?}"))
    }

    /// Storage rooted at the fixture directory, so a ranged read addresses the
    /// fixtures by name.
    /// Every HFile fixture in the repo, spanning one, two and three index levels.
    pub(super) const ALL_FIXTURES: &[&str] = &[
        "hudi_1_0_hbase_2_4_9_16KB_NONE_5000.hfile",
        "hudi_1_0_hbase_2_4_9_16KB_GZ_20000.hfile",
        "hudi_1_0_hbase_2_4_9_512KB_GZ_20000.hfile",
        "hudi_1_0_hbase_2_4_9_64KB_NONE_5000.hfile",
        "hudi_1_0_hbase_2_4_9_16KB_GZ_200_20_non_unique.hfile",
        "hudi_1_0_hbase_2_4_13_16KB_GZ_20000_fake_first_key.hfile",
        "hudi_1_0_hbase_2_4_13_1KB_GZ_20000_large_keys.hfile",
        "hudi_1_0_hbase_2_4_13_1KB_GZ_10000_large_keys_deep_index.hfile",
        "hudi_1_0_hbase_2_4_9_no_entry.hfile",
    ];

    pub(super) fn fixture_storage() -> std::sync::Arc<Storage> {
        let url =
            url::Url::from_directory_path(std::fs::canonicalize(test_data_dir()).unwrap()).unwrap();
        Storage::new_with_base_url(url).unwrap()
    }

    /// A trailer is file data, so its offsets are attacker- or corruption-
    /// controlled. Taking a valid fixture's final trailer alone gives a file
    /// whose `load_on_open_data_offset` points far past its own end, which is
    /// what the length arithmetic in `open_ranged` must not be handed
    /// unchecked: unguarded it underflows a `u64` and asks for a nonsense range.
    #[tokio::test]
    async fn a_load_on_open_offset_past_the_end_is_refused() -> Result<()> {
        let whole = read_test_hfile("hudi_1_0_hbase_2_4_9_16KB_NONE_5000.hfile");
        let trailer_only = &whole[whole.len() - TRAILER_SIZE..];

        let dir = std::env::temp_dir().join("hudi_rs_hfile_trailer_only_case");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let name = "trailer_only.hfile";
        std::fs::write(dir.join(name), trailer_only).unwrap();

        let url = url::Url::from_directory_path(std::fs::canonicalize(&dir).unwrap()).unwrap();
        let storage = Storage::new_with_base_url(url).unwrap();

        let message = match HFileReader::open_ranged(&storage, name).await {
            Ok(_) => panic!("a load-on-open offset past the end must be refused"),
            Err(e) => e.to_string(),
        };
        assert!(
            message.contains("past the end of its"),
            "expected the offset to be named as past the end, got: {message}"
        );

        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    /// Every fixture, read both ways, must yield identical records.
    ///
    /// This is the guard on the two multi-level index walks: the resident one
    /// slices each leaf block, the ranged one fetches a level per request, and
    /// nothing else forces them to agree. `..._deep_index` is the fixture with
    /// more than one index level, so it is the one that exercises the split.
    #[tokio::test]
    async fn ranged_and_resident_reads_agree_on_every_fixture() -> Result<()> {
        let fixtures = ALL_FIXTURES;
        let storage = fixture_storage();

        for name in fixtures {
            let mut resident = HFileReader::new(read_test_hfile(name))?;
            let expected = resident.collect_records()?;

            let ranged = HFileReader::open_ranged(&storage, name).await?;
            let entries = ranged.data_block_entries();
            let actual = ranged.read_records_batched(&entries).await?;

            assert_eq!(
                actual.len(),
                expected.len(),
                "{name}: ranged read returned {} records, resident read {}",
                actual.len(),
                expected.len()
            );
            assert_eq!(
                actual, expected,
                "{name}: records differ between the two reads"
            );
            assert_eq!(
                ranged.num_entries(),
                resident.num_entries(),
                "{name}: trailer entry count differs"
            );
        }
        Ok(())
    }

    #[test]
    fn test_read_uncompressed_hfile() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_16KB_NONE_5000.hfile");
        let reader = HFileReader::new(bytes).expect("Failed to create reader");

        // Check entry count
        assert_eq!(reader.num_entries(), 5000);
    }

    #[test]
    fn test_read_gzip_hfile() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_16KB_GZ_20000.hfile");
        let reader = HFileReader::new(bytes).expect("Failed to create reader");

        // Check entry count
        assert_eq!(reader.num_entries(), 20000);
    }

    #[test]
    fn test_read_empty_hfile() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_no_entry.hfile");
        let reader = HFileReader::new(bytes).expect("Failed to create reader");

        // Check entry count
        assert_eq!(reader.num_entries(), 0);
    }

    #[tokio::test]
    async fn test_open_nonexistent_file() {
        use crate::storage::Storage;
        use url::Url;
        let base_url = Url::parse("file:///nonexistent/path").unwrap();
        let storage = Storage::new_with_base_url(base_url).unwrap();
        let result = HFileReader::open(&storage, "nonexistent.hfile").await;
        assert!(result.is_err());
        // Use err() instead of unwrap_err() since HFileReader doesn't implement Debug
        match result.err() {
            Some(HFileError::InvalidFormat(_)) => {}
            Some(err) => panic!("Expected InvalidFormat error, got: {err:?}"),
            None => panic!("Expected error, got Ok"),
        }
    }

    #[test]
    fn test_seek_to_first_uncompressed() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_16KB_NONE_5000.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        // Seek to first
        assert!(reader.seek_to_first().expect("Failed to seek"));

        // Get first key-value
        let kv = reader.get_key_value().expect("Failed to get kv").unwrap();
        assert_eq!(kv.key().content_as_str().unwrap(), "hudi-key-000000000");

        let value = std::str::from_utf8(kv.value()).unwrap();
        assert_eq!(value, "hudi-value-000000000");
    }

    #[test]
    fn test_sequential_read_uncompressed() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_16KB_NONE_5000.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        // Seek to first
        assert!(reader.seek_to_first().expect("Failed to seek"));

        // Read first 10 entries
        for i in 0..10 {
            let kv = reader.get_key_value().expect("Failed to get kv").unwrap();
            let expected_key = format!("hudi-key-{i:09}");
            let expected_value = format!("hudi-value-{i:09}");

            assert_eq!(kv.key().content_as_str().unwrap(), expected_key);
            assert_eq!(std::str::from_utf8(kv.value()).unwrap(), expected_value);

            if i < 9 {
                assert!(reader.next().expect("Failed to move next"));
            }
        }
    }

    #[test]
    fn test_seek_to_key_exact() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_16KB_NONE_5000.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        // Seek to first
        assert!(reader.seek_to_first().expect("Failed to seek"));

        // Seek to specific key
        let lookup = Utf8Key::new("hudi-key-000000100");
        let result = reader.seek_to(&lookup).expect("Failed to seek");
        assert_eq!(result, SeekResult::Found);

        // Verify key
        let kv = reader.get_key_value().expect("Failed to get kv").unwrap();
        assert_eq!(kv.key().content_as_str().unwrap(), "hudi-key-000000100");
    }

    #[test]
    fn test_seek_to_key_eof() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_16KB_NONE_5000.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        // Seek to first
        assert!(reader.seek_to_first().expect("Failed to seek"));

        // Seek past last key
        let lookup = Utf8Key::new("hudi-key-999999999");
        let result = reader.seek_to(&lookup).expect("Failed to seek");
        assert_eq!(result, SeekResult::Eof);
    }

    #[test]
    fn test_seek_to_key_before_first() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_16KB_NONE_5000.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        // Seek to first
        assert!(reader.seek_to_first().expect("Failed to seek"));

        // Seek before first key
        let lookup = Utf8Key::new("aaa");
        let result = reader.seek_to(&lookup).expect("Failed to seek");
        assert_eq!(result, SeekResult::BeforeFileFirstKey);
    }

    #[test]
    fn test_iterate_all_entries() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_16KB_NONE_5000.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        let mut count = 0;
        for result in reader.iter().expect("Failed to create iterator") {
            let kv = result.expect("Failed to read kv");
            let expected_key = format!("hudi-key-{count:09}");
            assert_eq!(kv.key().content_as_str().unwrap(), expected_key);
            count += 1;
        }

        assert_eq!(count, 5000);
    }

    #[test]
    fn test_empty_hfile_seek() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_no_entry.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        // Seek to first should return false
        assert!(!reader.seek_to_first().expect("Failed to seek"));

        // Get key-value should return None
        assert!(reader.get_key_value().expect("Failed to get kv").is_none());
    }

    #[test]
    fn test_gzip_sequential_read() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_16KB_GZ_20000.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        // Seek to first
        assert!(reader.seek_to_first().expect("Failed to seek"));

        // Read first 10 entries
        for i in 0..10 {
            let kv = reader.get_key_value().expect("Failed to get kv").unwrap();
            let expected_key = format!("hudi-key-{i:09}");
            let expected_value = format!("hudi-value-{i:09}");

            assert_eq!(kv.key().content_as_str().unwrap(), expected_key);
            assert_eq!(std::str::from_utf8(kv.value()).unwrap(), expected_value);

            if i < 9 {
                assert!(reader.next().expect("Failed to move next"));
            }
        }
    }

    // ================== HFileRecord Tests ==================

    #[test]
    fn test_collect_records() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_16KB_NONE_5000.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        let records = reader.collect_records().expect("Failed to collect records");
        assert_eq!(records.len(), 5000);

        // Verify first and last records
        assert_eq!(records[0].key_as_str(), Some("hudi-key-000000000"));
        assert_eq!(
            std::str::from_utf8(records[0].value()).unwrap(),
            "hudi-value-000000000"
        );

        assert_eq!(records[4999].key_as_str(), Some("hudi-key-000004999"));
        assert_eq!(
            std::str::from_utf8(records[4999].value()).unwrap(),
            "hudi-value-000004999"
        );
    }

    #[test]
    fn test_record_iterator() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_16KB_NONE_5000.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        let mut count = 0;
        for result in reader.record_iter().expect("Failed to create iterator") {
            let record = result.expect("Failed to read record");
            let expected_key = format!("hudi-key-{count:09}");
            assert_eq!(record.key_as_str(), Some(expected_key.as_str()));
            count += 1;
        }

        assert_eq!(count, 5000);
    }

    #[test]
    fn test_get_record() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_16KB_NONE_5000.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        // Before seeking, should return None
        assert!(reader.get_record().expect("Failed to get record").is_none());

        // After seeking, should return a record
        reader.seek_to_first().expect("Failed to seek");
        let record = reader.get_record().expect("Failed to get record").unwrap();
        assert_eq!(record.key_as_str(), Some("hudi-key-000000000"));
    }

    #[test]
    fn test_lookup_records() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_16KB_NONE_5000.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        let keys = vec![
            "hudi-key-000000000",
            "hudi-key-000000100",
            "hudi-key-nonexistent",
        ];
        let results = reader.lookup_records(&keys).expect("Failed to lookup");

        assert_eq!(results.len(), 3);

        // First key should be found
        assert_eq!(results[0].0, "hudi-key-000000000");
        assert!(results[0].1.is_some());
        assert_eq!(
            results[0].1.as_ref().unwrap().key_as_str(),
            Some("hudi-key-000000000")
        );

        // Second key should be found
        assert_eq!(results[1].0, "hudi-key-000000100");
        assert!(results[1].1.is_some());
        assert_eq!(
            results[1].1.as_ref().unwrap().key_as_str(),
            Some("hudi-key-000000100")
        );

        // Third key should not be found
        assert_eq!(results[2].0, "hudi-key-nonexistent");
        assert!(results[2].1.is_none());
    }

    #[test]
    fn test_collect_records_by_prefix() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_16KB_NONE_5000.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        // Collect records with prefix "hudi-key-00000010" (should match 100-109)
        // Keys are 9-digit padded, so "000000100" to "000000109" match this prefix
        let records = reader
            .collect_records_by_prefix("hudi-key-00000010")
            .expect("Failed to collect by prefix");

        assert_eq!(records.len(), 10);
        for (i, record) in records.iter().enumerate() {
            let expected = format!("hudi-key-{:09}", 100 + i);
            assert_eq!(record.key_as_str(), Some(expected.as_str()));
        }
    }

    #[test]
    fn test_collect_records_empty_file() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_no_entry.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        let records = reader.collect_records().expect("Failed to collect records");
        assert!(records.is_empty());
    }

    #[test]
    fn test_hfile_record_ownership() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_16KB_NONE_5000.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        // Collect some records
        reader.seek_to_first().expect("Failed to seek");
        let record1 = reader.get_record().expect("Failed to get record").unwrap();
        reader.next().expect("Failed to move next");
        let record2 = reader.get_record().expect("Failed to get record").unwrap();

        // Records should be independent (owned data)
        assert_ne!(record1.key(), record2.key());
        assert_eq!(record1.key_as_str(), Some("hudi-key-000000000"));
        assert_eq!(record2.key_as_str(), Some("hudi-key-000000001"));

        // Can use records after reader has moved
        drop(reader);
        assert_eq!(record1.key_as_str(), Some("hudi-key-000000000"));
    }

    // ================== Additional Test Files ==================

    // Priority 1: Different Block Sizes

    #[test]
    fn test_read_512kb_blocks_gzip() {
        // 512KB block size, GZIP compression, 20000 entries
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_512KB_GZ_20000.hfile");
        let reader = HFileReader::new(bytes).expect("Failed to create reader");

        assert_eq!(reader.num_entries(), 20000);
    }

    #[test]
    fn test_512kb_blocks_sequential_read() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_512KB_GZ_20000.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        assert!(reader.seek_to_first().expect("Failed to seek"));

        // Read first 10 entries
        for i in 0..10 {
            let kv = reader.get_key_value().expect("Failed to get kv").unwrap();
            let expected_key = format!("hudi-key-{i:09}");
            let expected_value = format!("hudi-value-{i:09}");

            assert_eq!(kv.key().content_as_str().unwrap(), expected_key);
            assert_eq!(std::str::from_utf8(kv.value()).unwrap(), expected_value);

            if i < 9 {
                assert!(reader.next().expect("Failed to move next"));
            }
        }
    }

    #[test]
    fn test_512kb_blocks_seek() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_512KB_GZ_20000.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        assert!(reader.seek_to_first().expect("Failed to seek"));

        // Seek to key in second block (block 0 ends at ~8886)
        let lookup = Utf8Key::new("hudi-key-000008888");
        let result = reader.seek_to(&lookup).expect("Failed to seek");
        assert_eq!(result, SeekResult::Found);

        let kv = reader.get_key_value().expect("Failed to get kv").unwrap();
        assert_eq!(kv.key().content_as_str().unwrap(), "hudi-key-000008888");
    }

    #[test]
    fn test_read_64kb_blocks_uncompressed() {
        // 64KB block size, no compression, 5000 entries
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_64KB_NONE_5000.hfile");
        let reader = HFileReader::new(bytes).expect("Failed to create reader");

        assert_eq!(reader.num_entries(), 5000);
    }

    #[test]
    fn test_64kb_blocks_sequential_read() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_64KB_NONE_5000.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        assert!(reader.seek_to_first().expect("Failed to seek"));

        // Read first 10 entries
        for i in 0..10 {
            let kv = reader.get_key_value().expect("Failed to get kv").unwrap();
            let expected_key = format!("hudi-key-{i:09}");
            let expected_value = format!("hudi-value-{i:09}");

            assert_eq!(kv.key().content_as_str().unwrap(), expected_key);
            assert_eq!(std::str::from_utf8(kv.value()).unwrap(), expected_value);

            if i < 9 {
                assert!(reader.next().expect("Failed to move next"));
            }
        }
    }

    #[test]
    fn test_64kb_blocks_seek() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_64KB_NONE_5000.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        assert!(reader.seek_to_first().expect("Failed to seek"));

        // Seek to key in second block (block 0 ends at ~1110)
        let lookup = Utf8Key::new("hudi-key-000001688");
        let result = reader.seek_to(&lookup).expect("Failed to seek");
        assert_eq!(result, SeekResult::Found);

        let kv = reader.get_key_value().expect("Failed to get kv").unwrap();
        assert_eq!(kv.key().content_as_str().unwrap(), "hudi-key-000001688");
    }

    // Priority 2: Edge Cases

    #[test]
    fn test_read_non_unique_keys() {
        // 200 unique keys, each with 21 values (1 primary + 20 duplicates)
        // Total: 200 * 21 = 4200 entries
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_16KB_GZ_200_20_non_unique.hfile");
        let reader = HFileReader::new(bytes).expect("Failed to create reader");

        assert_eq!(reader.num_entries(), 4200);
    }

    #[test]
    fn test_non_unique_keys_sequential_read() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_16KB_GZ_200_20_non_unique.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        assert!(reader.seek_to_first().expect("Failed to seek"));

        // First entry for key 0
        let kv = reader.get_key_value().expect("Failed to get kv").unwrap();
        assert_eq!(kv.key().content_as_str().unwrap(), "hudi-key-000000000");
        assert_eq!(
            std::str::from_utf8(kv.value()).unwrap(),
            "hudi-value-000000000"
        );

        // Next 20 entries should be duplicates with _0 to _19 suffix
        for j in 0..20 {
            assert!(reader.next().expect("Failed to move next"));
            let kv = reader.get_key_value().expect("Failed to get kv").unwrap();
            assert_eq!(kv.key().content_as_str().unwrap(), "hudi-key-000000000");
            let expected_value = format!("hudi-value-000000000_{j}");
            assert_eq!(std::str::from_utf8(kv.value()).unwrap(), expected_value);
        }

        // Next entry should be key 1
        assert!(reader.next().expect("Failed to move next"));
        let kv = reader.get_key_value().expect("Failed to get kv").unwrap();
        assert_eq!(kv.key().content_as_str().unwrap(), "hudi-key-000000001");
    }

    #[test]
    fn test_non_unique_keys_seek() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_16KB_GZ_200_20_non_unique.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        assert!(reader.seek_to_first().expect("Failed to seek"));

        // Seek to a key - should find the first occurrence
        let lookup = Utf8Key::new("hudi-key-000000005");
        let result = reader.seek_to(&lookup).expect("Failed to seek");
        assert_eq!(result, SeekResult::Found);

        let kv = reader.get_key_value().expect("Failed to get kv").unwrap();
        assert_eq!(kv.key().content_as_str().unwrap(), "hudi-key-000000005");
        // First occurrence has the base value
        assert_eq!(
            std::str::from_utf8(kv.value()).unwrap(),
            "hudi-value-000000005"
        );
    }

    #[test]
    fn test_read_fake_first_key() {
        // File with fake first keys in meta index block
        // Keys have suffix "-abcdefghij"
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_13_16KB_GZ_20000_fake_first_key.hfile");
        let reader = HFileReader::new(bytes).expect("Failed to create reader");

        assert_eq!(reader.num_entries(), 20000);
    }

    #[test]
    fn test_fake_first_key_sequential_read() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_13_16KB_GZ_20000_fake_first_key.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        assert!(reader.seek_to_first().expect("Failed to seek"));

        // Read first 10 entries - keys have "-abcdefghij" suffix
        for i in 0..10 {
            let kv = reader.get_key_value().expect("Failed to get kv").unwrap();
            let expected_key = format!("hudi-key-{i:09}-abcdefghij");
            let expected_value = format!("hudi-value-{i:09}");

            assert_eq!(kv.key().content_as_str().unwrap(), expected_key);
            assert_eq!(std::str::from_utf8(kv.value()).unwrap(), expected_value);

            if i < 9 {
                assert!(reader.next().expect("Failed to move next"));
            }
        }
    }

    #[test]
    fn test_fake_first_key_seek_exact() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_13_16KB_GZ_20000_fake_first_key.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        assert!(reader.seek_to_first().expect("Failed to seek"));

        // Seek to exact key with suffix
        let lookup = Utf8Key::new("hudi-key-000000099-abcdefghij");
        let result = reader.seek_to(&lookup).expect("Failed to seek");
        assert_eq!(result, SeekResult::Found);

        let kv = reader.get_key_value().expect("Failed to get kv").unwrap();
        assert_eq!(
            kv.key().content_as_str().unwrap(),
            "hudi-key-000000099-abcdefghij"
        );
    }

    #[test]
    fn test_fake_first_key_seek_before_block_first() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_13_16KB_GZ_20000_fake_first_key.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        assert!(reader.seek_to_first().expect("Failed to seek"));

        // First, move to a known position
        let lookup = Utf8Key::new("hudi-key-000000469-abcdefghij");
        let result = reader.seek_to(&lookup).expect("Failed to seek");
        assert_eq!(result, SeekResult::Found);

        // Now seek to a key that falls between fake first key and actual first key of next block
        // Block 2 has fake first key "hudi-key-00000047" but actual first key "hudi-key-000000470-abcdefghij"
        let lookup = Utf8Key::new("hudi-key-000000470");
        let result = reader.seek_to(&lookup).expect("Failed to seek");
        // This should return BeforeBlockFirstKey since the lookup key is >= fake first key
        // but < actual first key
        assert_eq!(result, SeekResult::BeforeBlockFirstKey);
    }

    // Priority 3: Multi-level Index

    #[test]
    fn test_read_large_keys_2level_index() {
        // Large keys (>100 bytes), 2-level data block index
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_13_1KB_GZ_20000_large_keys.hfile");
        let reader = HFileReader::new(bytes).expect("Failed to create reader");

        assert_eq!(reader.num_entries(), 20000);
    }

    #[test]
    fn test_large_keys_2level_sequential_read() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_13_1KB_GZ_20000_large_keys.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        let large_key_prefix = "hudi-key-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\
            aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-";

        assert!(reader.seek_to_first().expect("Failed to seek"));

        // Read first 5 entries
        for i in 0..5 {
            let kv = reader.get_key_value().expect("Failed to get kv").unwrap();
            let expected_key = format!("{large_key_prefix}{i:09}");
            let expected_value = format!("hudi-value-{i:09}");

            assert_eq!(kv.key().content_as_str().unwrap(), expected_key);
            assert_eq!(std::str::from_utf8(kv.value()).unwrap(), expected_value);

            if i < 4 {
                assert!(reader.next().expect("Failed to move next"));
            }
        }
    }

    #[test]
    fn test_large_keys_2level_seek() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_13_1KB_GZ_20000_large_keys.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        let large_key_prefix = "hudi-key-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\
            aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-";

        assert!(reader.seek_to_first().expect("Failed to seek"));

        // Seek to a key deep in the file
        let lookup_key = format!("{large_key_prefix}000005340");
        let lookup = Utf8Key::new(&lookup_key);
        let result = reader.seek_to(&lookup).expect("Failed to seek");
        assert_eq!(result, SeekResult::Found);

        let kv = reader.get_key_value().expect("Failed to get kv").unwrap();
        assert_eq!(kv.key().content_as_str().unwrap(), lookup_key);
    }

    #[test]
    fn test_large_keys_2level_iterate_all() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_13_1KB_GZ_20000_large_keys.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        let mut count = 0;
        for result in reader.iter().expect("Failed to create iterator") {
            let _ = result.expect("Failed to read kv");
            count += 1;
        }

        assert_eq!(count, 20000);
    }

    #[test]
    fn test_read_large_keys_3level_index() {
        // Large keys, 3-level deep data block index
        let bytes =
            read_test_hfile("hudi_1_0_hbase_2_4_13_1KB_GZ_10000_large_keys_deep_index.hfile");
        let reader = HFileReader::new(bytes).expect("Failed to create reader");

        assert_eq!(reader.num_entries(), 10000);
    }

    #[test]
    fn test_large_keys_3level_sequential_read() {
        let bytes =
            read_test_hfile("hudi_1_0_hbase_2_4_13_1KB_GZ_10000_large_keys_deep_index.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        let large_key_prefix = "hudi-key-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\
            aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-";

        assert!(reader.seek_to_first().expect("Failed to seek"));

        // Read first 5 entries
        for i in 0..5 {
            let kv = reader.get_key_value().expect("Failed to get kv").unwrap();
            let expected_key = format!("{large_key_prefix}{i:09}");
            let expected_value = format!("hudi-value-{i:09}");

            assert_eq!(kv.key().content_as_str().unwrap(), expected_key);
            assert_eq!(std::str::from_utf8(kv.value()).unwrap(), expected_value);

            if i < 4 {
                assert!(reader.next().expect("Failed to move next"));
            }
        }
    }

    #[test]
    fn test_large_keys_3level_seek() {
        let bytes =
            read_test_hfile("hudi_1_0_hbase_2_4_13_1KB_GZ_10000_large_keys_deep_index.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        let large_key_prefix = "hudi-key-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\
            aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-";

        assert!(reader.seek_to_first().expect("Failed to seek"));

        // Seek to a key deep in the file
        let lookup_key = format!("{large_key_prefix}000005340");
        let lookup = Utf8Key::new(&lookup_key);
        let result = reader.seek_to(&lookup).expect("Failed to seek");
        assert_eq!(result, SeekResult::Found);

        let kv = reader.get_key_value().expect("Failed to get kv").unwrap();
        assert_eq!(kv.key().content_as_str().unwrap(), lookup_key);
    }

    #[test]
    fn test_large_keys_3level_iterate_all() {
        let bytes =
            read_test_hfile("hudi_1_0_hbase_2_4_13_1KB_GZ_10000_large_keys_deep_index.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        let mut count = 0;
        for result in reader.iter().expect("Failed to create iterator") {
            let _ = result.expect("Failed to read kv");
            count += 1;
        }

        assert_eq!(count, 10000);
    }

    #[test]
    fn test_large_keys_3level_last_key() {
        let bytes =
            read_test_hfile("hudi_1_0_hbase_2_4_13_1KB_GZ_10000_large_keys_deep_index.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        let large_key_prefix = "hudi-key-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\
            aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-";

        assert!(reader.seek_to_first().expect("Failed to seek"));

        // Seek to last key
        let lookup_key = format!("{large_key_prefix}000009999");
        let lookup = Utf8Key::new(&lookup_key);
        let result = reader.seek_to(&lookup).expect("Failed to seek");
        assert_eq!(result, SeekResult::Found);

        let kv = reader.get_key_value().expect("Failed to get kv").unwrap();
        assert_eq!(kv.key().content_as_str().unwrap(), lookup_key);
        assert_eq!(
            std::str::from_utf8(kv.value()).unwrap(),
            "hudi-value-000009999"
        );

        // Next should return false (EOF)
        assert!(!reader.next().expect("Failed to move next"));
    }

    #[test]
    fn test_large_keys_3level_seek_eof() {
        let bytes =
            read_test_hfile("hudi_1_0_hbase_2_4_13_1KB_GZ_10000_large_keys_deep_index.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        let large_key_prefix = "hudi-key-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\
            aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-";

        assert!(reader.seek_to_first().expect("Failed to seek"));

        // Seek past last key
        let lookup_key = format!("{large_key_prefix}000009999a");
        let lookup = Utf8Key::new(&lookup_key);
        let result = reader.seek_to(&lookup).expect("Failed to seek");
        assert_eq!(result, SeekResult::Eof);
    }

    // ================== Additional Coverage Tests ==================

    #[test]
    fn test_is_seeked() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_16KB_NONE_5000.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        assert!(!reader.is_seeked());
        reader.seek_to_first().expect("Failed to seek");
        assert!(reader.is_seeked());
    }

    #[test]
    fn test_get_key_value_not_seeked() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_16KB_NONE_5000.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        // Not seeked yet
        let result = reader.get_key_value().expect("Failed to get kv");
        assert!(result.is_none());
    }

    #[test]
    fn test_next_not_seeked() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_16KB_NONE_5000.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        // next() without seek should return false
        assert!(!reader.next().expect("Failed to next"));
    }

    #[test]
    fn test_seek_before_first_key() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_16KB_NONE_5000.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        reader.seek_to_first().expect("Failed to seek");

        // Seek to a key before the first key in the file
        let lookup = Utf8Key::new("aaa"); // Before "hudi-key-000000000"
        let result = reader.seek_to(&lookup).expect("Failed to seek");
        assert_eq!(result, SeekResult::BeforeFileFirstKey);
    }

    #[test]
    fn test_seek_in_range() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_16KB_NONE_5000.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        reader.seek_to_first().expect("Failed to seek");

        // First move past the first key to avoid BeforeBlockFirstKey result
        let lookup = Utf8Key::new("hudi-key-000000100");
        reader.seek_to(&lookup).expect("Failed to seek");

        // Now seek to a key that doesn't exist but is in range (between 100 and 101)
        let lookup = Utf8Key::new("hudi-key-000000100a");
        let result = reader.seek_to(&lookup).expect("Failed to seek");
        assert_eq!(result, SeekResult::InRange);
    }

    #[test]
    fn test_lookup_records_empty_keys() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_16KB_NONE_5000.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        let results = reader.lookup_records(&[]).expect("Failed to lookup");
        assert!(results.is_empty());
    }

    #[test]
    fn test_lookup_records_not_found() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_16KB_NONE_5000.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        let results = reader
            .lookup_records(&["nonexistent-key"])
            .expect("Failed to lookup");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "nonexistent-key");
        assert!(results[0].1.is_none());
    }

    #[test]
    fn test_lookup_records_found() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_16KB_NONE_5000.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        let results = reader
            .lookup_records(&["hudi-key-000000000", "hudi-key-000000001"])
            .expect("Failed to lookup");

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, "hudi-key-000000000");
        assert!(results[0].1.is_some());
        assert_eq!(results[1].0, "hudi-key-000000001");
        assert!(results[1].1.is_some());
    }

    #[test]
    fn test_collect_records_by_prefix_no_matches() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_16KB_NONE_5000.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        let records = reader
            .collect_records_by_prefix("nonexistent-prefix-")
            .expect("Failed to collect");
        assert!(records.is_empty());
    }

    #[test]
    fn test_collect_records_by_prefix_found() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_16KB_NONE_5000.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        // All keys start with "hudi-key-00000000" for keys 0-9
        let records = reader
            .collect_records_by_prefix("hudi-key-00000000")
            .expect("Failed to collect");
        // Keys 0-9 match this prefix
        assert_eq!(records.len(), 10);
    }

    #[test]
    fn test_trailer_info() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_16KB_NONE_5000.hfile");
        let reader = HFileReader::new(bytes).expect("Failed to create reader");

        assert_eq!(reader.num_entries(), 5000);
        // Just verify we can access trailer info
        assert!(reader.num_entries() > 0);
    }

    #[test]
    fn test_seek_result_enum() {
        // Test SeekResult values
        assert_eq!(SeekResult::BeforeBlockFirstKey as i32, -2);
        assert_eq!(SeekResult::BeforeFileFirstKey as i32, -1);
        assert_eq!(SeekResult::Found as i32, 0);
        assert_eq!(SeekResult::InRange as i32, 1);
        assert_eq!(SeekResult::Eof as i32, 2);

        // Test Debug implementation
        let _ = format!("{:?}", SeekResult::Found);
    }

    // ================== Metadata Table HFile Tests ==================
    //
    // These tests validate reading HFile from a Hudi metadata table's
    // "files" partition. The test data is from quickstart_trips_table
    // v8_trips_8i3u1d (8 inserts, 3 updates, 1 delete):
    //
    // Table schema: ts, uuid, rider, driver, fare, city
    // Partitions: city=chennai, city=san_francisco, city=sao_paulo
    // MOR table with metadata table enabled
    //
    // The HFile contains 4 records:
    // 1. "__all_partitions__" - List of all partition paths
    // 2. "city=chennai" - 2 parquet files (UUID: 6e1d5cc4-c487-487d-abbe-fe9b30b1c0cc)
    // 3. "city=san_francisco" - 2 parquet files (UUID: 036ded81-9ed4-479f-bcea-7145dfa0079b)
    // 4. "city=sao_paulo" - 2 parquet files (UUID: 8aa68f7e-afd6-4c94-b86c-8a886552e08d)

    use crate::metadata::table_record::{FilesPartitionRecord, decode_files_partition_record};
    use hudi_test::QuickstartTripsTable;

    /// Get the path to the files partition directory in the test table.
    fn files_partition_dir() -> PathBuf {
        let table_path = QuickstartTripsTable::V8Trips8I3U1D.path_to_mor_avro();
        PathBuf::from(table_path)
            .join(".hoodie")
            .join("metadata")
            .join("files")
    }

    /// Find the latest HFile in the files partition directory.
    /// HFile names follow pattern: files-0000-0_X-X-X_TIMESTAMP.hfile
    /// We pick the one with the latest timestamp.
    fn files_partition_hfile_path() -> PathBuf {
        let dir = files_partition_dir();
        let mut hfiles: Vec<_> = std::fs::read_dir(&dir)
            .unwrap_or_else(|e| panic!("Failed to read directory {dir:?}: {e}"))
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry
                    .path()
                    .extension()
                    .map(|ext| ext == "hfile")
                    .unwrap_or(false)
            })
            .collect();

        // Sort by filename to get the latest (timestamps are in filename)
        hfiles.sort_by_key(|e| e.file_name());

        hfiles
            .last()
            .map(|e| e.path())
            .unwrap_or_else(|| panic!("No HFile found in {dir:?}"))
    }

    fn read_metadata_table_hfile() -> Vec<u8> {
        let path = files_partition_hfile_path();
        std::fs::read(&path).unwrap_or_else(|_| panic!("Failed to read test file: {path:?}"))
    }

    /// Test reading and validating metadata table HFile structure.
    ///
    /// Validates entry count, keys in sorted order, and value structure.
    #[test]
    fn test_metadata_table_hfile_structure() {
        let bytes = read_metadata_table_hfile();
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        // Verify entry count: 1 for __all_partitions__ + 3 for partition paths
        assert_eq!(reader.num_entries(), 4);

        // Collect and verify keys in sorted order
        let records = reader.collect_records().expect("Failed to collect records");
        let keys: Vec<&str> = records
            .iter()
            .map(|r| r.key_as_str().expect("Key should be UTF-8"))
            .collect();

        assert_eq!(
            keys,
            vec![
                FilesPartitionRecord::ALL_PARTITIONS_KEY,
                "city=chennai",
                "city=san_francisco",
                "city=sao_paulo"
            ]
        );

        // Verify all values are non-empty (not tombstones)
        for record in &records {
            assert!(!record.is_deleted());
            assert!(record.value.len() > 50);
        }
    }

    /// Test decoding file listings from partition records.
    ///
    /// Validates actual file names by decoding Avro values and
    /// verifying against known files in the Hudi table.
    #[test]
    fn test_metadata_table_hfile_file_listings() {
        let bytes = read_metadata_table_hfile();
        // Create one reader for schema access, another for record collection
        let schema_reader = HFileReader::new(bytes.clone()).expect("Failed to create reader");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        let records = reader.collect_records().expect("Failed to collect records");

        // Expected files per partition (from the actual Hudi table v8_trips_8i3u1d)
        // city=chennai: 2 parquet files (6e1d5cc4-c487-487d-abbe-fe9b30b1c0cc)
        // city=san_francisco: 2 parquet files (036ded81-9ed4-479f-bcea-7145dfa0079b)
        // city=sao_paulo: 2 parquet files (8aa68f7e-afd6-4c94-b86c-8a886552e08d)

        for record in &records {
            let key = record.key_as_str().expect("Key should be UTF-8");
            let files_record = decode_files_partition_record(&schema_reader, record)
                .unwrap_or_else(|e| panic!("Failed to decode record for key {key}: {e}"));

            match key {
                FilesPartitionRecord::ALL_PARTITIONS_KEY => {
                    // Validate ALL_PARTITIONS record type and partitions
                    assert_eq!(
                        files_record.record_type,
                        crate::metadata::table_record::MetadataRecordType::AllPartitions
                    );
                    assert!(files_record.is_all_partitions());
                    assert_eq!(files_record.partition_names().len(), 3);
                }
                "city=chennai" => {
                    assert_eq!(
                        files_record.record_type,
                        crate::metadata::table_record::MetadataRecordType::Files
                    );
                    // Filter to parquet files only (MOR tables also have log files)
                    let parquet_files: Vec<_> = files_record
                        .active_file_names()
                        .into_iter()
                        .filter(|f| f.ends_with(".parquet"))
                        .collect();
                    assert_eq!(
                        parquet_files.len(),
                        2,
                        "chennai should have 2 parquet files"
                    );
                    for file in &parquet_files {
                        assert!(
                            file.contains("6e1d5cc4-c487-487d-abbe-fe9b30b1c0cc"),
                            "chennai file ID mismatch: {file}"
                        );
                    }
                    // Validate file sizes are populated
                    assert!(files_record.total_size() > 0);
                }
                "city=san_francisco" => {
                    let parquet_files: Vec<_> = files_record
                        .active_file_names()
                        .into_iter()
                        .filter(|f| f.ends_with(".parquet"))
                        .collect();
                    assert_eq!(
                        parquet_files.len(),
                        2,
                        "san_francisco should have 2 parquet files"
                    );
                    for file in &parquet_files {
                        assert!(
                            file.contains("036ded81-9ed4-479f-bcea-7145dfa0079b"),
                            "san_francisco file ID mismatch: {file}"
                        );
                    }
                }
                "city=sao_paulo" => {
                    let parquet_files: Vec<_> = files_record
                        .active_file_names()
                        .into_iter()
                        .filter(|f| f.ends_with(".parquet"))
                        .collect();
                    assert_eq!(
                        parquet_files.len(),
                        2,
                        "sao_paulo should have 2 parquet files"
                    );
                    for file in &parquet_files {
                        assert!(
                            file.contains("8aa68f7e-afd6-4c94-b86c-8a886552e08d"),
                            "sao_paulo file ID mismatch: {file}"
                        );
                    }
                }
                _ => panic!("Unexpected key: {key}"),
            }
        }
    }

    /// Test file info extraction from san_francisco partition.
    #[test]
    fn test_metadata_table_hfile_partition_file_extraction() {
        let bytes = read_metadata_table_hfile();
        let schema_reader = HFileReader::new(bytes.clone()).expect("Failed to create reader");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        // Seek to san_francisco partition (has parquet + log files in MOR table)
        reader.seek_to_first().expect("Failed to seek");
        let lookup = Utf8Key::new("city=san_francisco");
        assert_eq!(
            reader.seek_to(&lookup).expect("Failed to seek"),
            SeekResult::Found
        );

        let record = reader.get_record().expect("Failed to get record").unwrap();
        let files_record = decode_files_partition_record(&schema_reader, &record)
            .expect("Failed to decode san_francisco record");

        // Verify file extraction and sizes (MOR table has parquet + log files)
        assert!(
            files_record.files.len() >= 2,
            "san_francisco should have at least 2 files"
        );
        assert!(files_record.total_size() > 0, "Total size should be > 0");

        // Filter to parquet files only for specific validation
        let parquet_files: Vec<_> = files_record
            .files
            .iter()
            .filter(|(name, _)| name.ends_with(".parquet"))
            .collect();

        assert_eq!(
            parquet_files.len(),
            2,
            "san_francisco should have 2 parquet files"
        );

        for (file_name, file_info) in &parquet_files {
            assert!(
                file_name.contains("036ded81-9ed4-479f-bcea-7145dfa0079b"),
                "File should match san_francisco UUID: {file_name}"
            );
            assert!(file_info.size > 0, "File size should be > 0");
            assert!(!file_info.is_deleted, "File should not be deleted");
        }
    }

    /// Test seek operations on metadata table HFile.
    #[test]
    fn test_metadata_table_hfile_seek_operations() {
        let bytes = read_metadata_table_hfile();
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        reader.seek_to_first().expect("Failed to seek");

        // Seek before first key
        assert_eq!(
            reader.seek_to(&Utf8Key::new("AAA")).expect("seek"),
            SeekResult::BeforeFileFirstKey
        );

        // Seek after last key
        assert_eq!(
            reader.seek_to(&Utf8Key::new("zzz")).expect("seek"),
            SeekResult::Eof
        );

        // Reset and seek to non-existent key between valid keys
        reader.seek_to_first().expect("Failed to seek");
        let result = reader.seek_to(&Utf8Key::new("city=berlin")).expect("seek");
        assert!(matches!(
            result,
            SeekResult::InRange | SeekResult::BeforeBlockFirstKey
        ));
    }

    /// Test prefix scan for partition file listings.
    #[test]
    fn test_metadata_table_hfile_prefix_scan_with_file_validation() {
        let bytes = read_metadata_table_hfile();
        let schema_reader = HFileReader::new(bytes.clone()).expect("Failed to create reader");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        // Scan for partition keys
        let records = reader
            .collect_records_by_prefix("city=")
            .expect("Failed to collect by prefix");

        assert_eq!(records.len(), 3);

        // Verify each partition has valid file listings
        let mut total_parquet_files = 0;
        let mut total_size: i64 = 0;
        for record in &records {
            let files_record = decode_files_partition_record(&schema_reader, record)
                .expect("Failed to decode record");
            let active = files_record.active_file_names();
            assert!(!active.is_empty(), "Partition should have files");

            // Count only parquet files (MOR tables also have log files)
            let parquet_count = active.iter().filter(|f| f.ends_with(".parquet")).count();
            total_parquet_files += parquet_count;
            total_size += files_record.total_size();
        }

        // Total parquet: 2 (chennai) + 2 (san_francisco) + 2 (sao_paulo) = 6 files
        assert_eq!(
            total_parquet_files, 6,
            "Total parquet files across all partitions"
        );
        assert!(total_size > 0, "Total size across partitions should be > 0");
    }

    // ================== File Info and Meta Block Tests ==================

    #[test]
    fn test_get_file_info_last_key() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_16KB_NONE_5000.hfile");
        let reader = HFileReader::new(bytes).expect("Failed to create reader");

        // LASTKEY should be present in file info
        let last_key = reader.get_file_info("hfile.LASTKEY");
        assert!(last_key.is_some(), "LASTKEY should be present");

        // Parse the last key - it's the structured key bytes
        let last_key_bytes = last_key.unwrap();
        assert!(!last_key_bytes.is_empty(), "LASTKEY should not be empty");
    }

    #[test]
    fn test_get_file_info_not_found() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_16KB_NONE_5000.hfile");
        let reader = HFileReader::new(bytes).expect("Failed to create reader");

        // Non-existent key should return None
        let result = reader.get_file_info("nonexistent.key");
        assert!(result.is_none());
    }

    #[test]
    fn test_get_avro_schema_from_metadata_hfile() {
        let bytes = read_metadata_table_hfile();
        let reader = HFileReader::new(bytes).expect("Failed to create reader");

        // Metadata table HFiles should have embedded Avro schema
        let schema = reader.get_avro_schema().expect("Failed to get schema");
        assert!(schema.is_some(), "Metadata HFile should have Avro schema");

        let avro_schema = schema.unwrap();
        // Schema should be a record type for HoodieMetadataRecord
        assert!(
            matches!(avro_schema, AvroSchema::Record(_)),
            "Schema should be a record type"
        );
    }

    #[test]
    fn test_get_avro_schema_regular_hfile() {
        // Regular test HFiles don't have Avro schema in file info
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_16KB_NONE_5000.hfile");
        let reader = HFileReader::new(bytes).expect("Failed to create reader");

        let schema = reader.get_avro_schema().expect("Failed to get schema");
        // Regular HFiles typically don't have embedded Avro schema
        assert!(
            schema.is_none(),
            "Regular HFile should not have Avro schema"
        );
    }

    #[test]
    fn test_read_min_max_record_keys_from_metadata_hfile() {
        let bytes = read_metadata_table_hfile();
        let reader = HFileReader::new(bytes).expect("Failed to create reader");

        // Metadata table HFiles should have min/max record keys
        let result = reader.read_min_max_record_keys();

        // The metadata HFile may or may not have these keys depending on how it was created
        // If present, verify the structure
        if let Some((min_key, max_key)) = result {
            assert!(!min_key.is_empty(), "Min key should not be empty");
            assert!(!max_key.is_empty(), "Max key should not be empty");
            // Min should be <= Max lexicographically
            assert!(
                min_key <= max_key,
                "Min key should be <= Max key: {min_key} vs {max_key}"
            );
        }
    }

    #[test]
    fn test_read_min_max_record_keys_regular_hfile() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_16KB_NONE_5000.hfile");
        let reader = HFileReader::new(bytes).expect("Failed to create reader");

        // Regular test HFiles typically don't have min/max record keys
        let result = reader.read_min_max_record_keys();
        assert!(
            result.is_none(),
            "Regular HFile should not have min/max record keys"
        );
    }

    // ================== Error Handling Tests ==================

    #[test]
    fn test_invalid_hfile_too_small() {
        // File too small to contain a valid trailer
        let bytes = vec![0u8; 10];
        let result = HFileReader::new(bytes);
        assert!(result.is_err(), "Should fail for file too small");
    }

    #[test]
    fn test_invalid_hfile_bad_magic() {
        // Create a file with wrong magic bytes at the end
        let mut bytes = vec![0u8; 100];
        // HFile trailer magic is at the end - put garbage there
        bytes[96..100].copy_from_slice(b"BAAD");
        let result = HFileReader::new(bytes);
        assert!(result.is_err(), "Should fail for invalid magic");
    }

    // ================== Multi-Block Iteration Tests ==================

    #[test]
    fn test_iterate_across_multiple_blocks() {
        // Use GZIP file with 20000 entries - spans multiple blocks
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_16KB_GZ_20000.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        let mut count = 0;
        let mut prev_key: Option<String> = None;

        for result in reader.iter().expect("Failed to create iterator") {
            let kv = result.expect("Failed to read kv");
            let key = kv.key().content_as_str().unwrap().to_string();

            // Verify keys are in ascending order
            if let Some(ref prev) = prev_key {
                assert!(
                    key > *prev,
                    "Keys should be in ascending order: {key} > {prev}"
                );
            }
            prev_key = Some(key);
            count += 1;
        }

        assert_eq!(count, 20000, "Should iterate all 20000 entries");
    }

    #[test]
    fn test_seek_across_block_boundaries() {
        // Use 512KB blocks with 20000 entries
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_512KB_GZ_20000.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        reader.seek_to_first().expect("Failed to seek");

        // Seek to various keys that likely span different blocks
        let test_keys = [
            "hudi-key-000000000", // First key
            "hudi-key-000005000", // Middle
            "hudi-key-000010000", // Another block
            "hudi-key-000015000", // Another block
            "hudi-key-000019999", // Last key
        ];

        for expected_key in test_keys {
            let lookup = Utf8Key::new(expected_key);
            let result = reader.seek_to(&lookup).expect("Failed to seek");
            assert_eq!(result, SeekResult::Found, "Should find key: {expected_key}");

            let kv = reader.get_key_value().expect("Failed to get kv").unwrap();
            assert_eq!(
                kv.key().content_as_str().unwrap(),
                expected_key,
                "Key mismatch"
            );
        }
    }

    #[test]
    fn test_next_at_eof() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_16KB_NONE_5000.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        // Seek to last key
        reader.seek_to_first().expect("Failed to seek");
        let lookup = Utf8Key::new("hudi-key-000004999");
        let result = reader.seek_to(&lookup).expect("Failed to seek");
        assert_eq!(result, SeekResult::Found);

        // next() should return false at EOF
        assert!(!reader.next().expect("Failed to next"));

        // get_key_value should return None after EOF
        assert!(reader.get_key_value().expect("Failed to get kv").is_none());
    }

    #[test]
    fn test_collect_records_gzip() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_16KB_GZ_20000.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        let records = reader.collect_records().expect("Failed to collect records");
        assert_eq!(records.len(), 20000);

        // Verify first and last records
        assert_eq!(records[0].key_as_str(), Some("hudi-key-000000000"));
        assert_eq!(records[19999].key_as_str(), Some("hudi-key-000019999"));
    }

    #[test]
    fn test_lookup_records_across_blocks() {
        let bytes = read_test_hfile("hudi_1_0_hbase_2_4_9_16KB_GZ_20000.hfile");
        let mut reader = HFileReader::new(bytes).expect("Failed to create reader");

        // Look up keys that span different blocks
        let keys = vec![
            "hudi-key-000000000",
            "hudi-key-000005000",
            "hudi-key-000010000",
            "hudi-key-000015000",
            "hudi-key-000019999",
            "hudi-key-nonexistent",
        ];

        let results = reader.lookup_records(&keys).expect("Failed to lookup");
        assert_eq!(results.len(), 6);

        // First 5 should be found
        for (key, value) in results.iter().take(5) {
            assert!(value.is_some(), "Key {key} should be found");
        }

        // Last one should not be found
        assert!(
            results[5].1.is_none(),
            "Nonexistent key should not be found"
        );
    }
}

#[cfg(test)]
mod key_pushdown_tests {
    use super::*;

    /// Fixtures with enough data blocks for selection to mean anything, and the
    /// worst-case reduction each must still achieve. The metadata table's own
    /// HFiles are deliberately absent: every one of them holds a single data
    /// block, so selecting one of one is the whole file and no seek can be shown.
    /// Budgets are set just above what the read currently costs, so widening
    /// selection by even a block or two fails rather than fitting inside slack.
    /// A budget of "less than the full scan" would pass on a selection of all but
    /// one block.
    const MULTI_BLOCK_FIXTURES: &[(&str, u64)] = &[
        // (fixture, the most bytes a three-key seek may read)
        ("hudi_1_0_hbase_2_4_9_16KB_NONE_5000.hfile", 52_000),
        (
            "hudi_1_0_hbase_2_4_13_1KB_GZ_10000_large_keys_deep_index.hfile",
            600,
        ),
        // The writer shortens block index keys here, which is the case selection
        // has to over-include rather than miss on.
        (
            "hudi_1_0_hbase_2_4_13_16KB_GZ_20000_fake_first_key.hfile",
            3_200,
        ),
    ];

    /// Read every record of a fixture, and what that cost.
    async fn full_scan(path: &str) -> Result<(Vec<HFileRecord>, u64)> {
        let reader = HFileReader::open_ranged(&tests::fixture_storage(), path).await?;
        let entries = reader.data_block_entries();
        let counts = reader.reads().expect("a ranged reader counts its reads");
        let before = counts.bytes();
        let records = reader.read_records_batched(&entries).await?;
        Ok((records, counts.bytes() - before))
    }

    /// A key-set seek returns every key it asked for, and reads far less than a
    /// full scan.
    ///
    /// Both halves matter and neither implies the other. Returning the right
    /// records proves selection did not *miss* a block; reading fewer bytes
    /// proves it actually narrowed rather than quietly selecting everything.
    /// A test asserting only the records would pass on `data_block_entries()`.
    #[tokio::test]
    async fn a_key_seek_finds_its_keys_and_reads_less() -> Result<()> {
        for (path, byte_budget) in MULTI_BLOCK_FIXTURES {
            let (all, scan_bytes) = full_scan(path).await?;
            assert!(
                all.len() > 1000,
                "{path}: the fixture must be large enough for selection to matter"
            );

            // First, middle and last, so the selected blocks are scattered rather
            // than adjacent and cannot be coalesced into the whole file.
            let keys: Vec<String> = [0, all.len() / 2, all.len() - 1]
                .iter()
                .map(|i| String::from_utf8_lossy(&all[*i].key).to_string())
                .collect();
            let key_refs: Vec<&str> = keys.iter().map(String::as_str).collect();

            let reader = HFileReader::open_ranged(&tests::fixture_storage(), path).await?;
            let picked = reader.blocks_for_keys(&key_refs);
            // Three keys touch at most six blocks, counting the boundary rule.
            // "fewer than all" would pass on a selection of all but one.
            assert!(
                picked.len() <= 6,
                "{path}: three keys must select at most six blocks, picked {} of {}",
                picked.len(),
                reader.data_block_entries().len()
            );

            let counts = reader.reads().unwrap();
            let before = counts.bytes();
            let got = reader.read_records_batched(&picked).await?;
            let seek_bytes = counts.bytes() - before;

            let found: std::collections::HashSet<&[u8]> =
                got.iter().map(|r| r.key.as_slice()).collect();
            for key in &key_refs {
                assert!(
                    found.contains(key.as_bytes()),
                    "{path}: key {key:?} was in the file but not in the selected blocks"
                );
            }
            assert!(
                seek_bytes <= *byte_budget && seek_bytes < scan_bytes,
                "{path}: seek read {seek_bytes} bytes, budget {byte_budget}, \
                 full scan {scan_bytes}"
            );
        }
        Ok(())
    }

    /// A prefix seek returns every record carrying the prefix, and no record that
    /// does not, after the caller filters what the over-included blocks bring.
    #[tokio::test]
    async fn a_prefix_seek_covers_every_matching_record() -> Result<()> {
        for (path, _) in MULTI_BLOCK_FIXTURES {
            let (all, scan_bytes) = full_scan(path).await?;

            // A prefix taken from a real key by dropping its last two characters,
            // so it matches a neighbourhood rather than the whole file. A short
            // fixed prefix is not selective on every fixture: these keys share a
            // long common head, and six characters of it match everything, which
            // is why the narrowing assertion below is conditional.
            let sample = String::from_utf8_lossy(&all[all.len() / 3].key).to_string();
            let prefix = &sample[..sample.len().saturating_sub(2)];
            let expected: Vec<&[u8]> = all
                .iter()
                .map(|r| r.key.as_slice())
                .filter(|k| k.starts_with(prefix.as_bytes()))
                .collect();
            assert!(
                !expected.is_empty(),
                "{path}: prefix {prefix:?} must match something"
            );

            let reader = HFileReader::open_ranged(&tests::fixture_storage(), path).await?;
            let picked = reader.blocks_for_prefix(prefix);
            let counts = reader.reads().unwrap();
            let before = counts.bytes();
            let got = reader.read_records_batched(&picked).await?;
            let seek_bytes = counts.bytes() - before;

            let matched: Vec<&[u8]> = got
                .iter()
                .map(|r| r.key.as_slice())
                .filter(|k| k.starts_with(prefix.as_bytes()))
                .collect();
            assert_eq!(
                matched, expected,
                "{path}: a prefix seek must cover every matching record, in order"
            );
            // Narrowing is only required of a prefix that is actually selective.
            // One matching every record in the file has nothing to narrow, and
            // reading the file is then the right answer rather than a failure.
            if expected.len() < all.len() {
                assert!(
                    seek_bytes < scan_bytes,
                    "{path}: prefix {prefix:?} matches {} of {} records, so the seek \
                     must narrow, but it read {seek_bytes} bytes against a full scan's \
                     {scan_bytes}",
                    expected.len(),
                    all.len()
                );
            }
        }
        Ok(())
    }

    /// Selection is exhaustive over the whole key space: for *every* key in the
    /// file, the blocks selected for it must include one that holds it.
    ///
    /// This is what rules out the failure the design most fears — index bounds
    /// that under-include, dropping a key that exists. A sampled key cannot rule
    /// out an off-by-one bound; every key can.
    ///
    /// Run over **every** fixture, because the multi-level index walk is a second
    /// population path for `data_block_index` and the one most likely to break
    /// the lower-bound invariant. The fixtures span one, two and three levels.
    ///
    /// Each block is read once, up front, rather than once per key: at ~79,000
    /// keys over ~5,900 blocks the per-key read made this the slowest test in the
    /// crate by an order of magnitude, and it was checking the same thing.
    #[tokio::test]
    async fn no_key_in_any_fixture_is_missed_by_selection() -> Result<()> {
        for path in tests::ALL_FIXTURES {
            let reader = HFileReader::open_ranged(&tests::fixture_storage(), path).await?;
            let entries = reader.data_block_entries();
            if entries.is_empty() {
                continue; // the empty fixture has no keys to seek to
            }

            // offset -> the keys that block holds, from one pass over the blocks.
            let mut by_block: BTreeMap<u64, std::collections::HashSet<Vec<u8>>> = BTreeMap::new();
            for entry in &entries {
                let records = reader
                    .read_records_batched(std::slice::from_ref(entry))
                    .await?;
                by_block.insert(entry.offset, records.into_iter().map(|r| r.key).collect());
            }
            let all_keys: Vec<Vec<u8>> = by_block.values().flatten().cloned().collect();
            assert!(
                !all_keys.is_empty(),
                "{path}: the fixture must hold keys for this to check anything"
            );

            // Every key at once must reach every block, or a block is unreachable
            // through selection and its records can never be read.
            let owned: Vec<String> = all_keys
                .iter()
                .map(|k| String::from_utf8_lossy(k).to_string())
                .collect();
            let refs: Vec<&str> = owned.iter().map(String::as_str).collect();
            assert_eq!(
                reader.blocks_for_keys(&refs).len(),
                entries.len(),
                "{path}: selecting every key must select every block"
            );

            // And one key at a time, which is where an off-by-one bound shows up.
            for (key, text) in all_keys.iter().zip(&owned) {
                let picked = reader.blocks_for_keys(&[text.as_str()]);
                assert!(
                    !picked.is_empty() && picked.len() <= 2,
                    "{path}: key {text:?} selected {} blocks; expected one, or two \
                     when the probe lands exactly on a separator",
                    picked.len()
                );
                assert!(
                    picked.iter().any(|e| by_block[&e.offset].contains(key)),
                    "{path}: key {text:?} exists but no selected block holds it"
                );
            }
        }
        Ok(())
    }

    /// A key that is exactly a block's index key selects that block **and** the
    /// one before it.
    ///
    /// Not an optimisation lost: row keys need not be unique, and when a key's
    /// copies straddle a block boundary the writer's separator equals the key, so
    /// taking only the entry at or below it would drop the copies in the earlier
    /// block. Asserted here because no fixture in the repo actually straddles, so
    /// nothing else would notice if this rule were removed.
    #[tokio::test]
    async fn a_key_on_a_block_boundary_takes_the_previous_block_too() -> Result<()> {
        let path = "hudi_1_0_hbase_2_4_9_16KB_NONE_5000.hfile";
        let reader = HFileReader::open_ranged(&tests::fixture_storage(), path).await?;
        let entries = reader.data_block_entries();
        assert!(entries.len() > 2, "the fixture needs several blocks");

        // The second block's index key: an exact separator with a block before it.
        let separator = entries[1].first_key.clone();
        let key = std::str::from_utf8(separator.content()).expect("a utf8 row key");
        let picked = reader.blocks_for_keys(&[key]);
        assert_eq!(
            picked.len(),
            2,
            "an exact separator hit must take the previous block as well"
        );
        assert_eq!(picked[0].offset, entries[0].offset);
        assert_eq!(picked[1].offset, entries[1].offset);

        // The first block has nothing before it, so it stays a single selection.
        let first = entries[0].first_key.clone();
        let first_key = std::str::from_utf8(first.content()).expect("a utf8 row key");
        assert_eq!(reader.blocks_for_keys(&[first_key]).len(), 1);
        Ok(())
    }

    /// A key below everything in the file selects nothing, rather than selecting
    /// the first block and reading it for no reason.
    #[tokio::test]
    async fn a_key_below_the_file_selects_nothing() -> Result<()> {
        let path = "hudi_1_0_hbase_2_4_9_16KB_NONE_5000.hfile";
        let reader = HFileReader::open_ranged(&tests::fixture_storage(), path).await?;
        assert!(
            reader.blocks_for_keys(&[""]).is_empty(),
            "the empty key sorts below every block's index key, so nothing can hold it"
        );
        Ok(())
    }

    /// The exclusive upper bound of a prefix, including the all-0xFF case that
    /// has no bound and must fall back to the file's tail.
    #[test]
    fn prefix_upper_bound_increments_the_last_byte_it_can() {
        assert_eq!(prefix_upper_bound(b"ab"), Some(b"ac".to_vec()));
        assert_eq!(prefix_upper_bound(&[b'a', 0xFF]), Some(b"b".to_vec()));
        assert_eq!(prefix_upper_bound(&[0xFF, 0xFF]), None);
        assert_eq!(prefix_upper_bound(b""), None);
    }
}

#[cfg(test)]
mod threshold_sweep {
    use super::*;
    use crate::config::HudiConfigs;
    use crate::config::table::HudiTableConfig;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Instant;

    fn storage_for(dir: &str, threshold_mb: &str) -> Arc<Storage> {
        let url = url::Url::from_directory_path(std::fs::canonicalize(dir).unwrap()).unwrap();
        let configs = HudiConfigs::new([
            (
                HudiTableConfig::BasePath.as_ref().to_string(),
                url.as_str().to_string(),
            ),
            (
                crate::storage::reader::CONFIG_HFILE_WHOLE_READ_MAX_SIZE_MB.to_string(),
                threshold_mb.to_string(),
            ),
        ]);
        Storage::new(Arc::new(HashMap::new()), Arc::new(configs)).unwrap()
    }

    async fn point_lookup(storage: &Storage, name: &str, threshold_mb: u64, key: &str) -> usize {
        let reader = HFileReader::open_sized(storage, name, threshold_mb, None)
            .await
            .unwrap();
        let blocks = reader.blocks_for_keys(&[key]);
        reader.read_records_batched(&blocks).await.unwrap().len()
    }

    async fn full_scan(storage: &Storage, name: &str, threshold_mb: u64) -> usize {
        let reader = HFileReader::open_sized(storage, name, threshold_mb, None)
            .await
            .unwrap();
        let entries = reader.data_block_entries();
        let budget = reader.window_budget().unwrap_or(4 * 1024 * 1024);
        let mut total = 0;
        for window in HFileReader::plan_windows(&entries, budget) {
            total += reader.read_records_batched(&window).await.unwrap().len();
        }
        total
    }

    /// Does the threshold pick the faster strategy at every file size?
    ///
    /// Set `HFILE_SWEEP_DIR` to a directory of generated `.hfile` files. Ignored
    /// otherwise: the files are hundreds of megabytes and are not committed.
    #[tokio::test]
    #[ignore]
    async fn the_threshold_picks_the_faster_strategy_across_sizes() {
        let Ok(dir) = std::env::var("HFILE_SWEEP_DIR") else {
            eprintln!("HFILE_SWEEP_DIR unset");
            return;
        };
        // The shipped policy: a scan gets Hudi's threshold, a seek gets the much
        // smaller keyed bound. `HFileBaseFileReader::open` applies the same min.
        const SCAN_WHOLE_BELOW: u64 = 50 * 1024 * 1024;
        const SEEK_WHOLE_BELOW: u64 = crate::storage::reader::HFILE_WHOLE_READ_WITH_KEYS_MAX_SIZE;
        const ALWAYS_RANGED: u64 = 0;

        let mut names: Vec<String> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| {
                let name = e.ok()?.file_name().to_str()?.to_string();
                name.ends_with(".hfile").then_some(name)
            })
            .collect();
        names.sort_by_key(|n| {
            std::fs::metadata(std::path::Path::new(&dir).join(n))
                .unwrap()
                .len()
        });

        let default_storage = storage_for(&dir, "50");
        let ranged_storage = storage_for(&dir, "0");

        println!(
            "{:<16} {:>12} {:>13} | {:>10} {:>10} {:>7} | {:>10} {:>10} {:>7}",
            "file", "bytes", "seek/scan", "seek d", "seek r", "ratio", "scan d", "scan r", "ratio"
        );
        for name in &names {
            let bytes = std::fs::metadata(std::path::Path::new(&dir).join(name))
                .unwrap()
                .len();
            let rounds = if bytes > 32 * 1024 * 1024 { 5 } else { 15 };

            // A key from the middle of the file, so a seek is a real seek.
            let probe = HFileReader::open_ranged(&ranged_storage, name)
                .await
                .unwrap();
            let entries = probe.data_block_entries();
            let mid = &entries[entries.len() / 2];
            let key = String::from_utf8(
                probe
                    .read_records_batched(std::slice::from_ref(mid))
                    .await
                    .unwrap()[0]
                    .key
                    .clone(),
            )
            .unwrap();
            drop(probe);

            // Correctness first: a fast wrong answer is not a win.
            let seek_d = point_lookup(&default_storage, name, SEEK_WHOLE_BELOW, &key).await;
            let seek_r = point_lookup(&ranged_storage, name, ALWAYS_RANGED, &key).await;
            assert_eq!(seek_d, seek_r, "{name}: the two arms disagree on a seek");
            let scan_d = full_scan(&default_storage, name, SCAN_WHOLE_BELOW).await;
            let scan_r = full_scan(&ranged_storage, name, ALWAYS_RANGED).await;
            assert_eq!(scan_d, scan_r, "{name}: the two arms disagree on a scan");

            let mut sd = Vec::new();
            let mut sr = Vec::new();
            let mut cd = Vec::new();
            let mut cr = Vec::new();
            for _ in 0..rounds {
                let t = Instant::now();
                point_lookup(&default_storage, name, SEEK_WHOLE_BELOW, &key).await;
                sd.push(t.elapsed().as_micros());
                let t = Instant::now();
                point_lookup(&ranged_storage, name, ALWAYS_RANGED, &key).await;
                sr.push(t.elapsed().as_micros());
                let t = Instant::now();
                full_scan(&default_storage, name, SCAN_WHOLE_BELOW).await;
                cd.push(t.elapsed().as_micros());
                let t = Instant::now();
                full_scan(&ranged_storage, name, ALWAYS_RANGED).await;
                cr.push(t.elapsed().as_micros());
            }
            for v in [&mut sd, &mut sr, &mut cd, &mut cr] {
                v.sort();
            }
            let m = |v: &Vec<u128>| v[rounds / 2] as f64 / 1000.0;
            let side = match (bytes <= SEEK_WHOLE_BELOW, bytes <= SCAN_WHOLE_BELOW) {
                (true, _) => "whole/whole",
                (false, true) => "ranged/whole",
                (false, false) => "ranged/ranged",
            };
            println!(
                "{:<16} {:>12} {:>13} | {:>10.2} {:>10.2} {:>7.2} | {:>10.2} {:>10.2} {:>7.2}",
                name,
                bytes,
                side,
                m(&sd),
                m(&sr),
                m(&sd) / m(&sr),
                m(&cd),
                m(&cr),
                m(&cd) / m(&cr)
            );
        }
        println!(
            "d = default (whole below 50MB), r = always ranged; ratio < 1 means the default wins"
        );
    }
}
