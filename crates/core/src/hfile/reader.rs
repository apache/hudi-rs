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
    read_var_long, var_long_size_on_disk, BlockIndexEntry, DataBlock, HFileBlock, BLOCK_HEADER_SIZE,
};
use crate::hfile::block_type::HFileBlockType;
use crate::hfile::compression::CompressionCodec;
use crate::hfile::error::{HFileError, Result};
use crate::hfile::key::{compare_keys, Key, KeyValue, Utf8Key};
use crate::hfile::proto::InfoProto;
use crate::hfile::record::HFileRecord;
use crate::hfile::trailer::HFileTrailer;
use prost::Message;

/// Magic bytes indicating protobuf format in file info block
const PBUF_MAGIC: &[u8; 4] = b"PBUF";

/// Seek result codes (matching Java implementation)
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

/// HFile reader that supports sequential reads and seeks.
pub struct HFileReader {
    /// Raw file bytes
    bytes: Vec<u8>,
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
        let codec = trailer.compression_codec;

        let mut reader = Self {
            bytes,
            trailer,
            codec,
            data_block_index: BTreeMap::new(),
            meta_block_index: BTreeMap::new(),
            file_info: BTreeMap::new(),
            last_key: None,
            cursor: Cursor::default(),
            current_block: None,
            current_block_entry: None,
        };

        reader.initialize_metadata()?;
        Ok(reader)
    }

    /// Initialize metadata by reading index blocks and file info.
    fn initialize_metadata(&mut self) -> Result<()> {
        // Read the "load-on-open" section starting from load_on_open_data_offset
        let start = self.trailer.load_on_open_data_offset as usize;

        // Read root data index block
        let (data_index, offset) = self.read_root_index_block(start)?;
        self.data_block_index = data_index;

        // Handle multi-level index if needed
        if self.trailer.num_data_index_levels > 1 {
            self.load_multi_level_index()?;
        }

        // Read meta index block
        let (meta_index, offset) = self.read_meta_index_block(offset)?;
        self.meta_block_index = meta_index;

        // Read file info block
        self.read_file_info_block(offset)?;

        // Parse last key from file info
        if let Some(last_key_bytes) = self.file_info.get(FILE_INFO_LAST_KEY) {
            self.last_key = Some(Key::from_bytes(last_key_bytes.clone()));
        }

        // Check MVCC timestamp support
        self.check_mvcc_support()?;

        Ok(())
    }

    /// Check if the file uses MVCC timestamps (not supported).
    fn check_mvcc_support(&self) -> Result<()> {
        if let Some(version_bytes) = self.file_info.get(FILE_INFO_KEY_VALUE_VERSION) {
            if version_bytes.len() >= 4 {
                let version = i32::from_be_bytes([
                    version_bytes[0],
                    version_bytes[1],
                    version_bytes[2],
                    version_bytes[3],
                ]);
                if version == KEY_VALUE_VERSION_WITH_MVCC_TS {
                    if let Some(ts_bytes) = self.file_info.get(FILE_INFO_MAX_MVCC_TS) {
                        if ts_bytes.len() >= 8 {
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
        let block = HFileBlock::parse(&self.bytes[start..], self.codec)?;
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

        // Build final index map from leaf entries
        let mut index_map = BTreeMap::new();
        for i in 0..current_entries.len() {
            let entry = &current_entries[i];
            let next_key = if i + 1 < current_entries.len() {
                Some(current_entries[i + 1].first_key.clone())
            } else {
                None
            };
            index_map.insert(
                entry.first_key.clone(),
                BlockIndexEntry::new(entry.first_key.clone(), next_key, entry.offset, entry.size),
            );
        }

        self.data_block_index = index_map;
        Ok(())
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
        let block = HFileBlock::parse(&self.bytes[start..], self.codec)?;
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
        let block = HFileBlock::parse(&self.bytes[start..], self.codec)?;
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
        HFileBlock::parse(&self.bytes[offset..offset + size], self.codec)
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
                if let Some(entry) = &self.current_block_entry {
                    if self.is_at_first_key_of_block()
                        && compare_keys(&entry.first_key, lookup_key) != std::cmp::Ordering::Greater
                    {
                        return Ok(SeekResult::BeforeBlockFirstKey);
                    }
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
                if let Some(last_key) = &self.last_key {
                    if compare_keys(last_key, lookup_key) == std::cmp::Ordering::Less {
                        self.cursor.eof = true;
                        self.current_block = None;
                        self.current_block_entry = None;
                        return Ok(SeekResult::Eof);
                    }
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

    // ================== HFileRecord API for MDT ==================

    /// Convert a KeyValue to an owned HFileRecord.
    ///
    /// This extracts the key content (without length prefix) and value bytes
    /// into an owned struct suitable for MDT operations.
    fn key_value_to_record(kv: &KeyValue) -> HFileRecord {
        HFileRecord::new(kv.key().content().to_vec(), kv.value().to_vec())
    }

    /// Collect all records from the HFile as owned HFileRecords.
    ///
    /// This is useful for MDT operations where records need to be stored
    /// and merged with log file records.
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
        std::fs::read(&path).unwrap_or_else(|_| panic!("Failed to read test file: {:?}", path))
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
            let expected_key = format!("hudi-key-{:09}", i);
            let expected_value = format!("hudi-value-{:09}", i);

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
            let expected_key = format!("hudi-key-{:09}", count);
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
            let expected_key = format!("hudi-key-{:09}", i);
            let expected_value = format!("hudi-value-{:09}", i);

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
            let expected_key = format!("hudi-key-{:09}", count);
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
            let expected_key = format!("hudi-key-{:09}", i);
            let expected_value = format!("hudi-value-{:09}", i);

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
            let expected_key = format!("hudi-key-{:09}", i);
            let expected_value = format!("hudi-value-{:09}", i);

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
            let expected_value = format!("hudi-value-000000000_{}", j);
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
            let expected_key = format!("hudi-key-{:09}-abcdefghij", i);
            let expected_value = format!("hudi-value-{:09}", i);

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
            let expected_key = format!("{}{:09}", large_key_prefix, i);
            let expected_value = format!("hudi-value-{:09}", i);

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
        let lookup_key = format!("{}000005340", large_key_prefix);
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
            let expected_key = format!("{}{:09}", large_key_prefix, i);
            let expected_value = format!("hudi-value-{:09}", i);

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
        let lookup_key = format!("{}000005340", large_key_prefix);
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
        let lookup_key = format!("{}000009999", large_key_prefix);
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
        let lookup_key = format!("{}000009999a", large_key_prefix);
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
}
