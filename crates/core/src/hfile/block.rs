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
//! HFile block parsing and handling.

use crate::hfile::block_type::{HFileBlockType, MAGIC_LENGTH};
use crate::hfile::compression::CompressionCodec;
use crate::hfile::error::{HFileError, Result};
use crate::hfile::key::{Key, KeyValue, KEY_VALUE_HEADER_SIZE};

/// Size constants for block header
const SIZEOF_INT32: usize = 4;
const SIZEOF_INT64: usize = 8;
const SIZEOF_BYTE: usize = 1;

/// Block header size without checksum info (HFile v2)
const HEADER_SIZE_NO_CHECKSUM: usize = MAGIC_LENGTH + 2 * SIZEOF_INT32 + SIZEOF_INT64;

/// Block header size with checksum (HFile v3)
/// Header fields:
/// - 8 bytes: magic
/// - 4 bytes: on-disk size without header
/// - 4 bytes: uncompressed size without header
/// - 8 bytes: previous block offset
/// - 1 byte: checksum type
/// - 4 bytes: bytes per checksum
/// - 4 bytes: on-disk data size with header
pub const BLOCK_HEADER_SIZE: usize = HEADER_SIZE_NO_CHECKSUM + SIZEOF_BYTE + 2 * SIZEOF_INT32;

/// Checksum size (each checksum is 4 bytes)
const CHECKSUM_SIZE: usize = SIZEOF_INT32;

/// Parsed block header information.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct BlockHeader {
    pub block_type: HFileBlockType,
    pub on_disk_size_without_header: usize,
    pub uncompressed_size_without_header: usize,
    pub prev_block_offset: i64,
    pub checksum_type: u8,
    pub bytes_per_checksum: usize,
    pub on_disk_data_size_with_header: usize,
}

impl BlockHeader {
    /// Parse a block header from bytes.
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < BLOCK_HEADER_SIZE {
            return Err(HFileError::InvalidFormat(format!(
                "Buffer too small for block header: {} bytes, need {}",
                bytes.len(),
                BLOCK_HEADER_SIZE
            )));
        }

        let block_type = HFileBlockType::from_magic(bytes)?;

        let on_disk_size_without_header =
            i32::from_be_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]) as usize;

        let uncompressed_size_without_header =
            i32::from_be_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]) as usize;

        let prev_block_offset = i64::from_be_bytes([
            bytes[16], bytes[17], bytes[18], bytes[19], bytes[20], bytes[21], bytes[22], bytes[23],
        ]);

        let checksum_type = bytes[24];

        let bytes_per_checksum =
            i32::from_be_bytes([bytes[25], bytes[26], bytes[27], bytes[28]]) as usize;

        let on_disk_data_size_with_header =
            i32::from_be_bytes([bytes[29], bytes[30], bytes[31], bytes[32]]) as usize;

        Ok(Self {
            block_type,
            on_disk_size_without_header,
            uncompressed_size_without_header,
            prev_block_offset,
            checksum_type,
            bytes_per_checksum,
            on_disk_data_size_with_header,
        })
    }

    /// Calculate the number of checksum bytes.
    pub fn checksum_bytes_count(&self) -> usize {
        let on_disk_with_header = BLOCK_HEADER_SIZE + self.on_disk_size_without_header;
        let num_chunks = on_disk_with_header.div_ceil(self.bytes_per_checksum);
        num_chunks * CHECKSUM_SIZE
    }

    /// Returns the total on-disk size including header.
    pub fn on_disk_size_with_header(&self) -> usize {
        BLOCK_HEADER_SIZE + self.on_disk_size_without_header
    }
}

/// An HFile block with parsed content.
#[derive(Debug)]
pub struct HFileBlock {
    pub header: BlockHeader,
    /// Uncompressed block data (after header, before checksum)
    pub data: Vec<u8>,
}

impl HFileBlock {
    /// Parse a block from bytes, decompressing if necessary.
    pub fn parse(bytes: &[u8], codec: CompressionCodec) -> Result<Self> {
        let header = BlockHeader::parse(bytes)?;

        // Extract the compressed data (between header and checksum)
        let data_start = BLOCK_HEADER_SIZE;
        let data_end =
            data_start + header.on_disk_size_without_header - header.checksum_bytes_count();

        // For uncompressed blocks, on_disk_size == uncompressed_size
        // For compressed blocks, we need to decompress
        let data = if codec == CompressionCodec::None {
            bytes[data_start..data_start + header.uncompressed_size_without_header].to_vec()
        } else {
            let compressed_data = &bytes[data_start..data_end];
            codec.decompress(compressed_data, header.uncompressed_size_without_header)?
        };

        Ok(Self { header, data })
    }

    /// Returns the block type.
    pub fn block_type(&self) -> HFileBlockType {
        self.header.block_type
    }
}

/// A block index entry pointing to a data or meta block.
#[derive(Debug, Clone)]
pub struct BlockIndexEntry {
    /// First key in the block (may be shortened/fake for optimization)
    pub first_key: Key,
    /// First key of the next block (if present)
    pub next_block_first_key: Option<Key>,
    /// Offset of the block in the file
    pub offset: u64,
    /// On-disk size of the block
    pub size: u32,
}

impl BlockIndexEntry {
    /// Create a new block index entry.
    pub fn new(first_key: Key, next_block_first_key: Option<Key>, offset: u64, size: u32) -> Self {
        Self {
            first_key,
            next_block_first_key,
            offset,
            size,
        }
    }
}

impl PartialEq for BlockIndexEntry {
    fn eq(&self, other: &Self) -> bool {
        self.first_key == other.first_key
    }
}

impl Eq for BlockIndexEntry {}

impl PartialOrd for BlockIndexEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BlockIndexEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.first_key.cmp(&other.first_key)
    }
}

/// Data block containing key-value pairs.
pub struct DataBlock {
    /// Uncompressed block data
    data: Vec<u8>,
    /// End offset of content (excluding checksum)
    content_end: usize,
}

impl DataBlock {
    /// Create a data block from an HFileBlock.
    pub fn from_block(block: HFileBlock) -> Self {
        let content_end = block.data.len();
        Self {
            data: block.data,
            content_end,
        }
    }

    /// Read a key-value at the given offset within the data block.
    pub fn read_key_value(&self, offset: usize) -> KeyValue {
        KeyValue::parse(&self.data, offset)
    }

    /// Check if the offset is within valid content range.
    pub fn is_valid_offset(&self, offset: usize) -> bool {
        offset < self.content_end
    }

    /// Iterate over all key-value pairs in the block.
    #[allow(dead_code)]
    pub fn iter(&self) -> DataBlockIterator<'_> {
        DataBlockIterator {
            block: self,
            offset: 0,
        }
    }

    /// Returns the raw data.
    #[allow(dead_code)]
    pub fn data(&self) -> &[u8] {
        &self.data
    }
}

/// Iterator over key-value pairs in a data block.
pub struct DataBlockIterator<'a> {
    block: &'a DataBlock,
    offset: usize,
}

impl<'a> Iterator for DataBlockIterator<'a> {
    type Item = KeyValue;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.block.is_valid_offset(self.offset) {
            return None;
        }

        // Need at least 8 bytes for key/value lengths
        if self.offset + KEY_VALUE_HEADER_SIZE > self.block.content_end {
            return None;
        }

        let kv = self.block.read_key_value(self.offset);
        self.offset += kv.record_size();
        Some(kv)
    }
}

/// Read a variable-length encoded integer from bytes.
/// Returns (value, bytes_consumed).
pub fn read_var_long(bytes: &[u8], offset: usize) -> (u64, usize) {
    let mut result: u64 = 0;
    let mut shift = 0;
    let mut pos = offset;

    loop {
        if pos >= bytes.len() {
            break;
        }
        let b = bytes[pos] as u64;
        pos += 1;
        result |= (b & 0x7F) << shift;
        if b & 0x80 == 0 {
            break;
        }
        shift += 7;
    }

    (result, pos - offset)
}

/// Calculate the size of a variable-length encoded integer.
pub fn var_long_size_on_disk(bytes: &[u8], offset: usize) -> usize {
    let mut pos = offset;
    while pos < bytes.len() && bytes[pos] & 0x80 != 0 {
        pos += 1;
    }
    if pos < bytes.len() {
        pos += 1; // Include the last byte
    }
    pos - offset
}
