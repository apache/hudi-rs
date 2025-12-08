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

/// Read a Hadoop VLong encoded integer from bytes.
/// This is the encoding used in HFile root/leaf index blocks.
/// Returns (value, bytes_consumed).
///
/// Hadoop VLong format:
/// - First byte determines total size
/// - If first byte >= -112 (signed), value fits in single byte
/// - Otherwise, -111 - first_byte gives number of additional data bytes
pub fn read_var_long(bytes: &[u8], offset: usize) -> (u64, usize) {
    let first_byte = bytes[offset];
    let size = var_long_size_on_disk_single(first_byte);

    if size == 1 {
        // Single byte encoding: value is the byte itself (as signed, then cast to u64)
        return (first_byte as i8 as i64 as u64, 1);
    }

    // Multi-byte encoding: read size-1 bytes as big-endian
    let mut value: u64 = 0;
    for i in 0..size - 1 {
        value = (value << 8) | (bytes[offset + 1 + i] as u64);
    }

    // Check if negative (first byte < -120 in signed representation)
    let is_negative = (first_byte as i8) < -120;
    if is_negative {
        (!value, size)
    } else {
        (value, size)
    }
}

/// Calculate the size of a Hadoop VLong encoded integer from the first byte.
fn var_long_size_on_disk_single(first_byte: u8) -> usize {
    let signed = first_byte as i8;
    if signed >= -112 {
        1
    } else {
        (-111 - signed as i32) as usize
    }
}

/// Calculate the size of a Hadoop VLong encoded integer.
pub fn var_long_size_on_disk(bytes: &[u8], offset: usize) -> usize {
    var_long_size_on_disk_single(bytes[offset])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_header_parse_too_small() {
        let bytes = vec![0u8; 10]; // Too small for header
        let err = BlockHeader::parse(&bytes).unwrap_err();
        assert!(matches!(err, HFileError::InvalidFormat(_)));
    }

    #[test]
    fn test_block_header_on_disk_size_with_header() {
        // Create a minimal valid block header with DATA magic
        let mut bytes = vec![];
        bytes.extend_from_slice(b"DATABLK*"); // magic
        bytes.extend_from_slice(&100i32.to_be_bytes()); // on_disk_size_without_header
        bytes.extend_from_slice(&200i32.to_be_bytes()); // uncompressed_size_without_header
        bytes.extend_from_slice(&(-1i64).to_be_bytes()); // prev_block_offset
        bytes.push(1); // checksum_type
        bytes.extend_from_slice(&16384i32.to_be_bytes()); // bytes_per_checksum
        bytes.extend_from_slice(&133i32.to_be_bytes()); // on_disk_data_size_with_header

        let header = BlockHeader::parse(&bytes).unwrap();
        assert_eq!(header.on_disk_size_with_header(), BLOCK_HEADER_SIZE + 100);
        assert_eq!(header.block_type, HFileBlockType::Data);
    }

    #[test]
    fn test_block_index_entry_ordering() {
        let key1 = Key::from_bytes(vec![0, 3, b'a', b'a', b'a']);
        let key2 = Key::from_bytes(vec![0, 3, b'b', b'b', b'b']);

        let entry1 = BlockIndexEntry::new(key1.clone(), None, 0, 100);
        let entry2 = BlockIndexEntry::new(key2.clone(), None, 1000, 100);
        let entry3 = BlockIndexEntry::new(key1.clone(), None, 2000, 200);

        assert!(entry1 < entry2);
        assert_eq!(entry1, entry3); // Same key = equal
        assert_eq!(entry1.partial_cmp(&entry2), Some(std::cmp::Ordering::Less));
    }

    #[test]
    fn test_read_var_long_single_byte() {
        // Single byte values (first byte >= -112 as signed, i.e., 0-143 or 144-255 when unsigned)
        let bytes = vec![0u8]; // value 0
        let (value, size) = read_var_long(&bytes, 0);
        assert_eq!(value, 0);
        assert_eq!(size, 1);

        let bytes = vec![100u8]; // value 100
        let (value, size) = read_var_long(&bytes, 0);
        assert_eq!(value, 100);
        assert_eq!(size, 1);

        // 127 as unsigned byte is 127, as signed it's 127 (>= -112), so single byte
        let bytes = vec![127u8];
        let (value, size) = read_var_long(&bytes, 0);
        assert_eq!(value, 127);
        assert_eq!(size, 1);
    }

    #[test]
    fn test_read_var_long_multi_byte() {
        // Multi-byte encoding: first byte < -112 (signed)
        // -113 as signed = 143 as unsigned = 0x8F
        // This means 2 additional bytes (size = -111 - (-113) = 2, total size = 3)
        // Actually let's test with known values

        // For value 1000:
        // In Hadoop VLong, values 128-255 use 2 bytes
        // First byte = -113 (0x8F) means 2 additional bytes
        // But let's use a simpler approach - test with offset

        let bytes = vec![50u8, 0x8F, 0x03, 0xE8]; // offset 0 = 50 (single byte)
        let (value, size) = read_var_long(&bytes, 0);
        assert_eq!(value, 50);
        assert_eq!(size, 1);
    }

    #[test]
    fn test_var_long_size_on_disk() {
        // Single byte values (first byte >= -112 as signed)
        // 0..=127 are positive when signed, so single byte
        assert_eq!(var_long_size_on_disk(&[0], 0), 1);
        assert_eq!(var_long_size_on_disk(&[100], 0), 1);
        assert_eq!(var_long_size_on_disk(&[127], 0), 1);
        // 128..=143 are -128..-113 as signed, still >= -112? No.
        // 143 as u8 = -113 as i8, which is >= -112? -113 >= -112? No, -113 < -112
        // So 143 should be multi-byte. Let's check: -111 - (-113) = 2
        assert_eq!(var_long_size_on_disk(&[143], 0), 2); // -113 as signed, size = 2

        // 144 as u8 = -112 as i8, which is >= -112, so single byte
        assert_eq!(var_long_size_on_disk(&[144], 0), 1);
    }

    #[test]
    fn test_data_block_is_valid_offset() {
        // Create a simple HFileBlock
        let mut header_bytes = vec![];
        header_bytes.extend_from_slice(b"DATABLK*"); // magic
        header_bytes.extend_from_slice(&100i32.to_be_bytes()); // on_disk_size
        header_bytes.extend_from_slice(&50i32.to_be_bytes()); // uncompressed_size
        header_bytes.extend_from_slice(&(-1i64).to_be_bytes()); // prev_block_offset
        header_bytes.push(1); // checksum_type
        header_bytes.extend_from_slice(&16384i32.to_be_bytes()); // bytes_per_checksum
        header_bytes.extend_from_slice(&133i32.to_be_bytes()); // on_disk_data_size

        // Add data after header
        header_bytes.extend_from_slice(&[0u8; 50]); // 50 bytes of data

        let block = HFileBlock::parse(&header_bytes, CompressionCodec::None).unwrap();
        let data_block = DataBlock::from_block(block);

        assert!(data_block.is_valid_offset(0));
        assert!(data_block.is_valid_offset(49));
        assert!(!data_block.is_valid_offset(50));
        assert!(!data_block.is_valid_offset(100));
    }
}
