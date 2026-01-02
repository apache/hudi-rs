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
//! HFile trailer parsing.

use crate::hfile::block_type::{HFileBlockType, MAGIC_LENGTH};
use crate::hfile::compression::CompressionCodec;
use crate::hfile::error::{HFileError, Result};
use crate::hfile::proto::TrailerProto;
use prost::Message;

/// HFile trailer size (fixed at 4096 bytes for HFile v3)
pub const TRAILER_SIZE: usize = 4096;

/// HFile trailer containing file metadata.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct HFileTrailer {
    /// Major version (should be 3 for HFile v3)
    pub major_version: u32,
    /// Minor version
    pub minor_version: u32,
    /// Offset to file info block
    pub file_info_offset: u64,
    /// Offset to load-on-open section
    pub load_on_open_data_offset: u64,
    /// Total uncompressed size of data block index
    pub uncompressed_data_index_size: u64,
    /// Total uncompressed bytes in file
    pub total_uncompressed_bytes: u64,
    /// Number of entries in data block index
    pub data_index_count: u32,
    /// Number of entries in meta block index
    pub meta_index_count: u32,
    /// Number of key-value entries in file
    pub entry_count: u64,
    /// Number of levels in data block index
    pub num_data_index_levels: u32,
    /// Offset to first data block
    pub first_data_block_offset: u64,
    /// Offset past the last data block
    pub last_data_block_offset: u64,
    /// Comparator class name (not used by Hudi)
    pub comparator_class_name: String,
    /// Compression codec used for blocks
    pub compression_codec: CompressionCodec,
}

impl HFileTrailer {
    /// Read and parse the trailer from file bytes.
    ///
    /// The trailer is always at the end of the file with fixed size.
    pub fn read(file_bytes: &[u8]) -> Result<Self> {
        let file_size = file_bytes.len();
        if file_size < TRAILER_SIZE {
            return Err(HFileError::InvalidFormat(format!(
                "File too small for HFile trailer: {} bytes, need at least {}",
                file_size, TRAILER_SIZE
            )));
        }

        let trailer_start = file_size - TRAILER_SIZE;
        let trailer_bytes = &file_bytes[trailer_start..];

        // Verify trailer magic
        HFileBlockType::Trailer.check_magic(trailer_bytes)?;

        // Read version from last 4 bytes
        // Format: [minor_version (1 byte)] [major_version (3 bytes)]
        let version_bytes = &trailer_bytes[TRAILER_SIZE - 4..];
        let minor_version = version_bytes[0] as u32;
        let major_version = ((version_bytes[1] as u32) << 16)
            | ((version_bytes[2] as u32) << 8)
            | (version_bytes[3] as u32);

        if major_version != 3 {
            return Err(HFileError::UnsupportedVersion {
                major: major_version,
                minor: minor_version,
            });
        }

        // Parse protobuf content (after magic, before version)
        let proto_start = MAGIC_LENGTH;
        let proto_end = TRAILER_SIZE - 4;
        let proto_bytes = &trailer_bytes[proto_start..proto_end];

        // Protobuf is length-delimited
        let proto = Self::parse_length_delimited_proto(proto_bytes)?;

        let compression_codec = proto
            .compression_codec
            .map(CompressionCodec::from_id)
            .transpose()?
            .unwrap_or_default();

        Ok(Self {
            major_version,
            minor_version,
            file_info_offset: proto.file_info_offset.unwrap_or(0),
            load_on_open_data_offset: proto.load_on_open_data_offset.unwrap_or(0),
            uncompressed_data_index_size: proto.uncompressed_data_index_size.unwrap_or(0),
            total_uncompressed_bytes: proto.total_uncompressed_bytes.unwrap_or(0),
            data_index_count: proto.data_index_count.unwrap_or(0),
            meta_index_count: proto.meta_index_count.unwrap_or(0),
            entry_count: proto.entry_count.unwrap_or(0),
            num_data_index_levels: proto.num_data_index_levels.unwrap_or(1),
            first_data_block_offset: proto.first_data_block_offset.unwrap_or(0),
            last_data_block_offset: proto.last_data_block_offset.unwrap_or(0),
            comparator_class_name: proto.comparator_class_name.unwrap_or_default(),
            compression_codec,
        })
    }

    /// Parse a length-delimited protobuf message.
    fn parse_length_delimited_proto(bytes: &[u8]) -> Result<TrailerProto> {
        // Read varint length
        let (length, consumed) = read_varint(bytes);
        let proto_bytes = &bytes[consumed..consumed + length as usize];
        TrailerProto::decode(proto_bytes).map_err(HFileError::from)
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
