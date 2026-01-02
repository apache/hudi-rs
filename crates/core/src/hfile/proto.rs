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
//! Protobuf message definitions for HFile format.
//!
//! These are manually defined to match the HBase HFile protobuf schema.
//! The definitions correspond to:
//! - TrailerProto: HFile trailer metadata
//! - InfoProto/BytesBytesPair: File info key-value pairs

use prost::Message;

/// HFile trailer protobuf message.
///
/// Contains metadata about the HFile structure including offsets,
/// counts, and compression settings.
#[derive(Clone, PartialEq, Message)]
pub struct TrailerProto {
    #[prost(uint64, optional, tag = "1")]
    pub file_info_offset: Option<u64>,

    #[prost(uint64, optional, tag = "2")]
    pub load_on_open_data_offset: Option<u64>,

    #[prost(uint64, optional, tag = "3")]
    pub uncompressed_data_index_size: Option<u64>,

    #[prost(uint64, optional, tag = "4")]
    pub total_uncompressed_bytes: Option<u64>,

    #[prost(uint32, optional, tag = "5")]
    pub data_index_count: Option<u32>,

    #[prost(uint32, optional, tag = "6")]
    pub meta_index_count: Option<u32>,

    #[prost(uint64, optional, tag = "7")]
    pub entry_count: Option<u64>,

    #[prost(uint32, optional, tag = "8")]
    pub num_data_index_levels: Option<u32>,

    #[prost(uint64, optional, tag = "9")]
    pub first_data_block_offset: Option<u64>,

    #[prost(uint64, optional, tag = "10")]
    pub last_data_block_offset: Option<u64>,

    #[prost(string, optional, tag = "11")]
    pub comparator_class_name: Option<String>,

    #[prost(uint32, optional, tag = "12")]
    pub compression_codec: Option<u32>,

    #[prost(bytes, optional, tag = "13")]
    pub encryption_key: Option<Vec<u8>>,
}

/// A key-value pair with both key and value as byte arrays.
#[derive(Clone, PartialEq, Message)]
pub struct BytesBytesPair {
    #[prost(bytes, required, tag = "1")]
    pub first: Vec<u8>,

    #[prost(bytes, required, tag = "2")]
    pub second: Vec<u8>,
}

/// File info protobuf message containing a list of key-value pairs.
#[derive(Clone, PartialEq, Message)]
pub struct InfoProto {
    #[prost(message, repeated, tag = "1")]
    pub map_entry: Vec<BytesBytesPair>,
}
