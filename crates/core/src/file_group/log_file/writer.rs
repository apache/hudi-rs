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
//! Hudi log format v1 writer.

use std::collections::HashMap;

use crate::file_group::log_file::log_block::{BlockMetadataKey, BlockType};
use crate::file_group::log_file::log_format::MAGIC;

/// Writes self-contained log format v1 blocks.
pub struct LogFileWriter;

impl LogFileWriter {
    /// Encode one V1 log block with raw content and an empty footer.
    pub fn write_log_block(
        block_type: BlockType,
        header: HashMap<BlockMetadataKey, String>,
        content: &[u8],
    ) -> Vec<u8> {
        write_log_block(block_type, header, content)
    }
}

/// Encode one V1 log block with raw content and an empty footer.
///
/// Layout matches Java `HoodieLogFormatWriter`:
/// - header `block_length` includes the trailing size long (`getLogBlockLength`)
/// - footer size long equals `block_length + MAGIC.len()` (reader subtracts magic)
pub fn write_log_block(
    block_type: BlockType,
    header: HashMap<BlockMetadataKey, String>,
    content: &[u8],
) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&1u32.to_be_bytes());
    payload.extend_from_slice(&(block_type as u32).to_be_bytes());
    write_metadata(&mut payload, header);
    payload.extend_from_slice(&(content.len() as u64).to_be_bytes());
    payload.extend_from_slice(content);
    payload.extend_from_slice(&0u32.to_be_bytes());

    // Java includes the trailing size long in the header length.
    let block_length = (payload.len() + 8) as u64;
    let footer_length = block_length + MAGIC.len() as u64;
    let mut output = Vec::with_capacity(MAGIC.len() + 8 + payload.len() + 8);
    output.extend_from_slice(MAGIC);
    output.extend_from_slice(&block_length.to_be_bytes());
    output.extend_from_slice(&payload);
    output.extend_from_slice(&footer_length.to_be_bytes());
    output
}

fn write_metadata(output: &mut Vec<u8>, metadata: HashMap<BlockMetadataKey, String>) {
    output.extend_from_slice(&(metadata.len() as u32).to_be_bytes());
    for (key, value) in metadata {
        output.extend_from_slice(&(key as u32).to_be_bytes());
        output.extend_from_slice(&(value.len() as u32).to_be_bytes());
        output.extend_from_slice(value.as_bytes());
    }
}
