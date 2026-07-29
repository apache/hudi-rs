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
//! Minimal uncompressed HFile v3 writer used by metadata table writes.

use std::collections::BTreeMap;

use prost::Message;

use crate::hfile::block::BLOCK_HEADER_SIZE;
use crate::hfile::block_type::HFileBlockType;
use crate::hfile::error::{HFileError, Result};
use crate::hfile::proto::{BytesBytesPair, InfoProto, TrailerProto};
use crate::hfile::trailer::TRAILER_SIZE;

const CHECKSUM_TYPE_CRC32: u8 = 2;
const BYTES_PER_CHECKSUM: usize = 16 * 1024;

/// Writes the subset of HFile v3 required for Hudi metadata table base and log blocks.
pub struct HFileWriter;

impl HFileWriter {
    /// Write sorted key/value records into a single uncompressed HFile data block.
    pub fn write(
        entries: &[(String, Vec<u8>)],
        mut file_info: BTreeMap<String, Vec<u8>>,
    ) -> Result<Vec<u8>> {
        let mut entries = entries.to_vec();
        entries.sort_by(|left, right| left.0.cmp(&right.0));

        let first_key = entries
            .first()
            .map(|(key, _)| structured_key(key))
            .unwrap_or_default();
        let last_key = entries
            .last()
            .map(|(key, _)| structured_key(key))
            .unwrap_or_default();

        let mut data = Vec::new();
        for (key, value) in &entries {
            let key = structured_key(key);
            let key_len = i32::try_from(key.len())
                .map_err(|_| HFileError::InvalidFormat("HFile key is too large".to_string()))?;
            let value_len = i32::try_from(value.len())
                .map_err(|_| HFileError::InvalidFormat("HFile value is too large".to_string()))?;
            data.extend_from_slice(&key_len.to_be_bytes());
            data.extend_from_slice(&value_len.to_be_bytes());
            data.extend_from_slice(&key);
            data.extend_from_slice(value);
            data.push(0);
        }

        let mut output = write_block(HFileBlockType::Data, &data, -1)?;
        let data_block_size = output.len();

        let mut root_index = Vec::new();
        root_index.extend_from_slice(&0i64.to_be_bytes());
        root_index.extend_from_slice(
            &u32::try_from(data_block_size)
                .map_err(|_| HFileError::InvalidFormat("HFile block is too large".to_string()))?
                .to_be_bytes(),
        );
        write_hadoop_vlong(&mut root_index, first_key.len() as u64);
        root_index.extend_from_slice(&first_key);
        let root_index_offset = output.len() as u64;
        output.extend_from_slice(&write_block(HFileBlockType::RootIndex, &root_index, 0)?);

        let meta_index_offset = output.len() as u64;
        output.extend_from_slice(&write_block(
            HFileBlockType::RootIndex,
            &[],
            root_index_offset as i64,
        )?);

        file_info.insert("hfile.LASTKEY".to_string(), last_key);
        file_info.insert("KEY_VALUE_VERSION".to_string(), 1i32.to_be_bytes().to_vec());
        file_info.insert(
            "MAX_MEMSTORE_TS_KEY".to_string(),
            0i64.to_be_bytes().to_vec(),
        );
        let info = InfoProto {
            map_entry: file_info
                .into_iter()
                .map(|(first, second)| BytesBytesPair {
                    first: first.into_bytes(),
                    second,
                })
                .collect(),
        };
        let mut info_bytes = b"PBUF".to_vec();
        let encoded_info = info.encode_to_vec();
        write_protobuf_varint(&mut info_bytes, encoded_info.len() as u64);
        info_bytes.extend_from_slice(&encoded_info);
        let file_info_offset = output.len() as u64;
        output.extend_from_slice(&write_block(
            HFileBlockType::FileInfo,
            &info_bytes,
            meta_index_offset as i64,
        )?);

        let trailer = TrailerProto {
            file_info_offset: Some(file_info_offset),
            load_on_open_data_offset: Some(root_index_offset),
            uncompressed_data_index_size: Some(root_index.len() as u64),
            total_uncompressed_bytes: Some(data.len() as u64),
            data_index_count: Some(u32::from(!entries.is_empty())),
            meta_index_count: Some(0),
            entry_count: Some(entries.len() as u64),
            num_data_index_levels: Some(1),
            first_data_block_offset: Some(0),
            last_data_block_offset: Some(0),
            comparator_class_name: Some(
                "org.apache.hadoop.hbase.KeyValue$KeyOnlyKeyValue".to_string(),
            ),
            compression_codec: Some(2),
            encryption_key: None,
        };
        let encoded_trailer = trailer.encode_to_vec();
        let mut trailer_bytes = vec![0; TRAILER_SIZE];
        trailer_bytes[..8].copy_from_slice(HFileBlockType::Trailer.magic());
        let mut length_delimited = Vec::new();
        write_protobuf_varint(&mut length_delimited, encoded_trailer.len() as u64);
        length_delimited.extend_from_slice(&encoded_trailer);
        let trailer_end = 8 + length_delimited.len();
        trailer_bytes[8..trailer_end].copy_from_slice(&length_delimited);
        trailer_bytes[TRAILER_SIZE - 4..].copy_from_slice(&[0, 0, 0, 3]);
        output.extend_from_slice(&trailer_bytes);
        Ok(output)
    }
}

fn structured_key(content: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(content.len() + 12);
    key.extend_from_slice(&(content.len() as u16).to_be_bytes());
    key.extend_from_slice(content.as_bytes());
    key.push(0);
    key.extend_from_slice(&i64::MAX.to_be_bytes());
    key.push(4);
    key
}

fn write_block(block_type: HFileBlockType, data: &[u8], previous_offset: i64) -> Result<Vec<u8>> {
    let checksum_bytes = (BLOCK_HEADER_SIZE + data.len()).div_ceil(BYTES_PER_CHECKSUM) * 4;
    let on_disk_size = data
        .len()
        .checked_add(checksum_bytes)
        .ok_or_else(|| HFileError::InvalidFormat("HFile block size overflow".to_string()))?;
    let mut block = Vec::with_capacity(BLOCK_HEADER_SIZE + on_disk_size);
    block.extend_from_slice(block_type.magic());
    block.extend_from_slice(
        &i32::try_from(on_disk_size)
            .map_err(|_| HFileError::InvalidFormat("HFile block is too large".to_string()))?
            .to_be_bytes(),
    );
    block.extend_from_slice(
        &i32::try_from(data.len())
            .map_err(|_| HFileError::InvalidFormat("HFile block is too large".to_string()))?
            .to_be_bytes(),
    );
    block.extend_from_slice(&previous_offset.to_be_bytes());
    block.push(CHECKSUM_TYPE_CRC32);
    block.extend_from_slice(&(BYTES_PER_CHECKSUM as i32).to_be_bytes());
    block.extend_from_slice(
        &i32::try_from(BLOCK_HEADER_SIZE + data.len())
            .map_err(|_| HFileError::InvalidFormat("HFile block is too large".to_string()))?
            .to_be_bytes(),
    );
    block.extend_from_slice(data);
    block.resize(BLOCK_HEADER_SIZE + on_disk_size, 0);
    Ok(block)
}

fn write_hadoop_vlong(output: &mut Vec<u8>, value: u64) {
    if value <= 127 {
        output.push(value as u8);
        return;
    }
    let bytes = value.to_be_bytes();
    let first = bytes.iter().position(|byte| *byte != 0).unwrap_or(7);
    let significant = &bytes[first..];
    output.push((-(112 + significant.len() as i8)) as u8);
    output.extend_from_slice(significant);
}

fn write_protobuf_varint(output: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        output.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hfile::HFileReader;

    #[test]
    fn test_write_roundtrip_records() {
        let bytes = HFileWriter::write(
            &[
                ("charlie".to_string(), b"3".to_vec()),
                ("alpha".to_string(), b"1".to_vec()),
                ("bravo".to_string(), b"2".to_vec()),
            ],
            BTreeMap::new(),
        )
        .expect("write hfile");
        let mut reader = HFileReader::new(bytes).expect("read hfile");
        let records = reader.collect_records().expect("collect records");
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].key_as_str(), Some("alpha"));
        assert_eq!(records[2].key_as_str(), Some("charlie"));
    }
}
