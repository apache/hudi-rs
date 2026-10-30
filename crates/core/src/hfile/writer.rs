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
        file_info: BTreeMap<String, Vec<u8>>,
    ) -> Result<Vec<u8>> {
        let mut entries: Vec<&(String, Vec<u8>)> = entries.iter().collect();
        entries.sort_by(|left, right| left.0.cmp(&right.0));
        let mut builder = HFileBuilder::new(usize::MAX);
        for (key, value) in entries {
            builder.append(key.as_bytes(), value)?;
        }
        builder.finish(file_info)
    }
}

/// Builds an uncompressed HFile v3 from cells appended in key order, cutting a
/// new data block once the current one reaches the block size.
///
/// Cells are taken as key content and value slices, so a caller holding a cell
/// read from another HFile writes it without decoding the value.
pub struct HFileBuilder {
    block_size: usize,
    output: Vec<u8>,
    /// The data block being filled.
    data: Vec<u8>,
    /// Root index entries: block offset, on-disk size and first key.
    index: Vec<(u64, u32, Vec<u8>)>,
    first_key_of_block: Option<Vec<u8>>,
    last_key_content: Vec<u8>,
    last_data_block_offset: i64,
    total_uncompressed_bytes: u64,
    entry_count: u64,
}

impl HFileBuilder {
    /// A builder cutting data blocks at `block_size` bytes of cell data.
    pub fn new(block_size: usize) -> Self {
        Self {
            block_size: block_size.max(1),
            output: Vec::new(),
            data: Vec::new(),
            index: Vec::new(),
            first_key_of_block: None,
            last_key_content: Vec::new(),
            last_data_block_offset: -1,
            total_uncompressed_bytes: 0,
            entry_count: 0,
        }
    }

    /// Append one cell. Keys must arrive in ascending byte order, which is the
    /// order HFile readers seek by.
    pub fn append(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.entry_count > 0 && key < self.last_key_content.as_slice() {
            return Err(HFileError::InvalidFormat(format!(
                "HFile keys must be appended in ascending order: '{}' after '{}'",
                String::from_utf8_lossy(key),
                String::from_utf8_lossy(&self.last_key_content)
            )));
        }
        if self.first_key_of_block.is_none() {
            self.first_key_of_block = Some(structured_key(key)?);
        }
        let key_len = i32::try_from(key.len() + STRUCTURED_KEY_OVERHEAD)
            .map_err(|_| HFileError::InvalidFormat("HFile key is too large".to_string()))?;
        let value_len = i32::try_from(value.len())
            .map_err(|_| HFileError::InvalidFormat("HFile value is too large".to_string()))?;
        self.data.reserve(key.len() + value.len() + 21);
        self.data.extend_from_slice(&key_len.to_be_bytes());
        self.data.extend_from_slice(&value_len.to_be_bytes());
        push_structured_key(&mut self.data, key)?;
        self.data.extend_from_slice(value);
        self.data.push(0);
        self.last_key_content.clear();
        self.last_key_content.extend_from_slice(key);
        self.entry_count += 1;
        if self.data.len() >= self.block_size {
            self.flush_data_block()?;
        }
        Ok(())
    }

    /// Number of cells appended so far.
    pub fn entry_count(&self) -> u64 {
        self.entry_count
    }

    fn flush_data_block(&mut self) -> Result<()> {
        let Some(first_key) = self.first_key_of_block.take() else {
            return Ok(());
        };
        let offset = self.output.len() as u64;
        let block = write_block(
            HFileBlockType::Data,
            &self.data,
            self.last_data_block_offset,
        )?;
        let on_disk_size = u32::try_from(block.len())
            .map_err(|_| HFileError::InvalidFormat("HFile block is too large".to_string()))?;
        self.output.extend_from_slice(&block);
        self.total_uncompressed_bytes += self.data.len() as u64;
        self.index.push((offset, on_disk_size, first_key));
        self.last_data_block_offset = offset as i64;
        self.data.clear();
        Ok(())
    }

    /// Write the index, file info and trailer, returning the file's bytes.
    pub fn finish(mut self, mut file_info: BTreeMap<String, Vec<u8>>) -> Result<Vec<u8>> {
        self.flush_data_block()?;
        let mut output = std::mem::take(&mut self.output);
        // An empty file still carries a single empty data block, as readers
        // expect a data block before the load-on-open section.
        if self.index.is_empty() {
            output = write_block(HFileBlockType::Data, &[], -1)?;
        }
        let last_key = if self.entry_count > 0 {
            structured_key(&self.last_key_content)?
        } else {
            Vec::new()
        };

        let mut root_index = Vec::new();
        if self.index.is_empty() {
            root_index.extend_from_slice(&0i64.to_be_bytes());
            root_index.extend_from_slice(
                &u32::try_from(output.len())
                    .map_err(|_| HFileError::InvalidFormat("HFile block is too large".to_string()))?
                    .to_be_bytes(),
            );
            write_hadoop_vlong(&mut root_index, 0);
        }
        for (offset, size, first_key) in &self.index {
            root_index.extend_from_slice(&(*offset as i64).to_be_bytes());
            root_index.extend_from_slice(&size.to_be_bytes());
            write_hadoop_vlong(&mut root_index, first_key.len() as u64);
            root_index.extend_from_slice(first_key);
        }
        let root_index_offset = output.len() as u64;
        output.extend_from_slice(&write_block(
            HFileBlockType::RootIndex,
            &root_index,
            self.last_data_block_offset.max(0),
        )?);

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
            total_uncompressed_bytes: Some(self.total_uncompressed_bytes),
            data_index_count: Some(self.index.len() as u32),
            meta_index_count: Some(0),
            entry_count: Some(self.entry_count),
            num_data_index_levels: Some(1),
            first_data_block_offset: Some(0),
            last_data_block_offset: Some(self.last_data_block_offset.max(0) as u64),
            // Java's native HFile writer; an HBase-backed reader instantiates it.
            comparator_class_name: Some(
                "org.apache.hudi.io.storage.HoodieHBaseKVComparator".to_string(),
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

/// Bytes a structured key adds around its content: the u16 length, the
/// family terminator, the i64 timestamp and the type byte.
const STRUCTURED_KEY_OVERHEAD: usize = 2 + 1 + 8 + 1;

fn structured_key(content: &[u8]) -> crate::hfile::error::Result<Vec<u8>> {
    let mut key = Vec::with_capacity(content.len() + STRUCTURED_KEY_OVERHEAD);
    push_structured_key(&mut key, content)?;
    Ok(key)
}

/// Appends the structured key for `content` to `out`, so a data block is built
/// without a buffer per entry.
fn push_structured_key(out: &mut Vec<u8>, content: &[u8]) -> crate::hfile::error::Result<()> {
    // The KeyValue row-length field is u16; truncating would corrupt framing.
    let len = u16::try_from(content.len()).map_err(|_| {
        crate::hfile::error::HFileError::InvalidFormat(format!(
            "HFile row key exceeds 64KiB: {} bytes",
            content.len()
        ))
    })?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(content);
    out.push(0);
    out.extend_from_slice(&i64::MAX.to_be_bytes());
    out.push(4);
    Ok(())
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

    /// Cells spread over several data blocks read back in order, and a point
    /// lookup lands in the right block.
    #[test]
    fn test_builder_writes_multiple_data_blocks() {
        let mut builder = HFileBuilder::new(64);
        let keys: Vec<String> = (0..50).map(|i| format!("key{i:03}")).collect();
        for key in &keys {
            builder
                .append(key.as_bytes(), format!("value-{key}").as_bytes())
                .unwrap();
        }
        let bytes = builder
            .finish(BTreeMap::from([("schema".to_string(), b"s".to_vec())]))
            .unwrap();
        let trailer = crate::hfile::trailer::HFileTrailer::read(&bytes).unwrap();
        assert!(trailer.data_index_count > 1, "{}", trailer.data_index_count);
        assert_eq!(trailer.entry_count, 50);
        let mut reader = HFileReader::new(bytes).unwrap();
        let records = reader.collect_records().unwrap();
        assert_eq!(
            records
                .iter()
                .map(|r| r.key_as_str().unwrap().to_string())
                .collect::<Vec<_>>(),
            keys
        );
        let found = reader.lookup_records(&["key037"]).unwrap();
        assert_eq!(found[0].1.as_ref().unwrap().value(), b"value-key037");
    }

    #[test]
    fn test_builder_rejects_descending_keys() {
        let mut builder = HFileBuilder::new(1024);
        builder.append(b"b", b"1").unwrap();
        assert!(builder.append(b"a", b"2").is_err());
    }

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
        let mut reader = HFileReader::new(bytes.clone()).expect("read hfile");
        let records = reader.collect_records().expect("collect records");
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].key_as_str(), Some("alpha"));
        assert_eq!(records[2].key_as_str(), Some("charlie"));
        assert_eq!(
            crate::hfile::trailer::HFileTrailer::read(&bytes)
                .expect("read trailer")
                .comparator_class_name,
            "org.apache.hudi.io.storage.HoodieHBaseKVComparator"
        );
    }
}
