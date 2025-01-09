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

use crate::error::CoreError;
use crate::file_group::log_file::log_block::{
    BlockMetadataKey, BlockMetadataType, BlockType, LogBlock,
};
use crate::file_group::log_file::log_format::{LogFormatVersion, MAGIC};
use crate::storage::reader::StorageReader;
use crate::storage::Storage;
use crate::Result;
use bytes::BytesMut;
use std::collections::HashMap;
use std::io::{self, Read, Seek};
use std::sync::Arc;

pub const DEFAULT_BUFFER_SIZE: usize = 16 * 1024 * 1024;

#[allow(dead_code)]
#[derive(Debug)]
pub struct LogFileReader<R> {
    storage: Arc<Storage>,
    reader: R,
    buffer: BytesMut,
}

impl LogFileReader<StorageReader> {
    pub async fn new(storage: Arc<Storage>, relative_path: &str) -> Result<Self> {
        let reader = storage.get_storage_reader(relative_path).await?;
        Ok(Self {
            storage,
            reader,
            buffer: BytesMut::with_capacity(DEFAULT_BUFFER_SIZE),
        })
    }

    pub fn read_all_blocks(mut self) -> Result<Vec<LogBlock>> {
        let mut blocks = Vec::new();
        while let Some(block) = self.read_next_block()? {
            blocks.push(block);
        }
        Ok(blocks)
    }
}

impl<R: Read + Seek> LogFileReader<R> {
    /// Read [`MAGIC`] from the log file.
    ///
    /// Returns `Ok(true)` if the magic bytes are read successfully.
    ///
    /// Returns `Ok(false)` if the end of the file is reached.
    ///
    /// Returns an error if the magic bytes are invalid or an I/O error occurs.
    fn read_magic(&mut self) -> Result<bool> {
        let mut magic = [0u8; 6];
        match self.reader.read_exact(&mut magic) {
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
    fn read_block_length(&mut self) -> Result<u64> {
        let mut size_buf = [0u8; 8];
        self.reader.read_exact(&mut size_buf)?;
        Ok(u64::from_be_bytes(size_buf))
    }

    fn create_corrupted_block_if_needed(
        &mut self,
        _curent_pos: u64,
        _block_length: Option<u64>,
    ) -> Option<LogBlock> {
        // TODO: support creating corrupted block
        None
    }

    fn read_block_length_or_corrupted_block(
        &mut self,
        start_pos: u64,
    ) -> Result<(u64, Option<LogBlock>)> {
        match self.read_block_length() {
            Ok(length) => {
                if let Some(block) = self.create_corrupted_block_if_needed(start_pos, Some(length))
                {
                    Ok((0, Some(block)))
                } else {
                    Ok((length, None))
                }
            }
            Err(e) => Err(e),
        }
    }

    /// Read 4 bytes for [`LogFormatVersion`].
    fn read_format_version(&mut self) -> Result<LogFormatVersion> {
        let mut version_buf = [0u8; 4];
        self.reader.read_exact(&mut version_buf)?;
        LogFormatVersion::try_from(version_buf)
    }

    /// Read 4 bytes for [`BlockType`].
    fn read_block_type(&mut self, format_version: &LogFormatVersion) -> Result<BlockType> {
        if !format_version.has_block_type() {
            return Err(CoreError::LogFormatError(
                "Block type is not supported".to_string(),
            ));
        }
        let mut type_buf = [0u8; 4];
        self.reader.read_exact(&mut type_buf)?;
        BlockType::try_from(type_buf)
    }

    /// First, read 4 bytes for the number of entries in the metadata section (header or footer).
    ///
    /// Then for each entry,
    /// 1. Read 4 bytes for the key
    /// 2. Read 4 bytes for the value's length
    /// 3. Read the bytes of the length for the value
    ///
    /// See also [`BlockMetadataKey`].
    fn read_block_metadata(
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
        self.reader.read_exact(&mut num_entries_buf)?;
        let num_entries = u32::from_be_bytes(num_entries_buf);
        let mut metadata: HashMap<BlockMetadataKey, String> =
            HashMap::with_capacity(num_entries as usize);
        for _ in 0..num_entries {
            let mut key_buf = [0u8; 4];
            self.reader.read_exact(&mut key_buf)?;
            let key = BlockMetadataKey::try_from(key_buf)?;
            let mut value_len_buf = [0u8; 4];
            self.reader.read_exact(&mut value_len_buf)?;
            let value_len = u32::from_be_bytes(value_len_buf);
            let mut value_buf = vec![0u8; value_len as usize];
            self.reader.read_exact(&mut value_buf)?;
            let value =
                String::from_utf8(value_buf).map_err(|e| CoreError::Utf8Error(e.utf8_error()))?;
            metadata.insert(key, value);
        }
        Ok(metadata)
    }

    /// Read the content of the log block.
    fn read_content(
        &mut self,
        format_version: &LogFormatVersion,
        fallback_length: u64,
    ) -> Result<Vec<u8>> {
        let content_length = if format_version.has_content_length() {
            let mut content_length_buf = [0u8; 8];
            self.reader.read_exact(&mut content_length_buf)?;
            u64::from_be_bytes(content_length_buf)
        } else {
            fallback_length
        };
        let mut content_buf = vec![0u8; content_length as usize];
        self.reader.read_exact(&mut content_buf)?;
        Ok(content_buf)
    }

    /// Read 8 bytes for the total length of the log block.
    fn read_total_block_length(
        &mut self,
        format_version: &LogFormatVersion,
    ) -> Result<Option<u64>> {
        if !format_version.has_total_log_block_length() {
            return Ok(None);
        }
        let mut size_buf = [0u8; 8];
        self.reader.read_exact(&mut size_buf)?;
        Ok(Some(u64::from_be_bytes(size_buf)))
    }

    fn read_next_block(&mut self) -> Result<Option<LogBlock>> {
        if !self.read_magic()? {
            return Ok(None);
        }

        let curr_pos = self
            .reader
            .stream_position()
            .map_err(CoreError::ReadLogFileError)?;

        let (block_length, _) = self.read_block_length_or_corrupted_block(curr_pos)?;
        let format_version = self.read_format_version()?;
        let block_type = self.read_block_type(&format_version)?;
        let header = self.read_block_metadata(BlockMetadataType::Header, &format_version)?;
        let content = self.read_content(&format_version, block_length)?;
        let record_batches = LogBlock::decode_content(&block_type, content)?;
        let footer = self.read_block_metadata(BlockMetadataType::Footer, &format_version)?;
        let _ = self.read_total_block_length(&format_version)?;

        Ok(Some(LogBlock {
            format_version,
            block_type,
            header,
            record_batches,
            footer,
        }))
    }
}

impl<R: Read + Seek> Iterator for LogFileReader<R> {
    type Item = Result<LogBlock>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.read_next_block() {
            Ok(Some(block)) => Some(Ok(block)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::canonicalize;
    use std::path::PathBuf;
    use url::Url;

    fn get_sample_log_file() -> (PathBuf, String) {
        let dir = PathBuf::from("tests/data/log_files/valid_log_parquet");
        (
            canonicalize(dir).unwrap(),
            ".ee2ace10-7667-40f5-9848-0a144b5ea064-0_20250113230302428.log.1_0-188-387".to_string(),
        )
    }

    #[tokio::test]
    async fn test_read_sample_log_file() {
        let (dir, file_name) = get_sample_log_file();
        let dir_url = Url::from_directory_path(dir).unwrap();
        let storage = Storage::new_with_base_url(dir_url).unwrap();
        let reader = LogFileReader::new(storage, &file_name).await.unwrap();
        let blocks = reader.read_all_blocks().unwrap();
        assert_eq!(blocks.len(), 1);

        let block = &blocks[0];
        assert_eq!(block.format_version, LogFormatVersion::V1);
        assert_eq!(block.block_type, BlockType::ParquetData);
        assert_eq!(block.header.len(), 2);
        assert_eq!(
            block.header.get(&BlockMetadataKey::InstantTime).unwrap(),
            "20250113230424191"
        );
        assert!(block.header.contains_key(&BlockMetadataKey::Schema));
        assert_eq!(block.footer.len(), 0);

        let batches = block.record_batches.as_slice();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }
}
