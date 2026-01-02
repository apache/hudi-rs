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
//! Compression codec support for HFile blocks.

use crate::hfile::error::{HFileError, Result};
use flate2::read::GzDecoder;
use std::io::Read;

/// Compression codec IDs used in HFile.
/// These IDs are stored in the HFile trailer and must not change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompressionCodec {
    /// LZO compression (ID: 0)
    Lzo = 0,
    /// GZIP compression (ID: 1)
    Gzip = 1,
    /// No compression (ID: 2)
    #[default]
    None = 2,
    /// Snappy compression (ID: 3)
    Snappy = 3,
    /// LZ4 compression (ID: 4)
    Lz4 = 4,
    /// BZIP2 compression (ID: 5)
    Bzip2 = 5,
    /// ZSTD compression (ID: 6)
    Zstd = 6,
}

impl CompressionCodec {
    /// Decode compression codec from ID stored in HFile.
    pub fn from_id(id: u32) -> Result<Self> {
        match id {
            0 => Ok(CompressionCodec::Lzo),
            1 => Ok(CompressionCodec::Gzip),
            2 => Ok(CompressionCodec::None),
            3 => Ok(CompressionCodec::Snappy),
            4 => Ok(CompressionCodec::Lz4),
            5 => Ok(CompressionCodec::Bzip2),
            6 => Ok(CompressionCodec::Zstd),
            _ => Err(HFileError::UnsupportedCompression(id)),
        }
    }

    /// Decompress data using this codec.
    ///
    /// # Arguments
    /// * `compressed_data` - The compressed data bytes
    /// * `uncompressed_size` - Expected size of uncompressed data
    ///
    /// # Returns
    /// Decompressed data as a byte vector
    pub fn decompress(&self, compressed_data: &[u8], uncompressed_size: usize) -> Result<Vec<u8>> {
        match self {
            CompressionCodec::None => Ok(compressed_data.to_vec()),
            CompressionCodec::Gzip => {
                let mut decoder = GzDecoder::new(compressed_data);
                let mut decompressed = Vec::with_capacity(uncompressed_size);
                decoder.read_to_end(&mut decompressed).map_err(|e| {
                    HFileError::DecompressionError(format!("GZIP decompression failed: {}", e))
                })?;
                Ok(decompressed)
            }
            CompressionCodec::Lzo => Err(HFileError::DecompressionError(
                "LZO compression not yet supported".to_string(),
            )),
            CompressionCodec::Snappy => Err(HFileError::DecompressionError(
                "Snappy compression not yet supported".to_string(),
            )),
            CompressionCodec::Lz4 => Err(HFileError::DecompressionError(
                "LZ4 compression not yet supported".to_string(),
            )),
            CompressionCodec::Bzip2 => Err(HFileError::DecompressionError(
                "BZIP2 compression not yet supported".to_string(),
            )),
            CompressionCodec::Zstd => Err(HFileError::DecompressionError(
                "ZSTD compression not yet supported".to_string(),
            )),
        }
    }
}
