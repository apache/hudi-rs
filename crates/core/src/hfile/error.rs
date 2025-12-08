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
//! HFile error types.

use std::io;
use thiserror::Error;

pub type Result<T, E = HFileError> = std::result::Result<T, E>;

#[derive(Error, Debug)]
pub enum HFileError {
    #[error("Invalid HFile: {0}")]
    InvalidFormat(String),

    #[error("Invalid block magic: expected {expected}, got {actual}")]
    InvalidBlockMagic { expected: String, actual: String },

    #[error("Unsupported HFile version: major={major}, minor={minor}")]
    UnsupportedVersion { major: u32, minor: u32 },

    #[error("Unsupported compression codec: {0}")]
    UnsupportedCompression(u32),

    #[error("HFiles with MVCC timestamps are not supported")]
    UnsupportedMvccTimestamp,

    #[error("Decompression error: {0}")]
    DecompressionError(String),

    #[error("Protobuf decode error: {0}")]
    ProtobufError(#[from] prost::DecodeError),

    #[error("IO error: {0}")]
    IoError(#[from] io::Error),

    #[error("Unexpected block type: expected {expected}, got {actual}")]
    UnexpectedBlockType { expected: String, actual: String },

    #[error("Backward seek not supported: current position is ahead of lookup key")]
    BackwardSeekNotSupported,

    #[error("Not seeked: must call seek_to() before reading key-value pairs")]
    NotSeeked,

    #[error("End of file reached")]
    Eof,
}
