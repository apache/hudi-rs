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
//! The error and result types returned by hudi-core APIs.

use crate::config::error::ConfigError;
use crate::storage::error::StorageError;
use thiserror::Error;

/// Result type of hudi-core APIs, failing with [`CoreError`] by default.
pub type Result<T, E = CoreError> = std::result::Result<T, E>;

/// Errors returned by hudi-core APIs.
#[derive(Error, Debug)]
pub enum CoreError {
    /// An Arrow operation failed (schema mismatch, cast, or batch construction).
    #[error(transparent)]
    ArrowError(#[from] arrow::error::ArrowError),

    /// Avro encoding or decoding failed.
    #[error(transparent)]
    AvroError(#[from] apache_avro::Error),

    /// A config is missing or has an invalid value.
    #[error("Config error: {0}")]
    Config(#[from] ConfigError),

    /// Commit metadata on the timeline could not be parsed or lacks a required field.
    #[error("Commit metadata error: {0}")]
    CommitMetadata(String),

    /// Records could not be merged.
    #[error("{0}")]
    MergeRecordError(String),

    /// The merge map exceeded its configured peak-memory ceiling.
    #[error("{0}")]
    MemoryLimitExceeded(String),

    /// A schema or data type is invalid for the operation, such as a filter on an unknown
    /// column or a write whose columns do not match the table.
    #[error("Data type error: {0}")]
    Schema(String),

    /// The table schema could not be resolved.
    #[error("{0}")]
    SchemaNotFound(String),

    /// A file group, file slice, or data file name is invalid.
    #[error("File group error: {0}")]
    FileGroup(String),

    /// Reading or merging a file slice failed.
    #[error("{0}")]
    ReadFileSliceError(String),

    /// A log file does not follow the Hudi log format.
    #[error("{0}")]
    LogFormatError(String),

    /// A log block could not be decoded.
    #[error("{0}")]
    LogBlockError(String),

    /// Reading or writing an HFile failed.
    #[error("HFile error: {0}")]
    HFile(String),

    /// Reading or writing the metadata table failed.
    #[error("Metadata table error: {0}")]
    MetadataTable(String),

    /// A partition path does not match the table's partition fields.
    #[error("{0}")]
    InvalidPartitionPath(String),

    /// An argument or stored value is invalid.
    #[error("{0}")]
    InvalidValue(String),

    /// Reading or writing a Parquet file failed.
    #[error(transparent)]
    ParquetError(#[from] parquet::errors::ParquetError),

    /// An I/O error, raised mostly while reading log files.
    #[error("{0}")]
    ReadLogFileError(#[from] std::io::Error),

    /// An object store operation failed.
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    /// The timeline could not be loaded or holds an invalid instant.
    #[error("Timeline error: {0}")]
    Timeline(String),

    /// The timeline has no completed commit.
    #[error("Timeline has no completed commit.")]
    TimelineNoCommit,

    /// A timestamp string could not be parsed.
    #[error("{0}")]
    TimestampParsingError(String),

    /// The table, config, or operation is not supported.
    #[error("{0}")]
    Unsupported(String),

    /// A write was rejected or failed.
    #[error("Write error: {0}")]
    Write(String),

    /// Bytes expected to be UTF-8 were not.
    #[error(transparent)]
    Utf8Error(#[from] std::str::Utf8Error),
}
