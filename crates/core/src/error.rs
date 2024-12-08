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
use crate::config::error::ConfigError;
use crate::storage::error::StorageError;
use thiserror::Error;

pub type Result<T, E = CoreError> = std::result::Result<T, E>;

#[derive(Error, Debug)]
pub enum CoreError {
    #[error(transparent)]
    ArrowError(#[from] arrow::error::ArrowError),

    #[error("Config error: {0}")]
    Config(#[from] ConfigError),

    #[error("Data type error: {0}")]
    DataType(String),

    #[error("File group error: {0}")]
    FileGroup(String),

    #[error("{0}: {1:?}")]
    ReadFileSliceError(String, StorageError),

    #[error("{0}")]
    InvalidPartitionPath(String),

    #[error(transparent)]
    ParquetError(#[from] parquet::errors::ParquetError),

    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("Timeline error: {0}")]
    Timeline(String),

    #[error("{0}")]
    Unsupported(String),

    #[error(transparent)]
    Utf8Error(#[from] std::str::Utf8Error),
}
