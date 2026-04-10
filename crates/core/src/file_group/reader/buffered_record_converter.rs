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

//! Mirrors `org.apache.hudi.common.table.read.BufferedRecordConverter<T>`.
//!
//! Converts engine-specific records (`RecordBatch` in Rust / `T` in Java)
//! into [`BufferedRecord`] according to different [`IteratorMode`]s.

use crate::Result;
use crate::file_group::reader::buffered_record::BufferedRecord;
use arrow_array::RecordBatch;

/// Converter from engine records to [`BufferedRecord`].
///
/// Mirrors Java's `BufferedRecordConverter<T>` functional interface.
///
/// Implementations differ by [`super::iterator_mode::IteratorMode`]:
/// - `EngineRecord`: wraps the record directly (reuses a `BufferedRecord` envelope).
/// - `RecordKey`: extracts only the record key.
/// - `HoodieRecord`: full extraction with ordering value.
pub trait BufferedRecordConverter: Send + Sync + std::fmt::Debug {
    /// Convert an engine record into a [`BufferedRecord`].
    ///
    /// Mirrors Java's `BufferedRecord<T> convert(T record)`.
    fn convert(&self, record: RecordBatch) -> Result<BufferedRecord>;
}
