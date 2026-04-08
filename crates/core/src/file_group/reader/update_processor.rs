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

//! Mirrors `org.apache.hudi.common.table.read.UpdateProcessor`.
//!
//! Strategy interface for processing record updates during the
//! base-file-vs-log merge phase. The default implementation simply
//! passes through the merged record; more advanced implementations
//! can track stats, invoke callbacks, etc.

use crate::Result;
use super::buffered_record::BufferedRecord;
use super::read_stats::HoodieReadStats;

/// Strategy for processing record updates during merge iteration.
///
/// Mirrors Java's `UpdateProcessor<T>` interface.
///
/// Created by factory method based on merge mode and configuration.
/// Wrapped optionally in a `CallbackProcessor` for update callbacks.
pub trait UpdateProcessor: Send + Sync + std::fmt::Debug {
    /// Process an update (base record merged with log record).
    ///
    /// Returns the record to emit, or `None` to skip.
    fn process_update(
        &self,
        record_key: &str,
        previous_record: Option<&BufferedRecord>,
        merged_record: &BufferedRecord,
        is_delete: bool,
    ) -> Result<Option<BufferedRecord>>;
}

/// Default update processor: passes through the merged record as-is.
///
/// Corresponds to Java's `StandardUpdateProcessor<T>`.
#[derive(Debug)]
pub struct StandardUpdateProcessor;

impl UpdateProcessor for StandardUpdateProcessor {
    fn process_update(
        &self,
        _record_key: &str,
        _previous_record: Option<&BufferedRecord>,
        merged_record: &BufferedRecord,
        _is_delete: bool,
    ) -> Result<Option<BufferedRecord>> {
        Ok(Some(merged_record.clone()))
    }
}

/// Factory for creating `UpdateProcessor` instances.
pub fn create_update_processor(
    _read_stats: &HoodieReadStats,
    _emit_deletes: bool,
) -> Box<dyn UpdateProcessor> {
    Box::new(StandardUpdateProcessor)
}
