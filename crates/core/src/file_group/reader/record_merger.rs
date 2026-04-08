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

//! Mirrors `org.apache.hudi.common.table.read.BufferedRecordMerger` and
//! `org.apache.hudi.common.table.read.BufferedRecordMergerFactory`.
//!
//! The `BufferedRecordMerger` trait defines how records are merged during
//! log scanning (delta merge: log-vs-log) and at read time (final merge:
//! base-vs-log).

use crate::Result;
use super::buffered_record::{BufferedRecord, DeleteRecord};

/// Trait for merging buffered records during the file group read pipeline.
///
/// Mirrors Java's `BufferedRecordMerger<T>` interface.
///
/// Consumed by `KeyBasedFileGroupRecordBuffer`:
/// - `delta_merge` in `process_next_data_record` (log-vs-log within buffer)
/// - `delta_merge_delete` in `process_next_deleted_record` (delete-vs-existing)
/// - `final_merge` in `has_next_base_record` (base-vs-log at read time)
pub trait BufferedRecordMerger: Send + Sync + std::fmt::Debug {
    /// Merge a new log record with an existing buffered record.
    ///
    /// Returns `Some(merged)` if the record should be kept, `None` if dropped.
    ///
    /// Called during log scanning when a new record for the same key arrives.
    fn delta_merge(
        &self,
        new_record: &BufferedRecord,
        existing_record: Option<&BufferedRecord>,
    ) -> Result<Option<BufferedRecord>>;

    /// Merge a delete record with an existing buffered record.
    ///
    /// Returns `Some(delete)` if the delete wins, `None` if the existing record survives.
    fn delta_merge_delete(
        &self,
        delete_record: &DeleteRecord,
        existing_record: Option<&BufferedRecord>,
    ) -> Result<Option<DeleteRecord>>;

    /// Merge a base file record with a log record (final merge at read time).
    ///
    /// The `older_record` is from the base file, `newer_record` is from the log buffer.
    fn final_merge(
        &self,
        older_record: &BufferedRecord,
        newer_record: &BufferedRecord,
    ) -> Result<BufferedRecord>;
}

/// Event-time based record merger.
///
/// Resolves conflicts by comparing ordering values: the record with the
/// higher ordering value wins. This is the default merge strategy for
/// `COMMIT_TIME_ORDERING` and `EVENT_TIME_ORDERING` modes.
///
/// Created by `BufferedRecordMergerFactory` based on the merge mode.
#[derive(Debug)]
pub struct EventTimeRecordMerger;

impl BufferedRecordMerger for EventTimeRecordMerger {
    fn delta_merge(
        &self,
        new_record: &BufferedRecord,
        existing_record: Option<&BufferedRecord>,
    ) -> Result<Option<BufferedRecord>> {
        match existing_record {
            None => Ok(Some(new_record.clone())),
            Some(existing) => {
                // Compare ordering values: higher wins
                match (&new_record.ordering_value, &existing.ordering_value) {
                    (Some(new_val), Some(existing_val)) if new_val >= existing_val => {
                        Ok(Some(new_record.clone()))
                    }
                    (Some(_), Some(_)) => Ok(Some(existing.clone())),
                    // If either has no ordering value, new record wins (latest-write-wins)
                    _ => Ok(Some(new_record.clone())),
                }
            }
        }
    }

    fn delta_merge_delete(
        &self,
        delete_record: &DeleteRecord,
        existing_record: Option<&BufferedRecord>,
    ) -> Result<Option<DeleteRecord>> {
        match existing_record {
            None => Ok(Some(delete_record.clone())),
            Some(existing) => {
                match (&delete_record.ordering_value, &existing.ordering_value) {
                    (Some(del_val), Some(existing_val)) if del_val >= existing_val => {
                        Ok(Some(delete_record.clone()))
                    }
                    (Some(_), Some(_)) => Ok(None), // existing record survives
                    _ => Ok(Some(delete_record.clone())),
                }
            }
        }
    }

    fn final_merge(
        &self,
        _older_record: &BufferedRecord,
        newer_record: &BufferedRecord,
    ) -> Result<BufferedRecord> {
        // Log record always wins over base file record in final merge
        Ok(newer_record.clone())
    }
}

/// Factory for creating `BufferedRecordMerger` instances.
///
/// Mirrors Java's `BufferedRecordMergerFactory`.
pub struct BufferedRecordMergerFactory;

impl BufferedRecordMergerFactory {
    /// Create a merger based on the merge mode.
    ///
    /// Currently only supports event-time ordering. Other modes
    /// (partial update, custom payload) are placeholders.
    pub fn create(_merge_mode: &str) -> Box<dyn BufferedRecordMerger> {
        // For now, all modes use EventTimeRecordMerger
        Box::new(EventTimeRecordMerger)
    }
}
