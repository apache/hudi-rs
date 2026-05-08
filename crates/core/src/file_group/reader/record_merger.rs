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

use super::buffered_record::{BufferedRecord, DeleteRecord};
use crate::Result;

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

    /// Mirrors Java's `shouldKeepNewerRecord(olderRecord, newerRecord)`.
    ///
    /// Compares ordering values: higher wins. If either has no ordering value,
    /// the newer (log) record wins.
    ///
    /// TODO: Java has special handling for `isCommitTimeOrderingDelete()` where
    /// DELETE records always win regardless of ordering value. Not yet implemented.
    fn final_merge(
        &self,
        older_record: &BufferedRecord,
        newer_record: &BufferedRecord,
    ) -> Result<BufferedRecord> {
        match (&newer_record.ordering_value, &older_record.ordering_value) {
            (Some(new_val), Some(old_val)) if new_val >= old_val => Ok(newer_record.clone()),
            (Some(_), Some(_)) => Ok(older_record.clone()),
            // If either has no ordering value, newer (log) record wins
            _ => Ok(newer_record.clone()),
        }
    }
}

/// Commit-time based record merger.
///
/// Mirrors Java's `CommitTimeRecordMerger`. In this mode, the most recently
/// written record always wins — no ordering value comparison is performed.
///
/// Semantics:
/// - `delta_merge`: new record always wins (unconditional overwrite)
/// - `delta_merge_delete`: delete always wins (unconditional)
/// - `final_merge`: log record always wins over base record
#[derive(Debug)]
pub struct CommitTimeRecordMerger;

impl BufferedRecordMerger for CommitTimeRecordMerger {
    fn delta_merge(
        &self,
        new_record: &BufferedRecord,
        _existing_record: Option<&BufferedRecord>,
    ) -> Result<Option<BufferedRecord>> {
        // In commit time ordering, last writer always wins — no ordering comparison
        Ok(Some(new_record.clone()))
    }

    fn delta_merge_delete(
        &self,
        delete_record: &DeleteRecord,
        _existing_record: Option<&BufferedRecord>,
    ) -> Result<Option<DeleteRecord>> {
        // Delete always wins in commit time ordering
        Ok(Some(delete_record.clone()))
    }

    fn final_merge(
        &self,
        _older_record: &BufferedRecord,
        newer_record: &BufferedRecord,
    ) -> Result<BufferedRecord> {
        // Log record always wins over base file record
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
    /// - `"COMMIT_TIME_ORDERING"` → `CommitTimeRecordMerger` (last writer wins)
    /// - `"EVENT_TIME_ORDERING"` → `EventTimeRecordMerger` (ordering value comparison)
    /// - Others → Error (CUSTOM merge mode not yet supported)
    pub fn create(merge_mode: &str) -> Result<Box<dyn BufferedRecordMerger>> {
        match merge_mode {
            "COMMIT_TIME_ORDERING" => Ok(Box::new(CommitTimeRecordMerger)),
            "EVENT_TIME_ORDERING" => Ok(Box::new(EventTimeRecordMerger)),
            unsupported => Err(crate::error::CoreError::ReadFileSliceError(format!(
                "Unsupported merge mode for record merger: '{unsupported}'"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_group::reader::buffered_record::OrderingValue;

    fn make_data_record(key: &str, ordering: Option<i64>) -> BufferedRecord {
        BufferedRecord {
            record_key: key.to_string(),
            data: None,
            binary_data: None,
            ordering_value: ordering.map(OrderingValue::Long),
            is_delete: false,
        }
    }

    fn make_delete_record(key: &str, ordering: Option<i64>) -> DeleteRecord {
        DeleteRecord {
            record_key: key.to_string(),
            partition_path: String::new(),
            ordering_value: ordering.map(OrderingValue::Long),
        }
    }

    // =========================================================================
    // CommitTimeRecordMerger tests
    // =========================================================================

    /// Given: CommitTimeRecordMerger, new_record with lower ordering value than existing
    /// When:  delta_merge(new_record, Some(existing_record))
    /// Then:  new_record is returned (ordering value is irrelevant in commit-time mode)
    #[test]
    fn test_commit_time_delta_merge_new_always_wins() {
        let merger = CommitTimeRecordMerger;
        let new_rec = make_data_record("k1", Some(0));
        let existing = make_data_record("k1", Some(100));

        let result = merger.delta_merge(&new_rec, Some(&existing)).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().record_key, "k1");
    }

    /// Given: CommitTimeRecordMerger, no existing record
    /// When:  delta_merge(new_record, None)
    /// Then:  new_record is returned
    #[test]
    fn test_commit_time_delta_merge_no_existing() {
        let merger = CommitTimeRecordMerger;
        let new_rec = make_data_record("k1", Some(1));

        let result = merger.delta_merge(&new_rec, None).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().record_key, "k1");
    }

    /// Given: CommitTimeRecordMerger, delete with lower ordering than existing data
    /// When:  delta_merge_delete(delete, Some(existing))
    /// Then:  delete is returned (always wins in commit-time mode)
    #[test]
    fn test_commit_time_delta_merge_delete_always_wins() {
        let merger = CommitTimeRecordMerger;
        let delete = make_delete_record("k1", Some(0));
        let existing = make_data_record("k1", Some(100));

        let result = merger.delta_merge_delete(&delete, Some(&existing)).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().record_key, "k1");
    }

    /// Given: CommitTimeRecordMerger
    /// When:  final_merge(base_record, log_record)
    /// Then:  log_record is returned (log always wins over base)
    #[test]
    fn test_commit_time_final_merge_log_wins() {
        let merger = CommitTimeRecordMerger;
        let base = make_data_record("k1", Some(100));
        let log = make_data_record("k1", Some(1));

        let result = merger.final_merge(&base, &log).unwrap();
        assert_eq!(result.record_key, "k1");
        // Log record wins (ordering=1), not base (ordering=100)
        assert_eq!(result.ordering_value, Some(OrderingValue::Long(1)));
    }

    // =========================================================================
    // EventTimeRecordMerger tests
    // =========================================================================

    /// Given: EventTimeRecordMerger, new record has higher ordering (ts=2) than existing (ts=1)
    /// When:  delta_merge(new, Some(existing))
    /// Then:  new_record wins (higher ordering value)
    #[test]
    fn test_event_time_delta_merge_higher_ordering_wins() {
        let merger = EventTimeRecordMerger;
        let new_rec = make_data_record("k1", Some(2));
        let existing = make_data_record("k1", Some(1));

        let result = merger.delta_merge(&new_rec, Some(&existing)).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().ordering_value, Some(OrderingValue::Long(2)));
    }

    /// Given: EventTimeRecordMerger, new record has lower ordering (ts=0) than existing (ts=1)
    /// When:  delta_merge(new, Some(existing))
    /// Then:  existing_record survives (higher ordering value)
    #[test]
    fn test_event_time_delta_merge_lower_ordering_loses() {
        let merger = EventTimeRecordMerger;
        let new_rec = make_data_record("k1", Some(0));
        let existing = make_data_record("k1", Some(1));

        let result = merger.delta_merge(&new_rec, Some(&existing)).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().ordering_value, Some(OrderingValue::Long(1)));
    }

    /// Given: EventTimeRecordMerger, equal ordering values
    /// When:  delta_merge(new(ts=1), Some(existing(ts=1)))
    /// Then:  new_record wins (>= comparison)
    #[test]
    fn test_event_time_delta_merge_equal_ordering_new_wins() {
        let merger = EventTimeRecordMerger;
        let new_rec = make_data_record("new", Some(1));
        let existing = make_data_record("existing", Some(1));

        let result = merger.delta_merge(&new_rec, Some(&existing)).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().record_key, "new");
    }

    /// Given: EventTimeRecordMerger
    /// When:  delta_merge_delete(delete(ts=2), Some(existing(ts=1)))
    /// Then:  delete wins (higher ordering)
    /// When:  delta_merge_delete(delete(ts=0), Some(existing(ts=1)))
    /// Then:  existing survives (None returned, lower ordering delete loses)
    #[test]
    fn test_event_time_delta_merge_delete_ordering_check() {
        let merger = EventTimeRecordMerger;

        // Delete with higher ordering wins
        let delete_high = make_delete_record("k1", Some(2));
        let existing = make_data_record("k1", Some(1));
        let result = merger
            .delta_merge_delete(&delete_high, Some(&existing))
            .unwrap();
        assert!(result.is_some(), "delete(ts=2) should win over data(ts=1)");

        // Delete with lower ordering loses
        let delete_low = make_delete_record("k1", Some(0));
        let result = merger
            .delta_merge_delete(&delete_low, Some(&existing))
            .unwrap();
        assert!(result.is_none(), "delete(ts=0) should lose to data(ts=1)");
    }

    /// Given: EventTimeRecordMerger, no existing record
    /// When:  delta_merge(new, None)
    /// Then:  new_record is returned
    #[test]
    fn test_event_time_delta_merge_no_existing() {
        let merger = EventTimeRecordMerger;
        let new_rec = make_data_record("k1", Some(5));

        let result = merger.delta_merge(&new_rec, None).unwrap();
        assert!(result.is_some());
    }

    // =========================================================================
    // Factory tests
    // =========================================================================

    #[test]
    fn test_factory_commit_time_ordering() {
        let merger = BufferedRecordMergerFactory::create("COMMIT_TIME_ORDERING").unwrap();
        let new_rec = make_data_record("k", Some(0));
        let existing = make_data_record("k", Some(100));
        // CommitTime: new always wins regardless of ordering
        let result = merger.delta_merge(&new_rec, Some(&existing)).unwrap();
        assert_eq!(result.unwrap().ordering_value, Some(OrderingValue::Long(0)));
    }

    #[test]
    fn test_factory_event_time_ordering() {
        let merger = BufferedRecordMergerFactory::create("EVENT_TIME_ORDERING").unwrap();
        let new_rec = make_data_record("k", Some(0));
        let existing = make_data_record("k", Some(100));
        // EventTime: higher ordering wins → existing survives
        let result = merger.delta_merge(&new_rec, Some(&existing)).unwrap();
        assert_eq!(
            result.unwrap().ordering_value,
            Some(OrderingValue::Long(100))
        );
    }
}
