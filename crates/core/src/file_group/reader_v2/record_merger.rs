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

use super::buffered_record::{BufferedRecord, DeleteRecord, OrderingValue};
use crate::Result;

/// Mirrors Java `OrderingValues.isCommitTimeOrderingValue` (`orderingValue == null
/// || OrderingValues.isDefault(orderingValue)`, where `isDefault` ==
/// `Integer(0).equals(orderingValue)`). A DELETE carrying the default ordering
/// value is a "commit-time ordering delete" and wins unconditionally, regardless
/// of the existing record's ordering value (mirrors
/// `BufferedRecord.isCommitTimeOrderingDelete`).
///
/// The default is ONLY the null-coerced [`OrderingValue::Default`] sentinel (Java
/// `Integer(0)`) or an absent (`None`) ordering. A GENUINE `Long(0)` field value
/// is NOT default: `Integer(0).equals(Long(0))` is `false` in Java, so a
/// delete carrying a real ordering value of `0` is ordering-compared, not treated
/// as natural-order.
fn is_default_ordering(ordering_value: &Option<OrderingValue>) -> bool {
    matches!(ordering_value, None | Some(OrderingValue::Default))
}

/// Mirrors Java `BufferedRecord.isCommitTimeOrderingDelete`: a DELETE carrying
/// the default ordering value. Such deletes win unconditionally (natural order).
fn is_commit_time_ordering_delete(record: &BufferedRecord) -> bool {
    record.is_delete() && is_default_ordering(&record.ordering_value)
}

/// Mirrors Java `OrderingValues.isSameClass`: ordering values are only compared
/// when they are the same concrete type. Java throws on a cross-type
/// `compareTo`; we instead skip the comparison (caller lets the newer/delete
/// record win), avoiding the arbitrary cross-variant `Ord` on [`OrderingValue`].
///
/// Thin free-function wrapper over [`OrderingValue::is_same_class`] so the four
/// buffer-side call sites below read as `is_same_class(a, b)`; the single source
/// of truth is the method.
fn is_same_class(a: &OrderingValue, b: &OrderingValue) -> bool {
    a.is_same_class(b)
}

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
/// higher ordering value wins. This is the merge strategy for the
/// `EVENT_TIME_ORDERING` mode only; `COMMIT_TIME_ORDERING` uses
/// [`CommitTimeRecordMerger`] (last writer wins, no ordering comparison).
///
/// Created by `BufferedRecordMergerFactory` based on the merge mode. The read
/// path now accepts `EVENT_TIME_ORDERING` (`buffer/loader.rs::get_record_buffer`),
/// so this merger is live in production — it drives the scalar base-vs-log
/// `final_merge`, and the vectorized `pick_winner` kernel mirrors its semantics.
#[derive(Debug)]
pub struct EventTimeRecordMerger;

impl BufferedRecordMerger for EventTimeRecordMerger {
    /// Mirrors Java `EventTimeRecordMerger.deltaMerge` via `shouldKeepNewerRecord`.
    fn delta_merge(
        &self,
        new_record: &BufferedRecord,
        existing_record: Option<&BufferedRecord>,
    ) -> Result<Option<BufferedRecord>> {
        match existing_record {
            None => Ok(Some(new_record.clone())),
            Some(existing) if should_keep_newer_record(existing, new_record) => {
                Ok(Some(new_record.clone()))
            }
            Some(existing) => Ok(Some(existing.clone())),
        }
    }

    /// Mirrors Java `BufferedRecordMergerFactory.deltaMergeDeleteRecord`.
    ///
    /// The DELETE is obsolete (existing survives) ONLY when the existing record
    /// has a strictly greater ordering value of the SAME type AND the delete
    /// carries a non-default ordering value (`0` == natural order = always
    /// delete). A default-ordering delete, or one against an existing record
    /// that is itself a commit-time-ordering delete, follows Java's branches.
    fn delta_merge_delete(
        &self,
        delete_record: &DeleteRecord,
        existing_record: Option<&BufferedRecord>,
    ) -> Result<Option<DeleteRecord>> {
        match existing_record {
            None => Ok(Some(delete_record.clone())),
            Some(existing) if is_commit_time_ordering_delete(existing) => Ok(None),
            Some(existing) => {
                let choose_existing = !is_default_ordering(&delete_record.ordering_value)
                    && match (&delete_record.ordering_value, &existing.ordering_value) {
                        (Some(del_val), Some(existing_val)) => {
                            is_same_class(del_val, existing_val) && existing_val > del_val
                        }
                        _ => false,
                    };
                if choose_existing {
                    Ok(None) // existing record survives; DELETE is obsolete
                } else {
                    Ok(Some(delete_record.clone()))
                }
            }
        }
    }

    /// Mirrors Java `EventTimeRecordMerger.finalMerge`, which delegates straight
    /// to `shouldKeepNewerRecord` — so this does too, rather than restating the
    /// rule. The two spellings agreed, but one rule in two places is one that can
    /// stop agreeing.
    fn final_merge(
        &self,
        older_record: &BufferedRecord,
        newer_record: &BufferedRecord,
    ) -> Result<BufferedRecord> {
        if should_keep_newer_record(older_record, newer_record) {
            Ok(newer_record.clone())
        } else {
            Ok(older_record.clone())
        }
    }
}

/// Mirrors Java `BufferedRecordMergerFactory.shouldKeepNewerRecord`.
///
/// If either record is a commit-time-ordering delete the newer record is kept
/// (delete statements use a constant `0` ordering value not guaranteed to match
/// the other record's type). Otherwise the newer record wins on `>=` (ties go to
/// the newer record); a cross-type comparison keeps the newer record rather than
/// relying on the arbitrary cross-variant `Ord`.
pub(crate) fn should_keep_newer_record(
    old_record: &BufferedRecord,
    new_record: &BufferedRecord,
) -> bool {
    if is_commit_time_ordering_delete(new_record) || is_commit_time_ordering_delete(old_record) {
        return true;
    }
    match (&new_record.ordering_value, &old_record.ordering_value) {
        (Some(new_val), Some(old_val)) => !is_same_class(new_val, old_val) || new_val >= old_val,
        _ => true,
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

use crate::file_group::reader_v2::metadata_merger::{
    CustomMerger, MetadataPayloadMerger, resolve_custom_merger,
};

/// Factory for creating `BufferedRecordMerger` instances.
///
/// Mirrors Java's `BufferedRecordMergerFactory`.
pub struct BufferedRecordMergerFactory;

impl BufferedRecordMergerFactory {
    /// Create a merger for the table's merge mode.
    ///
    /// - `"COMMIT_TIME_ORDERING"` → `CommitTimeRecordMerger` (last writer wins)
    /// - `"EVENT_TIME_ORDERING"` → `EventTimeRecordMerger` (ordering value comparison)
    /// - `"CUSTOM"` → the merger its payload class names, if this crate has one
    /// - Others → Error
    ///
    /// A CUSTOM table whose payload has no merger here keeps erroring rather than
    /// being merged by an ordering rule it did not ask for, which is why the table
    /// config is a parameter: the merge mode alone cannot decide it.
    pub fn create_with(
        merge_mode: &str,
        table_config: &std::collections::HashMap<String, String>,
    ) -> Result<Box<dyn BufferedRecordMerger>> {
        match merge_mode {
            "COMMIT_TIME_ORDERING" => Ok(Box::new(CommitTimeRecordMerger)),
            "EVENT_TIME_ORDERING" => Ok(Box::new(EventTimeRecordMerger)),
            _ if merge_mode.eq_ignore_ascii_case("CUSTOM") => {
                match resolve_custom_merger(table_config) {
                    Some(CustomMerger::MetadataPayload) => Ok(Box::new(MetadataPayloadMerger)),
                    None => Err(crate::error::CoreError::ReadFileSliceError(
                        "CUSTOM merge mode names a payload class this crate does not \
                         implement a merger for."
                            .to_string(),
                    )),
                }
            }
            unsupported => Err(crate::error::CoreError::ReadFileSliceError(format!(
                "Unsupported merge mode for record merger: '{unsupported}'"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_group::reader_v2::buffered_record::OrderingValue;

    fn make_data_record(key: &str, ordering: Option<i64>) -> BufferedRecord {
        // A real single-row data record. These merger tests exercise only
        // ordering-value comparison and key/identity preservation (the payload is
        // never read), but using an Owned payload keeps `is_delete()` correct.
        use arrow_array::{Int64Array, StringArray};
        use arrow_schema::{DataType, Field, Schema};
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("ord", DataType::Int64, true),
        ]));
        let batch = arrow_array::RecordBatch::try_new(
            schema,
            vec![
                std::sync::Arc::new(StringArray::from(vec![key])),
                std::sync::Arc::new(Int64Array::from(vec![ordering])),
            ],
        )
        .unwrap();
        BufferedRecord::new_data(key.to_string(), batch, ordering.map(OrderingValue::Long))
    }

    fn make_delete_record(key: &str, ordering: Option<i64>) -> DeleteRecord {
        DeleteRecord {
            record_key: key.to_string(),
            ordering_value: ordering.map(OrderingValue::Long),
        }
    }

    fn composite(parts: &[i64]) -> OrderingValue {
        OrderingValue::Composite(parts.iter().copied().map(OrderingValue::Long).collect())
    }

    /// Regression test: a composite (multi-field) delete ordering value follows the
    /// scalar rule: a stale delete loses to a record whose composite ordering
    /// is higher, and a newer one wins.
    #[test]
    fn test_event_time_delta_merge_delete_composite_ordering() {
        let merger = EventTimeRecordMerger;

        let mut existing = make_data_record("k1", Some(0));
        existing.ordering_value = Some(composite(&[2, 1]));

        let stale_delete = DeleteRecord {
            record_key: "k1".to_string(),
            ordering_value: Some(composite(&[1, 50])),
        };
        let result = merger
            .delta_merge_delete(&stale_delete, Some(&existing))
            .unwrap();
        assert!(
            result.is_none(),
            "delete((1,50)) should lose to data((2,1))"
        );

        let newer_delete = DeleteRecord {
            record_key: "k1".to_string(),
            ordering_value: Some(composite(&[2, 2])),
        };
        let result = merger
            .delta_merge_delete(&newer_delete, Some(&existing))
            .unwrap();
        assert!(
            result.is_some(),
            "delete((2,2)) should win over data((2,1))"
        );
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

    /// EVENT_TIME ordering must compare String ordering values (e.g. ISO date
    /// precombine fields), not just integers: a lexicographically greater string
    /// wins, a lower one loses.
    #[test]
    fn test_event_time_delta_merge_string_ordering() {
        let merger = EventTimeRecordMerger;
        let mut existing = make_data_record("k1", Some(0));
        existing.ordering_value = Some(OrderingValue::String("2026-01".to_string()));
        let mut newer = make_data_record("k1", Some(0));
        newer.ordering_value = Some(OrderingValue::String("2026-02".to_string()));
        let mut older = make_data_record("k1", Some(0));
        older.ordering_value = Some(OrderingValue::String("2025-12".to_string()));

        // delta_merge returns the WINNER (data records never drop to None).
        let won = merger
            .delta_merge(&newer, Some(&existing))
            .unwrap()
            .unwrap();
        assert_eq!(
            won.ordering_value,
            Some(OrderingValue::String("2026-02".to_string())),
            "lexicographically greater string ordering wins"
        );
        let kept = merger
            .delta_merge(&older, Some(&existing))
            .unwrap()
            .unwrap();
        assert_eq!(
            kept.ordering_value,
            Some(OrderingValue::String("2026-01".to_string())),
            "lexicographically lower string ordering loses; existing survives"
        );
        // Equal string ordering: the new record wins via the `>=` tie-break,
        // closing the eq/lt/gt matrix for String updates (matches the Long path,
        // test_event_time_delta_merge_equal_ordering_new_wins).
        let mut equal = make_data_record("equal", Some(0));
        equal.ordering_value = Some(OrderingValue::String("2026-01".to_string()));
        let tie = merger
            .delta_merge(&equal, Some(&existing))
            .unwrap()
            .unwrap();
        assert_eq!(
            tie.record_key, "equal",
            "equal string ordering: new record wins via the >= tie-break"
        );
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

        // Delete with lower (non-default) ordering loses
        let existing_2 = make_data_record("k1", Some(2));
        let delete_low = make_delete_record("k1", Some(1));
        let result = merger
            .delta_merge_delete(&delete_low, Some(&existing_2))
            .unwrap();
        assert!(result.is_none(), "delete(ts=1) should lose to data(ts=2)");

        // Delete with the DEFAULT ordering value (the null-coerced `Default`
        // sentinel) is a commit-time-ordering delete and wins unconditionally,
        // even against a higher existing ts (mirrors Java
        // BufferedRecord.isCommitTimeOrderingDelete / OrderingValues.isDefault).
        // ordering value.
        let delete_default = DeleteRecord {
            record_key: "k1".to_string(),
            ordering_value: Some(OrderingValue::Default),
        };
        let result = merger
            .delta_merge_delete(&delete_default, Some(&existing_2))
            .unwrap();
        assert!(
            result.is_some(),
            "delete(ordering=Default) wins unconditionally over data(ts=2)"
        );

        // A delete with a GENUINE ordering value of 0 (a real bigint field
        // value, `Long(0)`) is NOT the default — it is ordering-compared, and so
        // LOSES to a higher existing ts (row kept). Java: `Integer(0).equals(Long(0))`
        // is `false`, so `OrderingValues.isDefault(Long(0))` is `false`.
        let delete_genuine_zero = make_delete_record("k1", Some(0));
        let result = merger
            .delta_merge_delete(&delete_genuine_zero, Some(&existing_2))
            .unwrap();
        assert!(
            result.is_none(),
            "delete(genuine ts=0) loses to data(ts=2); the row is kept"
        );
    }

    #[test]
    fn test_event_time_final_merge_commit_time_delete() {
        let merger = EventTimeRecordMerger;
        let base = make_data_record("k1", Some(102)); // older / base-file record

        // A no-ts `delete from` (the null-coerced `Default` ordering) wins over a
        // higher-ts base (mirrors Java EventTimeRecordMerger.finalMerge +
        // isCommitTimeOrderingDelete).
        let default_delete =
            BufferedRecord::new_delete("k1".to_string(), Some(OrderingValue::Default));
        let merged = merger.final_merge(&base, &default_delete).unwrap();
        assert!(
            merged.is_delete(),
            "default-ordering delete wins over higher-ts base"
        );

        // A delete carrying a GENUINE ordering value of 0 (`Long(0)`) is
        // NOT the default — it is ordering-compared and LOSES to the higher-ts
        // base, so the base survives (row kept).
        let genuine_zero_delete =
            BufferedRecord::new_delete("k1".to_string(), Some(OrderingValue::Long(0)));
        let merged = merger.final_merge(&base, &genuine_zero_delete).unwrap();
        assert!(
            !merged.is_delete(),
            "delete(genuine ts=0) loses to higher-ts base; base survives"
        );

        // A non-default delete with a LOWER ordering value loses; the base survives.
        let low_delete =
            BufferedRecord::new_delete("k1".to_string(), Some(OrderingValue::Long(50)));
        let merged = merger.final_merge(&base, &low_delete).unwrap();
        assert!(
            !merged.is_delete(),
            "lower-ts (non-default) delete loses; base survives"
        );
    }

    /// EVENT_TIME `final_merge` for a base-file record updated by a log DATA
    /// record (the common compaction/read-merge path) — distinct from
    /// `delta_merge` (log-vs-log). Mirrors Java `EventTimeRecordMerger.finalMerge`
    /// / `shouldKeepNewerRecord`: for non-delete records the newer (log) wins iff
    /// `log.ordering >= base.ordering`. Covers the three orderings (log ts `>` /
    /// `<` / `=` base ts); the winner is identified by its record key.
    #[test]
    fn test_event_time_final_merge_data_update() {
        let merger = EventTimeRecordMerger;
        // Base-file record at event-time 100 (the older record).
        let base = make_data_record("base", Some(100));

        // log ts > base ts -> log update wins.
        let higher = make_data_record("log", Some(200));
        let merged = merger.final_merge(&base, &higher).unwrap();
        assert_eq!(
            merged.record_key, "log",
            "log update with higher ordering (200 > 100) wins over base"
        );

        // log ts < base ts -> base survives.
        let lower = make_data_record("log", Some(50));
        let merged = merger.final_merge(&base, &lower).unwrap();
        assert_eq!(
            merged.record_key, "base",
            "log update with lower ordering (50 < 100) loses; base survives"
        );

        // log ts == base ts -> log wins via the `>=` tie-break.
        let equal = make_data_record("log", Some(100));
        let merged = merger.final_merge(&base, &equal).unwrap();
        assert_eq!(
            merged.record_key, "log",
            "equal-ordering (100 == 100) log update wins via the >= tie-break"
        );
    }

    #[test]
    fn test_is_same_class_guard_and_cross_type_delete() {
        // is_same_class is true only for matching OrderingValue variants.
        assert!(is_same_class(
            &OrderingValue::Long(1),
            &OrderingValue::Long(2)
        ));
        assert!(is_same_class(
            &OrderingValue::String("a".to_string()),
            &OrderingValue::String("b".to_string())
        ));
        assert!(!is_same_class(
            &OrderingValue::Long(1),
            &OrderingValue::String("x".to_string())
        ));

        // Cross-type delete vs existing: the decision must NOT rely on the
        // arbitrary cross-variant Ord (Long<String). With the isSameClass guard a
        // cross-type comparison can't "choose existing", so the delete wins.
        let merger = EventTimeRecordMerger;
        let existing = make_data_record("k1", Some(100)); // Long(100)
        let delete_str = DeleteRecord {
            record_key: "k1".to_string(),
            ordering_value: Some(OrderingValue::String("9".to_string())),
        };
        assert!(
            merger
                .delta_merge_delete(&delete_str, Some(&existing))
                .unwrap()
                .is_some(),
            "cross-type delete must win (no cross-variant Ord deciding existing survives)"
        );
    }

    /// A null ordering value coerces to `OrderingValue::Default` (Java
    /// `OrderingValues.getDefault()` == `Integer(0)`) and is COMPARED, not
    /// auto-winning. Mirrors Java `BufferedRecordMergerFactory.shouldKeepNewerRecord`
    /// (`newer.compareTo(older) >= 0`) with the null field coerced to the default.
    ///
    /// Discriminating: before the fix a null ordering surfaced as `None` and the
    /// `_ => true` arm made the newer/log record win unconditionally — so
    /// `newer=null vs older=100` wrongly kept the (null) newer record. With the
    /// coercion, `Default` compares as `0`, so the positive-ordering record wins.
    #[test]
    fn test_event_time_null_ordering_coerced_to_default() {
        let merger = EventTimeRecordMerger;
        // Helper: a data record whose ordering value is the null-coerced Default.
        let null_rec = |key: &str| {
            let mut r = make_data_record(key, Some(0));
            r.ordering_value = Some(OrderingValue::Default);
            r
        };

        // newer=null vs older=100 -> keep older(100). (delta_merge(new, existing))
        let kept = merger
            .delta_merge(
                &null_rec("newer"),
                Some(&make_data_record("older", Some(100))),
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            kept.record_key, "older",
            "null-ordering newer (Default==0) loses to older ts=100; older kept"
        );

        // newer=100 vs older=null -> keep newer.
        let won = merger
            .delta_merge(
                &make_data_record("newer", Some(100)),
                Some(&null_rec("older")),
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            won.record_key, "newer",
            "positive newer ts=100 wins over null-ordering older (Default==0)"
        );

        // both null -> newer wins (tie: Default >= Default).
        let tie = merger
            .delta_merge(&null_rec("newer"), Some(&null_rec("older")))
            .unwrap()
            .unwrap();
        assert_eq!(
            tie.record_key, "newer",
            "both null (Default==Default): newer wins via the >= tie-break"
        );

        // final_merge (base-vs-log) direction: a null-ordering log update must LOSE
        // to a positive-ordering base (the row keeps the real value).
        let base = make_data_record("base", Some(100));
        let log_null = null_rec("log");
        let merged = merger.final_merge(&base, &log_null).unwrap();
        assert_eq!(
            merged.record_key, "base",
            "null-ordering log update (Default==0) loses to base ts=100; base kept"
        );
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

    /// EVENT_TIME ordering by a `double` field (e.g. `weight`).
    /// The higher-weight record must win base-vs-log and log-vs-log; equal weights
    /// go to the newer record (`>=`), matching Java `EventTimeRecordMerger`. Before
    /// the fix, a `Float64` ordering column resolved to `None`, so every log record
    /// won unconditionally (EVENT_TIME silently degraded to commit-time).
    #[test]
    fn test_event_time_merge_double_ordering_weight() {
        use arrow_array::{Float64Array, StringArray};
        use arrow_schema::{DataType, Field, Schema};
        let mk = |key: &str, w: f64| {
            let schema = std::sync::Arc::new(Schema::new(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("weight", DataType::Float64, false),
            ]));
            let batch = arrow_array::RecordBatch::try_new(
                schema,
                vec![
                    std::sync::Arc::new(StringArray::from(vec![key])),
                    std::sync::Arc::new(Float64Array::from(vec![w])),
                ],
            )
            .unwrap();
            BufferedRecord::new_data(key.to_string(), batch, Some(OrderingValue::Double(w)))
        };
        let merger = EventTimeRecordMerger;

        // base(weight=5.0) vs log(weight=9.0) → higher-weight log wins.
        assert_eq!(
            merger
                .final_merge(&mk("k1", 5.0), &mk("k1", 9.0))
                .unwrap()
                .ordering_value,
            Some(OrderingValue::Double(9.0))
        );
        // base(weight=9.0) vs log(weight=5.0) → log is stale, base survives.
        assert_eq!(
            merger
                .final_merge(&mk("k1", 9.0), &mk("k1", 5.0))
                .unwrap()
                .ordering_value,
            Some(OrderingValue::Double(9.0))
        );
        // delta_merge: a higher-weight update wins over the buffered record.
        assert_eq!(
            merger
                .delta_merge(&mk("k1", 2.0), Some(&mk("k1", 1.0)))
                .unwrap()
                .unwrap()
                .ordering_value,
            Some(OrderingValue::Double(2.0))
        );
        // delta_merge: a lower-weight update loses; the buffered record survives.
        assert_eq!(
            merger
                .delta_merge(&mk("k1", 1.0), Some(&mk("k1", 2.0)))
                .unwrap()
                .unwrap()
                .ordering_value,
            Some(OrderingValue::Double(2.0))
        );
    }

    // =========================================================================
    // Factory tests
    // =========================================================================

    #[test]
    fn test_factory_commit_time_ordering() {
        let merger = BufferedRecordMergerFactory::create_with(
            "COMMIT_TIME_ORDERING",
            &std::collections::HashMap::new(),
        )
        .unwrap();
        let new_rec = make_data_record("k", Some(0));
        let existing = make_data_record("k", Some(100));
        // CommitTime: new always wins regardless of ordering
        let result = merger.delta_merge(&new_rec, Some(&existing)).unwrap();
        assert_eq!(result.unwrap().ordering_value, Some(OrderingValue::Long(0)));
    }

    #[test]
    fn test_factory_event_time_ordering() {
        let merger = BufferedRecordMergerFactory::create_with(
            "EVENT_TIME_ORDERING",
            &std::collections::HashMap::new(),
        )
        .unwrap();
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
