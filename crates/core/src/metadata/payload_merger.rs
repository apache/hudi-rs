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
//! Record merger for metadata-table payloads.
//!
//! Native equivalent of Java `HoodieMetadataPayload#preCombine` /
//! `MetadataPartitionType#combineMetadataPayloads`: the MDT's
//! `hoodie.properties` declares `hoodie.record.merge.mode=CUSTOM` with the
//! payload-based merge strategy id (all zeros) and
//! `hoodie.compaction.payload.class=org.apache.hudi.metadata.HoodieMetadataPayload`;
//! that combination resolves to this merger. Per-record-type semantics:
//!
//! - **files / `__all_partitions__`**: map-merge (`combineFileSystemMetadata`)
//!   — tombstones cancel earlier entries, sizes take the max, orphan
//!   tombstones are carried forward for the base-file merge stage.
//! - **column_stats / partition_stats**: `mergeColumnStatsRecords` — a
//!   tombstone on either side or a tight-bound newer record replaces the
//!   older one; otherwise ranges union and counts/sizes sum.
//! - **record_index** (and any other type): newer record wins; a newer
//!   tombstone deletes.

use std::collections::HashMap;

use crate::Result;
use crate::error::CoreError;
use crate::metadata::table::encode::{ColumnStatValue, ColumnStatsMetadata};
use crate::metadata::table_record::HoodieMetadataFileInfo;
use crate::storage::Storage;

/// Java `HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID`.
pub const PAYLOAD_BASED_MERGE_STRATEGY_UUID: &str = "00000000-0000-0000-0000-000000000000";
/// The only payload class valid for the metadata table.
pub const METADATA_PAYLOAD_CLASS: &str = "org.apache.hudi.metadata.HoodieMetadataPayload";

/// Merger implementing `HoodieMetadataPayload` combine semantics.
#[derive(Clone, Copy, Debug, Default)]
pub struct MetadataPayloadMerger;

impl MetadataPayloadMerger {
    /// Resolve the merger for a metadata table from its `hoodie.properties`
    /// under the data table's base path (`storage` is rooted at the data
    /// table). The declared merge strategy id must be the payload-based
    /// strategy and the payload class must be `HoodieMetadataPayload`;
    /// anything else is an error rather than a silently-wrong merge. A
    /// missing properties file resolves to the default (this) merger.
    pub async fn for_metadata_table(storage: &Storage) -> Result<Self> {
        let props_path = format!(
            "{}/metadata/.hoodie/hoodie.properties",
            crate::metadata::HUDI_METADATA_DIR
        );
        let bytes = match storage.get_file_data(&props_path).await {
            Ok(bytes) => bytes,
            Err(_) => return Ok(Self),
        };
        let text = String::from_utf8_lossy(&bytes);
        let props: HashMap<&str, &str> = text
            .lines()
            .filter(|line| !line.trim_start().starts_with('#'))
            .filter_map(|line| line.split_once('='))
            .map(|(k, v)| (k.trim(), v.trim()))
            .collect();
        Self::from_properties(&props)
    }

    fn from_properties(props: &HashMap<&str, &str>) -> Result<Self> {
        if let Some(strategy) = props.get("hoodie.record.merge.strategy.id")
            && *strategy != PAYLOAD_BASED_MERGE_STRATEGY_UUID
        {
            return Err(CoreError::MetadataTable(format!(
                "Unsupported metadata table merge strategy id `{strategy}`; \
                 only the payload-based strategy `{PAYLOAD_BASED_MERGE_STRATEGY_UUID}` is supported"
            )));
        }
        if let Some(payload_class) = props.get("hoodie.compaction.payload.class")
            && *payload_class != METADATA_PAYLOAD_CLASS
        {
            return Err(CoreError::MetadataTable(format!(
                "Unsupported metadata table payload class `{payload_class}`; \
                 expected `{METADATA_PAYLOAD_CLASS}`"
            )));
        }
        Ok(Self)
    }

    /// Merge a newer files-partition map into `existing`
    /// (`HoodieTableMetadataUtil#combineFileSystemMetadata`).
    pub fn merge_file_infos(
        &self,
        existing: &mut HashMap<String, HoodieMetadataFileInfo>,
        newer: &HashMap<String, HoodieMetadataFileInfo>,
    ) {
        for (name, new_info) in newer {
            match existing.get(name) {
                Some(old_info) => {
                    if new_info.is_deleted {
                        if old_info.is_deleted {
                            // Repeated tombstone: keep the newer one.
                            existing.insert(name.clone(), new_info.clone());
                        } else {
                            // Deletion cancels the entry AND the tombstone.
                            existing.remove(name);
                        }
                    } else {
                        // Sizes only ever grow; max keeps the merge
                        // commutative when MDT records arrive out of order.
                        existing.insert(
                            name.clone(),
                            HoodieMetadataFileInfo::new(
                                name.clone(),
                                old_info.size.max(new_info.size),
                                false,
                            ),
                        );
                    }
                }
                // New entry — a live file, or an orphan tombstone that must
                // be carried forward for the later base-file merge stage.
                None => {
                    existing.insert(name.clone(), new_info.clone());
                }
            }
        }
    }

    /// Merge column/partition stats
    /// (`HoodieTableMetadataUtil#mergeColumnStatsRecords`).
    pub fn merge_column_stats(
        &self,
        older: &ColumnStatsMetadata,
        newer: &ColumnStatsMetadata,
    ) -> ColumnStatsMetadata {
        // A tombstone on either side means the newer state simply replaces
        // the older one; same for a tight-bound newer record (exact stats
        // recomputed from files supersede accumulated bounds).
        if newer.is_deleted || older.is_deleted || newer.is_tight_bound {
            return newer.clone();
        }
        ColumnStatsMetadata {
            file_name: newer.file_name.clone(),
            column_name: newer.column_name.clone(),
            min_value: merge_bound(&older.min_value, &newer.min_value, Bound::Min),
            max_value: merge_bound(&older.max_value, &newer.max_value, Bound::Max),
            value_count: older.value_count + newer.value_count,
            null_count: older.null_count + newer.null_count,
            total_size: older.total_size + newer.total_size,
            total_uncompressed_size: older.total_uncompressed_size + newer.total_uncompressed_size,
            is_deleted: newer.is_deleted,
            is_tight_bound: newer.is_tight_bound,
            decoded_value_type_ordinal: newer.decoded_value_type_ordinal,
        }
    }
}

enum Bound {
    Min,
    Max,
}

/// Union two optional bounds; `None` means "no values seen" and yields the
/// other side (Java filters nulls before taking min/max).
fn merge_bound(
    older: &Option<ColumnStatValue>,
    newer: &Option<ColumnStatValue>,
    bound: Bound,
) -> Option<ColumnStatValue> {
    match (older, newer) {
        (Some(old), Some(new)) => match compare_stat_values(old, new) {
            Some(ordering) => {
                let take_old = match bound {
                    Bound::Min => ordering.is_lt(),
                    Bound::Max => ordering.is_gt(),
                };
                Some(if take_old { old.clone() } else { new.clone() })
            }
            // Incomparable variants (should not happen for one column):
            // fall back to the newer side.
            None => Some(new.clone()),
        },
        (Some(old), None) => Some(old.clone()),
        (None, new) => new.clone(),
    }
}

/// Natural-order comparison for same-variant stat values.
fn compare_stat_values(a: &ColumnStatValue, b: &ColumnStatValue) -> Option<std::cmp::Ordering> {
    use ColumnStatValue::*;
    match (a, b) {
        (Boolean(x), Boolean(y)) => x.partial_cmp(y),
        (Int(x), Int(y)) => x.partial_cmp(y),
        (Long(x), Long(y)) => x.partial_cmp(y),
        (Float(x), Float(y)) => x.partial_cmp(y),
        (Double(x), Double(y)) => x.partial_cmp(y),
        (Bytes(x), Bytes(y)) => x.partial_cmp(y),
        (String(x), String(y)) => x.partial_cmp(y),
        (Date(x), Date(y)) => x.partial_cmp(y),
        (TimeMicros(x), TimeMicros(y)) => x.partial_cmp(y),
        (TimestampMicros(x), TimestampMicros(y)) => x.partial_cmp(y),
        (LocalTimestampMicros(x), LocalTimestampMicros(y)) => x.partial_cmp(y),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stats(min: i64, max: i64, count: i64, tight: bool) -> ColumnStatsMetadata {
        ColumnStatsMetadata {
            file_name: "f".to_string(),
            column_name: "c".to_string(),
            min_value: Some(ColumnStatValue::Long(min)),
            max_value: Some(ColumnStatValue::Long(max)),
            value_count: count,
            null_count: 0,
            total_size: 10,
            total_uncompressed_size: 20,
            is_deleted: false,
            is_tight_bound: tight,
            decoded_value_type_ordinal: None,
        }
    }

    #[test]
    fn test_merge_column_stats_unions_ranges_and_sums_counts() {
        let merger = MetadataPayloadMerger;
        let merged = merger.merge_column_stats(&stats(5, 10, 3, false), &stats(1, 7, 2, false));
        assert_eq!(merged.min_value, Some(ColumnStatValue::Long(1)));
        assert_eq!(merged.max_value, Some(ColumnStatValue::Long(10)));
        assert_eq!(merged.value_count, 5);
        assert_eq!(merged.total_size, 20);
    }

    #[test]
    fn test_merge_column_stats_tight_bound_newer_wins() {
        let merger = MetadataPayloadMerger;
        let merged = merger.merge_column_stats(&stats(1, 100, 50, false), &stats(5, 10, 3, true));
        assert_eq!(merged.min_value, Some(ColumnStatValue::Long(5)));
        assert_eq!(merged.max_value, Some(ColumnStatValue::Long(10)));
        assert_eq!(merged.value_count, 3);
    }

    #[test]
    fn test_merge_column_stats_tombstone_replaces() {
        let merger = MetadataPayloadMerger;
        let mut tombstone = stats(0, 0, 0, false);
        tombstone.is_deleted = true;
        let merged = merger.merge_column_stats(&stats(1, 100, 50, false), &tombstone);
        assert!(merged.is_deleted);
    }

    #[test]
    fn test_merge_file_infos_java_semantics() {
        let merger = MetadataPayloadMerger;
        let mut existing = HashMap::from([
            (
                "a.parquet".to_string(),
                HoodieMetadataFileInfo::new("a.parquet".to_string(), 100, false),
            ),
            (
                "b.parquet".to_string(),
                HoodieMetadataFileInfo::new("b.parquet".to_string(), 50, false),
            ),
        ]);
        let newer = HashMap::from([
            // Bigger size wins.
            (
                "a.parquet".to_string(),
                HoodieMetadataFileInfo::new("a.parquet".to_string(), 200, false),
            ),
            // Tombstone cancels the entry entirely.
            (
                "b.parquet".to_string(),
                HoodieMetadataFileInfo::new("b.parquet".to_string(), 0, true),
            ),
            // Orphan tombstone is carried forward.
            (
                "c.parquet".to_string(),
                HoodieMetadataFileInfo::new("c.parquet".to_string(), 0, true),
            ),
        ]);
        merger.merge_file_infos(&mut existing, &newer);
        assert_eq!(existing["a.parquet"].size, 200);
        assert!(!existing.contains_key("b.parquet"));
        assert!(existing["c.parquet"].is_deleted);
    }

    #[test]
    fn test_from_properties_rejects_unknown_strategy() {
        let props = HashMap::from([("hoodie.record.merge.strategy.id", "deadbeef")]);
        assert!(MetadataPayloadMerger::from_properties(&props).is_err());
    }

    #[test]
    fn test_from_properties_accepts_payload_based() {
        let props = HashMap::from([
            (
                "hoodie.record.merge.strategy.id",
                PAYLOAD_BASED_MERGE_STRATEGY_UUID,
            ),
            ("hoodie.compaction.payload.class", METADATA_PAYLOAD_CLASS),
        ]);
        assert!(MetadataPayloadMerger::from_properties(&props).is_ok());
    }

    #[test]
    fn test_from_properties_rejects_foreign_strategy_and_payload() {
        use std::collections::HashMap;
        let mut props: HashMap<&str, &str> = HashMap::new();
        props.insert(
            "hoodie.record.merge.strategy.id",
            "deadbeef-0000-0000-0000-000000000000",
        );
        assert!(MetadataPayloadMerger::from_properties(&props).is_err());

        let mut props: HashMap<&str, &str> = HashMap::new();
        props.insert(
            "hoodie.compaction.payload.class",
            "com.example.OtherPayload",
        );
        assert!(MetadataPayloadMerger::from_properties(&props).is_err());

        // Missing keys resolve to the default merger.
        let props: HashMap<&str, &str> = HashMap::new();
        assert!(MetadataPayloadMerger::from_properties(&props).is_ok());
    }

    #[test]
    fn test_compare_stat_values_same_and_cross_variant() {
        use crate::metadata::table::encode::ColumnStatValue as V;
        use std::cmp::Ordering;
        let cases = vec![
            (V::Boolean(false), V::Boolean(true)),
            (V::Int(1), V::Int(2)),
            (V::Long(1), V::Long(2)),
            (V::Float(1.0), V::Float(2.0)),
            (V::Double(1.0), V::Double(2.0)),
            (V::Bytes(vec![1]), V::Bytes(vec![2])),
            (V::String("a".into()), V::String("b".into())),
            (V::Date(1), V::Date(2)),
            (V::TimeMicros(1), V::TimeMicros(2)),
            (V::TimestampMicros(1), V::TimestampMicros(2)),
            (V::LocalTimestampMicros(1), V::LocalTimestampMicros(2)),
        ];
        for (a, b) in &cases {
            assert_eq!(compare_stat_values(a, b), Some(Ordering::Less));
        }
        // Cross-variant comparison is undefined -> None.
        assert_eq!(compare_stat_values(&V::Int(1), &V::Long(1)), None);
    }
}
