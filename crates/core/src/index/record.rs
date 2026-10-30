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
//! Record-level index backed by the metadata table `record_index` partition.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::Result;
use crate::config::read::HudiReadConfig;
use crate::error::CoreError;
use crate::file_group::base_file::BaseFile;
use crate::file_group::base_file::reader::{KeyMatcher, KeyPredicate};
use crate::file_group::log_file::scanner::LogFileScanner;
use crate::hfile::{HFileReader, HFileRecord};
use crate::index::{HoodieIndex, HoodieKey, RecordLocation};
use crate::metadata::table::encode::decode_record_index_entry;
use crate::metadata::table::records::MetadataPartitionType;
use crate::storage::Storage;
use crate::storage::reader::{HFILE_WHOLE_READ_WITH_KEYS_MAX_SIZE, hfile_whole_read_max_size};
use crate::table::Table;
use crate::timeline::selector::InstantRange;
use crate::util::concurrency::bounded_in_order;
use crate::write::metadata::{
    DEFAULT_RLI_NUM_FILE_GROUPS, RECORD_INDEX_FILE_ID_PREFIX, epoch_millis_to_instant,
    map_record_key_to_file_group_index, mdt_partition_latest_slices, record_index_file_id,
    shard_count_from_slices,
};

/// Metadata-table record-level index (global: key = record key).
#[derive(Clone, Debug, Default)]
pub struct RecordIndex;

impl HoodieIndex for RecordIndex {
    async fn tag_location(
        &self,
        table: &Table,
        keys: &[HoodieKey],
    ) -> Result<HashMap<HoodieKey, Option<RecordLocation>>> {
        let mut locations = keys
            .iter()
            .cloned()
            .map(|key| (key, None))
            .collect::<HashMap<_, _>>();
        if keys.is_empty() {
            return Ok(locations);
        }

        let storage = table.file_system_view.storage.clone();
        let record_keys: Vec<&str> = keys.iter().map(|key| key.record_key.as_str()).collect();
        let found = lookup_record_index(table, storage, &record_keys).await?;
        for key in keys {
            if let Some(entry) = found.get(&key.record_key) {
                locations.insert(
                    key.clone(),
                    Some(RecordLocation {
                        file_id: entry.file_id.clone(),
                        instant_time: epoch_millis_to_instant(entry.instant_time_millis),
                        partition_path: entry.partition_path.clone(),
                    }),
                );
            }
        }
        Ok(locations)
    }
}

struct LoadedLocation {
    file_id: String,
    partition_path: String,
    instant_time_millis: i64,
}

/// Locations of `record_keys` in the index's latest state.
async fn lookup_record_index(
    table: &Table,
    storage: Arc<Storage>,
    record_keys: &[&str],
) -> Result<HashMap<String, LoadedLocation>> {
    Ok(lookup_as_of(table, storage, record_keys, None)
        .await?
        .unwrap_or_default())
}

/// The file groups holding `record_keys` in the snapshot whose latest commit
/// is `as_of`, or `None` when the index cannot answer for that snapshot.
pub(crate) async fn locate_file_groups(
    table: &Table,
    record_keys: &[&str],
    as_of: &str,
) -> Result<Option<HashSet<(String, String)>>> {
    let storage = table.file_system_view.storage.clone();
    Ok(lookup_as_of(table, storage, record_keys, Some(as_of))
        .await?
        .map(|found| {
            found
                .into_values()
                .map(|location| (location.partition_path, location.file_id))
                .collect()
        }))
}

/// Locations of `record_keys`, read from only the index shards and HFile
/// blocks that can hold them; with `as_of`, as of the snapshot whose latest
/// commit that is, and `None` when the index cannot describe it.
///
/// Keys are hashed to their `record_index` file group the way the writer
/// placed them, so a lookup opens the touched shards rather than the whole
/// partition, and inside a shard the HFile block index narrows the read to the
/// blocks the keys fall in. Values are Avro-decoded only for the requested keys.
async fn lookup_as_of(
    table: &Table,
    storage: Arc<Storage>,
    record_keys: &[&str],
    as_of: Option<&str>,
) -> Result<Option<HashMap<String, LoadedLocation>>> {
    let partition = MetadataPartitionType::RecordIndex.partition_name();
    // Slice-resolved: only each file group's LIVE slice (multi-base aware
    // after a Spark MDT compaction), logs in version order. One listing serves
    // both the shard modulus and the routing, so the two cannot disagree.
    let slices = mdt_partition_latest_slices(storage.as_ref(), partition).await?;
    if let Some(as_of) = as_of {
        // An empty partition answers every key with "absent", and a base file
        // compacted after the snapshot may hold later commits' changes, which
        // no instant filter can take back out.
        let compacted_after = |base: &str| {
            base.rsplit('/')
                .next()
                .and_then(|name| name.parse::<BaseFile>().ok())
                .is_none_or(|base| base.commit_timestamp.as_str() > as_of)
        };
        if slices.is_empty()
            || slices
                .iter()
                .any(|(_, base, _)| base.as_deref().is_some_and(compacted_after))
        {
            return Ok(None);
        }
    }
    let num_groups = shard_count_from_slices(
        &slices,
        RECORD_INDEX_FILE_ID_PREFIX,
        DEFAULT_RLI_NUM_FILE_GROUPS,
    )?;
    let mut keys_by_shard: HashMap<String, Vec<String>> = HashMap::new();
    for key in record_keys {
        let shard = map_record_key_to_file_group_index(key, num_groups);
        keys_by_shard
            .entry(record_index_file_id(shard))
            .or_default()
            .push((*key).to_string());
    }
    let touched: Vec<(KeyPredicate, Option<String>, Vec<String>)> = slices
        .into_iter()
        .filter_map(|(file_id, base, logs)| {
            keys_by_shard
                .remove(&file_id)
                .map(|keys| (KeyPredicate::Keys(keys), base, logs))
        })
        .collect();
    // Shard numbers are contiguous, but a listed id spelled other than
    // `record-index-NNNN-0` would still leave its keys unread, and an absent
    // key turns an upsert into a duplicate insert.
    if let Some(missing) = keys_by_shard.keys().next() {
        return Err(CoreError::MetadataTable(format!(
            "record keys route to record index file group {missing}, which is not in the record_index partition ({num_groups} file groups listed)"
        )));
    }

    // Only trust log blocks whose instant completed on the data timeline
    // (Java getValidInstantTimestamps): orphan MDT commits are skipped.
    let range = if touched.iter().any(|(_, _, logs)| !logs.is_empty()) {
        let mut valid = table.valid_metadata_instants().await?;
        if let Some(as_of) = as_of {
            // The snapshot holds the commits that had completed by `as_of` when
            // the table was opened; one pending then may have completed since.
            valid.retain(|instant| {
                instant.as_str() <= as_of && !table.timeline.pending_instants.contains(instant)
            });
        }
        Some(InstantRange::exact_match(valid, "UTC"))
    } else {
        None
    };
    // A Spark-compacted shard can be large; read whole only below the
    // threshold the base file reader uses for a keyed read.
    let whole_below =
        hfile_whole_read_max_size(&table.hudi_configs)?.min(HFILE_WHOLE_READ_WITH_KEYS_MAX_SIZE);

    // Shards hold disjoint keys, so each is folded on its own and the maps
    // are combined afterwards, with the same fan-out ceiling as a table read.
    let concurrency: usize = table
        .hudi_configs
        .get_or_default(HudiReadConfig::FileSliceReadConcurrency)
        .into();
    let range = range.as_ref();
    let per_shard = bounded_in_order(&touched, concurrency, |(predicate, base, logs)| {
        let storage = storage.clone();
        async move {
            let matcher = predicate.matcher();
            let mut merged: HashMap<String, LoadedLocation> = HashMap::new();
            if let Some(base) = base {
                let reader = HFileReader::open_sized(storage.as_ref(), base, whole_below, None)
                    .await
                    .map_err(|e| {
                        CoreError::HFile(format!("failed to open record_index base: {e:?}"))
                    })?;
                let blocks = reader.blocks_for_predicate(predicate);
                // A scattered batch can select most blocks of a large shard, so
                // they are fetched a window at a time. A resident file is under
                // the keyed whole-read bound and needs no windows.
                let windows = match reader.window_budget() {
                    Some(budget) => HFileReader::plan_windows(&blocks, budget),
                    None => vec![blocks],
                };
                for window in windows {
                    let records = reader.read_records_batched(&window).await.map_err(|e| {
                        CoreError::HFile(format!("failed to read record_index base: {e:?}"))
                    })?;
                    apply_hfile_records(&mut merged, &records, &matcher)?;
                }
            }
            if let (false, Some(range)) = (logs.is_empty(), range) {
                let scanner = LogFileScanner::new(table.hudi_configs.clone(), storage)
                    .with_key_predicate(Some(predicate.clone()));
                let records = scanner.scan_hfile_records(logs.clone(), range).await?;
                apply_hfile_records(&mut merged, &records, &matcher)?;
            }
            Ok::<_, CoreError>(merged)
        }
    })
    .await?;
    Ok(Some(per_shard.into_iter().flatten().collect()))
}

/// Fold `records` into `merged`, later records overriding earlier ones and a
/// tombstone removing the key.
///
/// A record `matcher` does not admit is dropped before its value is decoded: a
/// selected base block holds every key in its range, and a delete block names
/// every key its commit removed.
fn apply_hfile_records(
    merged: &mut HashMap<String, LoadedLocation>,
    records: &[HFileRecord],
    matcher: &KeyMatcher<'_>,
) -> Result<()> {
    for record in records {
        let key = record
            .key_as_str()
            .ok_or_else(|| CoreError::MetadataTable("record_index key must be utf8".to_string()))?;
        if !matcher.admits(key) {
            continue;
        }
        match decode_record_index_entry(key, record.value(), record.avro_schema())? {
            Some(entry) => {
                merged.insert(
                    key.to_string(),
                    LoadedLocation {
                        file_id: entry.file_id,
                        partition_path: entry.partition_path,
                        instant_time_millis: entry.instant_time_millis,
                    },
                );
            }
            None => {
                merged.remove(key);
            }
        }
    }
    Ok(())
}

/// Whether the table's record index is available for use.
///
/// Only `hoodie.table.metadata.partitions` can answer this: it is written after
/// the partition is actually bootstrapped, so it means "initialized", not
/// "wanted". Java gates on the same property (`isMetadataPartitionAvailable`)
/// and falls back to a global simple index otherwise. Honoring the write-side
/// wish `hoodie.metadata.record.index.enable` here would select an index that
/// was never built: every lookup would miss, so upserts would duplicate rows
/// and deletes would silently do nothing. That key still enables the partition
/// at table-create time, where it is bootstrapped for real.
pub fn is_record_index_enabled(table: &Table) -> bool {
    use crate::config::table::HudiTableConfig::MetadataTablePartitions;
    let from_partitions: Vec<String> = table
        .hudi_configs
        .get_or_default(MetadataTablePartitions)
        .into();
    from_partitions.iter().any(|p| p == "record_index")
}

/// Whether the record index is both advertised and actually populated.
///
/// Java requires BOTH (`isMetadataPartitionAvailable` AND a non-zero file
/// group count) before tagging through the RLI, falling back to a global index
/// otherwise "so that tagLocation is still accurate and there are no
/// duplicates". A partition that advertises the index but has no file groups
/// would answer every lookup with "not found", turning updates into duplicate
/// inserts and deletes into no-ops.
pub(crate) async fn record_index_has_file_groups(table: &Table) -> bool {
    if !is_record_index_enabled(table) {
        return false;
    }
    crate::write::metadata::mdt_partition_latest_slices(
        table.file_system_view.storage.as_ref(),
        MetadataPartitionType::RecordIndex.partition_name(),
    )
    .await
    .map(|slices| !slices.is_empty())
    .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hfile::HFileReader;
    use crate::metadata::table::encode::decode_record_index_entry;

    /// A missing shard also shrinks the modulus, so the keys it misroutes are
    /// mostly ones that live in intact shards, not ones in the missing shard.
    #[tokio::test]
    async fn test_lookup_errors_when_a_shard_is_missing() {
        use arrow_array::{Int64Array, RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema};

        let dir = tempfile::tempdir().unwrap();
        let mut table = Table::create(dir.path().to_str().unwrap())
            .with_table_name("t")
            .with_record_key_fields(["id"])
            .create()
            .await
            .unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["d"])),
                Arc::new(Int64Array::from(vec![1])),
            ],
        )
        .unwrap();
        table.append([batch]).await.unwrap();
        // `d` lives in shard 0 of 10, and mod 9 it would route to shard 1,
        // which is intact.
        assert_eq!(map_record_key_to_file_group_index("d", 10), 0);
        assert_eq!(map_record_key_to_file_group_index("d", 9), 1);
        let storage = table.file_system_view.storage.clone();
        let found = lookup_record_index(&table, storage, &["d"]).await.unwrap();
        assert!(found.contains_key("d"));

        let rli = dir.path().join(".hoodie/metadata/record_index");
        for entry in std::fs::read_dir(&rli).unwrap() {
            let path = entry.unwrap().path();
            if path.to_string_lossy().contains("record-index-0005-") {
                std::fs::remove_file(path).unwrap();
            }
        }
        let table = Table::new(dir.path().to_str().unwrap()).await.unwrap();
        let storage = table.file_system_view.storage.clone();
        let err = lookup_record_index(&table, storage, &["d"])
            .await
            .err()
            .unwrap()
            .to_string();
        assert!(err.contains("missing shards [5]"), "{err}");
    }

    #[tokio::test]
    async fn test_lookup_reads_selected_blocks_of_multi_block_spark_base() {
        let dir = tempfile::tempdir().unwrap();
        Table::create(dir.path().to_str().unwrap())
            .with_table_name("t")
            .with_record_key_fields(["id"])
            .create()
            .await
            .unwrap();
        let mdt = dir.path().join(".hoodie/metadata");
        let rli = mdt.join("record_index");
        for entry in std::fs::read_dir(&rli).unwrap() {
            let p = entry.unwrap().path();
            let name = p.file_name().unwrap().to_string_lossy().to_string();
            if name.starts_with("record-index-") && !name.starts_with("record-index-0000-") {
                std::fs::remove_file(&p).unwrap();
            }
        }
        let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(
            "../test/data/metadata_multi_block_hfile/record-index-0009-0_10-71-248_20260830075534042.hfile",
        );
        let bytes = std::fs::read(&fixture).unwrap();
        let compaction_instant = crate::write::append::generate_instant_time().await;
        std::fs::write(
            rli.join(format!(
                "record-index-0000-0_0-0-0_{compaction_instant}.hfile"
            )),
            &bytes,
        )
        .unwrap();
        let timeline = mdt.join(".hoodie/timeline");
        let latest = std::fs::read_dir(&timeline)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.to_string_lossy().ends_with(".deltacommit"))
            .max()
            .unwrap();
        std::fs::copy(
            latest,
            timeline.join(format!("{compaction_instant}_{compaction_instant}.commit")),
        )
        .unwrap();
        let table = Table::new(dir.path().to_str().unwrap()).await.unwrap();
        let storage = table.file_system_view.storage.clone();

        let mut reader = HFileReader::new(bytes.clone()).unwrap();
        let entries = reader.data_block_entries();
        let mut firsts = Vec::new();
        let mut lasts = Vec::new();
        for e in &entries {
            let recs = reader.records_in_block(e, |_| true).unwrap();
            firsts.push(recs.first().unwrap().key_as_str().unwrap().to_string());
            lasts.push(recs.last().unwrap().key_as_str().unwrap().to_string());
        }
        let all = reader.collect_records().unwrap();
        let mut expected = HashMap::new();
        for r in &all {
            let k = r.key_as_str().unwrap().to_string();
            let e = decode_record_index_entry(&k, r.value(), r.avro_schema())
                .unwrap()
                .unwrap();
            expected.insert(k, (e.file_id, e.partition_path, e.instant_time_millis));
        }
        // A Spark-compacted shard above the whole-read threshold, so the
        // base is opened in ranges and only the selected blocks are fetched.
        assert!(entries.len() > 1);
        assert!(bytes.len() as u64 > crate::storage::reader::HFILE_WHOLE_READ_WITH_KEYS_MAX_SIZE);
        // Block boundary keys, one at a time (the exact-separator path).
        for k in firsts.iter().chain(lasts.iter()) {
            let n = lookup_record_index(&table, storage.clone(), &[k.as_str()])
                .await
                .unwrap();
            assert_eq!(
                n.get(k).map(|l| (
                    l.file_id.clone(),
                    l.partition_path.clone(),
                    l.instant_time_millis
                )),
                expected.get(k).cloned(),
                "boundary {k}"
            );
        }
        // Every key at once.
        let keys: Vec<&str> = expected.keys().map(String::as_str).collect();
        let n = lookup_record_index(&table, storage.clone(), &keys)
            .await
            .unwrap();
        assert_eq!(n.len(), expected.len());
        for (k, v) in &expected {
            let got = n.get(k).map(|l| {
                (
                    l.file_id.clone(),
                    l.partition_path.clone(),
                    l.instant_time_millis,
                )
            });
            assert_eq!(got.as_ref(), Some(v), "key {k}");
        }
        // Keys just below the first key and between blocks must miss, not error.
        let below = format!("{}\u{0}", &firsts[0][..firsts[0].len() - 1]);
        let n = lookup_record_index(&table, storage.clone(), &["", below.as_str(), "\u{10FFFF}"])
            .await
            .unwrap();
        assert!(n.is_empty());
    }
}
