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
//! MDT payload matrix tests: decode files / column_stats / partition_stats
//! records and assert Spark-writer-shaped keys, values, and layout.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use hudi_core::config::HudiConfigs;
use hudi_core::config::table::{HudiTableConfig, TableTypeValue};
use hudi_core::file_group::log_file::scanner::{LogFileScanner, ScanResult};
use hudi_core::hfile::HFileReader;
use hudi_core::metadata::table::encode::{
    ColumnStatValue, ColumnStatsMetadata, decode_column_stats_entry,
};
use hudi_core::metadata::table::hash::{column_stats_index_key, partition_stats_index_key};
use hudi_core::metadata::table::records::{FilesPartitionRecord, decode_files_partition_record};
use hudi_core::storage::Storage;
use hudi_core::table::Table;
use hudi_core::timeline::InstantRange;
use tempfile::tempdir;
use url::Url;

fn trips_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("city", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn batch(rows: Vec<(&str, &str, i64)>) -> RecordBatch {
    RecordBatch::try_new(
        trips_schema(),
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|(id, _, _)| *id).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|(_, city, _)| *city).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|(_, _, value)| *value).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

async fn create_partitioned(dir: &Path, table_type: TableTypeValue) -> Table {
    Table::create(dir.to_str().unwrap())
        .with_table_name("trips")
        .with_table_type(table_type)
        .with_record_key_fields(["id"])
        .with_partition_fields(["city"])
        .create()
        .await
        .unwrap()
}

fn table_storage(dir: &Path) -> Arc<Storage> {
    let base_url = Url::from_directory_path(dir).unwrap();
    Storage::new(
        Arc::new(HashMap::new()),
        Arc::new(HudiConfigs::new([(
            HudiTableConfig::BasePath.as_ref().to_string(),
            base_url.as_str().to_string(),
        )])),
    )
    .unwrap()
}

fn list_mdt_partition_files(dir: &Path, mdt_partition: &str) -> (Vec<String>, Vec<String>) {
    let partition_dir = dir.join(".hoodie/metadata").join(mdt_partition);
    let mut bases = Vec::new();
    let mut logs = Vec::new();
    for entry in std::fs::read_dir(partition_dir).unwrap() {
        let name = entry.unwrap().file_name().to_string_lossy().to_string();
        if name.ends_with(".hfile") {
            bases.push(format!(".hoodie/metadata/{mdt_partition}/{name}"));
        } else if name.starts_with('.') && name.contains(".log.") {
            logs.push(format!(".hoodie/metadata/{mdt_partition}/{name}"));
        }
    }
    bases.sort();
    logs.sort();
    (bases, logs)
}

/// Read all stats records of an MDT partition (bases then logs, later wins).
async fn read_stats_records(
    table: &Table,
    dir: &Path,
    mdt_partition: &str,
) -> HashMap<String, ColumnStatsMetadata> {
    let storage = table_storage(dir);
    let (bases, logs) = list_mdt_partition_files(dir, mdt_partition);
    let mut merged = HashMap::new();
    for base in bases {
        let mut reader = HFileReader::open(storage.as_ref(), &base).await.unwrap();
        for record in reader.collect_records().unwrap() {
            let key = record.key_as_str().unwrap().to_string();
            if let Some(stats) =
                decode_column_stats_entry(record.value(), record.avro_schema()).unwrap()
            {
                merged.insert(key, stats);
            }
        }
    }
    if !logs.is_empty() {
        let scanner = LogFileScanner::new(table.hudi_configs.clone(), storage);
        let range = InstantRange::up_to("99991231235959999", "UTC");
        match scanner.scan(logs, &range).await.unwrap() {
            ScanResult::HFileRecords(records) => {
                for record in &records {
                    let key = record.key_as_str().unwrap().to_string();
                    if let Some(stats) =
                        decode_column_stats_entry(record.value(), record.avro_schema()).unwrap()
                    {
                        merged.insert(key, stats);
                    }
                }
            }
            ScanResult::Empty => {}
            ScanResult::RecordBatches(_) => panic!("stats logs must be HFile blocks"),
        }
    }
    merged
}

fn base_file_names(dir: &Path, partition: &str) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir.join(partition))
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|n| n.ends_with(".parquet"))
        .collect();
    names.sort();
    names
}

fn long_stat(value: i64) -> Option<ColumnStatValue> {
    Some(ColumnStatValue::Long(value))
}

fn string_stat(value: &str) -> Option<ColumnStatValue> {
    Some(ColumnStatValue::String(value.to_string()))
}

/// Find the completed deltacommit `{requested}_{completion}.deltacommit` for a
/// requested instant and return its completion time.
fn completed_deltacommit_completion_time(timeline: &Path, requested: &str) -> String {
    let prefix = format!("{requested}_");
    let name = std::fs::read_dir(timeline)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .find(|n| n.starts_with(&prefix) && n.ends_with(".deltacommit"))
        .unwrap_or_else(|| panic!("missing completed deltacommit for {requested}"));
    name.trim_end_matches(".deltacommit")
        .split_once('_')
        .map(|(_, completion)| completion.to_string())
        .unwrap()
}

#[tokio::test]
async fn test_bootstrap_layout_partitioned_matches_spark() {
    let dir = tempdir().unwrap();
    create_partitioned(dir.path(), TableTypeValue::CopyOnWrite).await;

    let props = std::fs::read_to_string(dir.path().join(".hoodie/hoodie.properties")).unwrap();
    assert!(
        props.contains(
            "hoodie.table.metadata.partitions=column_stats,files,partition_stats,record_index"
        ),
        "partition list must be alphabetically sorted:\n{props}"
    );

    // One bootstrap deltacommit per MDT partition, Java enum order, with a
    // completion time minted at completion (> requested).
    let timeline = dir.path().join(".hoodie/metadata/.hoodie/timeline");
    for instant in [
        "00000000000000000", // files
        "00000000000000001", // column_stats
        "00000000000000002", // record_index
        "00000000000000003", // partition_stats
    ] {
        let completion = completed_deltacommit_completion_time(&timeline, instant);
        assert!(
            completion.as_str() > instant,
            "bootstrap {instant} completion {completion} must be later"
        );
    }

    // Empty-table files bootstrap: only __all_partitions__ with no entries.
    let storage = table_storage(dir.path());
    let (bases, _) = list_mdt_partition_files(dir.path(), "files");
    assert_eq!(bases.len(), 1);
    let mut reader = HFileReader::open(storage.as_ref(), &bases[0])
        .await
        .unwrap();
    let records = reader.collect_records().unwrap();
    assert_eq!(
        records.len(),
        1,
        "bootstrap files HFile must hold one record"
    );
    assert_eq!(
        records[0].key_as_str(),
        Some(FilesPartitionRecord::ALL_PARTITIONS_KEY)
    );
    let decoded = decode_files_partition_record(&reader, &records[0]).unwrap();
    assert!(decoded.partition_names().is_empty());
}

#[tokio::test]
async fn test_bootstrap_layout_unpartitioned_drops_partition_stats() {
    let dir = tempdir().unwrap();
    Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .create()
        .await
        .unwrap();

    let props = std::fs::read_to_string(dir.path().join(".hoodie/hoodie.properties")).unwrap();
    assert!(
        props.contains("hoodie.table.metadata.partitions=column_stats,files,record_index"),
        "unpartitioned tables must not enable partition_stats:\n{props}"
    );
    assert!(!dir.path().join(".hoodie/metadata/partition_stats").exists());

    let timeline = dir.path().join(".hoodie/metadata/.hoodie/timeline");
    for instant in [
        "00000000000000000", // files
        "00000000000000001", // column_stats
        "00000000000000002", // record_index
    ] {
        completed_deltacommit_completion_time(&timeline, instant);
    }
    assert!(
        !std::fs::read_dir(&timeline).unwrap().any(|e| {
            e.unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("00000000000000003_")
        }),
        "partition_stats bootstrap commit must not exist for unpartitioned tables"
    );
}

#[tokio::test]
async fn test_partition_stats_meta_columns_span_all_commits() {
    // partition_stats must aggregate the always-indexed meta columns across
    // ALL surviving files. Writers pass the raw user schema (no _hoodie_*
    // fields); if that schema decided which columns to fetch for survivors,
    // the aggregate would collapse onto the newest commit's files and Spark
    // pruning would skip partitions that still hold matching rows.
    let dir = tempdir().unwrap();
    let mut table = create_partitioned(dir.path(), TableTypeValue::CopyOnWrite).await;
    table.append([batch(vec![("aaa", "sf", 1)])]).await.unwrap();
    table.append([batch(vec![("zzz", "sf", 2)])]).await.unwrap();

    let stats = read_stats_records(&table, dir.path(), "partition_stats").await;
    let key = partition_stats_index_key("city=sf", "_hoodie_record_key");
    let record_key_stats = stats
        .get(&key)
        .expect("_hoodie_record_key must be indexed in partition_stats");
    assert_eq!(
        record_key_stats.min_value,
        string_stat("aaa"),
        "partition_stats must still cover the first commit's file"
    );
    assert_eq!(record_key_stats.max_value, string_stat("zzz"));
}

#[tokio::test]
async fn test_truncated_stats_stay_indexed_as_non_tight_bounds() {
    // Parquet truncates long min/max (64 bytes by default), so the bounds are
    // UNKNOWN. Publishing a live record with null min/max would read as
    // "column is all nulls" and make data skipping prune a file that matches;
    // omitting the record instead leaves the file in the reader's
    // not-indexed rescue set.
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .create()
        .await
        .unwrap();
    let long_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("blob", DataType::Utf8, false),
    ]));
    let long_value = "x".repeat(400);
    table
        .append([RecordBatch::try_new(
            long_schema,
            vec![
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(StringArray::from(vec![long_value.as_str()])),
            ],
        )
        .unwrap()])
        .await
        .unwrap();

    // Truncated bounds are still valid bounds (parquet truncates min down and
    // max up), so the record IS published — never tight — and pruning stays
    // correct. Suppressing it instead would half-index the file, and Spark's
    // rescue set is per file, not per column.
    let stats = read_stats_records(&table, dir.path(), "column_stats").await;
    let blob: Vec<_> = stats
        .values()
        .filter(|r| r.column_name == "blob" && !r.is_deleted)
        .collect();
    assert_eq!(blob.len(), 1, "blob must stay indexed");
    assert!(
        blob[0].min_value.is_some(),
        "truncated bounds are still bounds"
    );
    assert!(!blob[0].is_tight_bound, "truncated bounds are never tight");
    let long_value = "x".repeat(400);
    if let Some(ColumnStatValue::String(min)) = &blob[0].min_value {
        assert!(
            long_value.starts_with(min.as_str()) || min.as_str() <= long_value.as_str(),
            "truncated min must remain a valid lower bound"
        );
    }

    // partition_stats must never publish null bounds: Spark's partition
    // pruning has no not-indexed rescue set, so that would prune everything.
    if !dir.path().join(".hoodie/metadata/partition_stats").exists() {
        return;
    }
    let partition_stats = read_stats_records(&table, dir.path(), "partition_stats").await;
    for (key, record) in &partition_stats {
        if record.is_deleted {
            continue;
        }
        assert!(
            record.min_value.is_some()
                || record.max_value.is_some()
                || record.value_count == record.null_count,
            "partition_stats record with null bounds prunes the partition: {key}"
        );
    }
}

#[tokio::test]
async fn test_append_column_and_partition_stats_partitioned_cow() {
    let dir = tempdir().unwrap();
    let mut table = create_partitioned(dir.path(), TableTypeValue::CopyOnWrite).await;
    table
        .append([batch(vec![("a", "sf", 1), ("b", "sf", 3), ("c", "nyc", 7)])])
        .await
        .unwrap();

    let col_stats = read_stats_records(&table, dir.path(), "column_stats").await;
    let sf_file = &base_file_names(dir.path(), "city=sf")[0];

    // Data column ranges from the parquet footer.
    let value_key = column_stats_index_key("city=sf", sf_file, "value");
    let value_stats = col_stats.get(&value_key).expect("value column indexed");
    assert_eq!(value_stats.min_value, long_stat(1));
    assert_eq!(value_stats.max_value, long_stat(3));
    assert_eq!(value_stats.value_count, 2);
    assert_eq!(value_stats.null_count, 0);
    assert!(!value_stats.is_tight_bound);
    assert!(!value_stats.is_deleted);
    assert_eq!(&value_stats.file_name, sf_file);
    assert_eq!(value_stats.column_name, "value");

    // Java always indexes these meta fields; never seqno / file name.
    for meta_col in [
        "_hoodie_commit_time",
        "_hoodie_partition_path",
        "_hoodie_record_key",
    ] {
        let key = column_stats_index_key("city=sf", sf_file, meta_col);
        assert!(col_stats.contains_key(&key), "missing meta col {meta_col}");
    }
    let record_key_stats = col_stats
        .get(&column_stats_index_key(
            "city=sf",
            sf_file,
            "_hoodie_record_key",
        ))
        .unwrap();
    assert_eq!(
        record_key_stats.min_value,
        Some(ColumnStatValue::String("a".to_string()))
    );
    assert_eq!(
        record_key_stats.max_value,
        Some(ColumnStatValue::String("b".to_string()))
    );
    for excluded in ["_hoodie_commit_seqno", "_hoodie_file_name"] {
        let key = column_stats_index_key("city=sf", sf_file, excluded);
        assert!(
            !col_stats.contains_key(&key),
            "{excluded} must not be indexed"
        );
    }

    // Partition stats: tight-bound aggregate keyed by (column, partition).
    let part_stats = read_stats_records(&table, dir.path(), "partition_stats").await;
    let sf_value = part_stats
        .get(&partition_stats_index_key("city=sf", "value"))
        .expect("partition stats for value");
    assert_eq!(sf_value.min_value, long_stat(1));
    assert_eq!(sf_value.max_value, long_stat(3));
    assert_eq!(sf_value.value_count, 2);
    assert!(sf_value.is_tight_bound);
    assert_eq!(sf_value.file_name, "city=sf");
    let nyc_value = part_stats
        .get(&partition_stats_index_key("city=nyc", "value"))
        .unwrap();
    assert_eq!(nyc_value.min_value, long_stat(7));
    assert_eq!(nyc_value.max_value, long_stat(7));
}

#[tokio::test]
async fn test_partition_stats_tight_after_cow_upsert() {
    let dir = tempdir().unwrap();
    let mut table = create_partitioned(dir.path(), TableTypeValue::CopyOnWrite).await;
    table
        .upsert([batch(vec![("a", "sf", 1), ("b", "sf", 2)])])
        .await
        .unwrap();
    table.upsert([batch(vec![("a", "sf", 10)])]).await.unwrap();

    // Same-fileId rewrite: partition stats must be recomputed tight from the
    // new base file (a=10, b=2) — not widened to include the replaced a=1.
    let part_stats = read_stats_records(&table, dir.path(), "partition_stats").await;
    let sf_value = part_stats
        .get(&partition_stats_index_key("city=sf", "value"))
        .expect("partition stats for value");
    assert_eq!(sf_value.min_value, long_stat(2));
    assert_eq!(sf_value.max_value, long_stat(10));
    assert_eq!(sf_value.value_count, 2);
    assert!(sf_value.is_tight_bound);

    // Both base file versions keep column_stats until clean (Java parity).
    let col_stats = read_stats_records(&table, dir.path(), "column_stats").await;
    let files = base_file_names(dir.path(), "city=sf");
    assert_eq!(files.len(), 2);
    for file in &files {
        let key = column_stats_index_key("city=sf", file, "value");
        assert!(
            col_stats.contains_key(&key),
            "column_stats missing for {file}"
        );
        assert!(!col_stats[&key].is_deleted);
    }
}

#[tokio::test]
async fn test_mor_log_files_get_column_stats_and_widen_partition_stats() {
    let dir = tempdir().unwrap();
    let mut table = create_partitioned(dir.path(), TableTypeValue::MergeOnRead).await;
    table
        .upsert([batch(vec![("a", "sf", 1), ("b", "sf", 2)])])
        .await
        .unwrap();
    table.upsert([batch(vec![("a", "sf", 10)])]).await.unwrap();

    // The MOR update went to a log file; it must be indexed by its log name.
    let mut log_names: Vec<String> = std::fs::read_dir(dir.path().join("city=sf"))
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|n| n.starts_with('.') && n.contains(".log."))
        .collect();
    log_names.sort();
    assert_eq!(log_names.len(), 1, "expected one MOR log file");
    let col_stats = read_stats_records(&table, dir.path(), "column_stats").await;
    let log_value_key = column_stats_index_key("city=sf", &log_names[0], "value");
    let log_stats = col_stats
        .get(&log_value_key)
        .expect("log file column_stats");
    assert_eq!(log_stats.min_value, long_stat(10));
    assert_eq!(log_stats.max_value, long_stat(10));
    assert_eq!(log_stats.value_count, 1);
    assert_eq!(&log_stats.file_name, &log_names[0]);

    // Tight aggregate over base (1,2) + log (10) files.
    let part_stats = read_stats_records(&table, dir.path(), "partition_stats").await;
    let sf_value = part_stats
        .get(&partition_stats_index_key("city=sf", "value"))
        .unwrap();
    assert_eq!(sf_value.min_value, long_stat(1));
    assert_eq!(sf_value.max_value, long_stat(10));
    assert_eq!(sf_value.value_count, 3);
    assert!(sf_value.is_tight_bound);
}

#[tokio::test]
async fn test_dynamic_partition_overwrite_defers_deletes_and_tightens_stats() {
    let dir = tempdir().unwrap();
    let mut table = create_partitioned(dir.path(), TableTypeValue::CopyOnWrite).await;
    table
        .append([batch(vec![("a", "sf", 1), ("b", "sf", 2), ("c", "nyc", 7)])])
        .await
        .unwrap();
    table
        .dynamic_partition_overwrite([batch(vec![("d", "sf", 5)])])
        .await
        .unwrap();

    // Replaced bases stay listed (deletes deferred to clean, Java parity).
    let storage = table_storage(dir.path());
    let (bases, logs) = list_mdt_partition_files(dir.path(), "files");
    let mut files_records: HashMap<String, Vec<(String, bool)>> = HashMap::new();
    let mut all_partition_entries: Vec<(String, bool)> = Vec::new();
    {
        let mut reader = HFileReader::open(storage.as_ref(), &bases[0])
            .await
            .unwrap();
        let schema = reader.get_avro_schema().unwrap().unwrap().clone();
        let mut records = reader.collect_records().unwrap();
        if !logs.is_empty() {
            let scanner = LogFileScanner::new(table.hudi_configs.clone(), storage.clone());
            let range = InstantRange::up_to("99991231235959999", "UTC");
            if let ScanResult::HFileRecords(log_records) = scanner.scan(logs, &range).await.unwrap()
            {
                records.extend(log_records);
            }
        }
        for record in &records {
            let key = record.key_as_str().unwrap().to_string();
            let decoded =
                hudi_core::metadata::table::records::decode_files_partition_record_with_schema(
                    record, &schema,
                )
                .unwrap();
            let entries: Vec<(String, bool)> = decoded
                .all_file_names()
                .iter()
                .map(|name| (name.to_string(), !decoded.has_active_file(name)))
                .collect();
            if key == FilesPartitionRecord::ALL_PARTITIONS_KEY {
                all_partition_entries.extend(entries);
            } else {
                files_records.entry(key).or_default().extend(entries);
            }
        }
    }
    // No "." tombstone in __all_partitions__ and no replaced-file tombstones.
    assert!(
        all_partition_entries.iter().all(|(name, deleted)| {
            !deleted && name != FilesPartitionRecord::NON_PARTITIONED_NAME
        }),
        "unexpected tombstones in __all_partitions__: {all_partition_entries:?}"
    );
    let sf_entries = files_records.get("city=sf").unwrap();
    let sf_files = base_file_names(dir.path(), "city=sf");
    assert_eq!(sf_files.len(), 2, "old + new base files on storage");
    for file in &sf_files {
        assert!(
            sf_entries
                .iter()
                .any(|(name, deleted)| name == file && !deleted),
            "file {file} must stay active in MDT files until clean: {sf_entries:?}"
        );
    }

    // Tight partition stats reflect only the replacing file (d=5).
    let part_stats = read_stats_records(&table, dir.path(), "partition_stats").await;
    let sf_value = part_stats
        .get(&partition_stats_index_key("city=sf", "value"))
        .unwrap();
    assert_eq!(sf_value.min_value, long_stat(5));
    assert_eq!(sf_value.max_value, long_stat(5));
    assert_eq!(sf_value.value_count, 1);
    assert!(sf_value.is_tight_bound);
    // Untouched partition unchanged.
    let nyc_value = part_stats
        .get(&partition_stats_index_key("city=nyc", "value"))
        .unwrap();
    assert_eq!(nyc_value.min_value, long_stat(7));
}
