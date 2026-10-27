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
//! Table version 9 (1.1.x/1.2 format) writer output: version keys,
//! ordering-fields swap, MDT at v9, and V2 column/partition stats
//! (primitive wrappers + `valueType`).

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use hudi_core::config::HudiConfigs;
use hudi_core::config::table::HudiTableConfig;
use hudi_core::file_group::log_file::scanner::{LogFileScanner, ScanResult};
use hudi_core::hfile::HFileReader;
use hudi_core::metadata::table::encode::{
    ColumnStatValue, ColumnStatsMetadata, decode_column_stats_entry,
};
use hudi_core::metadata::table::hash::{column_stats_index_key, partition_stats_index_key};
use hudi_core::storage::Storage;
use hudi_core::table::Table;
use hudi_core::timeline::InstantRange;
use tempfile::tempdir;
use url::Url;

fn batch(rows: Vec<(&str, &str, i64)>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("city", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
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

async fn create_v9(dir: &Path) -> Table {
    Table::create(dir.to_str().unwrap())
        .with_table_name("trips")
        .with_table_version(9)
        .with_record_key_fields(["id"])
        .with_partition_fields(["city"])
        .with_ordering_fields(["value"])
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

/// Read all stats records of an MDT partition (bases then logs, later wins).
async fn read_stats_records(
    table: &Table,
    dir: &Path,
    mdt_partition: &str,
) -> HashMap<String, ColumnStatsMetadata> {
    let storage = table_storage(dir);
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
        if let ScanResult::HFileRecords(records) = scanner.scan(logs, &range).await.unwrap() {
            for record in &records {
                let key = record.key_as_str().unwrap().to_string();
                if let Some(stats) =
                    decode_column_stats_entry(record.value(), record.avro_schema()).unwrap()
                {
                    merged.insert(key, stats);
                }
            }
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

#[tokio::test]
async fn test_tv9_create_properties() {
    let dir = tempdir().unwrap();
    create_v9(dir.path()).await;

    let props = std::fs::read_to_string(dir.path().join(".hoodie/hoodie.properties")).unwrap();
    assert!(props.contains("hoodie.table.version=9"), "{props}");
    assert!(props.contains("hoodie.table.initial.version=9"), "{props}");
    assert!(
        props.contains("hoodie.table.ordering.fields=value"),
        "{props}"
    );
    // tv9 drops the deprecated precombine key (EightToNineUpgradeHandler).
    assert!(
        !props.contains("hoodie.table.precombine.field"),
        "tv9 must not write precombine.field:\n{props}"
    );
    // Merge strategy ids are unchanged at tv9.
    assert!(
        props.contains("hoodie.record.merge.strategy.id=eeb8d96f-b1e4-49fd-bbf8-28ac514178e5"),
        "{props}"
    );
    assert!(
        props.contains(
            "hoodie.table.metadata.partitions=column_stats,files,partition_stats,record_index"
        ),
        "{props}"
    );

    // MDT follows the data table's version.
    let mdt_props = std::fs::read_to_string(
        dir.path()
            .join(".hoodie/metadata/.hoodie/hoodie.properties"),
    )
    .unwrap();
    assert!(mdt_props.contains("hoodie.table.version=9"), "{mdt_props}");
    assert!(
        mdt_props.contains("hoodie.table.initial.version=9"),
        "{mdt_props}"
    );

    // The column_stats index definition is registered with the meta fields;
    // commits widen it to the indexed data columns.
    assert!(
        props.contains("hoodie.table.index.defs.path=.hoodie/.index_defs/index.json"),
        "{props}"
    );
    assert_eq!(
        column_stats_index_definition(dir.path()),
        (
            vec![
                "_hoodie_commit_time".to_string(),
                "_hoodie_partition_path".to_string(),
                "_hoodie_record_key".to_string()
            ],
            "V2".to_string()
        )
    );
}

/// `sourceFields` and `version` of the `column_stats` index definition.
fn column_stats_index_definition(dir: &Path) -> (Vec<String>, String) {
    let text = std::fs::read_to_string(dir.join(".hoodie/.index_defs/index.json"))
        .expect("index definitions file");
    let json: serde_json::Value = serde_json::from_str(&text).unwrap();
    let def = &json["indexDefinitions"]["column_stats"];
    assert_eq!(def["indexType"], "column_stats", "{text}");
    assert_eq!(def["indexFunction"], "column_stats", "{text}");
    let fields = def["sourceFields"]
        .as_array()
        .unwrap_or_else(|| panic!("sourceFields missing: {text}"))
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    (fields, def["version"].as_str().unwrap().to_string())
}

fn typed_batch(rows: &[(&str, i128, &[u8], i128)]) -> RecordBatch {
    use arrow_array::{BinaryArray, Decimal128Array};
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("city", DataType::Utf8, false),
        Field::new("amount", DataType::Decimal128(10, 2), false),
        Field::new("payload", DataType::Binary, false),
        Field::new("wide", DataType::Decimal128(38, 10), false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(vec!["sf"; rows.len()])),
            Arc::new(
                Decimal128Array::from(rows.iter().map(|r| r.1).collect::<Vec<_>>())
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ),
            Arc::new(BinaryArray::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
            Arc::new(
                Decimal128Array::from(rows.iter().map(|r| r.3).collect::<Vec<_>>())
                    .with_precision_and_scale(38, 10)
                    .unwrap(),
            ),
        ],
    )
    .unwrap()
}

/// Spark (AVRO record type) indexes decimal and binary columns on both index
/// versions, except decimals that do not fit V1's decimal(30,15); the index
/// definition lists the same columns.
#[tokio::test]
async fn test_decimal_and_binary_columns_are_indexed_like_spark() {
    for version in [8, 9] {
        let dir = tempdir().unwrap();
        let mut table = Table::create(dir.path().to_str().unwrap())
            .with_table_name("trips")
            .with_table_version(version)
            .with_record_key_fields(["id"])
            .with_partition_fields(["city"])
            .create()
            .await
            .unwrap();
        table
            .append([typed_batch(&[
                ("a", 10_100, &[1, 1], 5),
                ("b", 10_300, &[3, 1], 7),
            ])])
            .await
            .unwrap();
        let col_stats = read_stats_records(&table, dir.path(), "column_stats").await;
        let first = &base_file_names(dir.path(), "city=sf")[0];
        let amount = col_stats
            .get(&column_stats_index_key("city=sf", first, "amount"))
            .unwrap_or_else(|| panic!("tv{version}: missing column_stats for amount"));
        let payload = col_stats
            .get(&column_stats_index_key("city=sf", first, "payload"))
            .unwrap_or_else(|| panic!("tv{version}: missing column_stats for payload"));
        assert_eq!(
            payload.min_value,
            Some(ColumnStatValue::Bytes(vec![1, 1])),
            "tv{version}"
        );
        assert_eq!(
            payload.max_value,
            Some(ColumnStatValue::Bytes(vec![3, 1])),
            "tv{version}"
        );
        let wide_indexed =
            col_stats.contains_key(&column_stats_index_key("city=sf", first, "wide"));

        // The partition aggregate merges this file's stats, read back from
        // the MDT, with the next commit's footer stats.
        table
            .append([typed_batch(&[("c", 99_000, &[9, 1], 1)])])
            .await
            .unwrap();

        let part_stats = read_stats_records(&table, dir.path(), "partition_stats").await;
        let sf_amount = part_stats
            .get(&partition_stats_index_key("city=sf", "amount"))
            .unwrap_or_else(|| panic!("tv{version}: missing partition_stats for amount"));
        assert!(sf_amount.is_tight_bound, "tv{version}");
        // V1 decodes the decimal(30,15) wrapper; V2 the column's own scale.
        let decimal = |unscaled: i128| {
            if version == 8 {
                ColumnStatValue::Decimal {
                    unscaled: unscaled * 10_i128.pow(13),
                    precision: 30,
                    scale: 15,
                }
            } else {
                ColumnStatValue::Decimal {
                    unscaled,
                    precision: 10,
                    scale: 2,
                }
            }
        };
        assert_eq!(amount.min_value, Some(decimal(10_100)), "tv{version}");
        assert_eq!(amount.max_value, Some(decimal(10_300)), "tv{version}");
        assert_eq!(sf_amount.min_value, Some(decimal(10_100)), "tv{version}");
        assert_eq!(sf_amount.max_value, Some(decimal(99_000)), "tv{version}");
        if version == 9 {
            assert_eq!(amount.decoded_value_type_ordinal, Some(10));
            assert_eq!(payload.decoded_value_type_ordinal, Some(8));
        }

        let mut expected: Vec<String> = [
            "_hoodie_commit_time",
            "_hoodie_partition_path",
            "_hoodie_record_key",
            "id",
            "city",
            "amount",
            "payload",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        if version == 9 {
            expected.push("wide".to_string());
        }
        assert_eq!(wide_indexed, version == 9, "tv{version}");
        let (fields, index_version) = column_stats_index_definition(dir.path());
        assert_eq!(fields, expected, "tv{version}");
        assert_eq!(index_version, if version == 9 { "V2" } else { "V1" });
    }
}

/// `hoodie.metadata.index.column.stats.column.list` narrows the indexed
/// columns and the index definition to the meta fields plus the list.
#[tokio::test]
async fn test_column_stats_column_list_config() {
    let dir = tempdir().unwrap();
    create_v9(dir.path()).await;
    let mut table = Table::new_with_options(
        dir.path().to_str().unwrap(),
        [(
            "hoodie.metadata.index.column.stats.column.list",
            "value, city",
        )],
    )
    .await
    .unwrap();
    table
        .append([batch(vec![("a", "sf", 1), ("b", "sf", 3)])])
        .await
        .unwrap();

    let col_stats = read_stats_records(&table, dir.path(), "column_stats").await;
    let sf_file = &base_file_names(dir.path(), "city=sf")[0];
    for column in ["_hoodie_record_key", "value", "city"] {
        assert!(
            col_stats.contains_key(&column_stats_index_key("city=sf", sf_file, column)),
            "missing column_stats for {column}"
        );
    }
    assert!(
        !col_stats.contains_key(&column_stats_index_key("city=sf", sf_file, "id")),
        "id is not in the column list"
    );
    let part_stats = read_stats_records(&table, dir.path(), "partition_stats").await;
    assert!(part_stats.contains_key(&partition_stats_index_key("city=sf", "value")));
    assert!(!part_stats.contains_key(&partition_stats_index_key("city=sf", "id")));
    let (fields, _) = column_stats_index_definition(dir.path());
    assert_eq!(
        fields,
        [
            "_hoodie_commit_time",
            "_hoodie_partition_path",
            "_hoodie_record_key",
            "value",
            "city"
        ]
    );
}

#[tokio::test]
async fn test_tv9_column_and_partition_stats_are_v2() {
    let dir = tempdir().unwrap();
    let mut table = create_v9(dir.path()).await;
    table
        .append([batch(vec![("a", "sf", 1), ("b", "sf", 3)])])
        .await
        .unwrap();

    let col_stats = read_stats_records(&table, dir.path(), "column_stats").await;
    let sf_file = &base_file_names(dir.path(), "city=sf")[0];

    // Long column: LONG ordinal 4, values decode back as longs.
    let value_stats = col_stats
        .get(&column_stats_index_key("city=sf", sf_file, "value"))
        .expect("value column stats");
    assert_eq!(value_stats.decoded_value_type_ordinal, Some(4));
    assert_eq!(value_stats.min_value, Some(ColumnStatValue::Long(1)));
    assert_eq!(value_stats.max_value, Some(ColumnStatValue::Long(3)));

    // String meta col: STRING ordinal 7.
    let key_stats = col_stats
        .get(&column_stats_index_key(
            "city=sf",
            sf_file,
            "_hoodie_record_key",
        ))
        .expect("record key stats");
    assert_eq!(key_stats.decoded_value_type_ordinal, Some(7));
    assert_eq!(
        key_stats.min_value,
        Some(ColumnStatValue::String("a".to_string()))
    );

    // Partition stats carry valueType too.
    let part_stats = read_stats_records(&table, dir.path(), "partition_stats").await;
    let sf_value = part_stats
        .get(&partition_stats_index_key("city=sf", "value"))
        .expect("partition stats for value");
    assert_eq!(sf_value.decoded_value_type_ordinal, Some(4));
    assert_eq!(sf_value.min_value, Some(ColumnStatValue::Long(1)));
    assert_eq!(sf_value.max_value, Some(ColumnStatValue::Long(3)));
    assert!(sf_value.is_tight_bound);
}

#[tokio::test]
async fn test_tv9_upsert_round_trip_with_v2_stats() {
    let dir = tempdir().unwrap();
    let mut table = create_v9(dir.path()).await;
    table
        .upsert([batch(vec![("a", "sf", 1), ("b", "sf", 2)])])
        .await
        .unwrap();
    table.upsert([batch(vec![("a", "sf", 10)])]).await.unwrap();

    // Tight-bound recompute must decode V2 survivor stats correctly.
    let part_stats = read_stats_records(&table, dir.path(), "partition_stats").await;
    let sf_value = part_stats
        .get(&partition_stats_index_key("city=sf", "value"))
        .expect("partition stats for value");
    assert_eq!(sf_value.min_value, Some(ColumnStatValue::Long(2)));
    assert_eq!(sf_value.max_value, Some(ColumnStatValue::Long(10)));
    assert_eq!(sf_value.decoded_value_type_ordinal, Some(4));

    let rows = table
        .read(&hudi_core::table::ReadOptions::new())
        .await
        .unwrap();
    let total: usize = rows.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total, 2);
}

#[tokio::test]
async fn test_tv8_stats_remain_v1() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .with_table_version(8)
        .with_record_key_fields(["id"])
        .with_partition_fields(["city"])
        .create()
        .await
        .unwrap();
    table.append([batch(vec![("a", "sf", 1)])]).await.unwrap();

    let col_stats = read_stats_records(&table, dir.path(), "column_stats").await;
    let sf_file = &base_file_names(dir.path(), "city=sf")[0];
    let value_stats = col_stats
        .get(&column_stats_index_key("city=sf", sf_file, "value"))
        .expect("value column stats");
    assert_eq!(
        value_stats.decoded_value_type_ordinal, None,
        "tv8 records must not carry valueType"
    );
}
