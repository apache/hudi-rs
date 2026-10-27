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
//! Merge-on-read reads of log blocks built byte by byte, for block shapes no
//! checked-in fixture carries: an older log block version and an Avro `enum`
//! column that the base file stores as a string.

use std::fs;
use std::path::Path;
use std::sync::Arc;

use apache_avro::types::Value;
use arrow::compute::concat_batches;
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use hudi_core::error::Result;
use hudi_core::table::{ReadOptions, Table};
use parquet::arrow::ArrowWriter;

const BASE_INSTANT: &str = "20250101000000000";
const LOG_INSTANT: &str = "20250102000000000";
const FILE_ID: &str = "f0000000-0000-0000-0000-000000000000-0";

const AVRO_SCHEMA: &str = r#"{"type":"record","name":"EnumRecord","namespace":"test","fields":[
{"name":"_hoodie_commit_time","type":["null","string"],"default":null},
{"name":"_hoodie_commit_seqno","type":["null","string"],"default":null},
{"name":"_hoodie_record_key","type":["null","string"],"default":null},
{"name":"_hoodie_partition_path","type":["null","string"],"default":null},
{"name":"_hoodie_file_name","type":["null","string"],"default":null},
{"name":"id","type":"string"},
{"name":"kind","type":{"type":"enum","name":"Kind","symbols":["A","B","C"]}},
{"name":"ts","type":"long"}]}"#;

const SYMBOLS: [&str; 3] = ["A", "B", "C"];

fn meta(v: &str) -> Value {
    Value::Union(1, Box::new(Value::String(v.to_string())))
}

fn log_record(id: &str, kind: &str, ts: i64) -> Value {
    let idx = SYMBOLS.iter().position(|s| *s == kind).unwrap() as u32;
    Value::Record(vec![
        ("_hoodie_commit_time".into(), meta(LOG_INSTANT)),
        (
            "_hoodie_commit_seqno".into(),
            meta(&format!("{LOG_INSTANT}_0_{id}")),
        ),
        ("_hoodie_record_key".into(), meta(id)),
        ("_hoodie_partition_path".into(), meta("")),
        ("_hoodie_file_name".into(), meta("")),
        ("id".into(), Value::String(id.to_string())),
        ("kind".into(), Value::Enum(idx, kind.to_string())),
        ("ts".into(), Value::Long(ts)),
    ])
}

/// One log file holding a single Avro data block stamped `block_version`.
fn write_log_file(path: &Path, block_version: u32, records: &[(&str, &str, i64)]) -> usize {
    let schema = apache_avro::Schema::parse_str(AVRO_SCHEMA).unwrap();
    let mut content = Vec::new();
    content.extend_from_slice(&block_version.to_be_bytes());
    content.extend_from_slice(&(records.len() as u32).to_be_bytes());
    for (id, kind, ts) in records {
        let datum = apache_avro::to_avro_datum(&schema, log_record(id, kind, *ts)).unwrap();
        content.extend_from_slice(&(datum.len() as u32).to_be_bytes());
        content.extend_from_slice(&datum);
    }

    let mut inner = Vec::new();
    inner.extend_from_slice(&1u32.to_be_bytes()); // log format version
    inner.extend_from_slice(&3u32.to_be_bytes()); // AvroData block
    inner.extend_from_slice(&2u32.to_be_bytes()); // header entries
    for (key, value) in [(0u32, LOG_INSTANT), (2u32, AVRO_SCHEMA)] {
        inner.extend_from_slice(&key.to_be_bytes());
        inner.extend_from_slice(&(value.len() as u32).to_be_bytes());
        inner.extend_from_slice(value.as_bytes());
    }
    inner.extend_from_slice(&(content.len() as u64).to_be_bytes());
    inner.extend_from_slice(&content);
    inner.extend_from_slice(&0u32.to_be_bytes()); // footer entries

    let block_length = (inner.len() + 8) as u64;
    let mut out = Vec::new();
    out.extend_from_slice(b"#HUDI#");
    out.extend_from_slice(&block_length.to_be_bytes());
    out.extend_from_slice(&inner);
    out.extend_from_slice(&(block_length + 6).to_be_bytes());
    fs::write(path, &out).unwrap();
    out.len()
}

fn write_base_file(path: &Path, rows: &[(&str, &str, i64)]) -> u64 {
    let utf8 = |name| Field::new(name, DataType::Utf8, true);
    let schema = Arc::new(Schema::new(vec![
        utf8("_hoodie_commit_time"),
        utf8("_hoodie_commit_seqno"),
        utf8("_hoodie_record_key"),
        utf8("_hoodie_partition_path"),
        utf8("_hoodie_file_name"),
        Field::new("id", DataType::Utf8, false),
        Field::new("kind", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, false),
    ]));
    let strings = |f: &dyn Fn(&(&str, &str, i64)) -> String| -> ArrayRef {
        Arc::new(StringArray::from(rows.iter().map(f).collect::<Vec<_>>()))
    };
    let columns = vec![
        strings(&|_| BASE_INSTANT.to_string()),
        strings(&|r| format!("{BASE_INSTANT}_0_{}", r.0)),
        strings(&|r| r.0.to_string()),
        strings(&|_| String::new()),
        strings(&|_| String::new()),
        strings(&|r| r.0.to_string()),
        strings(&|r| r.1.to_string()),
        Arc::new(Int64Array::from(
            rows.iter().map(|r| r.2).collect::<Vec<_>>(),
        )) as ArrayRef,
    ];
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    let mut writer = ArrowWriter::try_new(fs::File::create(path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    fs::metadata(path).unwrap().len()
}

fn write_stat(path: &str, base_file: &str, size: u64) -> serde_json::Value {
    serde_json::json!({
        "fileId": FILE_ID, "path": path, "baseFile": base_file, "prevCommit": BASE_INSTANT,
        "numWrites": 0, "numDeletes": 0, "numUpdateWrites": 0, "totalWriteBytes": size,
        "fileSizeInBytes": size, "partitionPath": "", "totalLogBlocks": 0,
    })
}

fn write_commit(root: &Path, file_name: &str, operation: &str, stat: serde_json::Value) {
    let commit = serde_json::json!({
        "partitionToWriteStats": { "": [stat] },
        "compacted": false,
        "extraMetadata": { "schema": AVRO_SCHEMA },
        "operationType": operation,
    });
    fs::write(root.join(".hoodie").join(file_name), commit.to_string()).unwrap();
}

/// A version 6 merge-on-read table: base rows `k0` and `k1`, then one log block
/// that updates `k1` and inserts `k2`, all carrying the enum column `kind`.
fn create_table(root: &Path, block_version: u32) {
    fs::create_dir_all(root.join(".hoodie")).unwrap();
    let base_name = format!("{FILE_ID}_0-1-0_{BASE_INSTANT}.parquet");
    let base_size = write_base_file(&root.join(&base_name), &[("k0", "A", 1), ("k1", "B", 1)]);
    write_commit(
        root,
        &format!("{BASE_INSTANT}.commit"),
        "INSERT",
        write_stat(&base_name, &base_name, base_size),
    );

    let log_name = format!(".{FILE_ID}_{BASE_INSTANT}.log.1_0-2-0");
    let log_size = write_log_file(
        &root.join(&log_name),
        block_version,
        &[("k1", "C", 2), ("k2", "A", 2)],
    );
    write_commit(
        root,
        &format!("{LOG_INSTANT}.deltacommit"),
        "UPSERT",
        write_stat(&log_name, &base_name, log_size as u64),
    );

    let props = [
        "hoodie.table.name=log_block_compat",
        "hoodie.table.type=MERGE_ON_READ",
        "hoodie.table.version=6",
        "hoodie.timeline.layout.version=1",
        "hoodie.table.recordkey.fields=id",
        "hoodie.table.precombine.field=ts",
        "hoodie.record.merge.mode=COMMIT_TIME_ORDERING",
        "hoodie.table.keygenerator.type=NON_PARTITION",
        "hoodie.datasource.write.drop.partition.columns=false",
    ];
    fs::write(root.join(".hoodie/hoodie.properties"), props.join("\n")).unwrap();
}

async fn read_id_and_kind(root: &Path) -> Result<Vec<(String, String)>> {
    let table = Table::new(root.to_str().unwrap()).await?;
    let batches = table.read(&ReadOptions::new()).await?;
    let batch = concat_batches(&batches[0].schema(), &batches)?;
    assert_eq!(
        batch.schema().field_with_name("kind")?.data_type(),
        &DataType::Utf8
    );
    let column = |name| {
        batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .clone()
    };
    let (ids, kinds) = (column("id"), column("kind"));
    let mut rows: Vec<(String, String)> = (0..batch.num_rows())
        .map(|i| (ids.value(i).to_string(), kinds.value(i).to_string()))
        .collect();
    rows.sort();
    Ok(rows)
}

fn expected() -> Vec<(String, String)> {
    [("k0", "A"), ("k1", "C"), ("k2", "A")]
        .iter()
        .map(|(id, kind)| (id.to_string(), kind.to_string()))
        .collect()
}

/// A log update and a log insert on an enum column merge onto a base file that
/// stores the column as a string.
#[tokio::test]
async fn test_mor_read_merges_enum_column_from_log_block() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    create_table(dir.path(), 3);
    assert_eq!(read_id_and_kind(dir.path()).await?, expected());
    Ok(())
}

/// Writers before table version 6 stamp their Avro data blocks V2. A table
/// upgraded from one keeps those blocks until compaction, and they merge the
/// same as V3.
#[tokio::test]
async fn test_mor_read_merges_block_version_2_avro_data_block() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    create_table(dir.path(), 2);
    assert_eq!(read_id_and_kind(dir.path()).await?, expected());
    Ok(())
}
