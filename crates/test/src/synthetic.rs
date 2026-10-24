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

//! A merge-on-read table written on the spot, for tests that need a shape the
//! shipped fixtures do not have: a base file with many row groups and log blocks
//! whose headers carry record positions.
//!
//! One non-partitioned file group: a parquet base file of `rows` records in row
//! groups of `row_group_rows`, then one Avro data block per log file updating
//! the base rows named by position, optionally followed by a delete block. Each
//! block carries the `RECORD_POSITIONS` header when asked to, the way Spark
//! writes it under `hoodie.write.record.positions`.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use roaring::RoaringTreemap;

pub const BASE_INSTANT: &str = "20250101000000000";
pub const LOG_INSTANT: &str = "20250102000000000";
pub const FILE_ID: &str = "f0000000-0000-0000-0000-000000000000-0";

const LOG_SCHEMA: &str = r#"{"type":"record","name":"SyntheticRecord","namespace":"synthetic","fields":[
{"name":"_hoodie_commit_time","type":["null","string"],"default":null},
{"name":"_hoodie_commit_seqno","type":["null","string"],"default":null},
{"name":"_hoodie_record_key","type":["null","string"],"default":null},
{"name":"_hoodie_partition_path","type":["null","string"],"default":null},
{"name":"_hoodie_file_name","type":["null","string"],"default":null},
{"name":"uuid","type":"string"},
{"name":"rider","type":"string"},
{"name":"fare","type":"double"},
{"name":"ts","type":"long"}]}"#;

/// What one log file holds.
#[derive(Debug, Clone, Default)]
pub struct LogFileSpec {
    /// Base rows (by position) the data block updates, in ascending order.
    pub updates: Vec<u64>,
    /// Base rows (by position) a delete block after the data block removes.
    pub deletes: Vec<u64>,
    /// Whether the blocks carry `RECORD_POSITIONS` headers.
    pub with_positions: bool,
}

#[derive(Debug, Clone)]
pub struct MorTableSpec {
    pub rows: u64,
    pub row_group_rows: usize,
    pub log_files: Vec<LogFileSpec>,
}

/// A written table and the names a reader needs to address its one file slice.
#[derive(Debug)]
pub struct SyntheticMorTable {
    pub base_path: PathBuf,
    pub base_file_name: String,
    pub log_file_names: Vec<String>,
}

impl SyntheticMorTable {
    pub fn base_url(&self) -> url::Url {
        url::Url::from_directory_path(&self.base_path).unwrap()
    }
}

/// A synthetic table in a directory that is removed with it.
#[derive(Debug)]
pub struct TempMorTable {
    _dir: tempfile::TempDir,
    pub table: SyntheticMorTable,
}

pub fn write_temp_mor_table(spec: &MorTableSpec) -> TempMorTable {
    let dir = tempfile::tempdir().unwrap();
    let table = write_mor_table(dir.path(), spec);
    TempMorTable { _dir: dir, table }
}

pub fn key_for(position: u64) -> String {
    format!("uuid{position:012}")
}

pub fn base_fare(position: u64) -> f64 {
    position as f64 * 1.5
}

pub fn updated_fare(position: u64) -> f64 {
    position as f64 * 9.5
}

pub fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("_hoodie_commit_time", DataType::Utf8, true),
        Field::new("_hoodie_commit_seqno", DataType::Utf8, true),
        Field::new("_hoodie_record_key", DataType::Utf8, true),
        Field::new("_hoodie_partition_path", DataType::Utf8, true),
        Field::new("_hoodie_file_name", DataType::Utf8, true),
        Field::new("uuid", DataType::Utf8, false),
        Field::new("rider", DataType::Utf8, false),
        Field::new("fare", DataType::Float64, false),
        Field::new("ts", DataType::Int64, false),
    ]))
}

fn base_batch(rows: u64, file_name: &str) -> RecordBatch {
    let keys: Vec<String> = (0..rows).map(key_for).collect();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![BASE_INSTANT; rows as usize])),
        Arc::new(StringArray::from(
            (0..rows)
                .map(|i| format!("{BASE_INSTANT}_0_{i}"))
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(keys.clone())),
        Arc::new(StringArray::from(vec![""; rows as usize])),
        Arc::new(StringArray::from(vec![file_name; rows as usize])),
        Arc::new(StringArray::from(keys)),
        Arc::new(StringArray::from(
            (0..rows).map(|i| format!("rider-{i}")).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            (0..rows).map(base_fare).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            (0..rows).map(|i| 1_000 + i as i64).collect::<Vec<_>>(),
        )),
    ];
    RecordBatch::try_new(schema(), columns).unwrap()
}

fn be32(n: u32) -> [u8; 4] {
    n.to_be_bytes()
}

fn be64(n: u64) -> [u8; 8] {
    n.to_be_bytes()
}

fn positions_header(positions: &[u64]) -> String {
    let bitmap: RoaringTreemap = positions.iter().copied().collect();
    let mut bytes = Vec::new();
    bitmap.serialize_into(&mut bytes).unwrap();
    STANDARD.encode(bytes)
}

/// Frame one log block: `#HUDI#`, an 8-byte length, format version 1, the block
/// type, the header map, the content behind its 8-byte length, an empty footer
/// and the trailing total length. The content of a data block is a 4-byte
/// block version, a 4-byte count, then each Avro record behind its length;
/// a delete block is its version then the Avro-encoded `HoodieDeleteRecordList`.
fn frame_block(block_type: u32, headers: &[(u32, String)], content: &[u8]) -> Vec<u8> {
    let mut header = Vec::new();
    header.extend_from_slice(&be32(headers.len() as u32));
    for (k, v) in headers {
        header.extend_from_slice(&be32(*k));
        header.extend_from_slice(&be32(v.len() as u32));
        header.extend_from_slice(v.as_bytes());
    }
    let mut inner = Vec::new();
    inner.extend_from_slice(&be32(1));
    inner.extend_from_slice(&be32(block_type));
    inner.extend_from_slice(&header);
    inner.extend_from_slice(&be64(content.len() as u64));
    inner.extend_from_slice(content);
    inner.extend_from_slice(&be32(0));

    let block_length = (inner.len() + 8) as u64;
    let mut out = Vec::new();
    out.extend_from_slice(b"#HUDI#");
    out.extend_from_slice(&be64(block_length));
    out.extend_from_slice(&inner);
    out.extend_from_slice(&be64(block_length + 6));
    out
}

fn data_block(updates: &[u64], with_positions: bool, seqno_base: usize) -> Vec<u8> {
    use apache_avro::types::Value;
    let schema = apache_avro::Schema::parse_str(LOG_SCHEMA).unwrap();
    let mut body = Vec::new();
    body.extend_from_slice(&be32(3));
    body.extend_from_slice(&be32(updates.len() as u32));
    for (i, &position) in updates.iter().enumerate() {
        let key = key_for(position);
        let string = |s: String| Value::Union(1, Box::new(Value::String(s)));
        let record = Value::Record(vec![
            ("_hoodie_commit_time".into(), string(LOG_INSTANT.into())),
            (
                "_hoodie_commit_seqno".into(),
                string(format!("{LOG_INSTANT}_0_{}", seqno_base + i)),
            ),
            ("_hoodie_record_key".into(), string(key.clone())),
            ("_hoodie_partition_path".into(), string(String::new())),
            ("_hoodie_file_name".into(), string(String::new())),
            ("uuid".into(), Value::String(key)),
            (
                "rider".into(),
                Value::String(format!("updated-rider-{position}")),
            ),
            ("fare".into(), Value::Double(updated_fare(position))),
            ("ts".into(), Value::Long(2_000 + position as i64)),
        ]);
        let encoded = apache_avro::to_avro_datum(&schema, record).unwrap();
        body.extend_from_slice(&be32(encoded.len() as u32));
        body.extend_from_slice(&encoded);
    }
    let mut headers = vec![
        (0u32, LOG_INSTANT.to_string()),
        (2u32, LOG_SCHEMA.to_string()),
    ];
    if with_positions {
        headers.push((5u32, positions_header(updates)));
        headers.push((8u32, BASE_INSTANT.to_string()));
    }
    frame_block(3, &headers, &body)
}

/// The wire schema of a version 3 delete block, shared with the reader.
const DELETE_RECORD_LIST_SCHEMA: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../core/schemas/HoodieDeleteRecordList.avsc"
));

/// The union branch of `orderingVal` that wraps a long, found by name so the
/// writer follows the schema file rather than a remembered position.
fn long_wrapper_branch(schema: &apache_avro::Schema) -> u32 {
    use apache_avro::Schema;
    let Schema::Record(list) = schema else {
        panic!("delete record list is a record");
    };
    let Schema::Array(array) = &list.fields[0].schema else {
        panic!("deleteRecordList is an array");
    };
    let Schema::Record(record) = array.items.as_ref() else {
        panic!("delete records are records");
    };
    let ordering = record
        .fields
        .iter()
        .find(|f| f.name == "orderingVal")
        .expect("orderingVal field");
    let Schema::Union(union) = &ordering.schema else {
        panic!("orderingVal is a union");
    };
    union
        .variants()
        .iter()
        .position(|variant| matches!(variant, Schema::Record(r) if r.name.name == "LongWrapper"))
        .expect("LongWrapper branch") as u32
}

fn delete_block(deletes: &[u64], with_positions: bool) -> Vec<u8> {
    use apache_avro::types::Value;
    let schema = apache_avro::Schema::parse_str(DELETE_RECORD_LIST_SCHEMA).unwrap();
    let long_branch = long_wrapper_branch(&schema);
    let records: Vec<Value> = deletes
        .iter()
        .map(|&position| {
            Value::Record(vec![
                (
                    "recordKey".into(),
                    Value::Union(1, Box::new(Value::String(key_for(position)))),
                ),
                (
                    "partitionPath".into(),
                    Value::Union(1, Box::new(Value::String(String::new()))),
                ),
                (
                    "orderingVal".into(),
                    Value::Union(
                        long_branch,
                        Box::new(Value::Record(vec![(
                            "value".into(),
                            Value::Long(2_000 + position as i64),
                        )])),
                    ),
                ),
            ])
        })
        .collect();
    let list = Value::Record(vec![("deleteRecordList".into(), Value::Array(records))]);
    let encoded = apache_avro::to_avro_datum(&schema, list).unwrap();
    let mut body = Vec::new();
    body.extend_from_slice(&be32(3));
    body.extend_from_slice(&be32(encoded.len() as u32));
    body.extend_from_slice(&encoded);
    let mut headers = vec![
        (0u32, LOG_INSTANT.to_string()),
        (2u32, LOG_SCHEMA.to_string()),
    ];
    if with_positions {
        headers.push((5u32, positions_header(deletes)));
        headers.push((8u32, BASE_INSTANT.to_string()));
    }
    frame_block(1, &headers, &body)
}

/// Write the table under `dir`, which must exist and be empty.
pub fn write_mor_table(dir: &Path, spec: &MorTableSpec) -> SyntheticMorTable {
    fs::create_dir_all(dir.join(".hoodie")).unwrap();

    let base_file_name = format!("{FILE_ID}_0-1-0_{BASE_INSTANT}.parquet");
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(spec.row_group_rows))
        .build();
    let file = fs::File::create(dir.join(&base_file_name)).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema(), Some(props)).unwrap();
    writer
        .write(&base_batch(spec.rows, &base_file_name))
        .unwrap();
    writer.close().unwrap();
    let base_size = fs::metadata(dir.join(&base_file_name)).unwrap().len();

    let mut log_file_names = Vec::new();
    let mut log_stats = Vec::new();
    for (index, log) in spec.log_files.iter().enumerate() {
        let version = index + 1;
        let name = format!(".{FILE_ID}_{BASE_INSTANT}.log.{version}_0-2-0");
        let mut bytes = data_block(&log.updates, log.with_positions, index * 1_000_000);
        if !log.deletes.is_empty() {
            bytes.extend(delete_block(&log.deletes, log.with_positions));
        }
        fs::write(dir.join(&name), &bytes).unwrap();
        let records = log.updates.len() + log.deletes.len();
        log_stats.push(format!(
            r#"{{"fileId":"{FILE_ID}","path":"{name}","baseFile":"{base_file_name}",
               "prevCommit":"{BASE_INSTANT}","numWrites":{records},"numDeletes":{deletes},
               "numUpdateWrites":{updates},"totalWriteBytes":{size},
               "fileSizeInBytes":{size},"partitionPath":"","tempPath":null,
               "totalLogRecords":{records},"totalLogFilesCompacted":0,
               "totalLogSizeCompacted":0,"totalLogBlocks":1,
               "totalCorruptLogBlock":0,"totalRollbackBlocks":0,"cdcStats":null,
               "minEventTime":null,"maxEventTime":null,"runtimeStats":null}}"#,
            deletes = log.deletes.len(),
            updates = log.updates.len(),
            size = bytes.len(),
        ));
        log_file_names.push(name);
    }

    let commit = format!(
        r#"{{"partitionToWriteStats":{{"":[{{"fileId":"{FILE_ID}","path":"{base_file_name}",
            "prevCommit":"null","numWrites":{rows},"numDeletes":0,"numUpdateWrites":0,
            "totalWriteBytes":{base_size},"fileSizeInBytes":{base_size},"partitionPath":"",
            "tempPath":null,"totalLogRecords":0,"totalLogFilesCompacted":0,
            "totalLogSizeCompacted":0,"totalLogBlocks":0,"totalCorruptLogBlock":0,
            "totalRollbackBlocks":0,"cdcStats":null,"minEventTime":null,"maxEventTime":null,
            "runtimeStats":null}}]}},"compacted":false,"extraMetadata":{{}},
            "operationType":"INSERT"}}"#,
        rows = spec.rows,
    );
    fs::write(dir.join(format!(".hoodie/{BASE_INSTANT}.commit")), commit).unwrap();
    if !log_stats.is_empty() {
        let delta_commit = format!(
            r#"{{"partitionToWriteStats":{{"":[{}]}},"compacted":false,
                "extraMetadata":{{}},"operationType":"UPSERT"}}"#,
            log_stats.join(",")
        );
        fs::write(
            dir.join(format!(".hoodie/{LOG_INSTANT}.deltacommit")),
            delta_commit,
        )
        .unwrap();
    }

    let props: HashMap<&str, &str> = HashMap::from([
        ("hoodie.table.name", "synthetic_mor"),
        ("hoodie.table.type", "MERGE_ON_READ"),
        ("hoodie.record.merge.mode", "COMMIT_TIME_ORDERING"),
        ("hoodie.table.version", "6"),
        ("hoodie.timeline.layout.version", "1"),
        ("hoodie.table.recordkey.fields", "uuid"),
        ("hoodie.table.precombine.field", "ts"),
        ("hoodie.archivelog.folder", "archived"),
        ("hoodie.datasource.write.drop.partition.columns", "false"),
        ("hoodie.table.keygenerator.type", "NON_PARTITION"),
    ]);
    let body: String = props.iter().map(|(k, v)| format!("{k}={v}\n")).collect();
    fs::write(dir.join(".hoodie/hoodie.properties"), body).unwrap();

    SyntheticMorTable {
        base_path: dir.to_path_buf(),
        base_file_name,
        log_file_names,
    }
}
