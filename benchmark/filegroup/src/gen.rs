// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! `fg-gen` — generate a copy-on-write Hudi table of a target size.
//!
//! Exists because the memory benchmark needs roughly a gigabyte spread over
//! several file groups, and a gigabyte cannot be committed to the repository —
//! the checked-in fixtures are tens of kilobytes. So the data is produced on
//! demand and thrown away.
//!
//! The table is **table version 6, timeline layout 1**, whose commit metadata is
//! JSON. Version 8 encodes it as Avro, which a generator would have to
//! reimplement; the read path under test does not depend on which, so the
//! simpler one is the honest choice.
//!
//! ```text
//! cargo run --release -p fg-bench --bin fg-gen -- \
//!     --out /tmp/bench_table --files 10 --total-bytes 1073741824
//! ```

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use clap::Parser;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

#[derive(Parser, Debug)]
#[command(name = "fg-gen", about = "Generate a Hudi table of a target size")]
struct Args {
    /// Directory to create the table in. Must not already exist.
    #[arg(long)]
    out: String,
    /// Number of file groups, one base file each.
    #[arg(long, default_value_t = 10)]
    files: usize,
    /// Approximate total size of the base files, in bytes.
    #[arg(long, default_value_t = 1024 * 1024 * 1024)]
    total_bytes: u64,
    /// Log files to write per file group. 0 keeps the table copy-on-write; any
    /// positive number makes it merge-on-read, so the read builds a merge map --
    /// the structure `hoodie.memory.merge.max.size` bounds and which a streaming
    /// read does not avoid.
    #[arg(long, default_value_t = 0)]
    log_files: usize,
    /// Shift the log records' key range by this many keys, making them inserts
    /// rather than updates.
    ///
    /// Zero (the default) reuses the base file's keys, so every log record
    /// updates a base row and the merged row count equals the base row count.
    /// A table generated that way cannot show whether excluding the base file
    /// worked: excluding it leaves updates with nothing to update, so the read
    /// returns nothing either way. Offsetting past the base's key range gives
    /// log records that survive on their own.
    #[arg(long, default_value_t = 0)]
    log_key_offset: u64,

    /// Records per log block. Each log file gets one block.
    #[arg(long, default_value_t = 200_000)]
    log_records: usize,
    /// Rows per parquet row group. Smaller groups let a reader release memory
    /// sooner, so this is the knob that makes a bounded read possible at all.
    #[arg(long, default_value_t = 100_000)]
    row_group_rows: usize,
}

const INSTANT: &str = "20250101000000000";
/// The log files are written at a later instant than the base file, so their
/// records win under commit-time ordering and the merge actually has work to do.
const LOG_INSTANT: &str = "20250102000000000";

/// Avro JSON for the generated schema. Written into each log block's header,
/// which is where the reader takes the writer schema from.
const LOG_SCHEMA: &str = r#"{"type":"record","name":"FgBenchRecord","namespace":"fg","fields":[
{"name":"_hoodie_commit_time","type":["null","string"],"default":null},
{"name":"_hoodie_commit_seqno","type":["null","string"],"default":null},
{"name":"_hoodie_record_key","type":["null","string"],"default":null},
{"name":"_hoodie_partition_path","type":["null","string"],"default":null},
{"name":"_hoodie_file_name","type":["null","string"],"default":null},
{"name":"uuid","type":"string"},
{"name":"rider","type":"string"},
{"name":"fare","type":"double"},
{"name":"ts","type":"long"}]}"#;

fn be32(n: u32) -> [u8; 4] {
    n.to_be_bytes()
}
fn be64(n: u64) -> [u8; 8] {
    n.to_be_bytes()
}

/// One Hudi log file holding a single Avro data block.
///
/// Framing, read off the decoder rather than guessed: the outer block is
/// `#HUDI#`, an 8-byte length, version 1, block type 3 (`AvroData`), the header
/// map, an 8-byte content length, the content, a footer count and a trailing
/// total length. The content itself is a 4-byte log-block version (V3 = 3), a
/// 4-byte record count, then each record length-prefixed.
fn write_log_file(path: &Path, start_key: u64, records: usize) -> u64 {
    use apache_avro::types::Value;

    let schema = apache_avro::Schema::parse_str(LOG_SCHEMA).expect("log schema");
    let mut body = Vec::new();
    body.extend_from_slice(&be32(3)); // log block version V3
    body.extend_from_slice(&be32(records as u32));
    for i in 0..records {
        let key = format!("uuid{:012}", start_key + i as u64);
        let rec = Value::Record(vec![
            (
                "_hoodie_commit_time".into(),
                Value::Union(1, Box::new(Value::String(LOG_INSTANT.into()))),
            ),
            (
                "_hoodie_commit_seqno".into(),
                Value::Union(1, Box::new(Value::String(format!("{LOG_INSTANT}_0_{i}")))),
            ),
            (
                "_hoodie_record_key".into(),
                Value::Union(1, Box::new(Value::String(key.clone()))),
            ),
            (
                "_hoodie_partition_path".into(),
                Value::Union(1, Box::new(Value::String(String::new()))),
            ),
            (
                "_hoodie_file_name".into(),
                Value::Union(1, Box::new(Value::String(String::new()))),
            ),
            ("uuid".into(), Value::String(key)),
            (
                "rider".into(),
                Value::String(format!("updated-rider-{:08}-padding", start_key + i as u64)),
            ),
            (
                "fare".into(),
                Value::Double((start_key + i as u64) as f64 * 9.5),
            ),
            (
                "ts".into(),
                Value::Long(1_800_000_000_000 + (start_key + i as u64) as i64),
            ),
        ]);
        let encoded = apache_avro::to_avro_datum(&schema, rec).expect("avro encode");
        body.extend_from_slice(&be32(encoded.len() as u32));
        body.extend_from_slice(&encoded);
    }

    let mut header = Vec::new();
    header.extend_from_slice(&be32(2)); // two header entries
    for (k, v) in [(0u32, LOG_INSTANT), (2u32, LOG_SCHEMA)] {
        header.extend_from_slice(&be32(k));
        header.extend_from_slice(&be32(v.len() as u32));
        header.extend_from_slice(v.as_bytes());
    }

    let mut inner = Vec::new();
    inner.extend_from_slice(&be32(1)); // block format version
    inner.extend_from_slice(&be32(3)); // BlockType::AvroData
    inner.extend_from_slice(&header);
    inner.extend_from_slice(&be64(body.len() as u64));
    inner.extend_from_slice(&body);
    inner.extend_from_slice(&be32(0)); // no footer entries

    let block_length = (inner.len() + 8) as u64;
    let mut out = Vec::new();
    out.extend_from_slice(b"#HUDI#");
    out.extend_from_slice(&be64(block_length));
    out.extend_from_slice(&inner);
    out.extend_from_slice(&be64(block_length + 6));
    fs::write(path, &out).expect("write log file");
    out.len() as u64
}

fn main() {
    let args = Args::parse();
    if let Err(e) = run(&args) {
        eprintln!("fg-gen failed: {e}");
        std::process::exit(1);
    }
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        // All five meta fields: the reader projects the full set, so omitting
        // any one fails the read rather than degrading it.
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

/// One row is about this many bytes on disk once written. Measured from the
/// first file rather than assumed, so `--total-bytes` lands close on any
/// platform and compression setting.
fn batch(from: u64, rows: usize) -> RecordBatch {
    let keys: Vec<String> = (0..rows)
        .map(|i| format!("uuid{:012}", from + i as u64))
        .collect();
    let riders: Vec<String> = (0..rows)
        .map(|i| format!("rider-{:08}-padding-to-widen-the-row", from + i as u64))
        .collect();
    let commit: Vec<&str> = vec![INSTANT; rows];
    let seqno: Vec<String> = (0..rows)
        .map(|i| format!("{INSTANT}_0_{}", from + i as u64))
        .collect();
    let part: Vec<&str> = vec![""; rows];
    let fname: Vec<&str> = vec![""; rows];
    let fares: Vec<f64> = (0..rows).map(|i| (from + i as u64) as f64 * 1.5).collect();
    let ts: Vec<i64> = (0..rows)
        .map(|i| 1_700_000_000_000 + (from + i as u64) as i64)
        .collect();

    let cols: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(commit)),
        Arc::new(StringArray::from(seqno)),
        Arc::new(StringArray::from(keys.clone())),
        Arc::new(StringArray::from(part)),
        Arc::new(StringArray::from(fname)),
        Arc::new(StringArray::from(keys)),
        Arc::new(StringArray::from(riders)),
        Arc::new(Float64Array::from(fares)),
        Arc::new(Int64Array::from(ts)),
    ];
    RecordBatch::try_new(schema(), cols).expect("batch")
}

fn write_one(path: &Path, target_bytes: u64, row_group_rows: usize, start_key: u64) -> u64 {
    let props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_max_row_group_size(row_group_rows)
        .build();
    let file = fs::File::create(path).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, schema(), Some(props)).expect("writer");

    let mut written = 0u64;
    let mut key = start_key;
    while written < target_bytes {
        let b = batch(key, row_group_rows);
        writer.write(&b).expect("write");
        key += row_group_rows as u64;
        // in_progress_size is the writer's own accounting, so the loop stops on
        // measured bytes rather than an estimate that drifts with compression.
        written = writer.in_progress_size() as u64 + writer.bytes_written() as u64;
    }
    writer.close().expect("close");
    fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

fn run(args: &Args) -> Result<(), String> {
    let root = PathBuf::from(&args.out);
    if root.exists() {
        return Err(format!("{} already exists", root.display()));
    }
    fs::create_dir_all(root.join(".hoodie")).map_err(|e| e.to_string())?;

    let per_file = args.total_bytes / args.files as u64;
    let mut stats = Vec::new();
    let mut log_stats = Vec::new();
    let mut total = 0u64;

    for i in 0..args.files {
        let file_id = format!("f{i:08}-0000-0000-0000-{:012}-0", i);
        let name = format!("{file_id}_0-1-{i}_{INSTANT}.parquet");
        let size = write_one(
            &root.join(&name),
            per_file,
            args.row_group_rows,
            (i as u64) * 100_000_000,
        );
        total += size;

        // Log file names carry the base instant, so the slice they belong to is
        // the one written above rather than a new file group.
        let mut log_bytes = 0u64;
        for v in 1..=args.log_files {
            let log_name = format!(".{file_id}_{INSTANT}.log.{v}_0-2-{i}");
            let this_log = write_log_file(
                &root.join(&log_name),
                (i as u64) * 100_000_000 + args.log_key_offset,
                args.log_records,
            );
            log_bytes += this_log;
            // `baseFile` as well as `path`: schema resolution reads the first
            // write stat's `path`, gets nothing from a `.log.` extension, and
            // falls through to the base file. Without it a generated table has
            // no resolvable schema once the delta commit becomes the latest.
            log_stats.push(format!(
                r#"{{"fileId":"{file_id}","path":"{log_name}","baseFile":"{name}",
                   "prevCommit":"{INSTANT}","numWrites":{records},"numDeletes":0,
                   "numUpdateWrites":{records},"totalWriteBytes":{this_log},
                   "fileSizeInBytes":{this_log},"partitionPath":"","tempPath":null,
                   "totalLogRecords":{records},"totalLogFilesCompacted":0,
                   "totalLogSizeCompacted":0,"totalLogBlocks":1,
                   "totalCorruptLogBlock":0,"totalRollbackBlocks":0,"cdcStats":null,
                   "minEventTime":null,"maxEventTime":null,"runtimeStats":null}}"#,
                records = args.log_records
            ));
        }
        total += log_bytes;
        eprintln!(
            "[fg-gen] {name}  {} MiB base + {} MiB log",
            size / (1024 * 1024),
            log_bytes / (1024 * 1024)
        );
        stats.push(format!(
            r#"{{"fileId":"{file_id}","path":"{name}","prevCommit":"null","numWrites":0,
               "numDeletes":0,"numUpdateWrites":0,"totalWriteBytes":{size},
               "fileSizeInBytes":{size},"partitionPath":"","tempPath":null,
               "totalLogRecords":0,"totalLogFilesCompacted":0,"totalLogSizeCompacted":0,
               "totalLogBlocks":0,"totalCorruptLogBlock":0,"totalRollbackBlocks":0,
               "cdcStats":null,"minEventTime":null,"maxEventTime":null,"runtimeStats":null}}"#
        ));
    }

    let commit = format!(
        r#"{{"partitionToWriteStats":{{"":[{}]}},"compacted":false,
            "extraMetadata":{{}},"operationType":"INSERT"}}"#,
        stats.join(",")
    );
    fs::write(root.join(format!(".hoodie/{INSTANT}.commit")), commit).map_err(|e| e.to_string())?;

    // The log blocks carry LOG_INSTANT, and a read through `Table` gates a log
    // block on its instant having completed. Writing only the base commit left
    // the generated table readable through a standalone `FileGroupReader` --
    // which has no timeline and so admits every block -- while `Table::read`
    // silently dropped every log record. The delta commit is what makes the two
    // paths agree.
    if !log_stats.is_empty() {
        let delta_commit = format!(
            r#"{{"partitionToWriteStats":{{"":[{}]}},"compacted":false,
                "extraMetadata":{{}},"operationType":"UPSERT"}}"#,
            log_stats.join(",")
        );
        fs::write(
            root.join(format!(".hoodie/{LOG_INSTANT}.deltacommit")),
            delta_commit,
        )
        .map_err(|e| e.to_string())?;
    }

    let mut props: HashMap<&str, String> = HashMap::new();
    props.insert("hoodie.table.name", "fg_bench_generated".to_string());
    props.insert(
        "hoodie.table.type",
        if args.log_files > 0 {
            "MERGE_ON_READ"
        } else {
            "COPY_ON_WRITE"
        }
        .to_string(),
    );
    props.insert(
        "hoodie.record.merge.mode",
        "COMMIT_TIME_ORDERING".to_string(),
    );
    props.insert("hoodie.table.version", "6".to_string());
    props.insert("hoodie.timeline.layout.version", "1".to_string());
    props.insert("hoodie.table.recordkey.fields", "uuid".to_string());
    props.insert("hoodie.table.precombine.field", "ts".to_string());
    props.insert("hoodie.archivelog.folder", "archived".to_string());
    props.insert(
        "hoodie.datasource.write.drop.partition.columns",
        "false".to_string(),
    );
    props.insert(
        "hoodie.table.keygenerator.type",
        "NON_PARTITION".to_string(),
    );
    let body: String = props.iter().map(|(k, v)| format!("{k}={v}\n")).collect();
    fs::write(root.join(".hoodie/hoodie.properties"), body).map_err(|e| e.to_string())?;

    eprintln!(
        "[fg-gen] wrote {} files, {} MiB total, to {}",
        args.files,
        total / (1024 * 1024),
        root.display()
    );
    Ok(())
}
