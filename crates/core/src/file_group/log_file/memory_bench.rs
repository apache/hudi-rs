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
//! Measures what reading a log file costs in memory, both ways.
//!
//! Each mode runs in its own process, because a freed allocation does not
//! return resident pages to the OS — measuring both in one process would charge
//! the second mode for whatever the first one peaked at.
//!
//! ```text
//! HUDI_BENCH=generate cargo test -p hudi-core --release --lib memory_bench -- --ignored --nocapture
//! HUDI_BENCH=eager    cargo test -p hudi-core --release --lib memory_bench -- --ignored --nocapture
//! HUDI_BENCH=lazy     cargo test -p hudi-core --release --lib memory_bench -- --ignored --nocapture
//! ```

use crate::config::HudiConfigs;
use crate::config::table::HudiTableConfig;
use crate::file_group::log_file::content::Decoder;
use crate::file_group::log_file::reader::LogFileReader;
use crate::storage::Storage;
use crate::timeline::selector::InstantRange;
use apache_avro::types::Record as AvroRecord;
use apache_avro::{Schema as AvroSchema, to_avro_datum};
use std::io::Write;
use std::sync::Arc;

const SCHEMA: &str = r#"{"type":"record","name":"r","fields":[
    {"name":"_hoodie_record_key","type":["null","string"],"default":null},
    {"name":"id","type":"long"},
    {"name":"payload","type":"string"}]}"#;

const MAGIC: &[u8] = b"#HUDI#";
const RECORDS_PER_BLOCK: usize = 20_000;
const BLOCKS: usize = 60;

/// Resident set size in MB, read from `ps`.
fn rss_mb() -> u64 {
    let pid = std::process::id();
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .expect("ps");
    String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse::<u64>()
        .unwrap_or(0)
        / 1024
}

fn build_block(instant: &str) -> Vec<u8> {
    let schema = AvroSchema::parse_str(SCHEMA).unwrap();

    let mut content = Vec::new();
    content.extend_from_slice(&3u32.to_be_bytes());
    content.extend_from_slice(&(RECORDS_PER_BLOCK as u32).to_be_bytes());
    for i in 0..RECORDS_PER_BLOCK {
        let mut rec = AvroRecord::new(&schema).unwrap();
        rec.put("_hoodie_record_key", Some(format!("k{i}")));
        rec.put("id", i as i64);
        rec.put("payload", "x".repeat(64));
        let body = to_avro_datum(&schema, rec).unwrap();
        content.extend_from_slice(&(body.len() as u32).to_be_bytes());
        content.extend_from_slice(&body);
    }

    let mut header = Vec::new();
    header.extend_from_slice(&2u32.to_be_bytes());
    for (key, value) in [(0u32, instant), (2u32, SCHEMA)] {
        header.extend_from_slice(&key.to_be_bytes());
        header.extend_from_slice(&(value.len() as u32).to_be_bytes());
        header.extend_from_slice(value.as_bytes());
    }

    let mut body = Vec::new();
    body.extend_from_slice(&1u32.to_be_bytes());
    body.extend_from_slice(&3u32.to_be_bytes());
    body.extend_from_slice(&header);
    body.extend_from_slice(&(content.len() as u64).to_be_bytes());
    body.extend_from_slice(&content);
    body.extend_from_slice(&0u32.to_be_bytes()); // empty footer

    // The recorded length spans everything after it, the trailing pointer
    // included; the trailing value counts the magic on top of that.
    let block_length = (body.len() + 8) as u64;
    let mut out = Vec::new();
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&block_length.to_be_bytes());
    out.extend_from_slice(&body);
    out.extend_from_slice(&(block_length + MAGIC.len() as u64).to_be_bytes());
    out
}

fn configs() -> Arc<HudiConfigs> {
    Arc::new(HudiConfigs::new([(HudiTableConfig::OrderingFields, "id")]))
}

#[tokio::test]
#[ignore = "writes and reads a large file; run one mode per process"]
async fn bench_log_read_memory() {
    let mode = std::env::var("HUDI_BENCH").unwrap_or_else(|_| "generate".to_string());
    let dir = std::env::temp_dir().join("hudi-rs-log-memory-bench");
    std::fs::create_dir_all(&dir).unwrap();
    let name = "bench.log.1_0-0-0";
    let path = dir.join(name);

    if mode == "generate" {
        let mut f = std::fs::File::create(&path).unwrap();
        for i in 0..BLOCKS {
            f.write_all(&build_block(&format!("2026010100000{i:04}")))
                .unwrap();
        }
        let mb = std::fs::metadata(&path).unwrap().len() / 1024 / 1024;
        println!("\nwrote {BLOCKS} blocks, {mb} MB at {}", path.display());
        return;
    }

    let file_mb = std::fs::metadata(&path)
        .expect("run HUDI_BENCH=generate first")
        .len()
        / 1024
        / 1024;
    let url = url::Url::from_directory_path(std::fs::canonicalize(&dir).unwrap()).unwrap();
    let baseline = rss_mb();
    let storage = Storage::new_with_base_url(url).unwrap();

    let (rows, peak) = match mode.as_str() {
        "eager" => {
            let mut reader = LogFileReader::new(configs(), storage, name).await.unwrap();
            let blocks = reader
                .read_all_blocks(&InstantRange::up_to("99991231235959999", "utc"))
                .unwrap();
            let rows: usize = blocks
                .iter()
                .filter_map(|b| b.content.as_records().map(|r| r.num_data_rows()))
                .sum();
            (rows, rss_mb())
        }
        "lazy" => {
            let mut reader = LogFileReader::new_streaming(configs(), storage, name)
                .await
                .unwrap();
            let mut blocks = reader.read_all_blocks_metadata_only().unwrap();
            let decoder = Decoder::new(configs());
            let mut rows = 0usize;
            let mut peak = rss_mb();
            for block in blocks.iter_mut() {
                block.inflate(&decoder).unwrap();
                rows += block
                    .content
                    .as_records()
                    .map(|r| r.num_data_rows())
                    .unwrap_or(0);
                peak = peak.max(rss_mb());
                // Released before the next block, which is the whole point.
                block.content = Default::default();
            }
            (rows, peak)
        }
        other => panic!("unknown HUDI_BENCH mode: {other}"),
    };

    println!(
        "\n{mode:<6} file={file_mb}MB rows={rows} baseline={baseline}MB peak={peak}MB (+{} MB)",
        peak.saturating_sub(baseline)
    );
}
