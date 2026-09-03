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
//! How much CPU one merged chunk costs.
//!
//! ```text
//! cargo test -p hudi-core --release --lib merge_cpu_bench -- --ignored --nocapture
//! ```
//!
//! Release only — a debug build says nothing about production CPU.
//!
//! # Why this number matters
//!
//! The merge is driven by whoever consumes the merged output, and where that consumer
//! runs decides how long a thread may be held. A tokio worker must not be blocked for
//! more than tens of microseconds; a blocking-pool thread may be held for as long as the
//! work takes. So "how long does one chunk take" is not a performance curiosity — it
//! decides which thread the merge is allowed to run on.
//!
//! # Method
//!
//! The base source is an in-memory `RecordBatchIterator`, so a `next()` performs no I/O
//! at all and every microsecond measured is merge CPU. Three details keep it honest:
//!
//! - **Log updates are strided uniformly** across the base key space. Clustering them at
//!   the start makes early chunks do all the work and leaves the median measuring chunks
//!   that merged nothing. `p95` landing near the median is the check that they did not.
//! - **The merge map stays in memory.** It holds roughly 20 MB against a 1 GiB default
//!   `hoodie.memory.merge.max.size`, so no spill I/O is counted as CPU. A case large
//!   enough to spill would be measuring RocksDB, not merging.
//! - **Every log key exists in the base**, so the output row count equals the base row
//!   count exactly. That is asserted: a fixture that silently stopped merging would
//!   otherwise report an impressively low cost for doing nothing.
//!
//! The result is a floor, not a ceiling. Parquet decode, schema evolution and the output
//! converter all run on the same thread in production and none of them are here.
//!
//! # Baseline
//!
//! Apple M2 Max, release, 2026-08-18, against the synchronous merge iterator:
//!
//! ```text
//!  cols rows/chunk     log%    median µs       p95 µs      ns/row
//!     3       1024      10%         55.0         64.3        53.7
//!     3       8192      10%        383.2        427.8        46.8
//!     3      65536      10%       3038.3       3449.9        46.4
//!    12       1024      10%        135.1        157.8       131.9
//!    12       8192      10%        991.7       1129.9       121.1
//!    12      65536      10%       8556.5       9500.0       130.6
//!    12     131072      10%      18856.6      18856.6       143.9
//!    12       8192       1%        274.8        317.0        33.5
//!    12       8192      50%       3980.5       4277.7       485.9
//! ```
//!
//! Cost is linear in rows per chunk and roughly linear in column count — about 10 ns per
//! row per column on top of a 20-40 ns/row key probe. Compare against the same machine
//! when the merge surface changes; the figure to compare is µs per chunk, whatever shape
//! the consumer takes.

use crate::config::table::HudiTableConfig;
use crate::file_group::log_file::log_block::{
    BlockMetadataKey, BlockType, LogBlock, LogBlockContent,
};
use crate::file_group::log_file::log_format::LogFormatVersion;
use crate::file_group::reader_v2::buffer::HoodieFileGroupRecordBuffer;
use crate::file_group::reader_v2::buffer::key_based::KeyBasedFileGroupRecordBuffer;
use crate::file_group::reader_v2::merge_iterator::{FileGroupMergeStream, new_stream_stats_handle};
use crate::file_group::reader_v2::reader_context::ReaderContext;
use crate::file_group::reader_v2::schema_handler::FileGroupReaderSchemaHandler;
use crate::file_group::record_batches::RecordBatches;
use arrow_array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

const MERGE_MODE: &str = "COMMIT_TIME_ORDERING";

/// Base rows shared by every case, so per-chunk figures stay comparable across them.
const TOTAL_ROWS: usize = 262_144;

/// Rows per log block, matching what the log scan feeds the buffer at a time.
const LOG_BLOCK_ROWS: usize = 8192;

/// What a worker thread may be blocked for, in microseconds. Tokio's guidance is
/// "tens of microseconds"; 100 is the round number at the permissive end of that.
const WORKER_BUDGET_US: f64 = 100.0;

/// `_hoodie_record_key` and `ts`, then `n_extra` payload columns cycling
/// Int64 / Float64 / Utf8 so the merge moves more than one kind of array.
fn wide_schema(n_extra: usize) -> SchemaRef {
    let mut fields = vec![
        Field::new("_hoodie_record_key", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, false),
    ];
    for i in 0..n_extra {
        let (name, ty) = match i % 3 {
            0 => (format!("i_{i}"), DataType::Int64),
            1 => (format!("f_{i}"), DataType::Float64),
            _ => (format!("s_{i}"), DataType::Utf8),
        };
        fields.push(Field::new(name, ty, true));
    }
    Arc::new(Schema::new(fields))
}

/// A batch carrying exactly the given key indices, at ordering value `ts`.
///
/// The log side needs arbitrary indices rather than a range, so its updates can be
/// spread across the base key space — see the module docs on striding.
fn batch_for_keys(schema: &SchemaRef, idx: &[usize], ts: i64) -> RecordBatch {
    let n = idx.len();
    let keys: Vec<String> = idx.iter().map(|i| format!("k{i:09}")).collect();
    let mut cols: Vec<Arc<dyn Array>> = vec![
        Arc::new(StringArray::from(keys)),
        Arc::new(Int64Array::from(vec![ts; n])),
    ];
    for f in schema.fields().iter().skip(2) {
        let col: Arc<dyn Array> = match f.data_type() {
            DataType::Int64 => Arc::new(Int64Array::from(
                idx.iter().map(|i| *i as i64).collect::<Vec<_>>(),
            )),
            DataType::Float64 => Arc::new(Float64Array::from(
                idx.iter().map(|i| *i as f64 * 1.5).collect::<Vec<_>>(),
            )),
            _ => Arc::new(StringArray::from(
                idx.iter()
                    .map(|i| format!("v-{i}-payload"))
                    .collect::<Vec<_>>(),
            )),
        };
        cols.push(col);
    }
    RecordBatch::try_new(schema.clone(), cols).unwrap()
}

fn contiguous_batch(schema: &SchemaRef, start: usize, n: usize, ts: i64) -> RecordBatch {
    batch_for_keys(schema, &(start..start + n).collect::<Vec<_>>(), ts)
}

fn build_buffer(schema: &SchemaRef) -> KeyBasedFileGroupRecordBuffer {
    let mut ctx = ReaderContext::empty();
    ctx.table_config.insert(
        HudiTableConfig::OrderingFields.as_ref().to_string(),
        "ts".to_string(),
    );
    ctx.rebuild_record_context(String::new());
    let mut handler = FileGroupReaderSchemaHandler::new()
        .with_table_schema(schema.clone())
        .with_data_schema(schema.clone());
    let key_field = ctx.record_key_field().to_string();
    let ordering = ctx.record_context.ordering_field_names.clone();
    handler
        .prepare_required_schema(
            true,
            &[key_field],
            &ordering,
            &ctx.table_config,
            false,
            MERGE_MODE,
        )
        .unwrap();
    ctx.schema_handler = handler;
    KeyBasedFileGroupRecordBuffer::new(Arc::new(ctx), MERGE_MODE.to_string(), false).unwrap()
}

struct Case {
    cols: usize,
    rows_per_chunk: usize,
    /// Percentage of base keys carrying a log update. Must divide 100.
    log_pct: usize,
}

struct Measurement {
    median_us: f64,
    p95_us: f64,
    ns_per_row: f64,
}

fn run(case: &Case) -> Measurement {
    let schema = wide_schema(case.cols - 2);
    let mut buffer = build_buffer(&schema);

    // Log side: every `stride`-th key, fed one block at a time. `ts = 2` beats the
    // base's `ts = 1`, so each of these keys is an update rather than a no-op.
    let stride = 100 / case.log_pct;
    let log_keys: Vec<usize> = (0..TOTAL_ROWS).step_by(stride).collect();
    for chunk in log_keys.chunks(LOG_BLOCK_ROWS) {
        let mut header = HashMap::new();
        header.insert(
            BlockMetadataKey::InstantTime,
            "20260101000000000".to_string(),
        );
        let mut block = LogBlock::new(
            LogFormatVersion::V1,
            BlockType::ParquetData,
            header,
            LogBlockContent::Records(RecordBatches::new_with_data_batches(vec![batch_for_keys(
                &schema, chunk, 2,
            )])),
            HashMap::new(),
        );
        buffer.process_data_block(&mut block).unwrap();
    }

    // Base side: one batch per simulated parquet row group.
    let batches: Vec<RecordBatch> = (0..TOTAL_ROWS / case.rows_per_chunk)
        .map(|c| contiguous_batch(&schema, c * case.rows_per_chunk, case.rows_per_chunk, 1))
        .collect();
    // The base source is handed to the stream rather than set on the buffer:
    // one batch per simulated parquet row group, so the merge pulls them the way
    // a streaming read would.
    use futures::StreamExt;
    let base: crate::file_group::reader_v2::merge_iterator::BaseBatchStream =
        futures::stream::iter(batches.into_iter().map(Ok)).boxed();

    let mut iter = FileGroupMergeStream::new_buffered(
        Box::new(buffer),
        base,
        schema.clone(),
        schema,
        None,
        new_stream_stats_handle(),
    );

    // Timed per call, not in aggregate: the per-chunk figure is the one that decides
    // which thread may run the merge.
    //
    // `block_on` per chunk because the merge is a stream now, not an iterator.
    // The measurement is a comparison across cases and the parking cost is the
    // same in each, so it shifts every number by the same small constant rather
    // than changing which case is dearer. There is no I/O in this fixture, so the
    // future is always immediately ready and never actually parks.
    let mut per_chunk_us: Vec<f64> = Vec::new();
    let mut rows = 0usize;
    loop {
        let started = Instant::now();
        let Some(next) = futures::executor::block_on(iter.next_chunk()) else {
            break;
        };
        per_chunk_us.push(started.elapsed().as_secs_f64() * 1e6);
        rows += next.unwrap().num_rows();
    }

    // Every log key exists in the base, so nothing is inserted and nothing is dropped.
    // Without this, a fixture that stopped merging would report a very fast nothing.
    assert_eq!(
        rows, TOTAL_ROWS,
        "expected every base row to survive the merge; the fixture is wrong"
    );

    per_chunk_us.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median_us = per_chunk_us[per_chunk_us.len() / 2];
    let p95_us = per_chunk_us[(per_chunk_us.len() * 95 / 100).min(per_chunk_us.len() - 1)];
    Measurement {
        median_us,
        p95_us,
        ns_per_row: median_us * 1000.0 / case.rows_per_chunk as f64,
    }
}

#[test]
#[ignore = "benchmark; run explicitly with --release --ignored"]
fn merge_cpu_per_chunk() {
    // Width and chunk size are the two things a chunk's cost scales with. Real parquet
    // row groups run to 100k rows and beyond, so the large chunks are the realistic
    // ones; the small ones show where the worker budget is crossed.
    let cases = [
        Case {
            cols: 3,
            rows_per_chunk: 1024,
            log_pct: 10,
        },
        Case {
            cols: 3,
            rows_per_chunk: 8192,
            log_pct: 10,
        },
        Case {
            cols: 3,
            rows_per_chunk: 65_536,
            log_pct: 10,
        },
        Case {
            cols: 12,
            rows_per_chunk: 1024,
            log_pct: 10,
        },
        Case {
            cols: 12,
            rows_per_chunk: 8192,
            log_pct: 10,
        },
        Case {
            cols: 12,
            rows_per_chunk: 65_536,
            log_pct: 10,
        },
        Case {
            cols: 12,
            rows_per_chunk: 131_072,
            log_pct: 10,
        },
        // How much of the base a delta commit actually touches.
        Case {
            cols: 12,
            rows_per_chunk: 8192,
            log_pct: 1,
        },
        Case {
            cols: 12,
            rows_per_chunk: 8192,
            log_pct: 50,
        },
    ];

    // The first run pays allocator warm-up and page faults that are not merge cost.
    let _ = run(&cases[0]);

    println!(
        "\n{:>5} {:>10} {:>8} {:>12} {:>12} {:>11}  vs worker budget",
        "cols", "rows/chunk", "log%", "median µs", "p95 µs", "ns/row"
    );
    println!("{}", "-".repeat(92));
    for case in &cases {
        let m = run(case);
        let verdict = if m.median_us <= WORKER_BUDGET_US {
            "within".to_string()
        } else {
            format!("{:.0}x over", m.median_us / WORKER_BUDGET_US)
        };
        println!(
            "{:>5} {:>10} {:>7}% {:>12.1} {:>12.1} {:>11.1}  {verdict}",
            case.cols, case.rows_per_chunk, case.log_pct, m.median_us, m.p95_us, m.ns_per_row
        );
    }
    println!(
        "\nworker budget = {WORKER_BUDGET_US:.0} µs; base rows = {TOTAL_ROWS}, in memory, no I/O\n"
    );
}
