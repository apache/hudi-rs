<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# fg-bench — file-group reader benchmark harness (perf M2)

Minimal harness that drives `HoodieFileGroupReader` directly and records
per-iteration wall/CPU/RSS plus the full `HoodieReadStats` (with the per-stage
timings added for the perf effort). This is the **M2 minimal** harness: no
docker, no flamegraphs, no resource-tier matrix — just enough to get
before/after numbers while the Tier-A alignment fixes land.

See the design in `02-bench-harness-design.md` (D1–D3, D9 minimal) and the
milestone scope in `03-milestones.md` (M2).

## Build & run

```shell
cargo build -p fg-bench --release

# Full read, 3 iterations (first is warmup), JSON to stdout
./target/release/fg-bench --table /abs/path/to/hudi/table

# Project columns + persist JSON
./target/release/fg-bench --table /abs/path/to/table \
    --columns id,ts,value --iterations 5 --output-json run.json

# Override the streaming chunk size (hoodie.read.stream.batch_size, ENG-42991).
# Omit to use the reader default (4096 rows). Smaller chunks reduce per-chunk
# wall overhead; the peak-RSS effect is observable only on the streaming
# (open()) path -- this bench drives read(), which retains the full output, so
# max_rss here is dominated by output retention, not chunk size.
./target/release/fg-bench --table /abs/path/to/table --batch-size 1024
```

### Key-based vs position-based merge

`--use-record-position` selects the position-based merge buffer (match base rows
to log records by base-file row position instead of record key). The table's log
blocks must carry `RECORD_POSITIONS` headers (written by Spark with
`hoodie.merge.use.record.positions` enabled); otherwise the reader falls back to
key-based merge per block. The chosen strategy is recorded in the report as
`merge_strategy` (`"key"` / `"position"`).

Compare the two strategies on the same MOR table:

```shell
./target/release/fg-bench --table /abs/path/to/mor/table \
    --iterations 6 --output-json key.json
./target/release/fg-bench --table /abs/path/to/mor/table --use-record-position \
    --iterations 6 --output-json position.json

# Median wall / RSS + per-stage timing deltas (position vs key baseline)
python3 benchmark/filegroup/compare.py key.json position.json
```

`--table` must be an absolute path or `file://` URI (the table loader needs a
base URI). The harness opens the table, discovers the latest file slice(s) via
`Table::get_file_slices`, builds one `HoodieFileGroupReader` per slice (full
table schema as data schema, requested = full schema or `--columns`), reads each
to completion, and sums rows + folds per-slice `HoodieReadStats`.

## What it measures (per iteration)

- `wall_ms` — wall-clock of the whole read.
- `user_ms` / `sys_ms` — `getrusage(RUSAGE_SELF)` CPU deltas vs the pre-iteration snapshot.
- `max_rss_kb` — `ru_maxrss` (peak RSS high-water mark, monotonic).
- `rows` — total output rows across slices.
- `read_stats` — the full `HoodieReadStats`, including the stage timings.
- `host` — `/proc/loadavg` (load1) + `MemAvailable` (`/proc/meminfo`) sampled
  just before the iteration.

The first iteration is marked `warmup: true` and is excluded from `summary`
(median/min/max wall over the measured iterations).

## Host-contention guard (minimal)

Before each iteration the harness samples host load + free memory. If
`load1 / nproc > 0.5` it prints a loud warning, tags that iteration
`contended: true`, and sets the top-level `contended: true`. This is the
**minimal** version: it records + flags but does NOT yet wait for a quiet
window or rerun contended iterations (that is the M4 full version). Always run
A/B pairs back-to-back in the same quiet window.

## Stage timings

Added to `HoodieReadStats` and wired with cheap `Instant`-based accumulation
(always-on, per block/batch — never per row):

| field | site | meaning |
|---|---|---|
| `base_read_ms` | `reader/mod.rs` | base parquet read + projection |
| `log_block_read_ms` | `log_record_reader.rs` Pass 1 | log-block metadata + byte read off storage |
| `log_block_decode_ms` | `buffer/key_based.rs` | `inflate_from_bytes` (inflate/decode), a **subset** of `merge_insert_ms` |
| `merge_insert_ms` | `log_record_reader.rs` Pass 3 | block dispatch into the merge map (decode + per-key upsert) |
| `final_merge_ms` | `reader/mod.rs` | `merge_and_collect_with_stats` (base+log final merge) |
| `output_build_ms` | `reader/mod.rs` | output projection assembly |
| `merge_map_peak_entries` | `buffer/key_based.rs` | peak merge-map entry count |

### Known coarseness / deviations

- **decode is nested inside merge_insert.** In hudi-rs the log block is inflated
  lazily inside the buffer's `process_data_block`, which runs inside the Pass-3
  merge-insert window. So `log_block_decode_ms` is a *subset* of
  `merge_insert_ms`, not a disjoint stage. The design's clean decode-vs-merge
  split would require refactoring the lazy-inflate path; we
  kept the coarser split and document it here.
- **lazy-inflate can shift decode cost into `log_block_read_ms`.** For some log
  layouts the block bytes are materialized during the Pass-1 metadata read, so
  `log_block_decode_ms` reads near-zero while `log_block_read_ms` absorbs it.
  Treat `log_block_read_ms + log_block_decode_ms + merge_insert_ms` as the
  log-path total; the individual split is indicative, not exact.
- **per-slice aggregation.** Timings/counters are summed across slices;
  `merge_map_peak_entries` is the max across slices. Wall/CPU/RSS are for the
  whole iteration (all slices).
- Stage timings are millisecond-granular; sub-ms stages on tiny fixtures read
  as `0`.

## Comparing runs

```shell
python3 benchmark/filegroup/compare.py baseline.json candidate.json [more.json ...]
```

Prints median wall, peak RSS, and the stage breakdown for each file with a
percent delta vs the first (baseline) file. Stdlib only.
