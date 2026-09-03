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

# fg-bench — file-group reader benchmark harness

Drives the public `FileGroupReader` over a Hudi table's file slices and records
per-iteration wall clock, CPU, peak RSS and row count. It exists to answer two
questions about a merge-on-read read: how long does it take, and does it stay
inside a memory bound.

Everything it configures is a Hudi config key, so a run measures the shipped
reader rather than a harness-specific assembly of its internals.

## Build & run

```shell
cargo build -p fg-bench --release

# Full read, 3 iterations (the first is a warmup), JSON to stdout
./target/release/fg-bench --table /abs/path/to/hudi/table

# Project columns + persist JSON
./target/release/fg-bench --table /abs/path/to/table \
    --columns id,ts,value --iterations 5 --output-json run.json

# Override the streaming chunk size (hoodie.read.stream.batch_size). Omit to use
# the reader default (4096 rows).
./target/release/fg-bench --table /abs/path/to/table --batch-size 1024
```

`--table` must be an absolute path or a `file://` URI — the table loader needs a
base URI. The harness opens the table, discovers its latest file slices via
`Table::get_file_slices`, and reads each to completion, summing rows.

## Generating a table

`fg-gen` writes a synthetic merge-on-read table, because the reads worth
measuring need more data than can be committed to the repository:

```shell
cargo run -p fg-bench --release --bin fg-gen -- \
    --out /tmp/fg-table --rows 2000000 --log-files 4
```

The generated table is table version 6, timeline layout 1, and carries a real
timeline: a `commit` for the base files and a `deltacommit` for the log files.
Both are required — a read through `Table` gates log blocks on their instant
having completed, so log records written at an instant that never commits are
dropped.

## What it measures (per iteration)

- `wall_ms` — wall clock of the whole read.
- `user_ms` / `sys_ms` — `getrusage(RUSAGE_SELF)` CPU deltas against the
  pre-iteration snapshot.
- `max_rss_kb` — `ru_maxrss`, a monotonic peak-RSS high-water mark.
- `rows` — total output rows across slices.
- `spilled` / `spill_peak_bytes` — whether the merge map's disk tier engaged,
  observed by watching the spill directory grow while the read runs.
- `accounting_drift` — true when peak RSS greatly exceeds the merge map's
  accounted retained bytes, i.e. the size accounting is not tracking the
  resident set.
- `host` — `load1` from `/proc/loadavg` and `MemAvailable` from `/proc/meminfo`,
  sampled just before the iteration.

The first iteration is marked `warmup: true` and excluded from `summary`
(median/min/max wall over the measured iterations).

Per-stage timings are **not** reported. They live in `HoodieReadStats`, which is
not on the public reader surface, and widening that surface for a benchmark is
not a trade worth making.

## Bounding memory

Three knobs turn the harness from a measurement into a gate:

| flag | effect |
|---|---|
| `--scan-memory-budget` | Total bytes the scan may use. Slices-in-flight is derived from it by `hudi_core::file_group::admission::slices_in_flight` — the same function the DataFusion plan uses — with `--slice-concurrency` as a ceiling it may not exceed. |
| `--merge-max-size` | `hoodie.memory.merge.max.size`. Set it low to push the merge map into its RocksDB disk tier and confirm the read degrades rather than growing. |
| `--max-rss-bytes` | Fails the run when peak RSS exceeds this. |

`FG_BENCH_ALLOC_CAP_BYTES` is stronger than `--max-rss-bytes`: it installs a
hard ceiling in the global allocator, so an allocation past the cap returns null
and the process aborts. `--max-rss-bytes` asserts after the fact, on a machine
that had the memory to spare; the allocator cap answers whether the read
survives on a machine that does not.

### Reading the spill report

`spill_peak_bytes` is *growth* over what the spill directory held when the
iteration started, so a shared directory with existing contents does not read as
spill. By default each run spills into a fresh per-run subdirectory, which also
keeps the 100 ms sampler off large unrelated trees.

Sampling happens during the read, not at its boundaries: RocksDB creates its
directory, writes, compacts, and removes it before the read call returns, so a
before/after comparison sees an empty directory both times. Even sampling during
the read, treat `spilled: true` as proof and `false` as unproven — compaction
can erase the evidence between two samples.

## Key-based vs position-based merge

`--use-record-position` selects the position-based merge buffer, which matches
base rows to log records by base-file row position instead of record key. The
table's log blocks must carry `RECORD_POSITIONS` headers (written by Spark with
`hoodie.merge.use.record.positions` enabled); otherwise the reader falls back to
key-based merge per block. The strategy is recorded in the report as
`merge_strategy` (`"key"` / `"position"`).

```shell
./target/release/fg-bench --table /abs/path/to/mor/table \
    --iterations 6 --output-json key.json
./target/release/fg-bench --table /abs/path/to/mor/table --use-record-position \
    --iterations 6 --output-json position.json

python3 benchmark/filegroup/compare.py key.json position.json
```

## Eager vs streaming

`--streaming` consumes the read as a stream of batches, dropping each as it
goes, so the whole result is never resident. Without it the harness calls the
eager read, which retains the full output. On a large table that difference
dominates `max_rss_kb`.

## Host-contention guard

Before each iteration the harness samples host load and free memory. If
`load1 / nproc > 0.5` it prints a warning, tags that iteration
`contended: true`, and sets the top-level `contended: true`. It records and
flags; it does not wait for a quiet window or rerun contended iterations. Run
A/B pairs back-to-back in the same quiet window.

## Comparing runs

```shell
python3 benchmark/filegroup/compare.py baseline.json candidate.json [more.json ...]
```

Prints median wall, peak RSS and the other per-iteration metrics for each file,
with a percent delta against the first (baseline) file. Stdlib only.
