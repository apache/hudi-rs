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

# Upsert / delete benchmark

Measures the time, bytes written and peak memory of small upsert and delete
commits on a large Hudi table, written by one Python process through the
`hudi` package's write API (`HudiTable.create`, `append`, `upsert`,
`delete_keys`).

| leg                | table type    | record-level index |
|--------------------|---------------|--------------------|
| `hudi-cow`         | copy-on-write | on                 |
| `hudi-mor`         | merge-on-read | on                 |
| `hudi-cow-noindex` | copy-on-write | off                |

Everything else is the writer's default (table version 9, metadata table on).
`hudi-cow-noindex` against `hudi-cow` shows what the index saves, and
`hudi-cow` against `hudi-mor` shows the write amplification. With random keys,
file-level key ranges prune nothing, so without the index locating the keys
touches the whole table; with it, the lookup cost follows the batch size, and
index upkeep shows up as `meta_bytes_written`.

Every leg's final table is checked against a Parquet oracle, and a leg that
does not match is reported as failed, not timed.

## Workload

All legs run the same commits on the same data, generated from `--seed`:

1. **Load**: create the table and write the base data in one call (timed, but
   not the subject).
2. **Upserts**: `--commits` commits of full rows, updating existing keys and
   inserting new ones where the scenario has them.
3. **Deletes**: `--commits` commits of existing keys.
4. **Read-back**: a timed full read, then two checks outside the timed region:
   a sample of `--verify-sample` updated, inserted, deleted and untouched keys,
   and the whole table against [the oracle](#the-oracle). The writer's
   reported update, insert and delete counts are checked per commit too.

Generated data is cached in the work dir and reused across runs.

## Scenarios

Each scenario has a default and a `--smoke` scale; a knob on the command line
overrides both.

### `uuid`

Random UUIDv4 keys, so every file's key range spans the whole key space.
Columns: `id` (key), `ts` (int64, ordering field), `city`, `fare`, `status`,
`note` (64 bytes). Each upsert takes `--upsert-batch` live keys and
`--insert-batch` new ones; each delete takes `--delete-batch` live keys.

| knob | default | smoke |
|---|---|---|
| `--rows` | 10,000,000 | 100,000 |
| `--commits` | 5 | 2 |
| `--upsert-batch` / `--insert-batch` / `--delete-batch` | 10,000 / 1,000 / 1,000 | same |

### `tpcds-web-returns`

TPC-DS `web_returns` from DuckDB's `tpcds` extension (downloaded by DuckDB on
first use), keyed on `(wr_order_number, wr_item_sk)` with no ordering field.
Each upsert updates `--matched-fraction` of the rows (`wr_return_quantity + 1`,
so every matched row changes) and inserts `--not-matched-fraction` new ones;
each delete removes `--delete-fraction`. Deletes pass `f1:v1,f2:v2` record key
strings, the form the writer stores.

| knob | default | smoke |
|---|---|---|
| `--scale-factor` | 10 | 1 |
| `--commits` | 5 | 2 |
| `--matched-fraction` / `--not-matched-fraction` / `--delete-fraction` | 0.01 / 0.10 / 0.05 | same |

### `nyc-taxi`

NYC TLC `yellow_tripdata_2022-01.parquet` (about 2.46M trips), downloaded once
to `<work-dir>/download/nyc-taxi/` and refused unless its SHA-256 is
`797d6f008e67c3e3371b7f957ab97dbc2b38a9fb0ac083cb5eae4b0297b32372`. The harness
adds `trip_id` (row number) as the key, with `tpep_pickup_datetime` as the
ordering field. Upserts change `fare_amount` and `tip_amount` on
`--upsert-fraction` of the trips without changing the pickup time, so the
incoming row must win an ordering tie.

| knob | default | smoke |
|---|---|---|
| `--rows` | 0 (whole file) | 100,000 |
| `--commits` | 5 | 2 |
| `--upsert-fraction` / `--delete-fraction` | 0.01 / 0.001 | same |

### `rag-chunks`

A retrieval pipeline's chunks table: `chunk_id` (`<doc_id>#<pos>`, key),
`doc_id`, `pos`, `text` (512 bytes), `embedding` (384 float32) and
`updated_at` (ordering field). Each upsert re-ingests every chunk of
`--changed-docs` documents; each delete removes every chunk of
`--removed-docs` documents.

| knob | default | smoke |
|---|---|---|
| `--docs` x `--chunks-per-doc` | 100,000 x 10 | 1,000 x 10 |
| `--commits` | 5 | 2 |
| `--changed-docs` / `--removed-docs` | 1,000 / 100 | 100 / 10 |

### `training-set`

A fine-tuning set curated in place: `example_id` (key), `messages` (a JSON
chat transcript), `label`, `source_chunk_id` and `updated_at` (ordering
field). After each commit, outside its timed region, the harness also times
the two reads a training job makes:

- **Incremental read** from the previous commit's completion time to this
  one's. After an upsert it must equal the upsert's input; after a delete it
  must be empty, since a Hudi incremental read does not return deletes.
- **Time travel** to the load commit, which must equal the loaded table.

Reclaim (below) is off here because both reads need superseded files.

| knob | default | smoke |
|---|---|---|
| `--rows` | 1,000,000 | 10,000 |
| `--commits` | 5 | 2 |
| `--upsert-batch` / `--insert-batch` / `--delete-batch` | 10,000 / 0 / 1,000 | 500 / 0 / 100 |

## Reproduce

Needs [`uv`](https://docs.astral.sh/uv/) (it provides Python 3.13) and the
Rust toolchain in `AGENTS.md`. From the repository root:

```bash
# Venv with the pinned requirements, then hudi built from this checkout
# (or pass wheels: run.sh install-hudi path/to/hudi-*.whl).
benchmark/upsert/run.sh setup
benchmark/upsert/run.sh install-hudi

# The harness's tests.
benchmark/upsert/.venv/bin/python -m pytest benchmark/upsert/tests -q

# Smoke every scenario. Smoke runs only warn on a busy machine.
for s in uuid tpcds-web-returns nyc-taxi rag-chunks training-set; do
  benchmark/upsert/run.sh smoke --scenario "$s"
done

# Full scale: one leg per invocation, one at a time, on an idle machine.
for s in uuid tpcds-web-returns nyc-taxi rag-chunks training-set; do
  for leg in hudi-cow hudi-mor hudi-cow-noindex; do
    benchmark/upsert/run.sh full --scenario "$s" --legs "$leg" --repeat 3 \
      --out "benchmark/upsert/results/$s-$leg.json"
  done
done

benchmark/upsert/run.sh summarize benchmark/upsert/results/*.json --markdown
```

`bench.py --help` lists every option. Data, tables and logs go under
`--work-dir` (default `$TMPDIR/hudi-upsert-bench`, or `UPSERT_BENCH_WORK_DIR`);
the 10M-row `uuid` data is about 0.7 GB. Tables are deleted after each leg
unless `--keep-tables` is set. A lock stops two runs of the same scenario,
scale and workload from sharing tables, and `--leg-timeout SECONDS` records a
repetition that runs too long as `timeout`.

## What is measured

Each phase (load, upserts, deletes, read-back) runs in its own subprocess, so
peak RSS is per phase.

- **Commit time**: only the library call, not reading the input or opening
  the table. The first commit in a phase also pays one-time setup, so the
  median is reported next to the mean.
- **Bytes and files written** per commit, split into data files and metadata
  (everything under `.hoodie/`, including the record-level index).
- **Peak RSS** per phase, plus RSS just before the call. Load and read-back
  hold the whole table in Arrow, so the upsert and delete phases show the
  writer's own memory.
- **Read-back time** of a full snapshot read.

Not measured: object storage (everything is local disk, so the numbers are
CPU-bound), concurrency, and table services.

**Repetitions.** `--repeat N` (1 to 5, default 3) rebuilds each leg's table and
replays the workload N times in a row. Every repetition runs the correctness
checks, and `summary.stats` reports mean, median, min, max and coefficient of
variation per timing. Bytes and files come from the first repetition; file
counts must match across repetitions and byte counts within 1 percent,
otherwise `summary.writes_consistent` is false.

**Headroom gate.** Before each leg and each timed call, the harness refuses to
go on if the 1-minute load average is above `--max-load1` (2.0), available
memory is below `--min-mem-gib` (16), or free disk is below 3x the base data.
A high load average alone is first given `--settle-timeout` seconds (180) to
come down. `--force` turns load and memory refusals into warnings and marks
the results, which `summarize.py` then renders only with `--allow-loaded`; a
disk shortfall always refuses.

**Reclaim.** After each commit, outside the timed region, the harness deletes
data files the latest file slices no longer reference, so a copy-on-write run
does not keep a table copy per commit. `--no-reclaim` turns it off.

### The oracle

The workload builder replays every commit on an in-memory copy of the base
data and writes `<work-dir>/oracle/<scenario>-<scale>-<workload>/expected.parquet`
(plus `base.parquet` for time travel). The read-back drops `_hoodie_*`
columns, sorts by key, and compares with `pyarrow.Table.equals`. Types must
match after normalizing only view/large variants and list item names, and
values must match exactly, floats included. A mismatch names the first
differing keys and values, or the missing, extra and duplicate keys.

## Results

None are committed. `summarize.py` shows each timing as the median with its
range across repetitions (`--stat mean` for the mean), flags timings with a
coefficient of variation above 10 percent, and prints the gate's readings
under the table.

## Adding a leg

A leg subclasses `Leg` in `bench.py` and registers with
`register_leg(name, factory)`. A leg outside this directory loads with
`--extra-legs PATH`, which calls the module's `register(api)`:

```python
def register(api):
    class MyLeg(api.Leg):
        name = "my-leg"
        ...

    api.register_leg("my-leg", MyLeg)
```

`plan` is the scenario's `plan.json` (key columns, ordering field, check
columns, whether incremental reads run).

| method | does |
|---|---|
| `config(plan)` | settings recorded in the results |
| `load(root, table, plan)` | create the table, write the data, return `{"marker": ...}` |
| `open(root, plan)` | open the table for a phase |
| `upsert(table)` | apply one upsert; return `num_updated` / `num_inserted` when known |
| `delete(keys)` | delete the rows matching `keys`; return `num_deleted` when known |
| `read()` | full read as a `pyarrow.Table` |
| `marker()` | the current version (training-set) |
| `incremental(prev, cur)` | rows between two markers (training-set); `config` declares `incremental_emits_deletes` |
| `time_travel(marker)` | the table as of a marker (training-set) |
| `live_data_files(root)` | data files of the current version |
| `classify(relpath)` | `"data"` or `"meta"` |
| `crash_cause(plan, phase, in_flight)` | optional cause when a phase dies on a signal |

Raising `LegUnavailable` records the leg as unavailable instead of failed.
