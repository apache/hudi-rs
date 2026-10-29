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

Measures how long a single-node Python process takes to apply small upsert and
delete commits to a large Hudi table through hudi-rs, how many bytes each
commit writes, and how much memory the writer uses. It drives the public write
API of the `hudi` Python package (`HudiTable.create`, `append`, `upsert`,
`delete_keys`), so the numbers are those of the shipped code path rather than
of a harness-side reimplementation.

Three legs cover the choices that decide the write cost:

| leg                | table type     | record-level index |
|--------------------|----------------|--------------------|
| `hudi-cow`         | copy-on-write  | on                 |
| `hudi-mor`         | merge-on-read  | on                 |
| `hudi-cow-noindex` | copy-on-write  | off                |

Everything else is the writer's default (table version 9, metadata table on).
One run of `bench.py --scenario <name>` puts every leg through the same
scenario against the same data, and every leg's end state is checked against a
parquet oracle; a leg whose table does not match is reported as failed rather
than timed. The five scenarios are described under [Scenarios](#scenarios).

## Why the record-level index matters

To apply an upsert, a writer first has to answer, for each incoming key,
whether the key already exists and which file group holds it. Only then can it
rewrite that file group's base file (copy-on-write) or append the change to its
log (merge-on-read).

Per-file min/max statistics on the key column let a writer skip files whose key
range cannot contain an incoming key, which keeps lookups cheap when keys
arrive roughly in order (time-ordered ids, auto-increment ids, keys correlated
with a partition column). Random keys defeat that: in the `uuid` scenario every
file's `[min, max]` key range spans nearly the whole key space, so statistics
eliminate nothing and locating the keys touches the whole table. With `F` file
groups and `U` incoming keys drawn uniformly, a given file group holds none of
them with probability `(1 - 1/F)^U`; at the default `U = 10,000` that is zero
for any realistic `F`.

Hudi's record-level index is a key-to-file-group map stored in the table's
metadata table (under `.hoodie/metadata`, as HFiles keyed by record key). With
it, locating `U` keys is `U` point lookups whose cost depends on `U`, not on
the table size. The index removes the lookup cost; it does not by itself remove
the rewrite, and what follows the lookup depends on the table type:

- copy-on-write (`hudi-cow`) rewrites each file group that holds an incoming
  key, which with random keys at these batch sizes is every file group;
- merge-on-read (`hudi-mor`) appends the changed records to log files of those
  file groups, so bytes written scale with the batch rather than the table.

`hudi-cow-noindex` against `hudi-cow` isolates what the index saves, and
`hudi-cow` against `hudi-mor` isolates the write amplification. Maintaining the
index is not free either; it shows up as `meta_bytes_written` per commit.

## Commit structure

Identical for every leg and every scenario, deterministic from `--seed`:

1. **Data** (once, cached in the work dir): the scenario's base table, written
   as parquet parts, and every commit's input, written next to it, so all legs
   apply byte-identical batches with identical keys. The builder replays each
   commit against an in-memory copy of the table, so the expected end state is
   exact.
2. **Bulk load**: create the table and write the base table in one call.
   Timed and reported, but not the subject of the benchmark.
3. **`--commits` upsert commits** of full rows: existing keys whose non-key
   columns change, plus new keys where the scenario has them.
4. **`--commits` delete commits** of existing keys.
5. **Read-back**: a full read of the table, timed, then two checks outside the
   timed region. The sampled check covers the exact row count, unique keys,
   and the scenario's check columns for `--verify-sample` keys in each of four
   groups: updated, inserted, deleted (must be absent) and untouched (must be
   unchanged). The oracle check compares the whole table with a plain parquet
   copy of the expected end state (see [the oracle](#the-oracle)). A leg is
   `correct` only when both pass. The writer's reported counts (rows updated,
   inserted, deleted) are also asserted per commit.

## Scenarios

Each scenario has a default scale and a smoke scale (`--smoke`); any knob given
on the command line overrides both. Knobs a scenario does not list are ignored.

### `uuid`

Random-key upserts, the case [Why the record-level index matters](#why-the-record-level-index-matters) describes.

| column   | type    | content                                                    |
|----------|---------|------------------------------------------------------------|
| `id`     | string  | UUIDv4, the record key                                     |
| `ts`     | int64   | monotonic across the whole run; the ordering field         |
| `city`   | string  | one of 20 values (not a partition column)                  |
| `fare`   | float64 | uniform in [2.5, 250], two decimals                        |
| `status` | string  | one of 4 values                                            |
| `note`   | string  | 64 bytes: eight words drawn from a 512-word vocabulary     |

Each upsert commit takes `--upsert-batch` keys sampled uniformly from the live
keys plus `--insert-batch` new keys; each delete commit takes `--delete-batch`
live keys. Checked columns: `ts`, `fare`, `status`.

| knob | default | smoke |
|---|---|---|
| `--rows` | 10,000,000 | 100,000 |
| `--commits` | 5 | 2 |
| `--upsert-batch` / `--insert-batch` / `--delete-batch` | 10,000 / 1,000 / 1,000 | same |

```bash
benchmark/upsert/run.sh smoke --scenario uuid
```

### `tpcds-web-returns`

A merge workload on TPC-DS `web_returns`, generated with DuckDB's `tpcds`
extension (`CALL dsdgen(sf=...)`; DuckDB downloads the extension on first use)
and keyed on `(wr_order_number, wr_item_sk)` with no ordering field. Each
upsert commit samples `--matched-fraction` of the live rows as updates and
`--not-matched-fraction` as inserts (the sampled rows with `wr_order_number`
shifted past the largest live order number); each delete commit removes
`--delete-fraction` of the live rows. Matched rows get
`wr_return_quantity + 1`, so every matched row really changes and a writer that
skips unchanged rows still does the same work. Checked columns:
`wr_return_quantity`, `wr_return_amt`.

| knob | default | smoke |
|---|---|---|
| `--scale-factor` | 10 | 1 |
| `--commits` | 5 | 2 |
| `--matched-fraction` / `--not-matched-fraction` / `--delete-fraction` | 0.01 / 0.10 / 0.05 | same |

```bash
benchmark/upsert/run.sh smoke --scenario tpcds-web-returns
```

### `nyc-taxi`

NYC TLC yellow taxi trips, `yellow_tripdata_2022-01.parquet`
(about 2.46M trips), downloaded once into `<work-dir>/download/nyc-taxi/` and
read only as parquet. The download must have SHA-256
`797d6f008e67c3e3371b7f957ab97dbc2b38a9fb0ac083cb5eae4b0297b32372`; any other
file is refused. The file has no natural key, so the harness adds
`trip_id`, the row number as a zero-padded string, as the record key, with
`tpep_pickup_datetime` as the ordering field. Each upsert commit rewrites
`--upsert-fraction` of the live trips with new `fare_amount` and `tip_amount`
(pickup time unchanged, so the ordering value ties and the incoming row has to
win); each delete commit removes `--delete-fraction` of the live trips.
Checked columns: `fare_amount`, `tip_amount`.

| knob | default | smoke |
|---|---|---|
| `--rows` | 0 (the whole file) | 100,000 (the first rows) |
| `--commits` | 5 | 2 |
| `--upsert-fraction` / `--delete-fraction` | 0.01 / 0.001 | same |

```bash
benchmark/upsert/run.sh smoke --scenario nyc-taxi
```

### `rag-chunks`

The chunks table of a retrieval pipeline: documents are split into chunks, each
with an embedding, and a changed document is re-ingested whole.

| column       | type            | content                                       |
|--------------|-----------------|-----------------------------------------------|
| `chunk_id`   | string          | `<doc_id>#<pos>`, the record key              |
| `doc_id`     | string          | `doc-00000042`                                |
| `pos`        | int32           | chunk position in the document                |
| `text`       | string          | 512 bytes: 64 words from a 512-word vocabulary |
| `embedding`  | list\<float32\> | 384 uniform random floats                     |
| `updated_at` | int64           | monotonic; the ordering field                 |

Each upsert commit re-ingests `--changed-docs` documents (every chunk gets new
`text`, `embedding` and `updated_at`); each delete commit removes every chunk of
`--removed-docs` documents. Checked columns: `updated_at`, `embedding`.

| knob | default | smoke |
|---|---|---|
| `--docs` x `--chunks-per-doc` | 100,000 x 10 (1M chunks) | 1,000 x 10 |
| `--commits` | 5 | 2 |
| `--changed-docs` / `--removed-docs` | 1,000 / 100 | 100 / 10 |

```bash
benchmark/upsert/run.sh smoke --scenario rag-chunks
```

### `training-set`

A fine-tuning dataset that is curated in place: `training_examples` keyed by
`example_id`, with `messages` (a JSON chat transcript as a string), `label`,
`source_chunk_id` and `updated_at` (the ordering field). Upsert commits replace
`--upsert-batch` examples with corrected ones (plus `--insert-batch` new ones,
0 by default); delete commits drop `--delete-batch` rejected examples. Checked
columns: `updated_at`, `label`, `messages`.

On top of the write metrics, after every commit (outside its timed region) the
harness times the two reads a training job makes, and records the rows each
returns:

- **Incremental read** of what that commit changed: an `Incremental` query
  with `start_timestamp` set to the instant the write returned. On table
  version 9 that bound is a completion time and the write returns the
  requested time, so this selects exactly the new commit. After an upsert the
  result must equal the upsert's input, compared like the read-back oracle
  (same keys, one row each, same columns, types and values). A Hudi incremental
  read does not return deletes (the leg declares this with
  `incremental_emits_deletes: false`), so after a delete it must return no
  rows; a leg that declares it returns deletes must return exactly the deleted
  keys. Anything else fails the leg.
- **Time travel** to the commit written by the load (`as_of_timestamp`), which
  must equal the loaded table, compared like the read-back oracle (every row,
  key, type and value), so upserted keys must still carry their loaded values.

Reclaim is off for this scenario because both reads need superseded files.

| knob | default | smoke |
|---|---|---|
| `--rows` | 1,000,000 | 10,000 |
| `--commits` | 5 | 2 |
| `--upsert-batch` / `--insert-batch` / `--delete-batch` | 10,000 / 0 / 1,000 | 500 / 0 / 100 |

```bash
benchmark/upsert/run.sh smoke --scenario training-set
```

### Known limitations

- Composite record keys (`tpcds-web-returns`) need apache/hudi-rs#871. Before
  the load, a scenario with a multi-field key probes the writer with a one-row
  write to a scratch table keyed on two fields; if that is rejected, the legs
  are `unavailable` with a reason naming #871. The legs pass the key as a list
  and delete by `f1:v1,f2:v2` record key strings, the form that change writes.

Any other exception from the hudi package fails the leg.

## Reproduce

The write API the legs call (`HudiTable.create`, `upsert`, `delete_keys`) is in
apache/hudi-rs#878, which is not on `main` yet. Build from a checkout that
includes it, or pass a wheel built from one; `run.sh install-hudi` exits with
an error when the installed `hudi` has no `HudiTable.create`.

Prerequisites: [`uv`](https://docs.astral.sh/uv/) (it provides Python 3.13 for
the venv) and, to build hudi from source, the Rust toolchain this repository
uses (see `AGENTS.md`). The pinned packages are in `requirements.txt`.

From the repository root:

```bash
# 1. Environment: venv + pinned packages, then hudi built from this checkout
#    (maturin develop --release through build-wrapper.sh), or from wheels:
#    benchmark/upsert/run.sh install-hudi path/to/hudi-*.whl
benchmark/upsert/run.sh setup
benchmark/upsert/run.sh install-hudi

# 2. The harness's own tests.
benchmark/upsert/.venv/bin/python -m pytest benchmark/upsert/tests -q

# 3. Smoke every scenario, every leg. Smoke runs are not timed results, so a
#    busy machine only warns; each writes <work-dir>/smoke-<scenario>.json.
for s in uuid tpcds-web-returns nyc-taxi rag-chunks training-set; do
  benchmark/upsert/run.sh smoke --scenario "$s"
done

# 4. Full scale: three repetitions of one leg per invocation, the invocations
#    one after another (never in parallel), on a machine with nothing else
#    running. The headroom gate refuses to time anything on a busy machine.
for s in uuid tpcds-web-returns nyc-taxi rag-chunks training-set; do
  for leg in hudi-cow hudi-mor hudi-cow-noindex; do
    benchmark/upsert/run.sh full --scenario "$s" --legs "$leg" --repeat 3 \
      --out "benchmark/upsert/results/$s-$leg.json"
  done
done

# 5. The table.
benchmark/upsert/run.sh summarize benchmark/upsert/results/*.json --markdown
```

To time anyway on a machine that fails the headroom gate (below), pass
`--force --settle-timeout 0`; the results are marked and `summarize.py` then
needs `--allow-loaded`. `bench.py --help` lists every option; `run.sh full`
and `run.sh smoke` pass their arguments through.

Generated data, tables and per-phase logs go under `--work-dir` (default
`$TMPDIR/hudi-upsert-bench`, or `UPSERT_BENCH_WORK_DIR`). At 10M rows the
`uuid` base data is about 0.7 GB. Each leg's table is deleted when the leg
finishes unless `--keep-tables` is passed; the base data stays, so later runs
reuse it. Tables and phase logs sit under `tables/<run>/` and `logs/<run>/`,
where `<run>` names the scenario, scale and workload, so runs at different
scales never share a directory. A run takes `locks/<run>.lock` for its
duration, and a second run of the same `<run>` refuses to start (a lock left by
a process that no longer exists is taken over). `--leg-timeout SECONDS` caps
each repetition's wall clock; a repetition that exceeds it is killed and the
leg is recorded as `timeout` with the commits it completed.

## What is measured

Each phase of each leg (load, upserts, deletes, read-back) runs in its own
subprocess, so peak RSS (`ru_maxrss`) is per phase and one leg's memory never
appears in another's.

**Repetitions.** `--repeat N` (default 3, 1 to 5) runs each leg N times, one
repetition after another and never in parallel. Each repetition deletes the
leg's table, rebuilds it from the same base data, replays the same workload in
its own phase subprocesses, and passes the headroom gate before every timed
call. Every repetition runs the full correctness checks, and a leg is
`correct` only if every repetition is. The results keep each repetition
(`legs.<leg>.repetitions[]`) and aggregate the load, read-back, incremental
and time-travel times and the per-commit upsert and delete times across them
(`summary.stats`: mean, median, min, max and coefficient of variation; for
upserts and deletes each repetition contributes its mean over its commits).
Bytes and files written do not depend on timing, so they are recorded once,
from the first repetition, and every other repetition is compared with it.
File counts must match exactly. Byte counts may drift slightly, because instant
times, commit metadata and file names embedded in the files differ from run to
run, so only a difference above 1 percent of the larger value counts; smaller
ones are not recorded. `summary.writes_consistent` is false and
`summary.write_mismatches` lists the step and field when either check fails. A
single repetition produces the same shape.

- **Commit time** brackets only the library call (`append`, `upsert`,
  `delete_keys`, the read). It excludes reading the commit's input from
  parquet, opening the table (once per phase), and the directory scans around
  each commit. The first commit in a phase also pays any one-time in-process
  setup, which is why the median is reported next to the mean.
- **Bytes written** per commit: the size of every file that appeared in the
  table directory plus the growth of any file that grew, split into data files
  (base and log files) and metadata (everything under `.hoodie/`, including
  the metadata table and the record-level index).
- **Files created** per commit, split the same way.
- **Peak RSS** per phase, with the process RSS just before the measured call
  (`rss_before_bytes`). The load phase holds the whole base table in Arrow
  memory as its input and the read-back phase holds the whole table as its
  output, so their peaks mostly reflect that; the upsert and delete phases are
  the ones that show the writer's own memory.
- **Read-back** time of a full snapshot read into Arrow.

**Headroom gate.** Timings from a busy machine say more about the machine
than about the writer, so before each leg and before every timed call (the
load, each commit, the read-back, and in `training-set` each incremental and
time-travel read) the harness reads the 1-minute load average,
available memory and free disk on the work dir's volume, and refuses to go on
when load1 is above `--max-load1` (default 2.0), available memory is below
`--min-mem-gib` (default 16), or free disk is below 3x the base dataset. Since
load1 is a moving average the benchmark itself raises, when it is the only
violation the gate first waits up to `--settle-timeout` seconds (default 180)
for it to come down, outside any timed region; memory and disk shortfalls do
not settle, so they are judged at once. Every reading is stored next to the
leg and commit it preceded. `--force` turns a load or memory refusal into a
warning and marks the reading `"headroom_violated": true`; a disk shortfall
refuses even then, because a copy-on-write commit can rewrite the whole table
and running the disk out mid-commit is worse than not running. `summarize.py`
refuses to render a file with any violated reading unless given
`--allow-loaded`. A `--smoke` run turns load and memory refusals into warnings
and does not wait for load to settle (`--settle-timeout` defaults to 0 there),
since it is a setup check and not a timed result; under `--force` the gate
never waits. Nothing else
should run on the machine while a leg is being timed, and legs are best run
one per invocation.

**Reclaim.** After each commit, outside the timed region, the harness deletes
data files the latest file slices no longer reference (`--no-reclaim` turns
this off). No later commit or read touches those files, so it changes no
measured operation; it keeps a copy-on-write run from accumulating a full
table copy per commit. Per-commit `reclaimed_files` / `reclaimed_bytes` are in
the results.

**Not measured.** Object storage: everything runs on local disk, where I/O is
cheap and the numbers are CPU-bound; on S3 every file a commit reads or
rewrites pays per-request latency that a local run does not show. Concurrency,
compaction, clustering and cleaning are out of scope.

### The oracle

The workload builder replays every commit against an in-memory Arrow copy of
the base table and writes the result as plain parquet:
`<work-dir>/oracle/<scenario>-<scale>-<workload>/expected.parquet`, sorted by
the record key, with every column and plain types (no view or large variants,
every field nullable). The `<workload>` suffix keeps two runs at the same
scale with different commit knobs from sharing a file. Scenarios with
incremental reads also get `base.parquet`, the loaded table, which time travel
is compared against.

In the read-back phase every leg's full table is normalized and compared with
`pyarrow.Table.equals`:

- engine columns (`_hoodie_*`) are dropped; any other missing or extra column
  is a mismatch;
- each column's type must equal the oracle's after only two normalizations,
  view and large types to plain and list item field names; any other
  difference (float64 read as string, int64 as int32, decimal as float64, a
  different timestamp unit or time zone) is a mismatch reported with both
  types, before any value is compared;
- columns are put in the oracle's order and rows are sorted by the record key.

The sampled check applies the same type rule to its check columns.

Nothing is compared with a tolerance: floats, including each element of the
`embedding` lists, must be identical, which the deterministic generators
allow. On a mismatch the results name the first differing keys per column
with both values (for a list, the first differing element), or the missing,
extra and duplicate keys. The oracle path and the row counts compared are in
each leg's `phases.readback.checks.oracle`.

## Results

No results are published yet; results files land in `results/` as
`<scenario>-<leg>.json`, one leg per file, each run alone on an idle machine
(see [Reproduce](#reproduce)). `summarize.py` shows each timing as the median
across repetitions with the range, for example `1.23 s (1.20 to 1.31)`
(`--stat mean` for the mean), a `Reps` column, and notes for any timing whose
coefficient of variation is above 10 percent and for writes that differ
between repetitions. It also prints the headroom readings (maximum load
average, minimum available memory and free disk seen by the gate) under the
table, so the readings travel with the numbers.

## Tests

The harness's own pass/fail logic (key strings, the sampled check, the oracle
comparison, byte accounting, the reclaim guard, the headroom gate, the run
lock, leg registration, the composite-key probe and record key form, the
incremental and time-travel checks, the workload replay, and the aggregation
across repetitions, including an end-to-end run of a plain-parquet leg at
`--repeat` 1 and 2) has unit tests that need only pyarrow:

```bash
benchmark/upsert/.venv/bin/python -m pytest benchmark/upsert/tests -q
```

## Adding a leg

A leg is a subclass of `Leg` in `bench.py`, made available to `--legs` with
`register_leg(name, factory)`. A leg can also live outside this directory:
`--extra-legs PATH` imports the module at `PATH` in the driver and in every
phase process and calls its `register(api)`, where `api.Leg`,
`api.LegUnavailable` and `api.register_leg` are the same objects `bench.py`
uses. Extra legs get the same oracle check, headroom gate and per-phase
processes as the built-in ones.

```python
def register(api):
    class MyLeg(api.Leg):
        name = "my-leg"
        ...

    api.register_leg("my-leg", MyLeg)
```

`plan` is the scenario's `plan.json`: key columns, ordering field, check
columns and whether the scenario needs incremental reads.

| method                        | does                                                        |
|-------------------------------|-------------------------------------------------------------|
| `config(plan)`                | dict recorded in the results (package, version, settings)   |
| `load(root, table, plan)`     | create the table under `root`, write the Arrow table, return `{"marker": ...}` for the load's version |
| `open(root, plan)`            | open the existing table for the phase                       |
| `upsert(table)`               | apply one upsert batch; return `num_updated` / `num_inserted` when known |
| `delete(keys)`                | delete the rows whose key columns match the Arrow table `keys`; return `num_deleted` when known |
| `read()`                      | full read as a `pyarrow.Table`                              |
| `marker()`                    | the current version (training-set only)                     |
| `incremental(prev, cur)`      | `(rows returned, changed rows, deleted keys or None)` between two markers (training-set only); `config` declares `incremental_emits_deletes` |
| `time_travel(marker)`         | the table as of a marker (training-set only)                |
| `live_data_files(root)`       | data files of the current version, relative to `root`       |
| `classify(relpath)`           | `"data"` or `"meta"` for a file in the table directory      |
| `crash_cause(plan, phase, in_flight)` | optional one-line cause when a phase dies on a signal |

Raise `LegUnavailable` from any of them when the library, or a capability the
scenario needs, is missing; the run then records the leg as unavailable with
the reason instead of failing it. A scenario is a `Scenario` subclass in
`scenarios.py` that writes the base table and returns each commit's upsert rows
and delete keys from the current state.
