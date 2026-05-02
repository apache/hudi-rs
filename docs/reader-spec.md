<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# hudi-rs Reader Spec

The public reader APIs hudi-rs exposes for Apache Hudi tables: the methods callers can call, the options they accept, and what callers can expect back.

For tutorials and end-to-end examples, see the [README](../README.md). Covers Rust core (`hudi`) and the Python bindings (`hudi`); writer APIs, DataFusion, and C++ bindings are out of scope here.

## Contents

1. [Read modes](#1-read-modes)
2. [`ReadOptions`](#2-readoptions)
3. [Filter expressions](#3-filter-expressions)
4. [Rust API](#4-rust-api)
5. [Python API](#5-python-api)
6. [Caller expectations](#6-caller-expectations)
7. [Stability and out of scope](#7-stability-and-out-of-scope)

## 1. Read modes

- **Snapshot** — latest table state at one commit (the latest by default; an explicit `as_of_timestamp` for time-travel).
- **Incremental** — records changed in the half-open range (`start_timestamp`, `end_timestamp`].
- **Per-file-slice** — read a single `FileSlice` the caller already selected (typically obtained from `get_file_slices` with the right options).

Snapshot and per-file-slice reads each have an **eager** form returning all batches and a **streaming** form yielding batches as they're read. Incremental reads currently expose only the eager form.

## 2. `ReadOptions`

`ReadOptions` (Rust) / `HudiReadOptions` (Python) is the single config object accepted by every reader API. `query_type` selects the read semantic (snapshot vs incremental); the timestamp fields are interpreted accordingly.

| Field              | Type                          | Default                                | Used by                                              |
|--------------------|-------------------------------|----------------------------------------|------------------------------------------------------|
| `query_type`       | `QueryType`                   | `Snapshot`                             | all read APIs (dispatch)                             |
| `filters`          | `Vec<(field, op, value)>`     | empty                                  | all read APIs (pruning + row-level mask)             |
| `projection`       | `Option<Vec<String>>`         | all columns                            | all read APIs                                        |
| `batch_size`       | `Option<usize>`               | `hoodie.read.stream.batch_size` (1024) | streaming APIs only                                  |
| `as_of_timestamp`  | `Option<String>`              | latest commit                          | `Snapshot` only                                      |
| `start_timestamp`  | `Option<String>`              | `19700101000000000`                    | `Incremental` only                                   |
| `end_timestamp`    | `Option<String>`              | latest commit                          | `Incremental` only                                   |
| `hudi_options`     | `HashMap<String, String>`     | empty                                  | overrides table-level Hudi configs for this read     |

Which fields each API consumes:

| API                                                              | `query_type` | `as_of_timestamp` | `start`/`end_timestamp` | `filters` | `projection` | `batch_size` | `hudi_options` |
|------------------------------------------------------------------|:------------:|:-----------------:|:-----------------------:|:---------:|:------------:|:------------:|:--------------:|
| `read` / `read_stream`                                           | yes          | when Snapshot     | when Incremental        | yes       | yes          | streaming    | yes            |
| `get_file_slices`                                                | yes          | when Snapshot     | when Incremental        | yes       | —            | —            | —              |
| `FileGroupReader::read_file_slice` / `_from_paths`               | —            | —                 | —                       | yes       | yes          | —            | —              |
| `FileGroupReader::read_file_slice_stream` / `_from_paths_stream` | —            | —                 | —                       | yes       | yes          | yes          | —              |

Notes:

- `read_stream` errors with `Unsupported` for `query_type = Incremental` — incremental streaming is not yet implemented.
- `hudi_options` is a per-read override bag — common use is `("hoodie.read.use.read_optimized.mode", "true")` to skip log merging for one read without mutating the `Table`.
- Per-slice reads are exposed only by `FileGroupReader`. The `Table` type owns logical reads (snapshot, incremental); per-slice reads are physical and belong at the file-group layer. To read one slice with table-level configs, build a `FileGroupReader` via `Table::create_file_group_reader_with_options` and call its per-slice methods.
- For parallel reads, call `get_file_slices(...)` and bucket the result with `hudi_core::util::collection::split_into_chunks` or your engine's preferred partitioning policy.

Timestamp formats are documented in [§6](#timestamps).

## 3. Filter expressions

A filter is a `(field, operator, value)` tuple of strings.

| Operator (case-insensitive)         | Cardinality |
|-------------------------------------|:-----------:|
| `=` `!=` `<` `<=` `>` `>=`          | 1           |
| `IN` `NOT IN`                       | ≥1          |

For `IN` / `NOT IN`, the value string is split on commas and trimmed: `("city", "IN", "sf,la,nyc")`.

The `field` may be any column. Filters drive three things:

- **Partition pruning** when the field is a partition column. Always applied.
- **File-level stats pruning** when the field is a data column with min/max stats in the metadata table. Snapshot/time-travel only — incremental file planning does not stats-prune.
- **Row-level mask** applied to every returned batch. This is the authoritative filter; pruning is best-effort.

Values are strings; they are cast to the target column's Arrow type at filter time. Unparseable values (e.g. `"abc"` against `Int64`) error.

A filter on an unknown column errors before any rows are returned. `Table` / `HudiTable` paths validate up front against the loaded schemas. `FileGroupReader` paths validate strictly against the read batch schema on the first batch — direct callers must not pass filters on columns absent from the parquet (notably partition columns when `hoodie.datasource.write.drop.partition.columns = true`); the table-level paths strip such filters automatically after using them for pruning.

## 4. Rust API

All public symbols are re-exported from the `hudi` crate.

### `Table`

| Method                                                                     | Returns                                              |
|----------------------------------------------------------------------------|------------------------------------------------------|
| `Table::new(base_uri)`                                                     | `Result<Table>`                                      |
| `Table::new_with_options(base_uri, options)`                               | `Result<Table>`                                      |
| `hudi_options()` / `storage_options()`                                     | `HashMap<String, String>`                            |
| `base_url()` / `table_name()` / `table_type()` / `is_mor()` / `timezone()` | `Url` / `String` / `String` / `bool` / `String`      |
| `get_schema()` / `get_schema_with_meta_fields()`                           | `Result<Schema>`                                     |
| `get_schema_in_avro_str()` / `get_schema_in_avro_str_with_meta_fields()`   | `Result<String>`                                     |
| `get_partition_schema()`                                                   | `Result<Schema>`                                     |
| `get_timeline()`                                                           | `&Timeline`                                          |
| `get_file_slices(&ReadOptions)`                                            | `Result<Vec<FileSlice>>` (dispatches on `query_type`) |
| `create_file_group_reader_with_options(options)`                           | `Result<FileGroupReader>`                            |
| `read(&ReadOptions)`                                                       | `Result<Vec<RecordBatch>>` (dispatches on `query_type`) |
| `read_stream(&ReadOptions)`                                                | `Result<BoxStream<'static, Result<RecordBatch>>>` (errors on `Incremental`) |
| `compute_table_stats()`                                                    | `Option<(u64, u64)>` — `(rows, byte_size)`           |

### `FileGroupReader`

| Method                                                                  | Returns                                              |
|-------------------------------------------------------------------------|------------------------------------------------------|
| `FileGroupReader::new_with_options(base_uri, options)`                  | `Result<FileGroupReader>`                            |
| `read_file_slice(&FileSlice, &ReadOptions)`                             | `Result<RecordBatch>` (base + merge logs)            |
| `read_file_slice_from_paths(base_path, log_paths, &ReadOptions)`        | `Result<RecordBatch>` (pass empty log_paths for base-only) |
| `read_file_slice_stream(&FileSlice, &ReadOptions)`                      | `Result<BoxStream<'static, Result<RecordBatch>>>`    |
| `read_file_slice_from_paths_stream(base_path, log_paths, &ReadOptions)` | `Result<BoxStream<'static, Result<RecordBatch>>>`    |
| `is_metadata_table()`                                                   | `bool`                                               |

### `ReadOptions` builder

```rust
ReadOptions::new()
    .with_query_type(QueryType::Snapshot)
    .with_filters([("city", "=", "san_francisco")])
    .with_projection(["rider", "city", "ts", "fare"])
    .with_batch_size(4096)
    .with_as_of_timestamp("20240101000000000")
    .with_start_timestamp("20240101000000000")
    .with_end_timestamp("20240201000000000")
    .with_hudi_option("hoodie.read.use.read_optimized.mode", "true");
```

### `TableBuilder`

```rust
use hudi::table::builder::TableBuilder as HudiTableBuilder;

let table = HudiTableBuilder::from_base_uri("/tmp/trips_table")
    .with_hudi_option("hoodie.read.use.read_optimized.mode", "true")
    .with_storage_option("aws_region", "us-west-2")
    .build()
    .await?;
```

Available pairs: `with_hudi_option` / `with_hudi_options`, `with_storage_option` / `with_storage_options`, `with_option` / `with_options` (the generic forms route by key prefix).

### Filter, Timeline, FileSlice

| Item                                                                                                          | Notes                                                  |
|---------------------------------------------------------------------------------------------------------------|--------------------------------------------------------|
| `Filter { field_name, operator, values }`, `Filter::new(...)`, `Filter::negate()`                             | One column predicate; cardinality-validated.           |
| `from_str_tuples(tuples)`                                                                                     | Parse `(&str, &str, &str)` tuples into `Vec<Filter>`.  |
| `enum ExprOperator { Eq, Ne, Lt, Lte, Gt, Gte, In, NotIn }`                                                   | Comparison operators.                                  |
| `col(name).eq / ne / lt / lte / gt / gte / in_list / not_in_list`                                             | DSL for building filters.                              |
| `Timeline::get_completed_commits(desc)` and `..._deltacommits` / `_replacecommits` / `_clustering_commits`    | Successful instants. `desc` reverses order.            |
| `Timeline::get_latest_commit_timestamp()`                                                                     | Latest commit timestamp.                               |
| `Timeline::get_instant_metadata_in_json(&Instant)`                                                            | Commit metadata for one instant.                       |
| `Timeline::get_latest_avro_schema()` / `get_latest_schema()`                                                  | Latest schema (Avro string / Arrow `Schema`).          |
| `FileSlice::file_id()` / `creation_instant_time()` / `has_log_file()`                                         | Slice identity / version / MOR-with-deltas flag.       |
| `FileSlice::base_file_relative_path()` / `log_file_relative_path(&LogFile)`                                   | Paths relative to the table base URI.                  |

## 5. Python API

All symbols are exported from the top-level `hudi` package.

### `HudiTableBuilder`

```python
from hudi import HudiTableBuilder

table = (
    HudiTableBuilder
    .from_base_uri("/tmp/trips_table")
    .with_hudi_option("hoodie.read.use.read_optimized.mode", "true")
    .with_storage_option("aws_region", "us-west-2")
    .build()
)
```

`with_hudi_option` and `with_option` accept a string key or a `HudiReadConfig` / `HudiTableConfig` enum member. The bulk variants (`with_hudi_options`, `with_options`) currently accept dicts of string keys.

### `HudiTable`

| Method / property                                                                                  | Returns                                  |
|----------------------------------------------------------------------------------------------------|------------------------------------------|
| `HudiTable(base_uri, options=None)`                                                                | `HudiTable`                              |
| `hudi_options()` / `storage_options()`                                                             | `Dict[str, str]`                         |
| properties: `table_name`, `table_type`, `is_mor`, `timezone`, `base_url`                           | table-level metadata                     |
| `get_schema()` / `get_schema_with_meta_fields()` / `get_partition_schema()`                        | `pyarrow.Schema`                         |
| `get_schema_in_avro_str()` / `get_schema_in_avro_str_with_meta_fields()`                           | `str`                                    |
| `get_timeline()`                                                                                   | `HudiTimeline`                           |
| `get_file_slices(options=None)`                                                                    | `List[HudiFileSlice]` (dispatches on `options.query_type`) |
| `create_file_group_reader_with_options(options=None)`                                              | `HudiFileGroupReader`                    |
| `read(options=None)`                                                                               | `List[pyarrow.RecordBatch]` (dispatches on `query_type`) |
| `read_stream(options=None)`                                                                        | `HudiRecordBatchStream` (errors on `Incremental`) |
| `compute_table_stats()`                                                                            | `Optional[Tuple[int, int]]`              |

### `HudiFileGroupReader`

| Method                                                                     | Returns                          |
|----------------------------------------------------------------------------|----------------------------------|
| `HudiFileGroupReader(base_uri, options=None)`                              | `HudiFileGroupReader`            |
| `read_file_slice(file_slice, options=None)`                                | `pyarrow.RecordBatch`            |
| `read_file_slice_from_paths(base_path, log_paths, options=None)`           | `pyarrow.RecordBatch` (pass empty log_paths for base-only) |
| `read_file_slice_stream(file_slice, options=None)`                         | `HudiRecordBatchStream`          |
| `read_file_slice_from_paths_stream(base_path, log_paths, options=None)`    | `HudiRecordBatchStream`          |

### `HudiReadOptions`

```python
from hudi import HudiQueryType, HudiReadOptions

options = HudiReadOptions(
    query_type=HudiQueryType.Snapshot,  # or HudiQueryType.Incremental
    filters=[("city", "=", "san_francisco")],
    projection=["rider", "city", "ts", "fare"],
    batch_size=4096,
    as_of_timestamp="20240101000000000",
    start_timestamp="20240101000000000",
    end_timestamp="20240201000000000",
    hudi_options={"hoodie.read.use.read_optimized.mode": "true"},
)
```

All fields optional; defaults match [§2](#2-readoptions).

### `HudiRecordBatchStream`

A single-use iterator returned by streaming APIs. `for batch in stream:` or `next(stream)`. Each yielded value is a `pyarrow.RecordBatch`.

### `HudiTimeline`, `HudiInstant`, `HudiFileSlice`

| Item                                                                                             | Notes                                                  |
|--------------------------------------------------------------------------------------------------|--------------------------------------------------------|
| `HudiTimeline.get_completed_commits(desc=False)` and `..._deltacommits` / `_replacecommits` / `_clustering_commits` | `List[HudiInstant]`. `desc` reverses order. |
| `HudiTimeline.get_instant_metadata_in_json(instant)`                                             | `str`                                                  |
| `HudiTimeline.get_latest_commit_timestamp()`                                                     | `str`                                                  |
| `HudiTimeline.get_latest_avro_schema()` / `get_latest_schema()`                                  | `str` / `pyarrow.Schema`                               |
| `HudiInstant` properties: `timestamp`, `action`, `state`, `epoch_mills`                          | read-only                                              |
| `HudiFileSlice` attributes: `file_id`, `partition_path`, `creation_instant_time`, `base_file_name`, `base_file_size`, `base_file_byte_size`, `log_file_names`, `num_records` | read-only |
| `HudiFileSlice.base_file_relative_path()` / `log_files_relative_paths()`                         | `str` / `List[str]`                                    |

## 6. Caller expectations

### Snapshot atomicity

A snapshot read is pinned to one completed commit timestamp; mid-read writes are not visible. The timeline is loaded once when `Table` / `HudiTable` is constructed and reused for every subsequent read on that instance — commits that land *after* construction require building a new instance to observe. When `as_of_timestamp` is unset, "latest commit" means the latest commit in the cached timeline, not in storage at call time.

### Incremental semantics

The range is half-open: (`start_timestamp`, `end_timestamp`]. A record updated multiple times within the range yields its latest in-range state, not the full update history. Records updated only outside the range are not returned.

### MOR streaming fallback

Streaming yields true streaming batches when the slice is base-file-only or `hoodie.read.use.read_optimized.mode = true`. For MOR slices with log files, the implementation collects-and-merges and yields the result as a single batch on the stream.

### `batch_size` and `projection`

`batch_size` controls rows per batch for streaming reads (default 1024); eager reads return one merged batch per file slice and ignore `batch_size`. Streaming pushes `projection` down to the parquet reader; eager reads project after merging. When `projection` is combined with `filters` on data columns not in `projection`, the read transparently widens to read those columns, then projects back down after the filter mask runs.

### Timestamps

`as_of_timestamp`, `start_timestamp`, and `end_timestamp` accept:

- Hudi timeline format (highest precedence): `yyyyMMddHHmmssSSS` or `yyyyMMddHHmmss`.
- Unix epoch in seconds, milliseconds, microseconds, or nanoseconds.
- RFC 3339 with timezone offset: `2024-03-15T14:25:30Z`, `2024-03-15T14:25:30+00:00`, `2024-03-15T14:25:30.123Z`.

A timezone offset (`Z` or `±HH:MM`) is required for RFC 3339 inputs — naive `T`-separated strings and date-only strings are rejected. Inputs are normalized into `hoodie.table.timeline.timezone`.

### Empty results

A table with no completed commits yields empty `Vec` / `List` for eager reads and `get_file_slices`, and an empty stream for `read_stream`.

### Errors

- Unknown filter column: schema error before any rows are returned. `Table` paths validate up front; `FileGroupReader` paths validate strictly on the first batch.
- Unparseable filter value (e.g. `"abc"` against `Int64`): schema error at filter evaluation.
- I/O failures (missing files, permission errors, malformed parquet): read errors.

## 7. Stability and out of scope

Reader APIs documented here are the supported public surface as of this release. The `FileGroupReader` direct-paths APIs are still labeled experimental in the README; expect minor signature evolution before they finalize.

`compute_table_stats()` returns `None` when statistics cannot be computed (no metadata table, non-Parquet base files, or footer reads fail). It does not yet account for log files in MOR tables, so estimates skew low for write-heavy MOR workloads.

Out of scope for this version: writer APIs, internal architecture (timeline parsing, log-record merging, metadata table layout), the full configuration key glossary (`HudiReadConfig` / `HudiTableConfig` members), and the DataFusion and C++ bindings — separate spec follow-ups.
