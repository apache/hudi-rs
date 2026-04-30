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

This document specifies the public reader APIs that hudi-rs exposes for Apache Hudi tables. It is a reference: it describes what each API does, what its inputs mean, and what guarantees it provides.

For tutorials and end-to-end examples, see the [README](../README.md). For the Hudi storage format itself, see the upstream [Apache Hudi documentation](https://hudi.apache.org/docs/overview).

## Contents

1. [Scope](#1-scope)
2. [Concepts](#2-concepts)
3. [Read modes](#3-read-modes)
4. [`ReadOptions`](#4-readoptions)
5. [Filter expressions](#5-filter-expressions)
6. [Rust API](#6-rust-api)
7. [Python API](#7-python-api)
8. [Behavioral notes](#8-behavioral-notes)
9. [Stability](#9-stability)
10. [Out of scope](#10-out-of-scope)

## 1. Scope

This v1 covers the **Rust core crate** (`hudi`) and the **Python bindings** (`hudi`). Other surfaces are out of scope here and are linked from the README:

- Apache DataFusion integration (`HudiDataSource` / `HudiDataFusionDataSource`).
- C++ bindings (cxx bridge over the file group reader).

Writer APIs are also out of scope; hudi-rs does not yet expose a public writer surface.

## 2. Concepts

A **Hudi table** is a directory tree at a base URI, holding a write-ahead **timeline** of commit metadata under `.hoodie/` plus the data files themselves. hudi-rs treats this layout as immutable input.

A **timeline instant** is a single timeline entry — a commit, deltacommit, replacecommit, or clustering commit — identified by an action and a timestamp.

A **file group** is the unit of update locality: an `(partition_path, file_id)` pair. Updates to a record always land in the same file group.

A **file slice** is one version of a file group at a given commit: a base file (Parquet, by default) plus zero or more log files (Avro/HFile) appended after the base. A snapshot read materializes one file slice per file group.

A table is one of two **types**:

- **Copy-on-Write (COW)** — every write produces a new base file. Reads return the latest base file per file group.
- **Merge-on-Read (MOR)** — writes produce log files appended to a file group. Reads merge the base file and its log files at read time.

## 3. Read modes

Reads in hudi-rs come in three modes:

- **Snapshot read.** Returns the latest table state at a single commit (the latest by default, or a specific timestamp via `as_of_timestamp` for *time-travel*).
- **Time-travel read.** A snapshot read pinned to an explicit `as_of_timestamp`.
- **Incremental read.** Returns records changed in the half-open range (`start_timestamp`, `end_timestamp`].

Snapshot and per-file-slice reads have both an **eager** form that returns all batches at once and a **streaming** form that yields batches as they are read. Incremental reads currently expose only the eager form. Streaming honors `batch_size`; eager reads return one merged batch per file slice.

## 4. `ReadOptions`

`ReadOptions` (Rust) and `HudiReadOptions` (Python) are the single configuration object accepted by every reader API. Fields:

| Field              | Type                          | Default                           | Used by                                                  |
|--------------------|-------------------------------|-----------------------------------|----------------------------------------------------------|
| `filters`          | `Vec<(field, op, value)>`     | empty                             | all read APIs (pruning + row-level mask)                 |
| `projection`       | `Option<Vec<String>>`         | `None` (all columns)              | all read APIs                                            |
| `batch_size`       | `Option<usize>`               | `hoodie.read.stream.batch_size` (1024) | streaming APIs only                                  |
| `as_of_timestamp`  | `Option<String>`              | latest commit                     | snapshot / time-travel APIs                              |
| `start_timestamp`  | `Option<String>`              | `19700101000000000` (epoch start) | incremental APIs                                         |
| `end_timestamp`    | `Option<String>`              | latest commit                     | incremental APIs                                         |

### Field interaction matrix

| API                                    | `as_of_timestamp` | `start_timestamp` / `end_timestamp` | `filters` | `projection` | `batch_size` |
|----------------------------------------|:-----------------:|:-----------------------------------:|:---------:|:------------:|:------------:|
| `read_snapshot`                        | yes               | ignored                             | yes       | yes          | ignored      |
| `read_snapshot_stream`                 | yes               | ignored                             | yes       | yes          | yes          |
| `get_file_slices`                      | yes               | ignored                             | yes       | n/a          | n/a          |
| `get_file_slices_splits`               | yes               | ignored                             | yes       | n/a          | n/a          |
| `read_incremental_records`             | ignored           | yes                                 | yes       | yes          | ignored      |
| `get_file_slices_between`              | ignored           | yes                                 | yes       | n/a          | n/a          |
| `get_file_slices_splits_between`       | ignored           | yes                                 | yes       | n/a          | n/a          |
| `read_file_slice` / `read_file_slice_by_base_file_path` / `read_file_slice_from_paths` | ignored | ignored | yes | yes | ignored |
| `read_file_slice_stream` / `read_file_slice_from_paths_stream` | ignored (on `FileGroupReader`); used to pin commit (on `Table`) | ignored | yes | yes | yes |

"ignored" means the field is accepted (the type is the same everywhere) but does not affect the result of that call.

Accepted timestamp formats are listed in [§8 Behavioral notes](#timestamp-formats).

## 5. Filter expressions

A filter is a `(field, operator, value)` tuple of strings.

### Operators

| Token (case-insensitive) | Meaning                  | Cardinality |
|--------------------------|--------------------------|:-----------:|
| `=`                      | equal                    | 1           |
| `!=`                     | not equal                | 1           |
| `<`                      | less than                | 1           |
| `<=`                     | less than or equal       | 1           |
| `>`                      | greater than             | 1           |
| `>=`                     | greater than or equal    | 1           |
| `IN`                     | value in set             | ≥1          |
| `NOT IN`                 | value not in set         | ≥1          |

For `IN` / `NOT IN`, the value string is split on commas and trimmed. Examples: `("city", "IN", "sf,la,nyc")`, `("city", "NOT IN", "boston, austin")`.

### Field targeting

The filter `field` may be any column name — partition or data:

- **Partition column.** The filter prunes whole partitions before reading.
- **Data column with statistics.** When the column has min/max stats in the metadata table, the filter prunes whole files via column-stats pruning.
- **Any column.** All filters are applied as a row-level mask after reading; only matching rows are returned.

A filter referencing an unknown column (not in the table or partition schema) is rejected at the entry point with a schema error — typos like `("rder_id", "=", "x")` fail loud rather than silently no-op.

### Pruning vs row mask

Pruning (partition + file-level) is **best effort**: hudi-rs uses it to skip data when it can prove a partition or file cannot match, but it does not need to be tight. The **row-level mask** is exact: any row in the result satisfies every filter.

### Value parsing

Values are strings; they are cast to the target column's Arrow type at filter-evaluation time. Unparseable values (e.g. `"abc"` against an `Int64` column) surface as a schema error from the reader, not a silent miss.

## 6. Rust API

All public symbols below are re-exported from the `hudi` crate.

### `Table`

Construct, inspect, and read a Hudi table.

| Method                                                                            | Description                                                                                             |
|-----------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------|
| `Table::new(base_uri) -> Result<Table>`                                           | Construct from a base URI with default options.                                                         |
| `Table::new_with_options(base_uri, options) -> Result<Table>`                     | Construct with mixed Hudi/storage options.                                                              |
| `hudi_options() -> HashMap<String,String>`                                        | The Hudi configuration in effect.                                                                       |
| `storage_options() -> HashMap<String,String>`                                     | The storage configuration in effect.                                                                    |
| `base_url() -> Url`                                                               | The table's base URL.                                                                                   |
| `table_name() -> String`                                                          | Table name from `hoodie.table.name`.                                                                    |
| `table_type() -> String`                                                          | `"COPY_ON_WRITE"` or `"MERGE_ON_READ"`.                                                                 |
| `is_mor() -> bool`                                                                | Convenience for `table_type() == MERGE_ON_READ`.                                                        |
| `timezone() -> String`                                                            | Timeline timezone.                                                                                      |
| `get_schema_in_avro_str() -> Result<String>`                                      | Latest Avro schema, no Hudi meta fields.                                                                |
| `get_schema_in_avro_str_with_meta_fields() -> Result<String>`                     | Latest Avro schema, with `_hoodie_*` meta fields.                                                       |
| `get_schema() -> Result<Schema>`                                                  | Latest Arrow schema, no meta fields.                                                                    |
| `get_schema_with_meta_fields() -> Result<Schema>`                                 | Latest Arrow schema, with meta fields.                                                                  |
| `get_partition_schema() -> Result<Schema>`                                        | Arrow schema of the partition columns.                                                                  |
| `get_timeline() -> &Timeline`                                                     | Borrow the table's timeline.                                                                            |
| `get_file_slices(&ReadOptions) -> Result<Vec<FileSlice>>`                         | File slices at a snapshot moment. Empty for an empty table.                                             |
| `get_file_slices_splits(num_splits, &ReadOptions) -> Result<Vec<Vec<FileSlice>>>` | Same, partitioned into roughly equal chunks for parallel reads.                                         |
| `get_file_slices_between(&ReadOptions) -> Result<Vec<FileSlice>>`                 | File slices changed in the incremental range.                                                           |
| `get_file_slices_splits_between(num_splits, &ReadOptions) -> Result<Vec<Vec<FileSlice>>>` | Same, partitioned into chunks.                                                                  |
| `create_file_group_reader_with_options(options) -> Result<FileGroupReader>`       | Build a `FileGroupReader` that inherits this table's configs and storage options.                       |
| `read_snapshot(&ReadOptions) -> Result<Vec<RecordBatch>>`                         | Eager snapshot read; returns one batch per file slice.                                                  |
| `read_incremental_records(&ReadOptions) -> Result<Vec<RecordBatch>>`              | Eager incremental read; returns the latest state of each changed record within the range.               |
| `read_snapshot_stream(&ReadOptions) -> Result<BoxStream<'static, Result<RecordBatch>>>` | Streaming snapshot read; chains streams from all file slices.                                     |
| `read_file_slice_stream(&FileSlice, &ReadOptions) -> Result<BoxStream<'static, Result<RecordBatch>>>` | Streaming read of one file slice via the table's resolved configs.                  |
| `compute_table_stats() -> Option<(u64, u64)>`                                     | Estimated `(num_rows, total_byte_size)` for scan planning. `None` when the metadata table is disabled or stats cannot be computed. |

### `FileGroupReader`

Direct file-slice reader. Useful when integrating with query engines that already plan files.

| Method                                                                                          | Description                                                                                                       |
|-------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------|
| `FileGroupReader::new_with_options(base_uri, options) -> Result<FileGroupReader>`               | Constructor that resolves table properties from storage at the given base URI.                                    |
| `read_file_slice_by_base_file_path(relative_path, &ReadOptions) -> Result<RecordBatch>`         | Read base file only; log files (if any) are ignored.                                                              |
| `read_file_slice(&FileSlice, &ReadOptions) -> Result<RecordBatch>`                              | Read base + merge log files.                                                                                      |
| `read_file_slice_from_paths(base_path, log_paths, &ReadOptions) -> Result<RecordBatch>`         | Read by explicit paths; useful when a query engine already produced the path list.                                |
| `read_file_slice_stream(&FileSlice, &ReadOptions) -> Result<BoxStream<'static, Result<RecordBatch>>>` | Streaming version. See [§8 MOR streaming fallback](#mor-streaming-fallback).                                |
| `read_file_slice_from_paths_stream(base_path, log_paths, &ReadOptions) -> Result<BoxStream<'static, Result<RecordBatch>>>` | Streaming by explicit paths.                                                           |
| `is_metadata_table() -> bool`                                                                   | Whether this reader points at a metadata table (base path ends with `.hoodie/metadata`).                          |

### `ReadOptions`

A fluent builder. Defaults from [§4](#4-readoptions).

```rust
ReadOptions::new()
    .with_filters([("city", "=", "san_francisco")])
    .with_projection(["rider", "city", "ts", "fare"])
    .with_batch_size(4096)
    .with_as_of_timestamp("20240101000000000")
    .with_start_timestamp("20240101000000000")
    .with_end_timestamp("20240201000000000");
```

All `with_*` methods take `IntoIterator` / `Into<String>` / `AsRef<str>` shapes for ergonomics.

### `Filter` and `ExprOperator`

| Item                                                                                  | Description                                                                                       |
|---------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------|
| `struct Filter { field_name, operator, values }`                                      | A single column predicate.                                                                        |
| `Filter::new(field, op, values) -> Result<Filter>`                                    | Validates value cardinality (1 for binary ops; ≥1 for `IN`/`NOT IN`).                             |
| `Filter::negate() -> Option<Filter>`                                                  | Negated form, e.g. `=` ↔ `!=`, `<` ↔ `>=`.                                                        |
| `from_str_tuples(tuples) -> Result<Vec<Filter>>`                                      | Parse `(&str, &str, &str)` tuples into `Filter`s.                                                 |
| `filters_to_row_mask(&[Filter], &RecordBatch) -> Result<BooleanArray>`                | Evaluate filters as a row mask (`AND`-combined). Filters whose column is absent are skipped.      |
| `enum ExprOperator { Eq, Ne, Lt, Lte, Gt, Gte, In, NotIn }`                           | Supported comparison operators.                                                                   |
| `col(name).eq(v) / ne / lt / lte / gt / gte / in_list / not_in_list`                  | A small DSL for building filters in code, returning a `Filter`.                                   |

### `Timeline`

Read-only access to the table's commit timeline. All `desc` flags reverse the result order.

| Method                                                                | Description                                                                                  |
|-----------------------------------------------------------------------|----------------------------------------------------------------------------------------------|
| `get_completed_commits(desc) -> Result<Vec<Instant>>`                 | Successful commits.                                                                          |
| `get_completed_deltacommits(desc) -> Result<Vec<Instant>>`            | Successful delta commits (MOR).                                                              |
| `get_completed_replacecommits(desc) -> Result<Vec<Instant>>`          | Successful replace commits (cluster, insert overwrite).                                      |
| `get_completed_clustering_commits(desc) -> Result<Vec<Instant>>`      | Successful clustering commits.                                                               |
| `get_instant_metadata_in_json(&Instant) -> Result<String>`            | The instant's commit metadata, JSON-encoded.                                                 |
| `get_latest_commit_timestamp() -> Result<String>`                     | Timestamp of the latest commit instant.                                                      |
| `create_view_as_of(timestamp) -> Result<TimelineView>`                | A view of the timeline as of the given timestamp.                                            |
| `get_latest_avro_schema() -> Result<String>`                          | Latest Avro schema written to the timeline.                                                  |
| `get_latest_schema() -> Result<Schema>`                               | Latest Arrow schema written to the timeline.                                                 |

### `FileSlice`

| Method                                              | Description                                                       |
|-----------------------------------------------------|-------------------------------------------------------------------|
| `file_id() -> &str`                                 | The file group ID.                                                |
| `creation_instant_time() -> &str`                   | Commit instant that created this slice's base file.               |
| `has_log_file() -> bool`                            | Whether the slice has any log files (MOR with deltas).            |
| `base_file_relative_path() -> Result<String>`       | Relative path of the base file from the table base URI.           |
| `log_file_relative_path(&LogFile) -> Result<String>` | Relative path of a single log file.                              |

`FileSlice` is the input type to every per-slice read method. Direct construction is reserved for engines integrating at the file-group level; most callers obtain instances from `Table::get_file_slices(_*)`.

### `TableBuilder`

A builder alternative to `Table::new_with_options`.

```rust
use hudi::table::builder::TableBuilder as HudiTableBuilder;

let table = HudiTableBuilder::from_base_uri("/tmp/trips_table")
    .with_hudi_option("hoodie.read.use.read_optimized.mode", "true")
    .with_storage_option("aws_region", "us-west-2")
    .build()
    .await?;
```

Available pairs: `with_hudi_option` / `with_hudi_options`, `with_storage_option` / `with_storage_options`, `with_option` / `with_options` (the generic forms route by key prefix).

## 7. Python API

All symbols below are exported from the top-level `hudi` package. The Python class names are `Hudi*` for clarity at the call site.

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

`with_hudi_option` and `with_option` accept either a string key or a `HudiReadConfig` / `HudiTableConfig` enum member. The bulk variants (`with_hudi_options`, `with_options`) currently accept dicts of string keys; pass `member.value` if you have an enum in hand.

### `HudiTable`

| Method / property                                                              | Description                                                                          |
|--------------------------------------------------------------------------------|--------------------------------------------------------------------------------------|
| `HudiTable(base_uri, options=None)`                                            | Constructor accepting mixed Hudi/storage options.                                    |
| `hudi_options() -> Dict[str, str]`                                             | Hudi configuration in effect.                                                        |
| `storage_options() -> Dict[str, str]`                                          | Storage configuration in effect.                                                     |
| property `table_name`, `table_type`, `is_mor`, `timezone`, `base_url`          | Table-level metadata.                                                                |
| `get_schema() -> pyarrow.Schema`                                               | Latest Arrow schema, no meta fields.                                                 |
| `get_schema_with_meta_fields() -> pyarrow.Schema`                              | Latest Arrow schema, with meta fields.                                               |
| `get_partition_schema() -> pyarrow.Schema`                                     | Partition column schema.                                                             |
| `get_schema_in_avro_str() -> str`                                              | Avro schema string, no meta fields.                                                  |
| `get_schema_in_avro_str_with_meta_fields() -> str`                             | Avro schema string, with meta fields.                                                |
| `get_timeline() -> HudiTimeline`                                               | The table's timeline.                                                                |
| `get_file_slices(options=None) -> List[HudiFileSlice]`                         | File slices at a snapshot moment.                                                    |
| `get_file_slices_splits(num_splits, options=None) -> List[List[HudiFileSlice]]` | Same, partitioned for parallel reads.                                               |
| `get_file_slices_between(options=None) -> List[HudiFileSlice]`                 | Changed file slices in the incremental range.                                        |
| `get_file_slices_splits_between(num_splits, options=None) -> List[List[HudiFileSlice]]` | Same, partitioned for parallel reads.                                       |
| `create_file_group_reader_with_options(options=None) -> HudiFileGroupReader`   | Build a `HudiFileGroupReader` inheriting this table's configs.                       |
| `read_snapshot(options=None) -> List[pyarrow.RecordBatch]`                     | Eager snapshot read.                                                                 |
| `read_incremental_records(options=None) -> List[pyarrow.RecordBatch]`          | Eager incremental read.                                                              |
| `read_snapshot_stream(options=None) -> HudiRecordBatchStream`                  | Streaming snapshot read.                                                             |
| `read_file_slice_stream(file_slice, options=None) -> HudiRecordBatchStream`    | Streaming read of one file slice.                                                    |
| `compute_table_stats() -> Optional[Tuple[int, int]]`                           | Estimated `(num_rows, total_byte_size)`; `None` when unavailable.                    |

### `HudiFileGroupReader`

| Method                                                                                  | Description                                            |
|-----------------------------------------------------------------------------------------|--------------------------------------------------------|
| `HudiFileGroupReader(base_uri, options=None)`                                           | Resolves table properties at the given base URI.       |
| `read_file_slice_by_base_file_path(relative_path, options=None) -> pyarrow.RecordBatch` | Base file only.                                        |
| `read_file_slice(file_slice, options=None) -> pyarrow.RecordBatch`                      | Base + merge logs.                                     |
| `read_file_slice_from_paths(base_path, log_paths, options=None) -> pyarrow.RecordBatch` | Read from explicit paths.                              |
| `read_file_slice_stream(file_slice, options=None) -> HudiRecordBatchStream`             | Streaming variant.                                     |
| `read_file_slice_from_paths_stream(base_path, log_paths, options=None) -> HudiRecordBatchStream` | Streaming variant by explicit paths.          |

### `HudiReadOptions`

```python
from hudi import HudiReadOptions

options = HudiReadOptions(
    filters=[("city", "=", "san_francisco")],
    projection=["rider", "city", "ts", "fare"],
    batch_size=4096,
    as_of_timestamp="20240101000000000",
    start_timestamp="20240101000000000",
    end_timestamp="20240201000000000",
)
```

All fields are optional; defaults match [§4](#4-readoptions). The shape mirrors Rust's `ReadOptions`.

### `HudiRecordBatchStream`

A single-use iterator returned by streaming APIs. Iterate with `for batch in stream:` or pull manually with `next(stream)`. Each yielded value is a `pyarrow.RecordBatch`.

### `HudiTimeline`

| Method                                                              | Description                                            |
|---------------------------------------------------------------------|--------------------------------------------------------|
| `get_completed_commits(desc=False) -> List[HudiInstant]`            | Successful commits.                                    |
| `get_completed_deltacommits(desc=False) -> List[HudiInstant]`       | Successful delta commits (MOR).                        |
| `get_completed_replacecommits(desc=False) -> List[HudiInstant]`     | Successful replace commits.                            |
| `get_completed_clustering_commits(desc=False) -> List[HudiInstant]` | Successful clustering commits.                         |
| `get_instant_metadata_in_json(instant) -> str`                      | The instant's commit metadata, JSON-encoded.           |
| `get_latest_commit_timestamp() -> str`                              | Timestamp of the latest commit instant.                |
| `get_latest_avro_schema() -> str`                                   | Latest Avro schema.                                    |
| `get_latest_schema() -> pyarrow.Schema`                             | Latest Arrow schema.                                   |

### `HudiInstant`

Read-only properties: `timestamp`, `action`, `state`, `epoch_mills`.

### `HudiFileSlice`

Read-only attributes: `file_id`, `partition_path`, `creation_instant_time`, `base_file_name`, `base_file_size`, `base_file_byte_size`, `log_file_names`, `num_records`. Methods: `base_file_relative_path() -> str`, `log_files_relative_paths() -> List[str]`.

## 8. Behavioral notes

### Snapshot atomicity

A snapshot read is pinned to a single completed commit timestamp (resolved at the start of the call) and reads only file slices visible at or before that timestamp. Concurrent writes are not visible mid-read.

### Incremental semantics

`read_incremental_records` and `get_file_slices_between` cover the half-open range (`start_timestamp`, `end_timestamp`]. A record updated multiple times within the range yields its latest in-range state, not the full update history. Records updated only outside the range are not returned.

### Pruning vs row mask

Partition pruning and file-level statistics pruning are best-effort optimizations. The row-level mask is applied after reading and is the authority on which rows are returned. A non-prunable filter (e.g., on a non-partition, non-stats column) still produces correct results, just with less I/O elimination.

A filter on a column that ends up missing from the read batch (most often: a partition column on a table that drops partition columns from data files) is silently skipped at row-mask time — pruning has already done its job.

### MOR streaming fallback

`FileGroupReader::read_file_slice_stream` (and `Table::read_file_slice_stream` / `Table::read_snapshot_stream`) yields true streaming batches when the slice is base-file-only or when `hoodie.read.use.read_optimized.mode=true`. For MOR slices with log files, the implementation falls back to the eager merge path and yields the result as a single batch on the stream. Streaming merge of base + log files is not yet implemented.

### `batch_size`

Controls rows per `RecordBatch` for streaming reads. Defaults to `hoodie.read.stream.batch_size` (1024). Eager reads ignore `batch_size` and return one merged batch per file slice.

### `projection`

Streaming pushes the projection down to the parquet reader; eager reads project after merging. When `projection` is combined with `filters` on data columns not in `projection`, the read transparently widens to include those columns and projects back down after the filter mask runs.

### Timestamp formats

`as_of_timestamp`, `start_timestamp`, and `end_timestamp` accept any of:

- Hudi timeline format (highest precedence): `yyyyMMddHHmmssSSS` or `yyyyMMddHHmmss`.
- Unix epoch in seconds, milliseconds, microseconds, or nanoseconds.
- ISO 8601 variants: `yyyy-MM-dd'T'HH:mm:ss[.SSS][+00:00|Z]`, `yyyy-MM-dd'T'HH:mm:ss`, `yyyy-MM-dd`.

Timestamps are normalized into the table's timeline timezone (`hoodie.table.timeline.timezone`).

### Empty tables

Reads against a table with no completed commits return:

- `read_snapshot` / `read_incremental_records`: empty `Vec`.
- `get_file_slices*`: empty `Vec`.
- `read_snapshot_stream`: an empty stream (terminates immediately).

### Errors

A filter on a column that is not in the table or partition schema is rejected with a schema error before any I/O. An unparseable filter value (e.g., `"abc"` against `Int64`) is rejected at filter evaluation. I/O failures (missing files, permission errors, malformed parquet) surface as read errors.

## 9. Stability

The reader APIs documented above are the supported public surface as of this release.

The `FileGroupReader` direct-paths APIs (`read_file_slice_*`) are the lower-level path used by query-engine integrations. The README labels file group reading as "experimental"; expect minor signature evolution there before the surface is finalized.

`compute_table_stats` returns `None` whenever statistics cannot be computed (no metadata table, non-Parquet base files, or footer reads fail). It does not yet account for log files in MOR tables, so estimates for write-heavy MOR workloads will skew low.

## 10. Out of scope

This v1 deliberately does not cover:

- **Writer APIs.** No public writer surface is available yet.
- **DataFusion integration.** `HudiDataSource` / `HudiDataFusionDataSource` are documented in the [README](../README.md#apache-datafusion) and will get their own spec section in a follow-up.
- **C++ bindings.** Documented in the [README](../README.md#file-group-reading-experimental); separate spec follow-up.
- **Internal architecture.** Timeline parsing, log-record merging strategy, metadata table layout. These are implementation details, not API.
- **Configuration key glossary.** `HudiReadConfig` and `HudiTableConfig` members are referenced inline above. A standalone configuration reference is a separate doc.
