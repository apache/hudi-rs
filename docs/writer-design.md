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

# hudi-rs Writer Design

hudi-rs implements a **single-node, Arrow-first native write path** for Apache
Hudi tables that is storage-compatible with the Apache Hudi Spark/Java writer:
tables written by hudi-rs are readable (and serviceable — compaction,
clustering, cleaning) by Spark, and vice versa. This document describes the
design as implemented in `crates/core/src/write/`.

## 1. Scope

**In scope**

- Table creation (`Table::create`) and write verbs: `append` / `append_only`,
  `upsert` / `upsert_with`, `update`, `delete` / `delete_keys`, `overwrite`,
  `dynamic_partition_overwrite`.
- `COPY_ON_WRITE` and `MERGE_ON_READ` (Parquet base files + Parquet-block log
  files), partitioned (hive-style) and non-partitioned tables.
- Table versions 8 (Hudi 1.0.x) and 9 (Hudi 1.1.x, the default), timeline
  layout v2 with completion-time instants.
- Metadata table (MDT) with `files`, `column_stats`, `partition_stats`, and
  `record_index` partitions, all enabled by default.
- Marker-based rollback of failed writes, LSM timeline archival, in-process
  locking.

**Not yet implemented**

- Write bindings for Python / C++ / DataFusion (the read path covers those).
- Table services as *drivers* (hudi-rs interoperates with Spark-run
  compaction, clustering, and cleaning, but does not schedule or execute them
  natively).
- Multi-writer concurrency control beyond in-process locking; bootstrap
  tables; CDC-format writes; `delete_partition`.

## 2. Design principles

1. **Storage parity over class parity.** The unit of compatibility is bytes on
   storage — timeline file names and contents, `hoodie.properties`, MDT record
   encodings, marker files — not the Java class hierarchy.
2. **Arrow-first.** Batches stay `RecordBatch` end to end; Avro is used only
   where the format requires it (commit metadata, MDT payloads, log-block
   framing).
3. **Async, object-store native.** All I/O goes through `object_store` via the
   `Storage` abstraction; no local-filesystem assumptions.
4. **Fail loud.** Unknown merge strategies, unsupported filters, and schema
   mismatches error instead of degrading silently.

## 3. Write path anatomy

Every verb follows the same lifecycle:

```
lock ─► generate instant time ─► request instant (fencing files) ─► unlock
      write markers ─► write data files (parallel task pool)
      update MDT partitions ─► commit MDT deltacommit
lock ─► generate completion time ─► complete data instant
      bookkeeping: marker cleanup, timeline archival ─► unlock
```

### 3.1 Instants and time generation

- `generate_instant_time` mints millisecond instants that are **monotonic
  across the process** (a static high-water mark plus a short skew wait guards
  against clock skew, mirroring Java's `TimeGenerator`).
- Instants are formatted in the table's declared
  `hoodie.table.timeline.timezone`. The default is `LOCAL`, matching the Spark
  writer, so interleaved writers on one host produce a correctly ordered
  timeline.
- Completed instants use the layout-v2 name
  `{requested}_{completion}.{action}`; every completed instant carries a
  completion time.
- The requested/inflight fencing files are written when the action is
  requested; inflight commit metadata carries the operation type, and
  `replacecommit.requested` carries the requested-replace plan.

### 3.2 Critical sections and locking

`LockProvider` (`write/lock.rs`) abstracts locking with a single
implementation, `InProcessLockProvider` (a per-base-path async mutex
registry). Two short critical sections per write:

1. **Request**: `lock → generate_instant_time → request_<action> → unlock`.
2. **Complete**: `lock → generate_completion_time → complete_<action> →
   marker cleanup + timeline archival → unlock`.

The data-writing work between the two sections runs outside any lock.

### 3.3 Ordering and fencing

Writes are ordered so that readers can always fence correctly:

```
fencing files → data files → MDT files → MDT deltacommit → data commit
```

MDT reads are **fenced by the data timeline**: only MDT deltacommits whose
instant is a *completed* data-timeline instant (or the MDT bootstrap prefix,
or an archived instant) are trusted. An MDT commit whose data commit never
completed is invisible, so a crash between the two commits cannot corrupt
reads.

### 3.4 Markers and rollback

- Markers use the timeline-server-based **format** without a server (single
  node): `.hoodie/.temp/{instant}/MARKERS{N}` files listing
  `{partition}/{file}.marker.{CREATE|MERGE}` entries, written before any data
  file.
- Failed writes are rolled back **eagerly** on the next table load for write:
  markers identify files to delete, orphan MDT deltacommits are removed, and a
  rollback plan + metadata (Avro OCF) are written like the Java writer's
  rollback action.

### 3.5 LSM timeline archival

After each completed action (inside the second critical section), the data
timeline is archived when completed commits exceed `hoodie.keep.max.commits`
(default 30), retaining `hoodie.keep.min.commits` (default 20): the oldest
instants move into `.hoodie/timeline/history/` as `HoodieLSMTimelineInstant`
parquet files tracked by `manifest_N` + `_version_` files. Readers union
archived instants back in wherever instant validity matters (snapshot
visibility floor, MDT fencing). The MDT's own timeline is *not* archived —
like the Java writer, MDT archival is bounded by MDT compaction, which is not
implemented natively.

## 4. Writing data

### 4.1 Verbs

| Verb | Semantics |
|------|-----------|
| `append` / `append_only` | Bulk-insert-like: new file groups per call, no index lookup, size-split into `hoodie.parquet.max.file.size` buckets. |
| `upsert` / `upsert_with` | Index-tagged updates + inserts. Updates rewrite their file group (COW) or append log blocks to their file slice (MOR). Inserts pack into existing small files first. |
| `update` | SQL-style column update on rows matching a filter. |
| `delete` / `delete_keys` | Filter- or key-based deletes. A file group left empty still receives an empty base file version so the deletion is durable for all readers. |
| `overwrite` | `INSERT_OVERWRITE_TABLE`: replaces all file groups via a replacecommit. |
| `dynamic_partition_overwrite` | `INSERT_OVERWRITE`: replaces only the partitions present in the input. |

### 4.2 Indexing

- **Record-level index (RLI)** in the MDT (10 shards, Java-compatible key
  hashing) is written and used for tagging by default.
- **SimpleIndex** (scan-based) is the fallback when RLI is unavailable.
- Locations found by the index are validated against the live file-system
  view, so records moved by clustering land in their new file groups.

### 4.3 Small-file packing and sizing

Upsert inserts follow the Java `UpsertPartitioner` model: file groups under
`hoodie.parquet.small.file.limit` (default 100 MB) absorb new inserts up to
`hoodie.parquet.max.file.size` (default 120 MB), using the average record
size estimated from commit history; the remainder splits into new file groups
of `hoodie.copyonwrite.insert.split.size` records (or auto-computed). MOR log
files count at `hoodie.logfile.to.parquet.compression.ratio` of their size.

### 4.4 Parallel write tasks

Data files are written on a task pool: encode work runs on blocking threads
and uploads run concurrently, bounded by `hoodie.write.task.parallelism`
(default `2 × cores`). Plans are computed sequentially, executed in parallel,
and assembled in plan order, so results are deterministic.

### 4.5 Schema handling

- Commit metadata records the **data schema** (no `_hoodie_*` meta fields).
- Incoming batches may differ from the table schema in nullability only
  (Spark evolves table schemas to all-nullable); such batches are aligned
  rather than rejected. Name/type/order changes still error.

## 5. Metadata table

The MDT is a MOR table at `.hoodie/metadata` (HFile base files, HFile-block
log files) with `hoodie.record.merge.mode=CUSTOM` and the payload-based merge
strategy id. Written partitions:

- **files** — per-partition file listings plus `__all_partitions__`.
- **column_stats** — per file × column, V2 encoding at table version 9
  (typed primitive wrappers + `valueType`), tight-bound.
- **partition_stats** — per partition × column, recomputed tight-bound on
  each write.
- **record_index** — record key → file group location, 10 shards.

Reading merges base HFile records with log records using
`MetadataPayloadMerger` (`metadata/payload_merger.rs`), the native equivalent
of Java's `HoodieMetadataPayload`: files-partition maps merge with tombstone
semantics, column/partition stats union ranges and sum counts unless the newer
record is tight-bound or a tombstone, record-index entries take the newest.
The merger is resolved from the MDT's declared merge-strategy id and errors on
anything unknown. Records decode with the **writer schema of the container
they came from** (base HFile or log block), so mixed-writer tables decode
correctly.

## 6. Query semantics (writer-relevant)

- **Snapshot / time-travel** reads resolve file slices as of the requested
  instant, honoring the archival floor.
- **Incremental** reads follow Hudi 1.x completion-time semantics: instants
  are selected by *completion* time in `(start, end]`; rows and MOR log blocks
  are then filtered by membership in the exact selected instant set. The
  default `end` is the latest completion time. This matches the Spark reader,
  including the boundary case where an instant's request time equals `start`
  but its completion is later.

## 7. Testing

### 7.1 Unit and integration tests (`cargo test -p hudi-core`)

Colocated unit tests plus dedicated integration suites under
`crates/core/tests/`:

| Suite | Covers |
|-------|--------|
| `table_write_tests.rs` | Verb semantics: append/upsert/update/delete/overwrite happy paths and edge cases (key uniqueness, schema validation, empty-group deletes, RLI consistency). |
| `table_write_lifecycle_tests.rs` | End-to-end verb lifecycles across COW/MOR × partitioned/unpartitioned. |
| `table_write_mdt_stats_tests.rs` | MDT population matrix: files/column_stats/partition_stats defaults and contents, bootstrap deltacommits, column selection rules, stats recompute. |
| `table_write_timeline_tests.rs` | Instant naming, completion times, fencing files, inflight/requested metadata, MDT fencing by the data timeline. |
| `table_write_rollback_tests.rs` | Marker writing, eager rollback of failed writes, orphan MDT cleanup, rollback plan/metadata files. |
| `table_write_archival_tests.rs` | LSM archival trigger/layout (`manifest_N`, `_version_`, level-0 parquet), reads spanning archived instants, MDT archival skip. |
| `table_write_tv9_tests.rs` | Table-version 9 specifics: property keys, ordering fields, V2 stats encoding; tv8 byte-stability. |
| `table_write_sizing_tests.rs` | Small-file packing, insert bucket assignment, parallel/sequential write equivalence. |
| `table_read_tests.rs` | Read semantics against checked-in fixture tables, including completion-time incremental queries and manual-reader/table-read agreement. |

### 7.2 Spark-in-the-loop parity harness (`make parity`)

The acceptance gate for interop lives in
`crates/core/tests/spark_parity_tests.rs` + `scripts/parity/` and runs real
`spark-submit` jobs against a local Spark + Hudi bundle (env-gated on
`HUDI_SPARK_PARITY=1`; see `scripts/parity/README.md`). Scenarios:

- **A — rs writes → Spark reads**: COW/MOR × partitioned/unpartitioned;
  Spark reads run twice (with and without `hoodie.metadata.enable`) and fail
  on mismatch, so MDT interop bugs surface as read differences.
- **B — Spark writes → rs reads**: the same matrix, written by Spark.
- **C — Spark table services on rs tables**: Spark compaction (MOR),
  clustering and cleaning (COW) on rs-written tables, verified by both
  readers, **followed by rs writes on top of the compacted/clustered table**
  (updates must land in post-service file groups).
- **D — interleaved writers**: rs and Spark alternating writes on one table.
- **E — mixed workload at scale**: 36 commits of
  append/update/insert/delete cycles over 5 partitions and 30+ file groups,
  enough for LSM archival to kick in; verified via snapshot, time-travel,
  and incremental (open-ended and bounded) queries in both engines.
- **F — CDC-enabled Spark table**: rs snapshot reads tolerate CDC artifacts;
  Spark CDC reads sanity-checked. (CDC-format incremental queries in rs are
  not yet implemented.)

## 8. Module map

```
crates/core/src/write/
  mod.rs        shared helpers: write task pool, fencing, bookkeeping,
                schema alignment, log/parquet file tasks
  create.rs     Table::create builder, hoodie.properties, MDT bootstrap
  append.rs     append verbs, instant time generation, parquet writer props
  rewrite.rs    upsert/update/delete/overwrite engines (COW rewrite groups,
                MOR delta logs), instant request/complete helpers
  sizing.rs     small-file packing: SizingConfig, insert bucket assignment
  metadata.rs   MDT updates: files, record_index, column/partition stats
  markers.rs    marker files (timeline-server-based format)
  rollback.rs   eager rollback of failed writes
  archival.rs   LSM timeline archival
  lock.rs       LockProvider + InProcessLockProvider
  keygen.rs     record key generation

crates/core/src/metadata/
  payload_merger.rs   HoodieMetadataPayload-equivalent record merger
  merger.rs           files-partition base/log merge
  table/              MDT read paths, record encode/decode
```
