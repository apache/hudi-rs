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

# Spark-in-the-loop parity harness

Cross-checks hudi-rs against the Apache Hudi Spark writer/reader:

- **A** — rs writes → Spark reads (COW/MOR × partitioned/unpartitioned)
- **B** — Spark writes → rs reads (same matrix)
- **C** — Spark table services (compaction/clustering/clean) on rs-written tables
- **D** — interleaved rs and Spark writers on the same table
- **E** — mixed-operation workload (append/update/insert/delete cycles), 5
  partitions, tens of file groups, 36 commits so LSM timeline archival kicks
  in; verified via snapshot, time-travel, and incremental queries in both
  engines
- **F** — CDC-enabled Spark table: rs snapshot reads tolerate CDC artifacts;
  Spark CDC read sanity-checked (rs-side CDC-format queries are not yet
  implemented)
- **G** — Spark data skipping over rs-written column_stats/partition_stats,
  alone and mixed with Spark's stats in one partition: filtered reads with
  and without data skipping must agree
- **I** — Spark upserts with SIMPLE, BLOOM and GLOBAL_BLOOM indexes on
  rs-written COW/MOR tables find every key rs wrote (base files, inserts,
  updates) and re-insert one rs deleted without duplicates; includes keys
  whose UTF-8 and UTF-16 orders differ, against the bloom footer key range.
  These tables set `hoodie.parquet.bloom.filter.enabled=true`: rs leaves the
  footer out by default, and Java's bloom index needs it
- **J** — Spark SQL UPDATE/DELETE/INSERT on rs-written COW/MOR tables
  registered in a non-default database, then rs writes on top

Spark snapshot reads always run twice — with and without
`hoodie.metadata.enable` — and fail on mismatch, so MDT interop bugs surface
as read differences.

Requirements (all local, no downloads):
- `SPARK_HOME` pointing at Spark 3.5.x
- A Hudi Spark 3.5 bundle jar; default: released 1.1.1 from `~/.m2`, override
  with `HUDI_SPARK_BUNDLE=/path/to/hudi-spark3.5-bundle_2.12-<v>.jar`
- Optional `HUDI_PARITY_JAVA_HOME` if the bundle needs a newer JVM than default

Run: `make parity` (sets `HUDI_SPARK_PARITY=1`; without it the tests no-op so
regular CI is unaffected).
