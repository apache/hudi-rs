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
//! A native Rust implementation of [Apache Hudi](https://hudi.apache.org).
//!
//! This crate creates, writes, and reads Hudi tables on local disk or cloud object storage
//! (`s3://`, `az://`, `gs://`), exchanging data as Arrow [`RecordBatch`]es. It re-exports
//! [`hudi_core`], and with the `datafusion` feature also the DataFusion table provider from
//! `hudi-datafusion`. [`table::Table`] is the entry point for both reads and writes.
//!
//! # Resources
//!
//! - Quick start: the [Python and Rust quick start guide][quick start guide] on the Hudi website,
//!   and the shorter [README quick start].
//! - Rust API reference: this crate, [`hudi-core`](https://docs.rs/hudi-core), and
//!   [`hudi-datafusion`](https://docs.rs/hudi-datafusion).
//! - Python API reference: the docstrings in the package's type stubs,
//!   [`python/hudi/_internal.pyi`][python stubs].
//! - Runnable Rust example: [`crates/core/examples/table_writes.rs`][rust example].
//! - Reader semantics: [`docs/reader-spec.md`][reader spec]. Writer design:
//!   [`docs/writer-design.md`][writer design].
//! - Packages: [crates.io](https://crates.io/crates/hudi) and
//!   [PyPI](https://pypi.org/project/hudi/). Source:
//!   [github.com/apache/hudi-rs](https://github.com/apache/hudi-rs).
//! - Hudi concepts (timeline, table types, query types):
//!   [hudi.apache.org/docs/overview](https://hudi.apache.org/docs/overview).
//!
//! [quick start guide]: https://hudi.apache.org/docs/python-rust-quick-start-guide
//! [README quick start]: https://github.com/apache/hudi-rs#quick-start
//! [python stubs]: https://github.com/apache/hudi-rs/blob/main/python/hudi/_internal.pyi
//! [rust example]: https://github.com/apache/hudi-rs/blob/main/crates/core/examples/table_writes.rs
//! [reader spec]: https://github.com/apache/hudi-rs/blob/main/docs/reader-spec.md
//! [writer design]: https://github.com/apache/hudi-rs/blob/main/docs/writer-design.md
//!
//! [`RecordBatch`]: https://docs.rs/arrow/latest/arrow/array/struct.RecordBatch.html
//!
//! # Features
//!
//! - `datafusion` (off by default): adds `HudiDataSource`, a DataFusion `TableProvider`, and
//!   `HudiTableFactory` for `CREATE EXTERNAL TABLE ... STORED AS HUDI`.
//! - `spill-rocksdb` (on by default): the on-disk tier of the merge-on-read merge map, used when a
//!   merge exceeds `hoodie.memory.merge.max.size`. RocksDB is built from source with `bindgen`, so
//!   it needs `libclang` and a C++ toolchain. With `default-features = false` the tier is gone, and
//!   a merge past the budget fails with [`CoreError::Unsupported`](error::CoreError::Unsupported)
//!   naming the config instead of spilling.
//!
//! # Create a table and write to it
//!
//! A write that changes data is one Hudi commit. [`Table::append`](table::Table::append) inserts without an
//! index lookup; [`Table::upsert`](table::Table::upsert) updates rows whose record key exists and
//! inserts the rest; [`Table::delete`](table::Table::delete) and
//! [`Table::delete_keys`](table::Table::delete_keys) remove rows by predicate or by key.
//!
//! ```
//! # use std::sync::Arc;
//! # use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
//! use hudi::config::table::TableTypeValue;
//! use hudi::table::{HoodieKey, Table};
//!
//! # fn trips(rows: &[(&str, &str, i64)]) -> RecordBatch {
//! #     let id: ArrayRef = Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.0)));
//! #     let city: ArrayRef = Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1)));
//! #     let ts: ArrayRef = Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.2)));
//! #     RecordBatch::try_from_iter([("id", id), ("city", city), ("ts", ts)]).unwrap()
//! # }
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let dir = tempfile::tempdir()?;
//! # let base_uri = dir.path().to_str().unwrap();
//! let mut table = Table::create(base_uri)
//!     .with_table_name("trips")
//!     .with_table_type(TableTypeValue::CopyOnWrite) // or MergeOnRead
//!     .with_record_key_fields(["id"])
//!     .with_partition_fields(["city"])
//!     .with_ordering_fields(["ts"]) // the larger `ts` wins when keys collide
//!     .create()
//!     .await?;
//!
//! // `trips` builds a batch with columns `id`, `city`, and `ts`.
//! table.append([trips(&[("a", "sf", 1), ("b", "sf", 1), ("c", "nyc", 1)])]).await?;
//!
//! let result = table.upsert([trips(&[("b", "sf", 2), ("d", "nyc", 2)])]).await?;
//! assert_eq!((result.num_updates, result.num_inserts), (1, 1));
//!
//! let result = table.delete("id = 'a'").await?;
//! assert_eq!(result.num_deletes, 1);
//!
//! let result = table
//!     .delete_keys([HoodieKey {
//!         record_key: "c".to_string(),
//!         partition_path: "city=nyc".to_string(),
//!     }])
//!     .await?;
//! assert_eq!(result.num_deletes, 1);
//! # Ok(())
//! # }
//! ```
//!
//! # Compact a merge-on-read table
//!
//! On a merge-on-read table, upserts, updates, and deletes write log files.
//! [`Table::compact`](table::Table::compact) (experimental) folds them into new base files under
//! one compaction commit. Rows keep their commit time, so incremental reads see no new rows from
//! it. Compaction can also run after every `hoodie.compact.inline.max.delta.commits` (default 5)
//! delta commits: in the write call with `hoodie.compact.inline=true`, or on a background task
//! with `hoodie.datasource.compaction.async.enable=true`, which
//! [`Table::await_compaction`](table::Table::await_compaction) waits for.
//!
//! ```
//! # use std::sync::Arc;
//! # use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
//! use hudi::config::table::TableTypeValue;
//! use hudi::table::Table;
//!
//! # fn trips(rows: &[(&str, &str, i64)]) -> RecordBatch {
//! #     let id: ArrayRef = Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.0)));
//! #     let city: ArrayRef = Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1)));
//! #     let ts: ArrayRef = Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.2)));
//! #     RecordBatch::try_from_iter([("id", id), ("city", city), ("ts", ts)]).unwrap()
//! # }
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let dir = tempfile::tempdir()?;
//! # let base_uri = dir.path().to_str().unwrap();
//! let mut table = Table::create(base_uri)
//!     .with_table_name("trips")
//!     .with_table_type(TableTypeValue::MergeOnRead)
//!     .with_record_key_fields(["id"])
//!     .with_partition_fields(["city"])
//!     .with_ordering_fields(["ts"])
//!     .create()
//!     .await?;
//! table.append([trips(&[("a", "sf", 1), ("b", "nyc", 1)])]).await?;
//! table.upsert([trips(&[("a", "sf", 2)])]).await?; // writes a log file in `city=sf`
//!
//! let result = table.compact().await?;
//! assert_eq!((result.num_file_groups, result.num_log_files), (1, 1));
//!
//! // Nothing left to compact: no commit is created.
//! assert!(table.compact().await?.requested_time.is_none());
//! # Ok(())
//! # }
//! ```
//!
//! # Snapshot read
//!
//! [`Table::new`](table::Table::new) opens an existing table. A read returns the latest committed
//! state, narrowed by optional filters and a projection from [`ReadOptions`](table::ReadOptions).
//!
//! ```
//! # use std::sync::Arc;
//! # use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
//! use hudi::table::{ReadOptions, Table};
//! # fn trips(rows: &[(&str, &str, i64)]) -> RecordBatch {
//! #     let id: ArrayRef = Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.0)));
//! #     let city: ArrayRef = Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1)));
//! #     let ts: ArrayRef = Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.2)));
//! #     RecordBatch::try_from_iter([("id", id), ("city", city), ("ts", ts)]).unwrap()
//! # }
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let dir = tempfile::tempdir()?;
//! # let base_uri = dir.path().to_str().unwrap();
//! # let mut writer = Table::create(base_uri)
//! #     .with_table_name("trips")
//! #     .with_record_key_fields(["id"])
//! #     .with_partition_fields(["city"])
//! #     .create()
//! #     .await?;
//! # writer.append([trips(&[("a", "sf", 1), ("b", "sf", 1), ("c", "nyc", 1)])]).await?;
//! let table = Table::new(base_uri).await?;
//! let options = ReadOptions::new()
//!     .with_filters([("city", "=", "sf")])?
//!     .with_projection(["id", "ts"]);
//! let batches = table.read(&options).await?;
//! let num_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
//! assert_eq!(num_rows, 2);
//! # Ok(())
//! # }
//! ```
//!
//! [`Table::read_stream`](table::Table::read_stream) yields the same snapshot one batch at a time.
//!
//! # Time-travel read
//!
//! [`ReadOptions::with_as_of_timestamp`](table::ReadOptions::with_as_of_timestamp) reads the table
//! as of an earlier commit. It accepts a Hudi instant time (`yyyyMMddHHmmssSSS` or
//! `yyyyMMddHHmmss`), Unix epoch time, or RFC 3339 with a timezone offset.
//!
//! ```
//! # use std::sync::Arc;
//! # use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
//! use hudi::table::{ReadOptions, Table};
//! # fn trips(rows: &[(&str, &str, i64)]) -> RecordBatch {
//! #     let id: ArrayRef = Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.0)));
//! #     let city: ArrayRef = Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1)));
//! #     let ts: ArrayRef = Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.2)));
//! #     RecordBatch::try_from_iter([("id", id), ("city", city), ("ts", ts)]).unwrap()
//! # }
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let dir = tempfile::tempdir()?;
//! # let base_uri = dir.path().to_str().unwrap();
//! # let mut table = Table::create(base_uri)
//! #     .with_table_name("trips")
//! #     .with_record_key_fields(["id"])
//! #     .with_partition_fields(["city"])
//! #     .create()
//! #     .await?;
//! let first = table.append([trips(&[("a", "sf", 1), ("b", "sf", 1)])]).await?;
//! table.append([trips(&[("c", "nyc", 2)])]).await?;
//!
//! // A write's requested time names its commit for time travel.
//! let as_of = first.requested_time.expect("the append made a commit");
//! let options = ReadOptions::new().with_as_of_timestamp(as_of);
//! let batches = table.read(&options).await?;
//! let num_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
//! assert_eq!(num_rows, 2);
//! # Ok(())
//! # }
//! ```
//!
//! # Incremental read
//!
//! An incremental query returns the records changed by commits in the half-open window
//! `(start, end]`; `end` defaults to everything committed so far. On tables at version 8 and
//! above (timeline layout 2) the window bounds commit completion times, so start from a write's
//! [`completion_time`](table::WriteResult::completion_time), not its `requested_time`.
//!
//! ```
//! # use std::sync::Arc;
//! # use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
//! use hudi::table::{QueryType, ReadOptions, Table};
//! # fn trips(rows: &[(&str, &str, i64)]) -> RecordBatch {
//! #     let id: ArrayRef = Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.0)));
//! #     let city: ArrayRef = Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1)));
//! #     let ts: ArrayRef = Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.2)));
//! #     RecordBatch::try_from_iter([("id", id), ("city", city), ("ts", ts)]).unwrap()
//! # }
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let dir = tempfile::tempdir()?;
//! # let base_uri = dir.path().to_str().unwrap();
//! # let mut table = Table::create(base_uri)
//! #     .with_table_name("trips")
//! #     .with_record_key_fields(["id"])
//! #     .with_partition_fields(["city"])
//! #     .create()
//! #     .await?;
//! let first = table.append([trips(&[("a", "sf", 1), ("b", "sf", 1)])]).await?;
//! table.upsert([trips(&[("b", "sf", 2), ("c", "nyc", 2)])]).await?;
//!
//! let start = first.completion_time.ok_or("layout 2 records a completion time")?;
//! let options = ReadOptions::new()
//!     .with_query_type(QueryType::Incremental)
//!     .with_start_timestamp(start);
//! let changed = table.read(&options).await?;
//! let num_rows: usize = changed.iter().map(|b| b.num_rows()).sum();
//! assert_eq!(num_rows, 2); // `b` updated, `c` inserted
//! # Ok(())
//! # }
//! ```
//!
//! # Query with DataFusion
//!
//! With the `datafusion` feature, register a table with a DataFusion `SessionContext` and query
//! it with SQL.
//!
//! ```
//! # use std::sync::Arc;
//! # use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
//! # fn trips(rows: &[(&str, &str, i64)]) -> RecordBatch {
//! #     let id: ArrayRef = Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.0)));
//! #     let city: ArrayRef = Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1)));
//! #     let ts: ArrayRef = Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.2)));
//! #     RecordBatch::try_from_iter([("id", id), ("city", city), ("ts", ts)]).unwrap()
//! # }
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let dir = tempfile::tempdir()?;
//! # let base_uri = dir.path().to_str().unwrap();
//! # let mut table = hudi::table::Table::create(base_uri)
//! #     .with_table_name("trips")
//! #     .with_record_key_fields(["id"])
//! #     .with_partition_fields(["city"])
//! #     .create()
//! #     .await?;
//! # table.append([trips(&[("a", "sf", 1), ("b", "sf", 1), ("c", "nyc", 1)])]).await?;
//! # #[cfg(feature = "datafusion")]
//! # {
//! use datafusion::prelude::SessionContext;
//! use hudi::HudiDataSource;
//!
//! let ctx = SessionContext::new();
//! let provider =
//!     HudiDataSource::new_with_options(base_uri, [("hoodie.read.input.partitions", "2")]).await?;
//! ctx.register_table("trips", Arc::new(provider))?;
//! let df = ctx.sql("SELECT id, ts FROM trips WHERE city = 'sf'").await?;
//! let batches = df.collect().await?;
//! let num_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
//! assert_eq!(num_rows, 2);
//! # }
//! # Ok(())
//! # }
//! ```

pub use hudi_core::*;

#[cfg(feature = "datafusion")]
pub use hudi_datafusion::*;
