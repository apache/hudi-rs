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
//! Core library of hudi-rs, a native Rust implementation of [Apache Hudi](https://hudi.apache.org).
//!
//! Most applications depend on the [`hudi`](https://docs.rs/hudi) crate, which re-exports this one;
//! the [Python and Rust quick start guide][quick start guide] walks through it, and the `hudi`
//! crate docs list every resource (API references, quick starts, examples, specs). The main entry
//! points:
//!
//! - [`table::Table`] opens a table ([`Table::new`](table::Table::new),
//!   [`TableBuilder`](table::builder::TableBuilder)) or creates one
//!   ([`Table::create`](table::Table::create)), and carries both the read API (`read`,
//!   `read_stream`, `get_file_slices`) and the write API (`append`, `upsert`, `delete`, `update`,
//!   `overwrite`, and the experimental `compact` for merge-on-read tables).
//! - [`table::ReadOptions`] selects the query type ([`table::QueryType`]), timestamps, filters,
//!   and projection for every read.
//! - [`write`](mod@write) holds the types the write API takes and returns, such as [`WriteResult`] and
//!   [`UpsertOptions`].
//! - [`file_group::reader::FileGroupReader`] reads one file slice at a time, for query engines
//!   that plan their own scans with [`Table::get_file_slices`](table::Table::get_file_slices).
//! - [`config`] defines the typed table and read configs, such as
//!   [`HudiTableConfig`](config::table::HudiTableConfig) and
//!   [`HudiReadConfig`](config::read::HudiReadConfig).
//!
//! Most fallible APIs return [`error::Result`], whose error type is [`error::CoreError`]; config
//! parsing returns [`config::error::Result`].
//!
//! [quick start guide]: https://hudi.apache.org/docs/python-rust-quick-start-guide
//!
//! # Example
//!
//! Plan a snapshot read as file slices and read each slice with a file group reader, the way a
//! query engine distributes a scan.
//!
//! ```
//! # use std::sync::Arc;
//! # use arrow::array::{ArrayRef, RecordBatch, StringArray};
//! # use arrow::datatypes::{DataType, Field, Schema};
//! use hudi_core::config::util::empty_options;
//! use hudi_core::table::{ReadOptions, Table};
//!
//! # #[tokio::main]
//! # async fn main() -> hudi_core::error::Result<()> {
//! # let dir = tempfile::tempdir()?;
//! # let base_uri = dir.path().to_str().unwrap();
//! # let mut writer = Table::create(base_uri)
//! #     .with_table_name("docs")
//! #     .with_record_key_fields(["id"])
//! #     .create()
//! #     .await?;
//! # let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
//! # let ids: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
//! # writer.append([RecordBatch::try_new(schema, vec![ids])?]).await?;
//! let table = Table::new(base_uri).await?;
//! let options = ReadOptions::new();
//! let reader = table
//!     .create_file_group_reader_with_options(Some(&options), empty_options())
//!     .await?;
//! let mut num_rows = 0;
//! for slice in table.get_file_slices(&options).await? {
//!     num_rows += reader.read_file_slice(&slice, &options).await?.num_rows();
//! }
//! assert_eq!(num_rows, 2);
//! # Ok(())
//! # }
//! ```

// The write-path async fns (e.g. `Table::delete_keys`) nest deeply enough that computing their
// layout exceeds the default limit of 128 on Linux targets.
#![recursion_limit = "256"]

mod avro_to_arrow;
mod bloom;
#[warn(missing_docs)]
pub mod config;
#[warn(missing_docs)]
pub mod error;
#[warn(missing_docs)]
pub mod expr;
pub mod file_group;
pub mod hfile;
pub mod index;
pub mod keygen;
pub mod merge;
pub mod metadata;
mod record;
pub mod schema;
pub mod statistics;
pub mod storage;
#[warn(missing_docs)]
pub mod table;
pub mod timeline;
pub mod util;
#[warn(missing_docs)]
pub mod write;

pub use index::{HoodieKey, SimpleIndex};
pub use write::{UpsertOptions, WriteResult};

use error::Result;
