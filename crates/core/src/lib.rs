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
//! Crate `hudi-core`.
//!
//! # The [config] module is responsible for managing configurations.
//!
//! **Example**
//!
//! ```rust
//! use hudi_core::config::read::HudiReadConfig::InputPartitions;
//! use hudi_core::table::Table as HudiTable;
//!
//! # #[tokio::main]
//! # async fn main() {
//! let options = [(InputPartitions, "2")];
//! HudiTable::new_with_options("/tmp/hudi_data", options).await;
//! # }
//! ```
//!
//! # The [table] module is responsible for managing Hudi tables.
//!
//! **Example**
//!
//! create hudi table
//! ```rust
//! use hudi_core::table::Table;
//!
//! pub async fn test() {
//!     let hudi_table = Table::new("/tmp/hudi_data").await.unwrap();
//! }
//! ```

// Re-export arrow dependencies based on feature flags.
// build.rs sets `use_arrow_54` only when arrow-54 is enabled WITHOUT arrow-default,
// so --all-features gracefully falls back to the default arrow version.

#[cfg(not(any(feature = "arrow-default", feature = "arrow-54")))]
compile_error!("Either 'arrow-default' or 'arrow-54' feature must be enabled.");

#[cfg(all(use_arrow_54, feature = "datafusion"))]
compile_error!(
    "Feature 'datafusion' requires 'arrow-default'. \
     DataFusion depends on object_store 0.12, which is incompatible with arrow-54."
);

// Arrow v54 re-exports (only when arrow-54 is the sole arrow feature)
#[cfg(use_arrow_54)]
pub extern crate arrow_arith_v54 as arrow_arith;
#[cfg(use_arrow_54)]
pub extern crate arrow_array_v54 as arrow_array;
#[cfg(use_arrow_54)]
pub extern crate arrow_buffer_v54 as arrow_buffer;
#[cfg(use_arrow_54)]
pub extern crate arrow_cast_v54 as arrow_cast;
#[cfg(use_arrow_54)]
pub extern crate arrow_ipc_v54 as arrow_ipc;
#[cfg(use_arrow_54)]
pub extern crate arrow_json_v54 as arrow_json;
#[cfg(use_arrow_54)]
pub extern crate arrow_ord_v54 as arrow_ord;
#[cfg(use_arrow_54)]
pub extern crate arrow_row_v54 as arrow_row;
#[cfg(use_arrow_54)]
pub extern crate arrow_schema_v54 as arrow_schema;
#[cfg(use_arrow_54)]
pub extern crate arrow_select_v54 as arrow_select;
#[cfg(use_arrow_54)]
pub extern crate arrow_v54 as arrow;
#[cfg(use_arrow_54)]
pub extern crate object_store_v54 as object_store;
#[cfg(use_arrow_54)]
pub extern crate parquet_v54 as parquet;

// Arrow default re-exports
#[cfg(not(use_arrow_54))]
pub extern crate arrow;
#[cfg(not(use_arrow_54))]
pub extern crate arrow_arith;
#[cfg(not(use_arrow_54))]
pub extern crate arrow_array;
#[cfg(not(use_arrow_54))]
pub extern crate arrow_buffer;
#[cfg(not(use_arrow_54))]
pub extern crate arrow_cast;
#[cfg(not(use_arrow_54))]
pub extern crate arrow_ipc;
#[cfg(not(use_arrow_54))]
pub extern crate arrow_json;
#[cfg(not(use_arrow_54))]
pub extern crate arrow_ord;
#[cfg(not(use_arrow_54))]
pub extern crate arrow_row;
#[cfg(not(use_arrow_54))]
pub extern crate arrow_schema;
#[cfg(not(use_arrow_54))]
pub extern crate arrow_select;
#[cfg(not(use_arrow_54))]
pub extern crate object_store;
#[cfg(not(use_arrow_54))]
pub extern crate parquet;

mod avro_to_arrow;
pub mod config;
pub mod error;
pub mod expr;
pub mod file_group;
pub mod hfile;
pub mod keygen;
pub mod merge;
pub mod metadata;
mod record;
pub mod schema;
pub mod statistics;
pub mod storage;
pub mod table;
pub mod timeline;
pub mod util;

use error::Result;
