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
//! use hudi_core::config::read::HudiReadConfig::{AsOfTimestamp, InputPartitions};
//! use hudi_core::table::Table as HudiTable;
//!
//! let options = [(InputPartitions, "2"), (AsOfTimestamp, "20240101010100000")];
//! HudiTable::new_with_options("/tmp/hudi_data", options);
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

pub mod config;
pub mod file_group;
pub mod storage;
pub mod table;
pub mod util;
