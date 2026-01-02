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
//! HFile reader implementation.
//!
//! HFile is an SSTable based row-oriented file format, optimized for
//! range scans and point lookups. HFile is used as the base file format
//! for Hudi's metadata table.
//!
//! See [Hudi's HFile format specification](https://github.com/apache/hudi/blob/master/hudi-io/hfile_format.md).
//!
//! # Example
//! ```ignore
//! use hudi_core::hfile::{HFileReader, Utf8Key, SeekResult};
//!
//! let bytes = std::fs::read("data.hfile")?;
//! let mut reader = HFileReader::new(bytes)?;
//!
//! // Iterate all entries
//! for result in reader.iter()? {
//!     let kv = result?;
//!     println!("{}: {:?}", kv.key(), kv.value());
//! }
//! ```

mod block;
mod block_type;
mod compression;
mod error;
mod key;
mod proto;
mod reader;
mod record;
mod trailer;

pub use block::BlockIndexEntry;
pub use block_type::HFileBlockType;
pub use error::{HFileError, Result};
pub use key::{Key, KeyValue, Utf8Key};
pub use reader::{HFileReader, HFileRecordIterator, SeekResult};
pub use record::HFileRecord;
