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
//! Write APIs for Hudi tables (pyIceberg-style Table verbs).

mod append;
mod create;
mod keygen;
pub(crate) mod metadata;
mod rewrite;

pub use append::{AppendResult, append_batches};
pub use create::TableCreateBuilder;
pub use rewrite::{
    UpsertOptions, WriteResult, delete_filter, delete_keys, overwrite_batches, upsert_batches,
};
