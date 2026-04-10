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

//! Mirrors `org.apache.hudi.common.table.read.ReaderParameters`.
//!
//! Pure query-object holding the four boolean flags that control how the
//! file group reader behaves.

/// Configuration flags for the file group reader.
///
/// These are extracted from the reader context and drive the buffer
/// selection logic in [`super::buffer::loader::DefaultFileGroupRecordBufferLoader`].
#[derive(Debug, Clone)]
pub struct ReaderParameters {
    /// Whether to use record positions for merging (position-based merge).
    pub use_record_position: bool,

    /// Whether to emit delete records in the output.
    pub emit_delete: bool,

    /// Whether to sort the output records.
    pub sort_output: bool,

    /// Whether to allow in-flight (uncommitted) instants during log scanning.
    pub allow_inflight_instants: bool,
}

impl Default for ReaderParameters {
    fn default() -> Self {
        Self {
            use_record_position: false,
            emit_delete: false,
            sort_output: false,
            allow_inflight_instants: false,
        }
    }
}
