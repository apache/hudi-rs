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

//! Mirrors `org.apache.hudi.common.table.read.HoodieReadStats`.
//!
//! Mutable accumulator for read statistics, written during log scanning
//! and buffer processing, read by the caller after the read completes.

/// Mutable accumulator for file group read statistics.
#[derive(Debug, Clone, Default)]
pub struct HoodieReadStats {
    pub num_inserts: u64,
    pub num_updates: u64,
    pub num_deletes: u64,
    pub total_log_read_time_ms: u64,
    pub total_log_records: u64,
    pub total_log_files_compacted: u64,
    pub total_log_blocks: u64,
    pub total_corrupt_log_blocks: u64,
    pub total_rollback_blocks: u64,
}
