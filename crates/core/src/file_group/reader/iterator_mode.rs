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

//! Mirrors `org.apache.hudi.common.table.read.IteratorMode`.
//!
//! Controls what form the output records take when iterating.

/// The mode in which the file group reader yields records.
///
/// In Java Hudi, this controls whether the iterator produces engine-native
/// records, HoodieRecord wrappers, or just record keys.
///
/// In hudi-rs, we always work with Arrow RecordBatch, so `EngineRecord`
/// is the primary mode. The other modes are kept for API symmetry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IteratorMode {
    /// Yield engine-native records (Arrow RecordBatch in Rust).
    #[default]
    EngineRecord,

    /// Yield HoodieRecord wrappers (not yet implemented in Rust).
    HoodieRecord,

    /// Yield only record keys (not yet implemented in Rust).
    RecordKey,
}

impl IteratorMode {
    pub fn from_str_opt(s: &str) -> Option<Self> {
        match s {
            "ENGINE_RECORD" => Some(Self::EngineRecord),
            "HOODIE_RECORD" => Some(Self::HoodieRecord),
            "RECORD_KEY" => Some(Self::RecordKey),
            _ => None,
        }
    }
}
