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

impl std::str::FromStr for IteratorMode {
    type Err = crate::config::error::ConfigError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "ENGINE_RECORD" => Ok(Self::EngineRecord),
            "HOODIE_RECORD" => Ok(Self::HoodieRecord),
            "RECORD_KEY" => Ok(Self::RecordKey),
            v => Err(crate::config::error::ConfigError::InvalidValue(
                v.to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    /// Every mode Java names round-trips, and the default is the one this crate
    /// actually implements — Arrow batches. A `Default` that drifted to an
    /// unimplemented mode would make the reader yield nothing recognisable.
    #[test]
    fn test_iterator_mode_parses_every_java_name() {
        assert_eq!(
            IteratorMode::from_str("ENGINE_RECORD").unwrap(),
            IteratorMode::EngineRecord
        );
        assert_eq!(
            IteratorMode::from_str("HOODIE_RECORD").unwrap(),
            IteratorMode::HoodieRecord
        );
        assert_eq!(
            IteratorMode::from_str("RECORD_KEY").unwrap(),
            IteratorMode::RecordKey
        );
        assert_eq!(IteratorMode::default(), IteratorMode::EngineRecord);
    }

    /// An unrecognised mode names the value it refused rather than silently
    /// falling back to the default — the same posture the reader-version and
    /// merge-mode parses take.
    #[test]
    fn test_iterator_mode_unknown_value_is_an_error_naming_it() {
        let err = IteratorMode::from_str("engine_record")
            .expect_err("matching is exact, so lower case is not a mode");
        assert!(
            err.to_string().contains("engine_record"),
            "the error must name the value, got: {err}"
        );
        assert!(IteratorMode::from_str("").is_err());
    }
}
