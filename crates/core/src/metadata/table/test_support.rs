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

//! Helpers shared by the metadata table's own tests and its two readers' tests.
//!
//! Both reader test modules need the same two things: the metadata table's own
//! properties, and one comparable shape to diff two readers' output in. Keeping one
//! copy of each means a change to the metadata table's properties, or to what
//! "these two readers agree" means, lands in one place.

use crate::config::HudiConfigs;
use crate::config::table::HudiTableConfig;
use crate::metadata::table::records::FilesPartitionRecord;
use std::collections::HashMap;
use std::sync::Arc;

/// One file entry, flattened for comparison: name, size, tombstone flag.
pub(crate) type FileEntry = (String, i64, bool);
/// One record, flattened: key, partition type, and its entries in a stable order.
pub(crate) type ComparableRecord = (String, i32, Vec<FileEntry>);

/// The metadata table's own properties, as its `hoodie.properties` states them,
/// for a metadata table rooted at `base_uri`.
///
/// The record-key pair is load-bearing: without it the key resolves to
/// `_hoodie_record_key`, which every metadata record leaves empty, and the merge
/// collapses every key onto one entry. The end timestamp is unbounded because a
/// metadata read always wants the latest state of the slice it was handed.
pub(crate) fn metadata_table_configs(base_uri: &str) -> Arc<HudiConfigs> {
    Arc::new(HudiConfigs::new([
        (HudiTableConfig::BasePath.as_ref(), base_uri.to_string()),
        (
            HudiTableConfig::BaseFileFormat.as_ref(),
            "hfile".to_string(),
        ),
        (HudiTableConfig::RecordKeyFields.as_ref(), "key".to_string()),
        (
            HudiTableConfig::PopulatesMetaFields.as_ref(),
            "false".to_string(),
        ),
        ("hoodie.record.merge.mode", "CUSTOM".to_string()),
        (
            "hoodie.record.merge.strategy.id",
            "00000000-0000-0000-0000-000000000000".to_string(),
        ),
        (
            "hoodie.compaction.payload.class",
            "org.apache.hudi.metadata.HoodieMetadataPayload".to_string(),
        ),
        (
            crate::config::read::HudiReadConfig::EndTimestamp.as_ref(),
            crate::file_group::reader_v2::MAX_INSTANT_TIME.to_string(),
        ),
    ]))
}

/// Both readers' output in a comparable shape, so a mismatch prints the values that
/// differ rather than a pair of hash maps.
///
/// Every field a reader decides is in here, `record_type` included: a comparison
/// that dropped one would let the readers diverge on it unnoticed.
pub(crate) fn comparable(records: &HashMap<String, FilesPartitionRecord>) -> Vec<ComparableRecord> {
    let mut out: Vec<_> = records
        .values()
        .map(|r| {
            let mut files: Vec<FileEntry> = r
                .files
                .values()
                .map(|f| (f.name.clone(), f.size, f.is_deleted))
                .collect();
            files.sort();
            (r.key.clone(), r.record_type as i32, files)
        })
        .collect();
    out.sort();
    out
}
