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

//! Calls the merge-on-read reader with the arguments the existing reader takes.
//!
//! The two readers want different things. The existing one is handed a base
//! path, some log paths and a [`ReadOptions`] per call, and works out the rest.
//! The merge-on-read reader wants a context, an input split, reader parameters
//! and its schemas up front, and reads one slice per instance. This turns the
//! first into the second.
//!
//! Filters and projection are deliberately *not* handled here. The caller
//! applies them to whatever comes back, through the same code the existing
//! reader's result goes through, so the two cannot drift on what a filter means.

use crate::Result;
use crate::config::HudiConfigs;
use crate::config::read::HudiReadConfig;
use crate::error::CoreError;
use crate::file_group::reader_v2::MAX_INSTANT_TIME;
use crate::file_group::reader_v2::engine::HoodieFileGroupReader;
use crate::file_group::reader_v2::input_split::InputSplit;
use crate::file_group::reader_v2::reader_parameters::ReaderParameters;
use crate::file_group::reader_v2::resolver::resolve_reader_context;
use crate::storage::Storage;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use std::sync::Arc;

/// Read one file slice through the merge-on-read reader.
///
/// `partition_path` is the partition the slice lives in, which the reader needs
/// to resolve partition-valued fields. `data_schema` is the table's schema; the
/// merge-on-read reader requires it up front rather than deriving it from the
/// data as the existing reader does.
pub(crate) async fn read_file_slice(
    hudi_configs: Arc<HudiConfigs>,
    storage: Arc<Storage>,
    base_file_path: &str,
    log_file_paths: Vec<String>,
    partition_path: String,
    data_schema: Option<SchemaRef>,
) -> Result<RecordBatch> {
    let has_log_files = !log_file_paths.is_empty();
    let hudi_configs = with_unbounded_end_timestamp(hudi_configs);
    let mut context = resolve_reader_context(&hudi_configs, has_log_files)?;
    context.rebuild_record_context(partition_path.clone());

    let input_split = InputSplit::new(
        // A slice with no base file reports an empty path; the engine keys its
        // log-only handling on `None`, not on an empty string.
        (!base_file_path.is_empty()).then(|| base_file_path.to_string()),
        None,
        log_file_paths,
        partition_path,
    );

    // Defaults chosen to match what the existing reader returns: deletes are
    // applied but never emitted, output keeps merge order, and only completed
    // instants are read.
    let mut reader = HoodieFileGroupReader::new(
        Arc::new(context),
        storage,
        input_split,
        ReaderParameters::default(),
        data_schema,
        None,
    )?;

    reader.read().await
}

/// Pin the read to the end of the timeline when the caller did not bound it.
///
/// [`resolve_reader_context`] requires an end timestamp, because a caller that
/// means to read a point in time and forgets to say so should not silently get
/// the present. The existing reader has no such requirement: it passes the
/// bound through as an `Option` and an absent one means unbounded.
///
/// This adapter has to reproduce the existing reader's semantics, not the
/// resolver's preference, so an absent bound becomes an explicit far-future
/// one. Every real instant sorts before [`MAX_INSTANT_TIME`], making the two
/// equivalent.
fn with_unbounded_end_timestamp(hudi_configs: Arc<HudiConfigs>) -> Arc<HudiConfigs> {
    let key = HudiReadConfig::EndTimestamp.as_ref();
    let mut options = hudi_configs.as_options();
    if options.contains_key(key) {
        return hudi_configs;
    }
    options.insert(key.to_string(), MAX_INSTANT_TIME.to_string());
    Arc::new(HudiConfigs::new(options))
}

/// Whether the merge-on-read reader can serve this read.
///
/// Returning a reason rather than a bool so the caller can say why it refused —
/// silently falling back would hide that the engine the caller asked for was
/// not the one that ran.
pub(crate) fn refuse_reason(
    is_metadata_table: bool,
    data_schema: Option<&SchemaRef>,
) -> Option<CoreError> {
    if is_metadata_table {
        return Some(CoreError::Unsupported(
            "The merge-on-read reader cannot read a metadata table: its base files and log \
             blocks are HFile, which that reader has no support for."
                .to_string(),
        ));
    }
    if data_schema.is_none() {
        return Some(CoreError::Unsupported(
            "The merge-on-read reader needs the table schema up front, and none was resolved \
             for this read."
                .to_string(),
        ));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::table::HudiTableConfig;

    fn schema() -> SchemaRef {
        Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int32,
            true,
        )]))
    }

    /// A metadata table is refused rather than attempted. The merge-on-read
    /// reader has no HFile support, so letting it try would fail somewhere
    /// deeper and less clearly.
    #[test]
    fn refuses_a_metadata_table() {
        let reason = refuse_reason(true, Some(&schema())).expect("should refuse");
        assert!(
            reason.to_string().contains("metadata table"),
            "reason should name the metadata table, got: {reason}"
        );
    }

    /// The existing reader derives its schema from the data it reads; this one
    /// cannot, so a read with no schema is refused rather than guessed at.
    #[test]
    fn refuses_when_no_schema_was_resolved() {
        let reason = refuse_reason(false, None).expect("should refuse");
        assert!(
            reason.to_string().contains("schema"),
            "reason should name the missing schema, got: {reason}"
        );
    }

    #[test]
    fn accepts_a_regular_table_with_a_schema() {
        assert!(refuse_reason(false, Some(&schema())).is_none());
    }

    /// The reader context comes from table configs, so a slice with no log
    /// files still resolves — the engine reduces to a base-file read.
    #[test]
    fn resolves_a_context_for_a_base_only_slice() {
        let configs = HudiConfigs::new([
            (HudiTableConfig::BasePath.as_ref(), "file:///tmp/t"),
            ("hoodie.read.end.timestamp", "20240101000000000"),
            (HudiTableConfig::OrderingFields.as_ref(), "ts"),
        ]);
        let context = resolve_reader_context(&configs, false).unwrap();
        assert!(!context.has_log_files);
    }
}
