// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Point lookups through the global record index at a captured committed snapshot.

use crate::config::{
    HudiConfigs,
    read::HudiReadConfig,
    table::{BaseFileFormatValue, HudiTableConfig},
};
use crate::file_group::reader_v2::{
    engine::HoodieFileGroupReader, input_split::InputSplit, reader_parameters::ReaderParameters,
    resolver::resolve_reader_context,
};
use crate::file_group::{
    base_file::reader::{BaseFileReadOptions, create_base_file_reader},
    file_slice::FileSlice,
};
use crate::metadata::table::{
    record_index::{self, RecordIndexLocation, record_index_shard},
    v2_reader::MetadataTableV2Reader,
};
use crate::storage::{RowFilterBuilder, Storage};
use crate::table::{file_pruner::FilePruner, partition::PartitionPruner};
use crate::{Result, error::CoreError, table::Table};
use arrow_array::{BooleanArray, RecordBatch, StringArray};
use arrow_schema::{DataType, SchemaRef};
use futures::{
    StreamExt, TryStreamExt,
    stream::{self, BoxStream},
};
use parquet::arrow::{
    ProjectionMask,
    arrow_reader::{ArrowPredicateFn, RowFilter, RowSelection, RowSelector},
};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

const KEY: &str = "_hoodie_record_key";

/// Frozen inputs shared by all workers of a point lookup.
///
/// Construct with [`Table::prepare_record_lookup`]. Does not refresh timelines
/// during execution. Missing retained files are errors, never absent records.
pub struct RecordLookup {
    table: Table,
    metadata_reader: MetadataTableV2Reader,
    shards: Vec<FileSlice>,
    slices: HashMap<(String, String), FileSlice>,
    storage: Arc<Storage>,
    schema: SchemaRef,
    output_schema: SchemaRef,
    projection: Option<Vec<String>>,
    configs: Arc<HudiConfigs>,
}

impl Table {
    /// Look up unique record keys in the latest committed snapshot.
    ///
    /// Output is unordered and omits missing/deleted keys. Requires an initialized
    /// global RLI and populated Hudi meta fields. Inflight writes are excluded.
    /// Cleaning or a concurrent commit during snapshot preparation can cause an
    /// error; retry the whole lookup, not an individual execution partition.
    ///
    /// ```ignore
    /// use futures::TryStreamExt;
    /// let keys = vec!["order-123".to_string()];
    /// let columns = vec!["order_id".to_string(), "amount".to_string()];
    /// let mut rows = table.lookup_records(&keys, Some(&columns)).await?;
    /// while let Some(batch) = rows.try_next().await? {
    ///     // Process matching live records.
    /// }
    /// ```
    pub async fn lookup_records(
        &self,
        keys: &[String],
        projection: Option<&[String]>,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        if keys.is_empty() {
            return Ok(Box::pin(stream::empty()));
        }
        let lookup = Arc::new(self.prepare_record_lookup(projection).await?);
        if keys.len() == 1 {
            let key = keys[0].clone();
            return Ok(Box::pin(
                stream::once(async move { lookup.lookup_record(&key).await })
                    .try_filter_map(|batch| async { Ok(batch) }),
            ));
        }
        let mut grouped = BTreeMap::<usize, Vec<&str>>::new();
        let unique: HashSet<&str> = keys.iter().map(String::as_str).collect();
        for key in unique {
            grouped
                .entry(lookup.shard_for_key(key)?)
                .or_default()
                .push(key);
        }
        let mut locations = Vec::new();
        for (shard, keys) in grouped {
            locations.extend(lookup.lookup_shard(shard, &keys).await?);
        }
        lookup.read_locations(locations)
    }

    /// Capture fresh committed state for a lookup executor, including DataFusion.
    ///
    /// Read options on this table do not override the latest-snapshot contract.
    pub async fn prepare_record_lookup(
        &self,
        projection: Option<&[String]>,
    ) -> Result<RecordLookup> {
        let options: HashMap<String, String> = self
            .hudi_options()
            .into_iter()
            .filter(|(k, _)| !k.starts_with("hoodie.read."))
            .chain(self.storage_options())
            .collect();
        let table = Table::new_with_options(self.base_url().as_str(), options).await?;
        if !table
            .get_metadata_table_partitions()
            .iter()
            .any(|p| p == "record_index")
        {
            return Err(CoreError::Unsupported(
                "Point lookup requires an initialized global record_index".into(),
            ));
        }
        let timestamp = table.timeline.get_latest_commit_timestamp()?;
        let schema = Arc::new(table.get_schema_with_meta_fields().await?);
        let populates_meta_fields: bool = table
            .hudi_configs
            .get_or_default(HudiTableConfig::PopulatesMetaFields)
            .into();
        if schema.index_of(KEY).is_err() || !populates_meta_fields {
            return Err(CoreError::Unsupported(
                "Point lookup requires populated _hoodie_record_key metadata".into(),
            ));
        }
        let output_schema = match projection {
            Some(names) => Arc::new(
                schema.project(
                    &names
                        .iter()
                        .map(|n| schema.index_of(n))
                        .collect::<std::result::Result<Vec<_>, _>>()?,
                )?,
            ),
            None => schema.clone(),
        };
        let metadata = table.new_metadata_table().await?;
        let valid = table.valid_instant_timestamps(&metadata).await?;
        let Some((reader, mut shards)) = metadata.partition_reader("record_index").await? else {
            return Err(CoreError::MetadataTable(
                "RLI has no committed shards".into(),
            ));
        };
        shards.sort_by(|a, b| a.file_id().cmp(b.file_id()));
        for (index, shard) in shards.iter().enumerate() {
            let expected = format!("record-index-{index:04}-0");
            if shard.file_id() != expected {
                return Err(CoreError::Unsupported(format!(
                    "Unsupported or incomplete global RLI layout: expected {expected}, found {}",
                    shard.file_id()
                )));
            }
        }
        // Metadata must include the latest data commit before an index miss is authoritative.
        if !metadata
            .timeline
            .completed_commits
            .iter()
            .any(|i| i.timestamp == timestamp)
        {
            return Err(CoreError::MetadataTable(format!(
                "RLI timeline does not cover data commit {timestamp}"
            )));
        }
        let view = table.timeline.create_view_as_of(&timestamp).await?;
        let partition_pruner = PartitionPruner::new(
            &[],
            &table.get_partition_schema().await?,
            &table.hudi_configs,
        )?;
        let slices = table
            .file_system_view
            .get_file_slices(
                &partition_pruner,
                &FilePruner::empty(),
                &schema,
                &view,
                Some(crate::table::fs_view::MetadataListing {
                    table: &metadata,
                    valid_instants: &valid,
                }),
                None,
            )
            .await?
            .into_iter()
            .map(|s| ((s.partition_path.clone(), s.file_id().to_string()), s))
            .collect();
        let after = crate::timeline::Timeline::new_from_storage(
            table.hudi_configs.clone(),
            Arc::new(table.storage_options()),
        )
        .await?;
        if table.timeline.completed_commits != after.completed_commits {
            return Err(CoreError::MetadataTable(
                "Timeline changed while capturing lookup; retry the lookup".into(),
            ));
        }
        let mut config = table.hudi_options();
        config.insert(HudiReadConfig::EndTimestamp.as_ref().to_string(), timestamp);
        let configs = Arc::new(HudiConfigs::new(config));
        let storage = Storage::new(Arc::new(table.storage_options()), configs.clone())?;
        Ok(RecordLookup {
            table,
            metadata_reader: reader.with_valid_instants(valid),
            shards,
            slices,
            storage,
            schema,
            output_schema,
            projection: projection.map(<[String]>::to_vec),
            configs,
        })
    }
}

impl RecordLookup {
    /// Schema of lookup output, including the requested projection.
    pub fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    /// Compute the physical Hudi shard; execution partition numbers may differ.
    pub fn shard_for_key(&self, key: &str) -> Result<usize> {
        record_index_shard(key, self.shards.len())
    }

    /// Read one metadata shard for its assigned keys, merging committed metadata logs.
    pub async fn lookup_shard(
        &self,
        shard: usize,
        keys: &[&str],
    ) -> Result<Vec<RecordIndexLocation>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let slice = self
            .shards
            .get(shard)
            .ok_or_else(|| CoreError::MetadataTable(format!("RLI shard {shard} out of range")))?;
        for key in keys {
            if self.shard_for_key(key)? != shard {
                return Err(CoreError::MetadataTable(format!(
                    "Key routed to incorrect RLI shard {shard}"
                )));
            }
        }
        let batch = self
            .metadata_reader
            .read_partition_batch(slice, keys, "record_index")
            .await?;
        record_index::decode(&batch, keys)
    }

    async fn lookup_record(self: Arc<Self>, key: &str) -> Result<Option<RecordBatch>> {
        let locations = self.lookup_shard(self.shard_for_key(key)?, &[key]).await?;
        let batches: Vec<_> = self.read_locations(locations)?.try_collect().await?;
        let batch = arrow::compute::concat_batches(&self.output_schema, &batches)?;
        match batch.num_rows() {
            0 => Ok(None),
            1 => Ok(Some(batch)),
            n => Err(CoreError::MetadataTable(format!(
                "Global RLI returned {n} records for one key"
            ))),
        }
    }

    /// Group locations by file and stream their merged results. Callers may route
    /// locations to different workers, but each file should belong to one worker.
    pub fn read_locations(
        self: &Arc<Self>,
        locations: Vec<RecordIndexLocation>,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let mut files = BTreeMap::<(String, String), Vec<RecordIndexLocation>>::new();
        for location in locations {
            let identity = (location.partition_path.clone(), location.file_id.clone());
            if !self.slices.contains_key(&identity) {
                return Err(CoreError::MetadataTable(format!(
                    "RLI references a file group absent from the committed snapshot: {identity:?}"
                )));
            }
            files.entry(identity).or_default().push(location);
        }
        let lookup = self.clone();
        Ok(Box::pin(
            stream::iter(files)
                .then(move |(id, locations)| {
                    let lookup = lookup.clone();
                    async move { lookup.read_file(&lookup.slices[&id], locations).await }
                })
                .try_flatten(),
        ))
    }

    async fn read_file(
        &self,
        slice: &FileSlice,
        locations: Vec<RecordIndexLocation>,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let keys: Arc<HashSet<String>> =
            Arc::new(locations.iter().map(|l| l.key.clone()).collect());
        let path = slice.base_file_relative_path()?;
        if let Some(path) = path.as_ref().filter(|p| !p.ends_with(".parquet")) {
            return Err(CoreError::Unsupported(format!(
                "Point lookup requires a Parquet data base file, found '{path}'"
            )));
        }
        let mut context =
            resolve_reader_context(&self.configs, slice.has_log_file(), path.as_deref())?;
        context.rebuild_record_context(slice.partition_path.clone());
        context.completion_gate_inputs =
            Some(Arc::new(self.table.timeline.completion_gate_inputs()));
        // Key-based merging preserves log bitmap alignment when filtering Parquet log blocks.
        context.should_merge_use_record_position = false;
        context.row_filter_builder = Some(key_filter(keys.clone()));
        context.mor_pk_safe = true;
        let selection = match &path {
            Some(path) => self.validated_selection(path, &locations, &keys).await?,
            None => None,
        };
        let mut requested = self.projection.clone().unwrap_or_else(|| {
            self.schema
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect()
        });
        if !requested.iter().any(|c| c == KEY) {
            requested.push(KEY.into());
        }
        let requested = Arc::new(
            self.schema.project(
                &requested
                    .iter()
                    .map(|n| self.schema.index_of(n))
                    .collect::<std::result::Result<Vec<_>, _>>()?,
            )?,
        );
        let mut reader = HoodieFileGroupReader::new(
            Arc::new(context),
            self.storage.clone(),
            InputSplit::new(
                path,
                slice.base_file.as_ref().map(|b| b.commit_timestamp.clone()),
                slice
                    .log_files
                    .iter()
                    .map(|l| slice.log_file_relative_path(l))
                    .collect::<Result<Vec<_>>>()?,
                slice.partition_path.clone(),
            ),
            ReaderParameters {
                base_row_selection: selection,
                ..Default::default()
            },
            Some(self.schema.clone()),
            Some(requested),
        )?;
        let output_schema = self.output_schema.clone();
        let batches = reader.open_stream().await?;
        Ok(Box::pin(batches.map(move |batch| {
            let batch = batch?;
            let mask = key_mask(&batch, &keys)?;
            let batch = arrow::compute::filter_record_batch(&batch, &mask)?;
            let indices = output_schema
                .fields()
                .iter()
                .map(|f| batch.schema().index_of(f.name()))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(batch.project(&indices)?)
        })))
    }

    async fn validated_selection(
        &self,
        path: &str,
        locations: &[RecordIndexLocation],
        keys: &HashSet<String>,
    ) -> Result<Option<RowSelection>> {
        let Some(positions) = locations
            .iter()
            .map(|l| l.position.and_then(|p| usize::try_from(p).ok()))
            .collect::<Option<Vec<_>>>()
        else {
            return Ok(None);
        };
        let base = create_base_file_reader(&self.storage, &BaseFileFormatValue::Parquet)?;
        let (metadata, _) = base
            .get_metadata_and_stats(path, &arrow_schema::Schema::empty())
            .await?;
        if positions
            .iter()
            .any(|&p| p as u64 >= metadata.num_records as u64)
        {
            return Ok(None);
        }
        let selection = positions_to_selection(positions)?;
        // Validate against the exact immutable file before excluding any base rows.
        // The RLI instant alone does not prove that compaction preserved ordinals.
        let batch = base
            .read_data(
                path,
                BaseFileReadOptions::new()
                    .with_projection([KEY])
                    .with_row_selection(selection.clone()),
            )
            .await?;
        let mask = key_mask(&batch, keys)?;
        if batch.num_rows() != keys.len()
            || mask.false_count() != 0
            || distinct_keys(&batch)?.len() != keys.len()
        {
            return Ok(None);
        }
        Ok(Some(selection))
    }
}

fn positions_to_selection(mut positions: Vec<usize>) -> Result<RowSelection> {
    positions.sort_unstable();
    positions.dedup();
    let mut cursor = 0;
    let mut selectors = Vec::with_capacity(positions.len() * 2);
    for position in positions {
        if position > cursor {
            selectors.push(RowSelector::skip(position - cursor));
        }
        selectors.push(RowSelector::select(1));
        cursor = position
            .checked_add(1)
            .ok_or_else(|| CoreError::MetadataTable("RLI row position overflows usize".into()))?;
    }
    Ok(RowSelection::from(selectors))
}

fn distinct_keys(batch: &RecordBatch) -> Result<HashSet<String>> {
    let column = batch
        .column_by_name(KEY)
        .ok_or_else(|| CoreError::Schema(format!("Missing {KEY}")))?;
    let column = arrow_cast::cast(column, &DataType::Utf8)?;
    let strings = column
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| CoreError::Schema(format!("Invalid {KEY}")))?;
    Ok(strings.iter().flatten().map(str::to_string).collect())
}

fn key_mask(batch: &RecordBatch, keys: &HashSet<String>) -> Result<BooleanArray> {
    let column = batch
        .column_by_name(KEY)
        .ok_or_else(|| CoreError::Schema(format!("Missing {KEY}")))?;
    let column = arrow_cast::cast(column, &DataType::Utf8)?;
    let strings = column
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| CoreError::Schema(format!("Invalid {KEY}")))?;
    Ok(BooleanArray::from_iter(strings.iter().map(|key| {
        Some(key.is_some_and(|key| keys.contains(key)))
    })))
}

fn key_filter(keys: Arc<HashSet<String>>) -> RowFilterBuilder {
    Arc::new(move |descriptor, schema| {
        let index = schema.index_of(KEY).ok()?;
        let keys = keys.clone();
        Some(RowFilter::new(vec![Box::new(ArrowPredicateFn::new(
            ProjectionMask::roots(descriptor, [index]),
            move |batch| {
                key_mask(&batch, &keys)
                    .map_err(|e| arrow_schema::ArrowError::ComputeError(e.to_string()))
            },
        ))]))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;
    use hudi_test::QuickstartTripsTable;

    #[tokio::test]
    async fn test_lookup_matches_committed_mor_snapshot() {
        let table = Table::new(&QuickstartTripsTable::V8Trips8I3U1D.path_to_mor_avro())
            .await
            .unwrap();
        let batches = table.read(&crate::table::ReadOptions::new()).await.unwrap();
        let expected = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
        let keys: Vec<String> = distinct_keys(&expected).unwrap().into_iter().collect();
        let projection = vec![KEY.to_string(), "rider".to_string()];
        let batches: Vec<RecordBatch> = table
            .lookup_records(&keys, Some(&projection))
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let schema = Arc::new(
            expected
                .schema()
                .project(
                    &projection
                        .iter()
                        .map(|n| expected.schema().index_of(n).unwrap())
                        .collect::<Vec<_>>(),
                )
                .unwrap(),
        );
        let actual = arrow::compute::concat_batches(&schema, &batches).unwrap();
        assert_eq!(actual.num_rows(), expected.num_rows());
        assert_eq!(
            distinct_keys(&actual).unwrap(),
            distinct_keys(&expected).unwrap()
        );
        let pairs = |batch: &RecordBatch| -> BTreeMap<String, String> {
            let keys = batch
                .column_by_name(KEY)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let riders = batch
                .column_by_name("rider")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..batch.num_rows())
                .map(|i| (keys.value(i).to_string(), riders.value(i).to_string()))
                .collect()
        };
        assert_eq!(pairs(&actual), pairs(&expected));
    }

    #[tokio::test]
    async fn test_lookup_validates_positions_and_falls_back_after_reordering() {
        let table = Table::new(&QuickstartTripsTable::V8Trips8I3U1D.path_to_mor_avro())
            .await
            .unwrap();
        let lookup = Arc::new(table.prepare_record_lookup(None).await.unwrap());
        let base = create_base_file_reader(&lookup.storage, &BaseFileFormatValue::Parquet).unwrap();
        let mut exercised = false;
        for slice in lookup.slices.values() {
            let Some(path) = slice.base_file_relative_path().unwrap() else {
                continue;
            };
            let batch = base
                .read_data(&path, BaseFileReadOptions::new().with_projection([KEY]))
                .await
                .unwrap();
            let keys = batch
                .column_by_name(KEY)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for (position, key) in keys.iter().enumerate() {
                let key = key.unwrap();
                let locations = lookup
                    .lookup_shard(lookup.shard_for_key(key).unwrap(), &[key])
                    .await
                    .unwrap();
                let Some(location) = locations.first() else {
                    continue;
                };
                if location.file_id != slice.file_id()
                    || location.partition_path != slice.partition_path
                {
                    continue;
                }
                let mut location = location.clone();
                let wanted = HashSet::from([key.to_string()]);
                location.position = Some(position as u64);
                assert!(
                    lookup
                        .validated_selection(&path, std::slice::from_ref(&location), &wanted)
                        .await
                        .unwrap()
                        .is_some()
                );
                if keys.len() > 1 {
                    location.position = Some(((position + 1) % keys.len()) as u64);
                    assert!(
                        lookup
                            .validated_selection(&path, std::slice::from_ref(&location), &wanted)
                            .await
                            .unwrap()
                            .is_none()
                    );
                }
                location.position = Some(u64::MAX);
                assert!(
                    lookup
                        .validated_selection(&path, std::slice::from_ref(&location), &wanted)
                        .await
                        .unwrap()
                        .is_none()
                );
                let result: Vec<_> = lookup
                    .read_locations(vec![location])
                    .unwrap()
                    .try_collect()
                    .await
                    .unwrap();
                assert_eq!(result.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
                exercised = true;
                break;
            }
            if exercised {
                break;
            }
        }
        assert!(
            exercised,
            "fixture must include an indexed base-file record"
        );
    }

    #[tokio::test]
    async fn test_lookup_refreshes_and_keeps_inflight_commit_invisible_after_it_completes() {
        let (directory, path) = tokio::task::spawn_blocking(|| {
            let zip = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../test/data/quickstart_trips_table/mor/avro/v8_trips_8i3u1d.zip");
            let extracted = hudi_test::extract_test_table_fresh(&zip);
            let directory = tempfile::tempdir().unwrap();
            let root = directory.path().join("fixture");
            std::fs::rename(extracted, &root).unwrap();
            let path = root.join("v8_trips_8i3u1d");
            (directory, path)
        })
        .await
        .unwrap();
        let table = Table::new(path.to_str().unwrap()).await.unwrap();
        let timestamp = table.timeline.get_latest_commit_timestamp().unwrap();
        let timeline_dir = path.join(".hoodie/timeline");
        let mut entries = tokio::fs::read_dir(&timeline_dir).await.unwrap();
        let mut completed = None;
        while let Some(entry) = entries.next_entry().await.unwrap() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with(&format!("{timestamp}_")) && name.ends_with(".deltacommit") {
                completed = Some(entry.path());
                break;
            }
        }
        let completed = completed.expect("fixture must end in a delta commit");
        let pending = timeline_dir.join(format!("{timestamp}.deltacommit.inflight"));
        tokio::fs::rename(&completed, &pending).await.unwrap();
        let prior = Table::new(path.to_str().unwrap()).await.unwrap();
        let expected = prior.read(&crate::table::ReadOptions::new()).await.unwrap();
        let mut keys: Vec<String> = expected
            .iter()
            .flat_map(|b| distinct_keys(b).unwrap())
            .collect();
        keys.push("not-present".into());
        let projection = vec![KEY.to_string(), "rider".to_string()];
        let frozen = Arc::new(
            table
                .prepare_record_lookup(Some(&projection))
                .await
                .unwrap(),
        );
        assert_ne!(
            frozen.table.timeline.get_latest_commit_timestamp().unwrap(),
            timestamp
        );
        tokio::fs::rename(&pending, &completed).await.unwrap();
        let mut locations = Vec::new();
        for key in &keys {
            locations.extend(
                frozen
                    .lookup_shard(frozen.shard_for_key(key).unwrap(), &[key])
                    .await
                    .unwrap(),
            );
        }
        let actual: Vec<_> = frozen
            .read_locations(locations)
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let pairs = |batches: &[RecordBatch]| {
            let mut result = BTreeMap::new();
            for batch in batches {
                let keys = batch
                    .column_by_name(KEY)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let riders = batch
                    .column_by_name("rider")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                for row in 0..batch.num_rows() {
                    assert!(
                        result
                            .insert(keys.value(row).to_string(), riders.value(row).to_string())
                            .is_none()
                    );
                }
            }
            result
        };
        assert_eq!(pairs(&actual), pairs(&expected));
        let latest = table.prepare_record_lookup(None).await.unwrap();
        assert_eq!(
            latest.table.timeline.get_latest_commit_timestamp().unwrap(),
            timestamp
        );
        drop(directory);
    }

    #[test]
    fn test_positions_to_selection_sorts_deduplicates_and_checks_overflow() {
        let selection = positions_to_selection(vec![8, 2, 3, 2]).unwrap();
        assert_eq!(
            selection,
            RowSelection::from(vec![
                RowSelector::skip(2),
                RowSelector::select(2),
                RowSelector::skip(4),
                RowSelector::select(1)
            ])
        );
        assert!(positions_to_selection(vec![usize::MAX]).is_err());
    }
}
