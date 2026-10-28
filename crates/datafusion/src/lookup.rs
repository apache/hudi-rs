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

//! DataFusion routing for global-RLI point lookups.
//!
//! Repartition metadata requests by the Hudi shard ID, then repartition resolved
//! locations by data file group. DataFusion's hash chooses workers, never Hudi shards.

use super::HudiDataSource;
use arrow_array::{RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use datafusion::{
    datasource::memory::MemorySourceConfig,
    execution::context::SessionContext,
    physical_plan::{Partitioning, collect_partitioned, repartition::RepartitionExec},
};
use datafusion_common::{DataFusionError, Result};
use datafusion_physical_expr::{PhysicalExpr, expressions::Column};
use futures::{
    StreamExt, TryStreamExt,
    stream::{self, BoxStream},
};
use hudi_core::{metadata::table::record_index::RecordIndexLocation, table::Table};
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

fn external(error: hudi_core::error::CoreError) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

impl HudiDataSource {
    /// Look up keys at fresh committed state using DataFusion's execution partitions.
    /// Results are unordered; duplicate input keys return one live record.
    /// This does not change the provider's cached schema or ordinary scan snapshot.
    pub async fn lookup_records(
        &self,
        session: &SessionContext,
        keys: &[String],
        projection: Option<&[String]>,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        lookup_records(session, &self.table, keys, projection).await
    }
}

/// Execute a global-RLI lookup with DataFusion hash repartitioning.
///
/// The session's target partition count bounds concurrent metadata/data readers.
/// All workers share one captured committed snapshot. Only keys and locations are
/// collected for routing; data files are read through streaming lookup workers.
///
/// ```ignore
/// use futures::TryStreamExt;
/// use hudi_datafusion::lookup::lookup_records;
/// let keys = vec!["order-123".to_string(), "order-456".to_string()];
/// let mut rows = lookup_records(&session, &table, &keys, None).await?;
/// while let Some(batch) = rows.try_next().await? {
///     // Process matching live records.
/// }
/// ```
pub async fn lookup_records(
    session: &SessionContext,
    table: &Table,
    keys: &[String],
    projection: Option<&[String]>,
) -> Result<BoxStream<'static, Result<RecordBatch>>> {
    if keys.is_empty() {
        return Ok(Box::pin(stream::empty()));
    }
    let lookup = Arc::new(
        table
            .prepare_record_lookup(projection)
            .await
            .map_err(external)?,
    );
    let unique: Vec<&str> = keys
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let shards = unique
        .iter()
        .map(|key| lookup.shard_for_key(key).map(|v| v as u64))
        .collect::<hudi_core::error::Result<Vec<_>>>()
        .map_err(external)?;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("shard", DataType::UInt64, false),
        ])),
        vec![
            Arc::new(StringArray::from(unique)),
            Arc::new(UInt64Array::from(shards)),
        ],
    )?;
    let concurrency = session
        .state()
        .config()
        .target_partitions()
        .max(1)
        .min(keys.len());
    let routed = route(session, batch, &["shard"], concurrency).await?;
    let locations = stream::iter(routed)
        .map(|batches| {
            let lookup = lookup.clone();
            async move {
                let mut groups = BTreeMap::<usize, Vec<String>>::new();
                for batch in batches {
                    let keys = strings(&batch, "key")?;
                    let shards = integers(&batch, "shard")?;
                    for row in 0..batch.num_rows() {
                        groups
                            .entry(shards.value(row) as usize)
                            .or_default()
                            .push(keys.value(row).to_string());
                    }
                }
                let mut locations = Vec::new();
                for (shard, keys) in groups {
                    let keys: Vec<&str> = keys.iter().map(String::as_str).collect();
                    locations.extend(lookup.lookup_shard(shard, &keys).await.map_err(external)?);
                }
                Ok::<_, DataFusionError>(locations)
            }
        })
        .buffer_unordered(concurrency)
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    if locations.is_empty() {
        return Ok(Box::pin(stream::empty()));
    }
    // Carry an index instead of duplicating the decoded location payload in Arrow.
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("partition", DataType::Utf8, false),
            Field::new("file_id", DataType::Utf8, false),
            Field::new("location", DataType::UInt64, false),
        ])),
        vec![
            Arc::new(StringArray::from(
                locations
                    .iter()
                    .map(|l| l.partition_path.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                locations
                    .iter()
                    .map(|l| l.file_id.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from_iter_values(0..locations.len() as u64)),
        ],
    )?;
    let routed = route(session, batch, &["partition", "file_id"], concurrency).await?;
    let mut slots: Vec<Option<RecordIndexLocation>> = locations.into_iter().map(Some).collect();
    let mut streams = Vec::new();
    for batches in routed {
        let mut locations = Vec::new();
        for batch in batches {
            for index in integers(&batch, "location")?.values() {
                let location = slots
                    .get_mut(*index as usize)
                    .and_then(Option::take)
                    .ok_or_else(|| {
                        DataFusionError::Execution("Invalid or duplicate routed location".into())
                    })?;
                locations.push(location);
            }
        }
        streams.push(
            lookup
                .read_locations(locations)
                .map_err(external)?
                .map_err(external),
        );
    }
    Ok(Box::pin(stream::select_all(streams)))
}

async fn route(
    session: &SessionContext,
    batch: RecordBatch,
    columns: &[&str],
    partitions: usize,
) -> Result<Vec<Vec<RecordBatch>>> {
    let schema = batch.schema();
    let expressions = columns
        .iter()
        .map(|name| Ok(Arc::new(Column::new(name, schema.index_of(name)?)) as Arc<dyn PhysicalExpr>))
        .collect::<Result<Vec<_>>>()?;
    let input = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None)?;
    // A logical repartition may be optimized away for small inputs. Routing is
    // an execution requirement here, including when a lookup has only a few keys.
    let plan = Arc::new(RepartitionExec::try_new(
        input,
        Partitioning::Hash(expressions, partitions),
    )?);
    collect_partitioned(plan, session.task_ctx()).await
}

fn strings<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray> {
    batch
        .column_by_name(name)
        .and_then(|a| a.as_any().downcast_ref())
        .ok_or_else(|| DataFusionError::Execution(format!("Invalid routing column {name}")))
}

fn integers<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a UInt64Array> {
    batch
        .column_by_name(name)
        .and_then(|a| a.as_any().downcast_ref())
        .ok_or_else(|| DataFusionError::Execution(format!("Invalid routing column {name}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::context::SessionConfig;
    use hudi_core::metadata::table::record_index::record_index_shard;
    use hudi_test::QuickstartTripsTable;

    #[tokio::test]
    async fn test_routing_keeps_physical_shards_together_without_using_worker_as_shard() {
        let session =
            SessionContext::new_with_config(SessionConfig::new().with_target_partitions(3));
        let keys = ["a", "k", "😀", "polygenelubricants", "customer-1"];
        let shards: Vec<u64> = keys
            .iter()
            .map(|key| record_index_shard(key, 10).unwrap() as u64)
            .collect();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("shard", DataType::UInt64, false),
            ])),
            vec![
                Arc::new(StringArray::from(keys.to_vec())),
                Arc::new(UInt64Array::from(shards.clone())),
            ],
        )
        .unwrap();
        let routed = route(&session, batch, &["shard"], 3).await.unwrap();
        assert_eq!(routed.len(), 3);
        assert!(
            routed
                .iter()
                .filter(|batches| batches.iter().any(|b| b.num_rows() > 0))
                .count()
                > 1
        );
        let mut owners = BTreeMap::new();
        let mut actual = BTreeMap::new();
        for (worker, batches) in routed.iter().enumerate() {
            for batch in batches {
                for row in 0..batch.num_rows() {
                    let shard = integers(batch, "shard").unwrap().value(row);
                    if let Some(previous) = owners.insert(shard, worker) {
                        assert_eq!(previous, worker);
                    }
                    let key = strings(batch, "key").unwrap().value(row).to_string();
                    assert!(actual.insert(key, shard).is_none());
                }
            }
        }
        assert_eq!(
            actual,
            keys.into_iter().map(String::from).zip(shards).collect()
        );
    }

    #[tokio::test]
    async fn test_datafusion_lookup_matches_core_and_deduplicates_keys() {
        let session =
            SessionContext::new_with_config(SessionConfig::new().with_target_partitions(3));
        let table = Table::new(&QuickstartTripsTable::V8Trips8I3U1D.path_to_mor_avro())
            .await
            .unwrap();
        let snapshot = table
            .read(&hudi_core::table::ReadOptions::new())
            .await
            .unwrap();
        let mut keys: Vec<String> = snapshot
            .iter()
            .flat_map(|batch| {
                strings(batch, "_hoodie_record_key")
                    .unwrap()
                    .iter()
                    .flatten()
                    .map(str::to_string)
            })
            .collect();
        keys.push(keys[0].clone());
        keys.push("not-present".into());
        let projection = vec!["_hoodie_record_key".to_string(), "rider".to_string()];
        let actual: Vec<RecordBatch> = lookup_records(&session, &table, &keys, Some(&projection))
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let pairs = |batches: &[RecordBatch]| {
            let mut result = BTreeMap::new();
            for batch in batches {
                for row in 0..batch.num_rows() {
                    let key = strings(batch, "_hoodie_record_key")
                        .unwrap()
                        .value(row)
                        .to_string();
                    let rider = strings(batch, "rider").unwrap().value(row).to_string();
                    assert!(result.insert(key, rider).is_none());
                }
            }
            result
        };
        assert_eq!(pairs(&actual), pairs(&snapshot));
        assert!(actual.iter().all(|b| b.num_columns() == 2));
    }
}
