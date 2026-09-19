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
//! Insert bucket assignment with small-file packing (Java `UpsertPartitioner`
//! / `SparkUpsertDeltaCommitPartitioner`).
//!
//! Upsert inserts fill existing small file groups first — base files (COW) or
//! whole file slices at parquet-equivalent size (MOR) below
//! `hoodie.parquet.small.file.limit` — then overflow into new file groups of
//! roughly `hoodie.parquet.max.file.size`, using an average record size
//! estimated from recent commit metadata. `append` intentionally skips all of
//! this (bulk-insert semantics).

use std::collections::HashMap;

use crate::Result;
use crate::file_group::file_slice::FileSlice;
use crate::metadata::commit::HoodieCommitMetadata;
use crate::storage::Storage;
use crate::table::Table;

/// Java `hoodie.parquet.small.file.limit` default (100 MiB).
const DEFAULT_SMALL_FILE_LIMIT: i64 = 104_857_600;
/// Java `hoodie.copyonwrite.insert.split.size` default.
const DEFAULT_INSERT_SPLIT_SIZE: usize = 500_000;
/// Java `hoodie.logfile.to.parquet.compression.ratio` default.
const DEFAULT_LOG_TO_PARQUET_RATIO: f64 = 0.35;
/// How many recent commits to inspect for record-size estimation.
const RECORD_SIZE_ESTIMATION_MAX_COMMITS: usize = 5;

/// Sizing knobs, all optional with Java defaults.
#[derive(Debug, Clone)]
pub(crate) struct SizingConfig {
    pub small_file_limit: i64,
    pub max_file_size: i64,
    pub insert_split_size: usize,
    pub auto_split: bool,
    pub record_size_estimate: i64,
    pub log_to_parquet_ratio: f64,
}

impl SizingConfig {
    pub(crate) fn from_table(table: &Table) -> Self {
        let options = table.hudi_configs.as_options();
        let get_i64 = |key: &str, default: i64| {
            options
                .get(key)
                .and_then(|v| v.parse().ok())
                .unwrap_or(default)
        };
        Self {
            small_file_limit: get_i64("hoodie.parquet.small.file.limit", DEFAULT_SMALL_FILE_LIMIT),
            max_file_size: get_i64(
                "hoodie.parquet.max.file.size",
                crate::write::append::DEFAULT_MAX_FILE_SIZE_BYTES,
            ),
            insert_split_size: options
                .get("hoodie.copyonwrite.insert.split.size")
                .and_then(|v| v.parse().ok())
                .unwrap_or(DEFAULT_INSERT_SPLIT_SIZE),
            auto_split: options
                .get("hoodie.copyonwrite.insert.auto.split")
                .and_then(|v| v.parse().ok())
                .unwrap_or(true),
            record_size_estimate: get_i64(
                "hoodie.copyonwrite.record.size.estimate",
                crate::write::append::DEFAULT_RECORD_SIZE_ESTIMATE,
            ),
            log_to_parquet_ratio: options
                .get("hoodie.logfile.to.parquet.compression.ratio")
                .and_then(|v| v.parse().ok())
                .unwrap_or(DEFAULT_LOG_TO_PARQUET_RATIO),
        }
    }

    /// Records per NEW insert file group (Java `insertRecordsPerBucket`).
    pub(crate) fn insert_records_per_bucket(&self, avg_record_size: i64) -> usize {
        if self.auto_split {
            (self.max_file_size / avg_record_size.max(1)).max(1) as usize
        } else {
            self.insert_split_size.max(1)
        }
    }
}

/// Java `RecordSizeEstimator.averageBytesPerRecord`: scan recent commits
/// newest-first and use the first with enough written bytes to be
/// representative (`totalBytesWritten > smallFileLimit × threshold(1.0)`);
/// fall back to the static estimate.
pub(crate) async fn average_record_size(
    table: &Table,
    storage: &Storage,
    config: &SizingConfig,
) -> i64 {
    let timeline_dir = crate::write::append::timeline_dir(table);
    let Ok(files) = storage.list_files(Some(&timeline_dir)).await else {
        return config.record_size_estimate;
    };
    let mut completed: Vec<String> = files
        .into_iter()
        .map(|f| f.name)
        .filter(|n| {
            n.chars().next().is_some_and(|c| c.is_ascii_digit())
                && n.contains('_')
                && (n.ends_with(".commit") || n.ends_with(".deltacommit"))
        })
        .collect();
    completed.sort();
    for name in completed
        .iter()
        .rev()
        .take(RECORD_SIZE_ESTIMATION_MAX_COMMITS)
    {
        let Ok(bytes) = storage
            .get_file_data(&format!("{timeline_dir}/{name}"))
            .await
        else {
            continue;
        };
        let metadata = if name.ends_with(".commit") || name.ends_with(".deltacommit") {
            match HoodieCommitMetadata::from_avro_bytes(&bytes)
                .or_else(|_| HoodieCommitMetadata::from_json_bytes(&bytes))
            {
                Ok(metadata) => metadata,
                Err(_) => continue,
            }
        } else {
            continue;
        };
        let (mut total_bytes, mut total_records) = (0i64, 0i64);
        for (_, stat) in metadata.iter_write_stats() {
            total_bytes += stat.total_write_bytes.unwrap_or(0);
            total_records += stat.num_writes.unwrap_or(0);
        }
        if total_records > 0 && total_bytes > config.small_file_limit {
            return (total_bytes / total_records).max(1);
        }
    }
    config.record_size_estimate
}

/// An existing small file group inserts can be packed into.
#[derive(Debug, Clone)]
pub(crate) struct SmallFileGroup {
    pub file_id: String,
    /// Parquet-equivalent bytes already in the group.
    pub size_bytes: i64,
}

/// Small file groups in a partition's latest file slices.
///
/// COW measures the base file; MOR measures the whole slice at
/// parquet-equivalent size (base + logs × compression ratio), including
/// log-only slices — Java `SparkUpsertDeltaCommitPartitioner.getSmallFiles`.
pub(crate) fn small_file_groups(
    slices: &[&FileSlice],
    is_mor: bool,
    config: &SizingConfig,
) -> Vec<SmallFileGroup> {
    let mut out = Vec::new();
    for slice in slices {
        let base_size = slice
            .base_file
            .as_ref()
            .and_then(|base_file| base_file.file_metadata.as_ref())
            .map(|m| m.size as i64)
            .unwrap_or(0);
        let size = if is_mor {
            let log_size: i64 = slice
                .log_files
                .iter()
                .filter_map(|l| l.file_metadata.as_ref().map(|m| m.size as i64))
                .sum();
            base_size + (log_size as f64 * config.log_to_parquet_ratio) as i64
        } else {
            base_size
        };
        if size > 0 && size < config.small_file_limit {
            out.push(SmallFileGroup {
                file_id: slice.file_id().to_string(),
                size_bytes: size,
            });
        }
    }
    // Deterministic packing order: fill the smallest groups first.
    out.sort_by_key(|g| g.size_bytes);
    out
}

/// Where one partition's insert rows land.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum InsertBucket {
    /// Pack into an existing small file group.
    Existing { file_id: String },
    /// Create a new file group.
    New,
}

/// Assign `num_inserts` rows of one partition to buckets: small file groups
/// first (capacity `(maxFileSize - currentSize) / avgRecordSize`), then new
/// groups of `insert_records_per_bucket` rows each.
///
/// Returns `(bucket, row_count)` in assignment order.
pub(crate) fn assign_insert_buckets(
    num_inserts: usize,
    small_files: &[SmallFileGroup],
    avg_record_size: i64,
    config: &SizingConfig,
) -> Vec<(InsertBucket, usize)> {
    let mut remaining = num_inserts;
    let mut out = Vec::new();
    for small in small_files {
        if remaining == 0 {
            break;
        }
        let capacity =
            ((config.max_file_size - small.size_bytes) / avg_record_size.max(1)).max(0) as usize;
        if capacity == 0 {
            continue;
        }
        let take = capacity.min(remaining);
        out.push((
            InsertBucket::Existing {
                file_id: small.file_id.clone(),
            },
            take,
        ));
        remaining -= take;
    }
    let per_bucket = config.insert_records_per_bucket(avg_record_size);
    while remaining > 0 {
        let take = per_bucket.min(remaining);
        out.push((InsertBucket::New, take));
        remaining -= take;
    }
    out
}

/// Latest file slices grouped by partition (pre-commit view).
pub(crate) async fn latest_slices_by_partition(
    table: &Table,
) -> Result<HashMap<String, Vec<FileSlice>>> {
    let slices = table
        .get_file_slices(&crate::table::ReadOptions::new())
        .await?;
    let mut by_partition: HashMap<String, Vec<FileSlice>> = HashMap::new();
    for slice in slices {
        by_partition
            .entry(slice.partition_path.clone())
            .or_default()
            .push(slice);
    }
    Ok(by_partition)
}
