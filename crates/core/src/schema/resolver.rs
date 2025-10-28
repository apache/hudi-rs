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
use crate::avro_to_arrow::to_arrow_schema;
use crate::config::table::HudiTableConfig;
use crate::error::{CoreError, Result};
use crate::metadata::commit::HoodieCommitMetadata;
use crate::schema::prepend_meta_fields;
use crate::storage::Storage;
use crate::table::Table;
use apache_avro::schema::Schema as AvroSchema;
use arrow_schema::{Schema, SchemaRef};
use serde_json::{Map, Value};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

/// Resolves the [`arrow_schema::Schema`] for a given Hudi table.
///
/// The resolution process follows these steps:
/// - If the timeline has commit metadata, read the schema field from it.
///   - If the commit metadata has no schema, read the schema from the base file pointed by the first entry in the write-status of the commit metadata.
/// - If the timeline has no commit metadata, read [`HudiTableConfig::CreateSchema`] from `hoodie.properties`.
pub async fn resolve_schema(table: &Table) -> Result<Schema> {
    let timeline = table.get_timeline();
    match timeline.get_latest_commit_metadata().await {
        Ok(metadata) => {
            resolve_schema_from_commit_metadata(&metadata, timeline.storage.clone()).await
        }
        Err(CoreError::TimelineNoCommit) => {
            if let Some(create_schema) = table.hudi_configs.try_get(HudiTableConfig::CreateSchema) {
                let avro_schema_str: String = create_schema.into();
                let arrow_schema = arrow_schema_from_avro_schema_str(&avro_schema_str)?;
                prepend_meta_fields(SchemaRef::new(arrow_schema))
            } else {
                Err(CoreError::SchemaNotFound(
                    "No completed commit, and no create schema for the table.".to_string(),
                ))
            }
        }
        Err(e) => Err(e),
    }
}

/// Resolves the [`apache_avro::schema::Schema`] as a [`String`] for a given Hudi table.
///
/// The resolution process follows these steps:
/// - If the timeline has commit metadata, read the schema field from it.
/// - If the timeline has no commit metadata, read [`HudiTableConfig::CreateSchema`] from `hoodie.properties`.
///
/// ### Note
///
/// - For resolving Avro schema, we don't read the schema from a base file like we do when resolving Arrow schema.
/// - Avro schema does not contain [`MetaField`]s.
pub async fn resolve_avro_schema(table: &Table) -> Result<String> {
    let timeline = table.get_timeline();
    match timeline.get_latest_commit_metadata().await {
        Ok(metadata) => resolve_avro_schema_from_commit_metadata(&metadata),
        Err(CoreError::TimelineNoCommit) => {
            if let Some(create_schema) = table.hudi_configs.try_get(HudiTableConfig::CreateSchema) {
                let create_schema: String = create_schema.into();
                Ok(sanitize_avro_schema_str(&create_schema))
            } else {
                Err(CoreError::SchemaNotFound(
                    "No completed commit, and no create schema for the table.".to_string(),
                ))
            }
        }
        Err(e) => Err(e),
    }
}

pub(crate) async fn resolve_schema_from_commit_metadata(
    commit_metadata: &Map<String, Value>,
    storage: Arc<Storage>,
) -> Result<Schema> {
    let avro_schema_str = match resolve_avro_schema_from_commit_metadata(commit_metadata) {
        Ok(s) => s,
        Err(CoreError::SchemaNotFound(_)) => {
            return resolve_schema_from_base_file(commit_metadata, storage).await
        }
        Err(e) => return Err(e),
    };

    let arrow_schema = arrow_schema_from_avro_schema_str(&avro_schema_str)?;
    prepend_meta_fields(SchemaRef::new(arrow_schema))
}

pub(crate) fn resolve_avro_schema_from_commit_metadata(
    commit_metadata: &Map<String, Value>,
) -> Result<String> {
    if commit_metadata.is_empty() {
        return Err(CoreError::CommitMetadata(
            "Commit metadata is empty.".to_string(),
        ));
    }

    match extract_avro_schema_from_commit_metadata(commit_metadata) {
        Some(schema) => Ok(schema),
        None => Err(CoreError::SchemaNotFound(
            "No schema found in the commit metadata.".to_string(),
        )),
    }
}

async fn resolve_schema_from_base_file(
    commit_metadata: &Map<String, Value>,
    storage: Arc<Storage>,
) -> Result<Schema> {
    let metadata = HoodieCommitMetadata::from_json_map(commit_metadata)?;

    // Get the first write stat from any partition
    let (partition, first_stat) = metadata.iter_write_stats().next().ok_or_else(|| {
        CoreError::CommitMetadata(
            "Failed to resolve the latest schema: no write status in commit metadata".to_string(),
        )
    })?;

    // Try to get the base file path from either 'path' or 'baseFile' field
    if let Some(path) = &first_stat.path {
        if path.ends_with(".parquet") {
            return Ok(storage.get_parquet_file_schema(path).await?);
        }
    }

    // Handle deltacommit case with baseFile
    if let Some(base_file) = &first_stat.base_file {
        let parquet_file_path_buf = PathBuf::from_str(partition)
            .map_err(|e| {
                CoreError::CommitMetadata(format!("Failed to resolve the latest schema: {}", e))
            })?
            .join(base_file);
        let path = parquet_file_path_buf.to_str().ok_or_else(|| {
            CoreError::CommitMetadata(
                "Failed to resolve the latest schema: invalid file path".to_string(),
            )
        })?;
        return Ok(storage.get_parquet_file_schema(path).await?);
    }

    Err(CoreError::CommitMetadata(
        "Failed to resolve the latest schema: no file path found".to_string(),
    ))
}

fn sanitize_avro_schema_str(avro_schema_str: &str) -> String {
    avro_schema_str.trim().replace("\\:", ":")
}

fn arrow_schema_from_avro_schema_str(avro_schema_str: &str) -> Result<Schema> {
    let s = sanitize_avro_schema_str(avro_schema_str);
    let avro_schema = AvroSchema::parse_str(&s)
        .map_err(|e| CoreError::Schema(format!("Failed to parse Avro schema: {}", e)))?;

    to_arrow_schema(&avro_schema)
}

fn extract_avro_schema_from_commit_metadata(
    commit_metadata: &Map<String, Value>,
) -> Option<String> {
    commit_metadata
        .get("extraMetadata")
        .and_then(|v| v.as_object())
        .and_then(|obj| {
            obj.get("schema")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        })
}
