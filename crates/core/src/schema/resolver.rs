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
use crate::config::table::BaseFileFormatValue;
use crate::config::table::HudiTableConfig;
use crate::error::{CoreError, Result};
use crate::file_group::base_file::lance::LanceBaseFileReader;
use crate::file_group::base_file::parquet::ParquetBaseFileReader;
use crate::metadata::commit::HoodieCommitMetadata;
use crate::schema::{prepend_meta_fields, prepend_meta_fields_to_avro_schema_str};
use crate::storage::Storage;
use crate::table::Table;
use apache_avro::schema::Schema as AvroSchema;
use arrow_schema::{Schema, SchemaRef};
use serde_json::{Map, Value};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

/// Resolves the data [`arrow_schema::Schema`] for a given Hudi table, without Hudi meta fields.
///
/// The resolution process follows these steps:
/// 1. Try to get the schema from the timeline (commit metadata or base file).
/// 2. Fall back to [`HudiTableConfig::CreateSchema`] from `hoodie.properties`.
pub async fn resolve_data_schema(table: &Table) -> Result<Schema> {
    match table.get_timeline().get_latest_schema().await {
        Ok(schema) => Ok(schema),
        Err(CoreError::TimelineNoCommit) => resolve_data_schema_from_create_schema(table),
        Err(e) => Err(e),
    }
}

/// Resolves the [`arrow_schema::Schema`] for a given Hudi table, with Hudi meta fields prepended.
pub async fn resolve_schema(table: &Table) -> Result<Schema> {
    let data_schema = resolve_data_schema(table).await?;
    prepend_meta_fields(SchemaRef::new(data_schema))
}

/// Resolves the [`apache_avro::schema::Schema`] as a [`String`] for a given Hudi table.
///
/// The resolution process follows these steps:
/// 1. Try to get the Avro schema from the timeline (commit metadata).
/// 2. Fall back to [`HudiTableConfig::CreateSchema`] from `hoodie.properties`.
///
/// ### Note
///
/// - For resolving Avro schema, we don't read the schema from a base file like we do when resolving Arrow schema.
/// - Avro schema does not contain [`MetaField`]s.
pub async fn resolve_avro_schema(table: &Table) -> Result<String> {
    match table.get_timeline().get_latest_avro_schema().await {
        Ok(schema) => Ok(schema),
        Err(CoreError::TimelineNoCommit) => resolve_avro_schema_from_create_schema(table),
        Err(e) => Err(e),
    }
}

/// Same as [`resolve_avro_schema`] but with Hudi meta fields prepended to the schema.
pub async fn resolve_avro_schema_with_meta_fields(table: &Table) -> Result<String> {
    let avro_schema_str = resolve_avro_schema(table).await?;
    prepend_meta_fields_to_avro_schema_str(&avro_schema_str)
}

fn resolve_data_schema_from_create_schema(table: &Table) -> Result<Schema> {
    if let Some(create_schema) = table.hudi_configs.try_get(HudiTableConfig::CreateSchema)? {
        let avro_schema_str: String = create_schema.into();
        arrow_schema_from_avro_schema_str(&avro_schema_str)
    } else {
        Err(CoreError::SchemaNotFound(
            "No completed commit, and no create schema for the table.".to_string(),
        ))
    }
}

fn resolve_avro_schema_from_create_schema(table: &Table) -> Result<String> {
    if let Some(create_schema) = table.hudi_configs.try_get(HudiTableConfig::CreateSchema)? {
        let create_schema: String = create_schema.into();
        Ok(sanitize_avro_schema_str(&create_schema))
    } else {
        Err(CoreError::SchemaNotFound(
            "No completed commit, and no create schema for the table.".to_string(),
        ))
    }
}

pub(crate) async fn resolve_data_schema_from_commit_metadata(
    commit_metadata: &Map<String, Value>,
    storage: Arc<Storage>,
) -> Result<Schema> {
    let avro_schema_str = match resolve_avro_schema_from_commit_metadata(commit_metadata) {
        Ok(s) => s,
        Err(CoreError::SchemaNotFound(_)) => {
            return resolve_schema_from_base_file(commit_metadata, storage).await;
        }
        Err(e) => return Err(e),
    };

    arrow_schema_from_avro_schema_str(&avro_schema_str)
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
    if let Some(path) = &first_stat.path
        && let Some(schema) = read_schema_by_extension(path, &storage).await?
    {
        return Ok(schema);
    }

    // Handle deltacommit case with baseFile
    if let Some(base_file) = &first_stat.base_file {
        let base_file_path_buf = PathBuf::from_str(partition)
            .map_err(|e| {
                CoreError::CommitMetadata(format!("Failed to resolve the latest schema: {e}"))
            })?
            .join(base_file);
        let path = base_file_path_buf.to_str().ok_or_else(|| {
            CoreError::CommitMetadata(
                "Failed to resolve the latest schema: invalid file path".to_string(),
            )
        })?;
        if let Some(schema) = read_schema_by_extension(path, &storage).await? {
            return Ok(schema);
        }
    }

    Err(CoreError::CommitMetadata(
        "Failed to resolve the latest schema: no file path found".to_string(),
    ))
}

/// Reads the Arrow schema from a base file using the reader matching the
/// path's extension. Returns `Ok(None)` for unrecognized or unsupported
/// extensions so callers can fall through to the next resolution step.
async fn read_schema_by_extension(path: &str, storage: &Arc<Storage>) -> Result<Option<Schema>> {
    match BaseFileFormatValue::from_extension(path) {
        Some(BaseFileFormatValue::Parquet) => Ok(Some(
            ParquetBaseFileReader::new(storage.clone())
                .get_schema(path)
                .await?,
        )),
        Some(BaseFileFormatValue::Lance) => Ok(Some(
            LanceBaseFileReader::new(storage.clone())
                .get_schema(path)
                .await?,
        )),
        _ => Ok(None),
    }
}

pub(crate) fn sanitize_avro_schema_str(avro_schema_str: &str) -> String {
    avro_schema_str.trim().replace("\\:", ":")
}

fn arrow_schema_from_avro_schema_str(avro_schema_str: &str) -> Result<Schema> {
    let s = sanitize_avro_schema_str(avro_schema_str);
    let avro_schema = AvroSchema::parse_str(&s)
        .map_err(|e| CoreError::Schema(format!("Failed to parse Avro schema: {e}")))?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_resolve_avro_schema_from_commit_metadata_with_schema() {
        let metadata = json!({
            "extraMetadata": {
                "schema": r#"{"type":"record","name":"TestRecord","fields":[{"name":"id","type":"int"}]}"#
            }
        })
        .as_object()
        .unwrap()
        .clone();

        let result = resolve_avro_schema_from_commit_metadata(&metadata);
        assert!(result.is_ok());
        let schema = result.unwrap();
        assert!(schema.contains("TestRecord"));
    }

    #[test]
    fn test_resolve_avro_schema_from_commit_metadata_empty() {
        let metadata = Map::new();
        let result = resolve_avro_schema_from_commit_metadata(&metadata);
        assert!(result.is_err());
        assert!(matches!(result, Err(CoreError::CommitMetadata(_))));
    }

    #[test]
    fn test_resolve_avro_schema_from_commit_metadata_no_schema() {
        let metadata = json!({
            "extraMetadata": {
                "other": "value"
            }
        })
        .as_object()
        .unwrap()
        .clone();

        let result = resolve_avro_schema_from_commit_metadata(&metadata);
        assert!(result.is_err());
        assert!(matches!(result, Err(CoreError::SchemaNotFound(_))));
    }

    #[test]
    fn test_sanitize_avro_schema_str() {
        let schema_with_escape = r#"test\:schema"#;
        let sanitized = sanitize_avro_schema_str(schema_with_escape);
        assert_eq!(sanitized, "test:schema");

        let schema_with_whitespace = "  test schema  ";
        let sanitized = sanitize_avro_schema_str(schema_with_whitespace);
        assert_eq!(sanitized, "test schema");
    }

    #[test]
    fn test_extract_avro_schema_from_commit_metadata() {
        let metadata = json!({
            "extraMetadata": {
                "schema": "test_schema"
            }
        })
        .as_object()
        .unwrap()
        .clone();

        let schema = extract_avro_schema_from_commit_metadata(&metadata);
        assert_eq!(schema, Some("test_schema".to_string()));
    }

    #[tokio::test]
    async fn test_resolve_schema_from_base_file_lance_path() {
        use hudi_test::SampleTable;
        use url::Url;

        let table_path = SampleTable::V9LanceNonpartitioned.path_to_cow();
        let base_url = Url::from_directory_path(&table_path).unwrap();
        let storage = Storage::new_with_base_url(base_url).unwrap();

        let lance_file_name = std::fs::read_dir(&table_path)
            .unwrap()
            .filter_map(|e| e.ok())
            .find(|e| e.path().extension().is_some_and(|ext| ext == "lance"))
            .unwrap()
            .file_name()
            .into_string()
            .unwrap();

        // Construct commit metadata with a Lance base-file `path` and no
        // schema in `extraMetadata`, forcing the resolver onto the base-file
        // fallback path.
        let metadata = json!({
            "partitionToWriteStats": {
                "": [{
                    "path": lance_file_name,
                    "partitionPath": ""
                }]
            }
        })
        .as_object()
        .unwrap()
        .clone();

        let schema = resolve_schema_from_base_file(&metadata, storage)
            .await
            .expect("Lance schema should resolve from base file");
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"name"));
    }

    #[test]
    fn test_extract_avro_schema_from_commit_metadata_none() {
        let metadata = json!({
            "other": "value"
        })
        .as_object()
        .unwrap()
        .clone();

        let schema = extract_avro_schema_from_commit_metadata(&metadata);
        assert_eq!(schema, None);
    }
}
