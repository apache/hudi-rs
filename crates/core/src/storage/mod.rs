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
//! This module is responsible for interacting with the storage layer.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, SchemaRef};
use async_recursion::async_recursion;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, parse_url_opts};
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask, parquet_to_arrow_schema};
use parquet::file::metadata::ParquetMetaData;
use parquet::schema::types::SchemaDescriptor;
use url::Url;

use crate::config::HudiConfigs;
use crate::config::table::HudiTableConfig;
use crate::statistics::StatisticsContainer;
use crate::storage::error::StorageError::{Creation, InvalidPath};
use crate::storage::error::{Result, StorageError};
use crate::storage::file_metadata::FileMetadata;
use crate::storage::reader::StorageReader;
use crate::storage::util::join_url_segments;

pub mod error;
pub mod file_metadata;
pub mod reader;
pub mod util;

/// Options for reading Parquet files with streaming.
#[derive(Clone, Debug, Default)]
pub struct ParquetReadOptions {
    /// Target batch size (number of rows per batch).
    pub batch_size: Option<usize>,
    /// Top-level column projection by name. Drives `ProjectionMask::roots`
    /// (top-level only; reads every leaf inside any selected struct/list/map).
    pub projection: Option<Vec<String>>,
    /// Nested-aware column projection. When set, takes precedence over
    /// `projection` and drives `ProjectionMask::leaves` against the parquet
    /// leaf-column tree, so unused parquet leaves inside structs are NOT
    /// read from disk.  ENG-40168 path B2 — saves I/O when Spark's nested-
    /// column pruning narrows a struct to a subset of its subfields.
    pub nested_projection: Option<SchemaRef>,
}

impl ParquetReadOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = Some(batch_size);
        self
    }

    /// Sets top-level column projection by column names.
    pub fn with_projection<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.projection = Some(columns.into_iter().map(|s| s.into()).collect());
        self
    }

    /// Sets nested-aware column projection from an Arrow schema.  Causes
    /// `get_parquet_file_stream` to mask parquet at the leaf level, so
    /// e.g. `fare<currency>` is read from disk without `fare.amount`.
    pub fn with_nested_projection(mut self, schema: SchemaRef) -> Self {
        self.nested_projection = Some(schema);
        self
    }
}

/// Returns true if `parquet_path` (relative to a top-level field) selects a
/// leaf that is included in `arrow_field`'s nested type.
///
/// Examples:
/// - `arrow_field = fare: struct<currency>`, `parquet_path = ["currency"]` → true
/// - `arrow_field = fare: struct<currency>`, `parquet_path = ["amount"]`   → false
/// - `arrow_field = fare: struct<currency>`, `parquet_path = []`           → true (whole struct selected)
///
/// For `List`/`LargeList`/`Map` fields we conservatively return `true` for
/// any path inside, since narrowing struct subfields *inside an array element*
/// is not common in this code path and would require matching parquet's
/// "list/element" / "key_value/key|value" path levels.
fn arrow_includes_parquet_leaf(arrow_field: &Field, parquet_path: &[String]) -> bool {
    if parquet_path.is_empty() {
        return true;
    }
    match arrow_field.data_type() {
        DataType::Struct(fields) => {
            let head = &parquet_path[0];
            for f in fields.iter() {
                if f.name() == head {
                    return arrow_includes_parquet_leaf(f, &parquet_path[1..]);
                }
            }
            false
        }
        // Conservative: include any parquet leaf below a List/Map/etc.
        // Spark's nested-column pruning rarely narrows array elements via
        // this path; if/when it does, expand this match.
        _ => true,
    }
}

/// Compute parquet leaf-column indices selected by `requested` (an Arrow
/// schema potentially containing narrowed nested structs).
///
/// Iterates the parquet leaf columns in their declared order and matches each
/// path's first element against a top-level field in `requested`.  If the
/// rest of the path is selected (nested narrowing), the leaf index is kept.
fn compute_leaf_indices(
    parquet_schema: &SchemaDescriptor,
    requested: &arrow_schema::Schema,
) -> Vec<usize> {
    let mut indices = Vec::new();
    for (idx, col) in parquet_schema.columns().iter().enumerate() {
        let parts: Vec<String> = col.path().parts().to_vec();
        if parts.is_empty() {
            continue;
        }
        let top = &parts[0];
        if let Ok(arrow_field) = requested.field_with_name(top) {
            if arrow_includes_parquet_leaf(arrow_field, &parts[1..]) {
                indices.push(idx);
            }
        }
    }
    indices
}

/// A stream of record batches from a Parquet file with its schema.
///
/// # Implementation Note
///
/// This struct uses a `BoxStream` internally, which requires a heap allocation per file.
/// For high-performance scenarios with many small files, this adds minimal overhead since:
/// - Batch processing amortizes the allocation cost (each batch may contain thousands of rows)
/// - The streaming benefit (lazy evaluation, reduced memory) outweighs the Box allocation cost
/// - For typical parquet files (>10MB), the Box overhead (~40-80 bytes) is negligible
pub struct ParquetFileStream {
    schema: SchemaRef,
    stream: BoxStream<'static, std::result::Result<RecordBatch, parquet::errors::ParquetError>>,
}

impl ParquetFileStream {
    /// Returns the Arrow schema of the Parquet file.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Consumes self and returns the inner stream.
    pub fn into_stream(
        self,
    ) -> BoxStream<'static, std::result::Result<RecordBatch, parquet::errors::ParquetError>> {
        self.stream
    }
}

impl futures::Stream for ParquetFileStream {
    type Item = std::result::Result<RecordBatch, parquet::errors::ParquetError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.stream.as_mut().poll_next(cx)
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct Storage {
    pub(crate) base_url: Arc<Url>,
    pub(crate) object_store: Arc<dyn ObjectStore>,
    pub(crate) options: Arc<HashMap<String, String>>,
    pub(crate) hudi_configs: Arc<HudiConfigs>,
}

impl Storage {
    pub const CLOUD_STORAGE_PREFIXES: [&'static str; 3] = ["AWS_", "AZURE_", "GOOGLE_"];

    pub fn new(
        options: Arc<HashMap<String, String>>,
        hudi_configs: Arc<HudiConfigs>,
    ) -> Result<Arc<Storage>> {
        let base_url = match hudi_configs.try_get(HudiTableConfig::BasePath) {
            Some(v) => v.to_url()?,
            None => {
                return Err(Creation(format!(
                    "{} is required.",
                    HudiTableConfig::BasePath.as_ref()
                )));
            }
        };

        match parse_url_opts(&base_url, options.as_ref()) {
            Ok((object_store, _)) => Ok(Arc::new(Storage {
                base_url: Arc::new(base_url),
                object_store: Arc::new(object_store),
                options,
                hudi_configs,
            })),
            Err(e) => Err(Creation(format!("Failed to create storage: {e}"))),
        }
    }

    #[cfg(test)]
    pub fn new_with_base_url(base_url: Url) -> Result<Arc<Storage>> {
        let mut hudi_options = HashMap::new();
        hudi_options.insert(
            HudiTableConfig::BasePath.as_ref().to_string(),
            base_url.as_str().to_string(),
        );
        Self::new(
            Arc::new(HashMap::new()),
            Arc::new(HudiConfigs::new(hudi_options)),
        )
    }

    #[cfg(feature = "datafusion")]
    pub fn register_object_store(
        &self,
        runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
    ) {
        runtime_env.register_object_store(self.base_url.as_ref(), self.object_store.clone());
    }

    #[cfg(test)]
    /// Get basic file metadata (name, size) without loading the file content.
    async fn get_file_metadata_not_populated(&self, relative_path: &str) -> Result<FileMetadata> {
        let obj_url = join_url_segments(&self.base_url, &[relative_path])?;
        let obj_path = ObjPath::from_url_path(obj_url.path())?;
        let meta = self.object_store.head(&obj_path).await?;
        let name = meta.location.filename().ok_or_else(|| {
            InvalidPath(format!("Failed to get file name from: {:?}", meta.location))
        })?;
        Ok(FileMetadata::new(name.to_string(), meta.size))
    }

    /// Get full file metadata for a Parquet file, including record counts from Parquet metadata.
    pub async fn get_file_metadata(&self, relative_path: &str) -> Result<FileMetadata> {
        let obj_url = join_url_segments(&self.base_url, &[relative_path])?;
        let obj_path = ObjPath::from_url_path(obj_url.path())?;
        let obj_store = self.object_store.clone();
        let obj_meta = obj_store.head(&obj_path).await?;
        let location = obj_meta.location.clone();
        let file_name = location
            .filename()
            .ok_or_else(|| InvalidPath(format!("Failed to get file name from: {:?}", &obj_meta)))?;
        let size = obj_meta.size;
        let reader = ParquetObjectReader::new(obj_store, obj_path).with_file_size(size);
        let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
        let parquet_meta = builder.metadata().clone();
        let num_records = parquet_meta.file_metadata().num_rows();
        let size_bytes = parquet_meta
            .row_groups()
            .iter()
            .map(|rg| rg.total_byte_size())
            .sum::<i64>();
        Ok(FileMetadata {
            name: file_name.to_string(),
            size,
            byte_size: size_bytes,
            num_records,
            fully_populated: true,
        })
    }

    pub async fn get_parquet_file_metadata(&self, relative_path: &str) -> Result<ParquetMetaData> {
        let obj_url = join_url_segments(&self.base_url, &[relative_path])?;
        let obj_path = ObjPath::from_url_path(obj_url.path())?;
        let obj_store = self.object_store.clone();
        let meta = obj_store.head(&obj_path).await?;
        let reader = ParquetObjectReader::new(obj_store, obj_path).with_file_size(meta.size);
        let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
        Ok(builder.metadata().as_ref().clone())
    }

    pub async fn get_parquet_file_schema(
        &self,
        relative_path: &str,
    ) -> Result<arrow_schema::Schema> {
        let parquet_meta = self.get_parquet_file_metadata(relative_path).await?;
        Ok(parquet_to_arrow_schema(
            parquet_meta.file_metadata().schema_descr(),
            None,
        )?)
    }

    /// Get column statistics for a Parquet file.
    ///
    /// # Arguments
    /// * `relative_path` - Relative path to the Parquet file
    /// * `schema` - Arrow schema to use for extracting statistics
    pub async fn get_parquet_column_stats(
        &self,
        relative_path: &str,
        schema: &arrow_schema::Schema,
    ) -> Result<StatisticsContainer> {
        let parquet_meta = self.get_parquet_file_metadata(relative_path).await?;
        Ok(StatisticsContainer::from_parquet_metadata(
            &parquet_meta,
            schema,
        ))
    }

    pub async fn get_file_data(&self, relative_path: &str) -> Result<Bytes> {
        let obj_url = join_url_segments(&self.base_url, &[relative_path])?;
        let obj_path = ObjPath::from_url_path(obj_url.path())?;
        let result = self.object_store.get(&obj_path).await?;
        let bytes = result.bytes().await?;
        Ok(bytes)
    }

    pub async fn get_file_data_from_absolute_path(&self, absolute_path: &str) -> Result<Bytes> {
        let obj_path = ObjPath::from_absolute_path(PathBuf::from(absolute_path))?;
        let result = self.object_store.get(&obj_path).await?;
        let bytes = result.bytes().await?;
        Ok(bytes)
    }

    pub async fn get_parquet_file_data(&self, relative_path: &str) -> Result<RecordBatch> {
        let obj_url = join_url_segments(&self.base_url, &[relative_path])?;
        let obj_path = ObjPath::from_url_path(obj_url.path())?;
        let obj_store = self.object_store.clone();
        let meta = obj_store.head(&obj_path).await?;

        // read parquet
        let reader = ParquetObjectReader::new(obj_store, obj_path).with_file_size(meta.size);
        let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
        let schema = builder.schema().clone();

        let all_cols: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        log::debug!(
            "Parquet full read (non-streaming) for '{relative_path}': \
             reading all {n} cols: [{cols}]",
            n = all_cols.len(),
            cols = all_cols.join(", "),
        );

        let mut stream = builder.build()?;
        let mut batches = Vec::new();

        while let Some(r) = stream.next().await {
            batches.push(r?)
        }

        if batches.is_empty() {
            return Ok(RecordBatch::new_empty(schema.clone()));
        }

        Ok(concat_batches(&schema, &batches)?)
    }

    /// Read a Parquet file with column projection (non-streaming).
    ///
    /// Like [`get_parquet_file_data`], but only reads the columns present in
    /// `projection_schema`. Reuses the projection infrastructure from
    /// [`get_parquet_file_stream`].
    ///
    /// Mirrors Java's `readerContext.getFileRecordIterator(pathInfo, start, len,
    /// tableSchema, requiredSchema, storage)` where `requiredSchema` drives
    /// column pruning.
    pub async fn get_parquet_file_data_projected(
        &self,
        relative_path: &str,
        projection_schema: &arrow_schema::SchemaRef,
    ) -> Result<RecordBatch> {
        log::debug!(
            "Parquet projected read (non-streaming) for '{relative_path}': \
             projecting {n} top-level cols: [{cols}]",
            n = projection_schema.fields().len(),
            cols = projection_schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>()
                .join(", "),
        );

        // Use nested-aware projection so any narrowed struct subfields
        // (e.g. `fare<currency>` from a wider `fare<amount,currency>`) are
        // honored at the parquet leaf-column level — saves disk I/O for
        // unused subfields. Top-level-only narrowing still works because the
        // leaf walk picks every leaf below an unnarrowed top-level field.
        // ENG-40168 path B2.
        let options =
            ParquetReadOptions::new().with_nested_projection(projection_schema.clone());
        let mut stream = self.get_parquet_file_stream(relative_path, options).await?;
        let schema = stream.schema.clone();

        let mut batches = Vec::new();
        while let Some(r) = stream.stream.next().await {
            batches.push(r?);
        }

        if batches.is_empty() {
            return Ok(RecordBatch::new_empty(schema));
        }

        Ok(concat_batches(&schema, &batches)?)
    }

    /// Get a streaming reader for a Parquet file.
    ///
    /// Returns a [ParquetFileStream] that yields record batches as they are read,
    /// without loading all data into memory at once.
    ///
    /// # Arguments
    /// * `relative_path` - The relative path to the Parquet file.
    /// * `options` - Options for reading the Parquet file (batch size, projection).
    pub async fn get_parquet_file_stream(
        &self,
        relative_path: &str,
        options: ParquetReadOptions,
    ) -> Result<ParquetFileStream> {
        let obj_url = join_url_segments(&self.base_url, &[relative_path])?;
        let obj_path = ObjPath::from_url_path(obj_url.path())?;
        let obj_store = self.object_store.clone();
        let meta = obj_store.head(&obj_path).await?;

        let reader = ParquetObjectReader::new(obj_store, obj_path).with_file_size(meta.size);
        let mut builder = ParquetRecordBatchStreamBuilder::new(reader).await?;

        if let Some(batch_size) = options.batch_size {
            log::debug!(
                "Parquet reader: batch_size={batch_size} for '{relative_path}'"
            );
            builder = builder.with_batch_size(batch_size);
        }

        // Handle projection: convert column names to indices using builder's schema.
        // Build `projected_schema` that reflects the actual columns in the stream.
        let projected_schema: SchemaRef;
        // ENG-40168 leaf-level parquet projection: opt-in via
        // `HUDI_B2_ENABLED=1` (or `true`). Default is OFF because the
        // current implementation regresses TestMORDataSource by ~6x at
        // N=50000 — see investigation subtask under ENG-40168. When OFF,
        // fall back to the top-level `ProjectionMask::roots` path even
        // when a nested schema was supplied (top-level names are derived
        // from the nested schema's top-level fields).
        let b2_enabled: bool = std::env::var("HUDI_B2_ENABLED")
            .map(|v| v == "1" || v.to_ascii_lowercase() == "true")
            .unwrap_or(false);
        let mut effective_options = options.clone();
        if !b2_enabled {
            if let Some(ref nested_schema) = effective_options.nested_projection {
                let top_level_names: Vec<String> = nested_schema
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect();
                effective_options.nested_projection = None;
                effective_options.projection = Some(top_level_names);
            }
        }
        if let Some(ref nested_schema) = effective_options.nested_projection {
            // ENG-40168 path B2 — leaf-level parquet projection.
            //
            // `nested_schema` is the requested Arrow schema (possibly with
            // narrowed structs). We compute the parquet *leaf* indices it
            // selects, then mask via `ProjectionMask::leaves` so the parquet
            // reader only fetches/decodes those leaves.
            let leaf_indices =
                compute_leaf_indices(builder.parquet_schema(), nested_schema.as_ref());
            log::debug!(
                "Parquet leaf-projection for '{relative_path}': {n_leaf} parquet leaves selected by nested arrow schema [{cols}]",
                n_leaf = leaf_indices.len(),
                cols = nested_schema
                    .fields()
                    .iter()
                    .map(|f| f.name().as_str())
                    .collect::<Vec<_>>()
                    .join(", "),
            );
            let projection_mask =
                ProjectionMask::leaves(builder.parquet_schema(), leaf_indices);
            builder = builder.with_projection(projection_mask);
            // Don't set `projected_schema` here — the post-projection
            // schema will be read off the *built* stream below, in parquet
            // physical-leaf order (which is what the stream actually
            // produces). Using `nested_schema` as-is is wrong because the
            // caller's field order is unrelated to parquet's declared
            // order. Downstream `ProjectionConverter` (B1) reorders by
            // name to match the caller's expected output. Set a placeholder
            // here that will be overwritten before constructing the stream.
            projected_schema = Arc::new(arrow_schema::Schema::empty());
        } else if let Some(ref column_names) = effective_options.projection {
            let arrow_schema = builder.schema();
            let file_cols: Vec<&str> = arrow_schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect();
            let pruned_cols: Vec<&str> = file_cols
                .iter()
                .copied()
                .filter(|&c| !column_names.iter().any(|n| n == c))
                .collect();
            log::debug!(
                "Parquet column projection for '{relative_path}': \
                 file schema [{file_schema}] ({n_file} cols), \
                 projecting [{proj}] ({n_proj} cols), \
                 pruning [{pruned}] ({n_pruned} cols)",
                file_schema = file_cols.join(", "),
                n_file = file_cols.len(),
                proj = column_names.join(", "),
                n_proj = column_names.len(),
                pruned = pruned_cols.join(", "),
                n_pruned = pruned_cols.len(),
            );
            let projection: Vec<usize> = column_names
                .iter()
                .map(|name| {
                    arrow_schema.index_of(name).map_err(|_| {
                        let available = arrow_schema
                            .fields()
                            .iter()
                            .map(|f| f.name().as_str())
                            .collect::<Vec<_>>()
                            .join(", ");
                        StorageError::InvalidColumn(format!(
                            "Column '{name}' not found in parquet file schema. Available columns: [{available}]"
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            // Build the projected schema in parquet root column order (ascending index).
            // ProjectionMask::roots produces columns sorted by their parquet index,
            // so the schema must match that order.
            let mut sorted_projection = projection.clone();
            sorted_projection.sort();
            let projected_fields: Vec<std::sync::Arc<arrow_schema::Field>> = sorted_projection
                .iter()
                .map(|&idx| arrow_schema.field(idx).clone().into())
                .collect();
            projected_schema = std::sync::Arc::new(arrow_schema::Schema::new(projected_fields));

            let projection_mask = parquet::arrow::ProjectionMask::roots(
                builder.parquet_schema(),
                projection.iter().copied(),
            );
            builder = builder.with_projection(projection_mask);
        } else {
            let arrow_schema = builder.schema();
            let all_cols: Vec<&str> = arrow_schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect();
            log::debug!(
                "Parquet column projection for '{relative_path}': no projection, \
                 reading all {n} cols: [{cols}]",
                n = all_cols.len(),
                cols = all_cols.join(", "),
            );
            projected_schema = arrow_schema.clone();
        }

        let stream = builder.build()?;

        // For the leaf-projection path the stream itself is the source of
        // truth for the post-projection schema (parquet emits leaves in
        // declared file order, not the caller's requested order). Use the
        // stream's schema directly when the caller asked for nested
        // projection; otherwise keep the schema we computed above.
        let final_schema = if effective_options.nested_projection.is_some() {
            stream.schema().clone()
        } else {
            projected_schema
        };

        Ok(ParquetFileStream {
            schema: final_schema,
            stream: Box::pin(stream),
        })
    }

    pub async fn get_storage_reader(&self, relative_path: &str) -> Result<StorageReader> {
        let obj_url = join_url_segments(&self.base_url, &[relative_path])?;
        let obj_path = ObjPath::from_url_path(obj_url.path())?;
        let obj_store = self.object_store.clone();
        let obj_meta = obj_store.head(&obj_path).await?;
        StorageReader::new(obj_store, obj_meta)
            .await
            .map_err(StorageError::ReaderError)
    }

    pub async fn list_dirs(&self, subdir: Option<&str>) -> Result<Vec<String>> {
        let dir_paths = self.list_dirs_as_obj_paths(subdir).await?;
        let mut dirs = Vec::new();
        for dir in dir_paths {
            dirs.push(
                dir.filename()
                    .ok_or_else(|| InvalidPath(format!("Failed to get file name from: {dir:?}")))?
                    .to_string(),
            )
        }
        Ok(dirs)
    }

    async fn list_dirs_as_obj_paths(&self, subdir: Option<&str>) -> Result<Vec<ObjPath>> {
        let prefix_url = join_url_segments(&self.base_url, &[subdir.unwrap_or_default()])?;
        let prefix_path = ObjPath::from_url_path(prefix_url.path())?;
        let list_res = self
            .object_store
            .list_with_delimiter(Some(&prefix_path))
            .await?;
        Ok(list_res.common_prefixes)
    }

    pub async fn list_files(&self, subdir: Option<&str>) -> Result<Vec<FileMetadata>> {
        let prefix_url = join_url_segments(&self.base_url, &[subdir.unwrap_or_default()])?;
        let prefix_path = ObjPath::from_url_path(prefix_url.path())?;
        let list_res = self
            .object_store
            .list_with_delimiter(Some(&prefix_path))
            .await?;
        let mut file_metadata = Vec::new();
        for obj_meta in list_res.objects {
            let location = obj_meta.location;
            let name = location
                .filename()
                .ok_or_else(|| InvalidPath(format!("Failed to get file name from {location:?}")))?;

            if name.ends_with(".crc") {
                continue;
            }

            file_metadata.push(FileMetadata::new(name.to_string(), obj_meta.size));
        }
        Ok(file_metadata)
    }
}

/// Get relative paths of leaf directories under a given directory.
///
/// **Example**
/// - /usr/hudi/table_name
/// - /usr/hudi/table_name/.hoodie
/// - /usr/hudi/table_name/dt=2024/month=01/day=01
/// - /usr/hudi/table_name/dt=2025/month=02
///
/// the result is \[".hoodie", "dt=2024/mont=01/day=01", "dt=2025/month=02"\]
#[async_recursion]
pub async fn get_leaf_dirs(storage: &Storage, subdir: Option<&str>) -> Result<Vec<String>> {
    let mut leaf_dirs = Vec::new();
    let child_dirs = storage.list_dirs(subdir).await?;
    if child_dirs.is_empty() {
        leaf_dirs.push(subdir.unwrap_or_default().to_owned());
    } else {
        for child_dir in child_dirs {
            let mut next_subdir = PathBuf::new();
            if let Some(curr) = subdir {
                next_subdir.push(curr);
            }
            next_subdir.push(child_dir);
            let next_subdir = next_subdir
                .to_str()
                .ok_or_else(|| InvalidPath(format!("Failed to convert path: {next_subdir:?}")))?;
            let curr_leaf_dir = get_leaf_dirs(storage, Some(next_subdir)).await?;
            leaf_dirs.extend(curr_leaf_dir);
        }
    }
    Ok(leaf_dirs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{Field, Fields, Schema};
    use std::collections::HashSet;
    use std::fs::canonicalize;
    use std::path::Path;

    // ── Path B2: arrow_includes_parquet_leaf ────────────────────────────

    #[test]
    fn test_arrow_includes_parquet_leaf_empty_path_selects_whole_field() {
        let f = Field::new("anything", DataType::Utf8, true);
        // Empty path = "the whole field below this point is selected".
        assert!(arrow_includes_parquet_leaf(&f, &[]));
    }

    #[test]
    fn test_arrow_includes_parquet_leaf_struct_subfield_present() {
        let f = Field::new(
            "fare",
            DataType::Struct(Fields::from(vec![
                Field::new("currency", DataType::Utf8, true),
            ])),
            true,
        );
        assert!(arrow_includes_parquet_leaf(&f, &["currency".to_string()]));
    }

    #[test]
    fn test_arrow_includes_parquet_leaf_struct_subfield_pruned() {
        // `fare<currency>` should NOT include the `amount` parquet leaf
        // (this is the ENG-40168 nested-narrowing case).
        let f = Field::new(
            "fare",
            DataType::Struct(Fields::from(vec![
                Field::new("currency", DataType::Utf8, true),
            ])),
            true,
        );
        assert!(!arrow_includes_parquet_leaf(&f, &["amount".to_string()]));
    }

    #[test]
    fn test_arrow_includes_parquet_leaf_struct_path_recurses() {
        // outer<inner<a>>; selecting only `a` within `inner`.
        let inner = Field::new(
            "inner",
            DataType::Struct(Fields::from(vec![Field::new(
                "a",
                DataType::Int64,
                true,
            )])),
            true,
        );
        let outer = Field::new(
            "outer",
            DataType::Struct(Fields::from(vec![inner])),
            true,
        );
        assert!(arrow_includes_parquet_leaf(
            &outer,
            &["inner".to_string(), "a".to_string()]
        ));
        assert!(!arrow_includes_parquet_leaf(
            &outer,
            &["inner".to_string(), "b".to_string()]
        ));
    }

    #[test]
    fn test_arrow_includes_parquet_leaf_list_is_conservative() {
        // For now, anything inside a List is always included (conservative).
        let elem = Field::new(
            "element",
            DataType::Struct(Fields::from(vec![
                Field::new("amount", DataType::Float64, true),
                Field::new("currency", DataType::Utf8, true),
            ])),
            true,
        );
        let list = Field::new(
            "tip_history",
            DataType::List(Arc::new(elem)),
            true,
        );
        // Parquet path inside a list will start with "list" / "element".
        // We don't recurse into List today — return true.
        assert!(arrow_includes_parquet_leaf(
            &list,
            &["list".to_string(), "element".to_string(), "amount".to_string()]
        ));
        assert!(arrow_includes_parquet_leaf(
            &list,
            &["list".to_string(), "element".to_string(), "currency".to_string()]
        ));
    }

    /// Round-trip: `with_nested_projection` populates the field; `with_projection`
    /// does not (and vice versa).  Sanity-check the API surface.
    #[test]
    fn test_parquet_read_options_accessors() {
        let opts1 = ParquetReadOptions::new().with_projection(["a", "b"]);
        assert_eq!(opts1.projection.as_deref(), Some(&["a".to_string(), "b".to_string()][..]));
        assert!(opts1.nested_projection.is_none());

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        let opts2 = ParquetReadOptions::new().with_nested_projection(schema.clone());
        assert!(opts2.projection.is_none());
        assert!(opts2.nested_projection.is_some());
        assert_eq!(
            opts2.nested_projection.as_ref().unwrap().fields().len(),
            1
        );
    }

    // ──────────────────────────────────────────────────────────────────

    #[test]
    fn test_storage_new_error_no_base_path() {
        let options = Arc::new(HashMap::new());
        let hudi_configs = Arc::new(HudiConfigs::empty());
        let result = Storage::new(options, hudi_configs);

        assert!(
            result.is_err(),
            "Should return error when no base path is provided."
        );
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Failed to create storage")
        );
    }

    #[test]
    fn test_storage_new_error_invalid_url() {
        let options = Arc::new(HashMap::new());
        let hudi_configs = Arc::new(HudiConfigs::new([(
            HudiTableConfig::BasePath,
            "http://invalid_url",
        )]));
        let result = Storage::new(options, hudi_configs);

        assert!(
            result.is_err(),
            "Should return error when no base path is invalid."
        );
        assert!(matches!(result.unwrap_err(), Creation(_)));
    }

    #[tokio::test]
    async fn storage_list_dirs() {
        let base_url = Url::from_directory_path(
            canonicalize(Path::new("tests/data/timeline/commits_stub")).unwrap(),
        )
        .unwrap();
        let storage = Storage::new_with_base_url(base_url).unwrap();
        let first_level_dirs: HashSet<String> =
            storage.list_dirs(None).await.unwrap().into_iter().collect();
        assert_eq!(
            first_level_dirs,
            vec![".hoodie", "part1", "part2", "part3"]
                .into_iter()
                .map(String::from)
                .collect()
        );
        let second_level_dirs: Vec<String> = storage.list_dirs(Some("part2")).await.unwrap();
        assert_eq!(second_level_dirs, vec!["part22"]);
        let no_dirs = storage.list_dirs(Some("part1")).await.unwrap();
        assert!(no_dirs.is_empty());
    }

    #[tokio::test]
    async fn storage_list_dirs_as_paths() {
        let base_url = Url::from_directory_path(
            canonicalize(Path::new("tests/data/timeline/commits_stub")).unwrap(),
        )
        .unwrap();
        let storage = Storage::new_with_base_url(base_url).unwrap();
        let first_level_dirs: HashSet<ObjPath> = storage
            .list_dirs_as_obj_paths(None)
            .await
            .unwrap()
            .into_iter()
            .collect();
        let expected_paths: HashSet<ObjPath> = vec![".hoodie", "part1", "part2", "part3"]
            .into_iter()
            .map(|dir| {
                ObjPath::from_url_path(join_url_segments(&storage.base_url, &[dir]).unwrap().path())
                    .unwrap()
            })
            .collect();
        assert_eq!(first_level_dirs, expected_paths);
    }

    #[tokio::test]
    async fn storage_list_files() {
        let base_url = Url::from_directory_path(
            canonicalize(Path::new("tests/data/timeline/commits_stub")).unwrap(),
        )
        .unwrap();
        let storage = Storage::new_with_base_url(base_url).unwrap();
        let file_info_1: Vec<FileMetadata> = storage
            .list_files(None)
            .await
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(file_info_1, vec![FileMetadata::new("a.parquet", 0)]);
        let file_info_2: Vec<FileMetadata> = storage
            .list_files(Some("part1"))
            .await
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(file_info_2, vec![FileMetadata::new("b.parquet", 0)],);
        let file_info_3: Vec<FileMetadata> = storage
            .list_files(Some("part2/part22"))
            .await
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(file_info_3, vec![FileMetadata::new("c.parquet", 0)],);
    }

    #[tokio::test]
    async fn storage_list_files_excludes_crc_files() {
        let base_url = Url::from_directory_path(
            canonicalize(Path::new("tests/data/timeline/commits_stub")).unwrap(),
        )
        .unwrap();
        let storage = Storage::new_with_base_url(base_url).unwrap();

        let files = storage.list_files(None).await.unwrap();

        assert!(!files.iter().any(|f| f.name.ends_with(".crc")));
        assert_eq!(files, vec![FileMetadata::new("a.parquet", 0)]);
    }

    #[tokio::test]
    async fn use_storage_to_get_leaf_dirs() {
        let base_url = Url::from_directory_path(
            canonicalize(Path::new("tests/data/timeline/commits_stub")).unwrap(),
        )
        .unwrap();
        let storage = Storage::new_with_base_url(base_url).unwrap();
        let leaf_dirs = get_leaf_dirs(&storage, None).await.unwrap();
        assert_eq!(
            leaf_dirs,
            vec![".hoodie", "part1", "part2/part22", "part3/part32/part33"]
        );
    }

    #[tokio::test]
    async fn use_storage_to_get_leaf_dirs_for_leaf_dir() {
        let base_url =
            Url::from_directory_path(canonicalize(Path::new("tests/data/leaf_dir")).unwrap())
                .unwrap();
        let storage = Storage::new_with_base_url(base_url).unwrap();
        let leaf_dirs = get_leaf_dirs(&storage, None).await.unwrap();
        assert_eq!(
            leaf_dirs,
            vec![""],
            "Listing a leaf dir should get the relative path to itself."
        );
    }

    #[tokio::test]
    async fn storage_get_file_info() {
        let base_url =
            Url::from_directory_path(canonicalize(Path::new("tests/data")).unwrap()).unwrap();
        let storage = Storage::new_with_base_url(base_url).unwrap();
        let file_metadata = storage
            .get_file_metadata_not_populated("a.parquet")
            .await
            .unwrap();
        assert_eq!(file_metadata.name, "a.parquet");
        assert_eq!(file_metadata.size, 866);
    }

    #[tokio::test]
    async fn storage_get_parquet_file_data() {
        let base_url =
            Url::from_directory_path(canonicalize(Path::new("tests/data")).unwrap()).unwrap();
        let storage = Storage::new_with_base_url(base_url).unwrap();
        let file_data = storage.get_parquet_file_data("a.parquet").await.unwrap();
        assert_eq!(file_data.num_rows(), 5);
    }
}
