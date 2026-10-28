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

use std::collections::HashMap;
use std::io::Cursor;
use std::str::FromStr;

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStoreExt, PutPayload};
use parquet::arrow::ArrowWriter;

use crate::Result;
use crate::config::table::HudiTableConfig::{
    MetadataTablePartitions, PopulatesMetaFields, RecordMergeStrategy,
};
use crate::error::CoreError;
use crate::merge::RecordMergeStrategyValue;
use crate::metadata::HUDI_METADATA_DIR;
use crate::metadata::commit::{HoodieCommitMetadata, HoodieWriteStat};
use crate::storage::error::StorageError;
use crate::storage::util::join_url_segments;
use crate::table::Table;
use crate::timeline::instant::Instant;
use crate::write::append::arrow_schema_to_avro_json;

/// Result of an append write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendResult {
    pub instant: String,
    pub commit_relative_path: String,
    pub base_file_path: String,
    pub num_rows: usize,
}

/// Minimal writer for append-only tables below table version 8 (timeline layout v1).
#[derive(Clone, Debug)]
pub(crate) struct AppendOnlyWriter {
    table: Table,
}

impl AppendOnlyWriter {
    pub(crate) fn new(table: &Table) -> Self {
        Self {
            table: table.clone(),
        }
    }

    pub(crate) async fn write(&self, batch: RecordBatch) -> Result<AppendResult> {
        self.ensure_append_only()?;
        self.ensure_unpartitioned()?;
        self.ensure_no_meta_fields()?;
        self.ensure_no_metadata_table().await?;
        let schema_json = arrow_schema_to_avro_json(batch.schema().as_ref())?;

        crate::write::set_commit_timezone(&crate::write::metadata::timeline_timezone(
            &self.table.hudi_configs,
        ));
        let request_instant = crate::write::append::generate_instant_time().await;
        let instant = Instant::from_str(&format!("{request_instant}.commit"))?;

        let file_id = format!("bulk-insert-{request_instant}");
        let write_token = "0-0-0";
        let base_file_name = format!("{file_id}_{write_token}_{request_instant}.parquet");
        let base_file_path = base_file_name.clone();

        let file_bytes = self.write_parquet_bytes(&batch)?;
        let file_size = file_bytes.len() as i64;

        self.put_file(&base_file_path, file_bytes).await?;

        let commit_metadata = self.build_commit_metadata(
            &file_id,
            &base_file_path,
            batch.num_rows() as i64,
            file_size,
            schema_json,
        );
        let commit_relative_path = instant.relative_path_with_base(HUDI_METADATA_DIR)?;
        if let Err(error) = self
            .write_commit_file(&commit_relative_path, &commit_metadata)
            .await
        {
            // An errored PUT may still have stored the commit, which needs the
            // base file. Without the commit the file must go: no requested
            // instant marks it pending, so it reads as committed once a later
            // commit starts the timeline after this instant.
            let storage = &self.table.file_system_view.storage;
            if let Ok(false) = storage.exists(&commit_relative_path).await {
                let _ = storage.delete_file(&base_file_path).await;
            }
            return Err(error);
        }

        Ok(AppendResult {
            instant: request_instant,
            commit_relative_path,
            base_file_path,
            num_rows: batch.num_rows(),
        })
    }

    fn ensure_append_only(&self) -> Result<()> {
        let strategy: String = self
            .table
            .hudi_configs
            .get_or_default(RecordMergeStrategy)
            .into();

        if RecordMergeStrategyValue::from_str(&strategy)? != RecordMergeStrategyValue::AppendOnly {
            return Err(CoreError::Unsupported(
                "append_only requires hoodie.record.merge.mode=append_only".to_string(),
            ));
        }

        Ok(())
    }

    fn ensure_unpartitioned(&self) -> Result<()> {
        let partition_fields: Vec<String> = self
            .table
            .hudi_configs
            .get_or_default(crate::config::table::HudiTableConfig::PartitionFields)
            .into();

        if !partition_fields.is_empty() {
            return Err(CoreError::Unsupported(
                "append_only currently supports only unpartitioned append-only tables".to_string(),
            ));
        }

        Ok(())
    }

    /// The written base file carries only the batch's columns, so a table that
    /// populates the `_hoodie_*` meta fields would get a file without them.
    fn ensure_no_meta_fields(&self) -> Result<()> {
        let populates_meta_fields: bool = self
            .table
            .hudi_configs
            .get_or_default(PopulatesMetaFields)
            .into();
        if populates_meta_fields {
            return Err(CoreError::Unsupported(
                "append_only currently supports only tables with hoodie.populate.meta.fields=false"
                    .to_string(),
            ));
        }
        Ok(())
    }

    /// The metadata table is not updated, so its file listings would miss the
    /// new base file.
    async fn ensure_no_metadata_table(&self) -> Result<()> {
        let partitions: Vec<String> = self
            .table
            .hudi_configs
            .get_or_default(MetadataTablePartitions)
            .into();
        let has_metadata_dir = self
            .table
            .file_system_view
            .storage
            .list_dirs(Some(HUDI_METADATA_DIR))
            .await?
            .iter()
            .any(|dir| dir == "metadata");
        if partitions.iter().any(|p| !p.is_empty()) || has_metadata_dir {
            return Err(CoreError::Unsupported(
                "append_only currently supports only tables without a metadata table".to_string(),
            ));
        }
        Ok(())
    }

    fn write_parquet_bytes(&self, batch: &RecordBatch) -> Result<Vec<u8>> {
        let cursor = Cursor::new(Vec::new());
        let mut writer = ArrowWriter::try_new(cursor, batch.schema(), None)?;
        writer.write(batch)?;
        let cursor = writer.into_inner()?;
        Ok(cursor.into_inner())
    }

    async fn put_file(&self, relative_path: &str, bytes: Vec<u8>) -> Result<()> {
        let storage = self.table.file_system_view.storage.clone();
        let object_url = join_url_segments(&storage.base_url, &[relative_path])?;
        let object_path = Path::from_url_path(object_url.path()).map_err(StorageError::from)?;
        storage
            .object_store
            .put(&object_path, PutPayload::from(Bytes::from(bytes)))
            .await
            .map_err(StorageError::from)?;
        Ok(())
    }

    fn build_commit_metadata(
        &self,
        file_id: &str,
        base_file_path: &str,
        num_rows: i64,
        file_size: i64,
        schema_json: String,
    ) -> HoodieCommitMetadata {
        let write_stat = HoodieWriteStat {
            file_id: Some(file_id.to_string()),
            path: Some(base_file_path.to_string()),
            base_file: None,
            log_files: None,
            prev_commit: None,
            num_writes: Some(num_rows),
            num_deletes: Some(0),
            num_update_writes: Some(0),
            num_inserts: Some(num_rows),
            total_write_bytes: Some(file_size),
            total_write_errors: Some(0),
            partition_path: Some(String::new()),
            total_log_records: None,
            total_log_files: None,
            total_updated_records_compacted: None,
            total_log_blocks: None,
            total_corrupt_log_block: None,
            total_rollback_blocks: None,
            file_size_in_bytes: Some(file_size),
            log_version: None,
            log_offset: None,
            prev_base_file: None,
            min_event_time: None,
            max_event_time: None,
            total_log_files_compacted: None,
            total_log_read_time_ms: None,
            total_log_size_compacted: None,
            temp_path: None,
            num_updates: Some(0),
        };

        HoodieCommitMetadata {
            version: Some(1),
            operation_type: Some("BULK_INSERT".to_string()),
            partition_to_write_stats: Some(HashMap::from([(String::new(), vec![write_stat])])),
            compacted: Some(false),
            // Spark reads a null extraMetadata as a null map and fails on it.
            extra_metadata: Some(HashMap::from([("schema".to_string(), schema_json)])),
        }
    }

    async fn write_commit_file(
        &self,
        commit_relative_path: &str,
        metadata: &HoodieCommitMetadata,
    ) -> Result<()> {
        self.put_file(commit_relative_path, metadata.to_json_bytes()?)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int32Array;
    use arrow_schema::{DataType, Field, Schema};

    use super::*;
    use crate::storage::Storage;
    use crate::storage::fault::{FaultyPutStore, PutFault};
    use crate::table::ReadOptions;

    async fn version_6_table(base_dir: &std::path::Path) -> Table {
        let hoodie_dir = base_dir.join(".hoodie");
        std::fs::create_dir_all(&hoodie_dir).unwrap();
        let properties = [
            "hoodie.table.name=t",
            "hoodie.table.type=COPY_ON_WRITE",
            "hoodie.table.version=6",
            "hoodie.timeline.layout.version=1",
            "hoodie.populate.meta.fields=false",
            "hoodie.base.file.format=parquet",
        ]
        .join("\n");
        std::fs::write(hoodie_dir.join("hoodie.properties"), properties).unwrap();
        Table::new(base_dir.to_str().unwrap()).await.unwrap()
    }

    async fn row_count(base_uri: &str) -> usize {
        Table::new(base_uri)
            .await
            .unwrap()
            .read(&ReadOptions::new())
            .await
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum()
    }

    /// A commit PUT that errors may still have stored the commit, so the base
    /// file it references must survive the error.
    #[tokio::test]
    async fn test_failed_commit_put_keeps_written_base_file() {
        for (fault, rows_after_error) in [(PutFault::LandedThenError, 2), (PutFault::NotLanded, 0)]
        {
            let dir = tempfile::tempdir().unwrap();
            let uri = dir.path().to_str().unwrap();
            let mut table = version_6_table(dir.path()).await;
            let storage = table.file_system_view.storage.clone();
            let store =
                FaultyPutStore::new_for_any_put(storage.object_store.clone(), fault, |path| {
                    path.as_ref().ends_with(".commit")
                });
            table.file_system_view.storage = Storage::new_with_object_store(
                storage.base_url.as_ref().clone(),
                store.clone(),
                storage.hudi_configs.clone(),
            );

            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
                vec![Arc::new(Int32Array::from(vec![1, 2]))],
            )
            .unwrap();
            let result = table.append_only([batch]).await;
            assert!(result.is_err(), "{fault:?}: the faulted PUT must surface");
            assert!(!store.is_armed(), "{fault:?}: the commit PUT was not hit");
            assert_eq!(row_count(uri).await, rows_after_error, "{fault:?}");
        }
    }

    /// A later commit moves the timeline start past the failed instant, so a
    /// base file left without its commit would then read as committed.
    #[tokio::test]
    async fn test_failed_commit_put_then_append_reads_only_committed_rows() {
        for (fault, rows_after_retry) in [(PutFault::LandedThenError, 5), (PutFault::NotLanded, 3)]
        {
            let dir = tempfile::tempdir().unwrap();
            let uri = dir.path().to_str().unwrap();
            let mut table = version_6_table(dir.path()).await;
            let storage = table.file_system_view.storage.clone();
            let store =
                FaultyPutStore::new_for_any_put(storage.object_store.clone(), fault, |path| {
                    path.as_ref().ends_with(".commit")
                });
            table.file_system_view.storage = Storage::new_with_object_store(
                storage.base_url.as_ref().clone(),
                store,
                storage.hudi_configs.clone(),
            );
            let batch = |ids: Vec<i32>| {
                RecordBatch::try_new(
                    Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
                    vec![Arc::new(Int32Array::from(ids))],
                )
                .unwrap()
            };
            assert!(table.append_only([batch(vec![1, 2])]).await.is_err());

            let mut retry = Table::new(uri).await.unwrap();
            retry.append_only([batch(vec![3, 4, 5])]).await.unwrap();
            assert_eq!(row_count(uri).await, rows_after_retry, "{fault:?}");
        }
    }

    /// The commit records the write schema as Avro, so a batch whose schema
    /// Avro cannot express is refused before any file is written.
    #[tokio::test]
    async fn test_schema_without_avro_form_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let mut table = version_6_table(dir.path()).await;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "ts",
                DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
                false,
            )])),
            vec![Arc::new(arrow::array::TimestampNanosecondArray::from(
                vec![1],
            ))],
        )
        .unwrap();
        assert!(table.append_only([batch]).await.is_err());
        let written: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.file_name())
            .filter(|name| name != ".hoodie")
            .collect();
        assert!(written.is_empty(), "unexpected files: {written:?}");
    }
}
