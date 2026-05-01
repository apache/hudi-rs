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
 * AS IS BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use std::collections::HashMap;
use std::io::Cursor;
use std::str::FromStr;
use std::sync::atomic::{AtomicI64, Ordering};

use apache_avro::Writer as AvroWriter;
use apache_avro::schema::AvroSchema;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use chrono::Utc;
use object_store::PutPayload;
use object_store::path::Path;
use parquet::arrow::ArrowWriter;

use crate::Result;
use crate::config::table::HudiTableConfig::{
    RecordMergeStrategy, TableVersion, TimelineLayoutVersion, TimelinePath,
};
use crate::error::CoreError;
use crate::merge::RecordMergeStrategyValue;
use crate::metadata::HUDI_METADATA_DIR;
use crate::metadata::commit::{HoodieCommitMetadata, HoodieWriteStat};
use crate::storage::error::StorageError;
use crate::storage::util::join_url_segments;
use crate::table::Table;
use crate::timeline::instant::Instant;

/// Result of a bulk insert write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BulkInsertResult {
    pub instant: String,
    pub commit_relative_path: String,
    pub base_file_path: String,
    pub num_rows: usize,
}

/// Minimal bulk insert writer for append-only tables.
#[derive(Clone, Debug)]
pub struct BulkInsertWriter {
    table: Table,
}

static LAST_EPOCH_MILLIS: AtomicI64 = AtomicI64::new(0);

impl BulkInsertWriter {
    pub fn new(table: &Table) -> Self {
        Self {
            table: table.clone(),
        }
    }

    pub async fn write(&self, batch: RecordBatch) -> Result<BulkInsertResult> {
        self.ensure_append_only()?;
        self.ensure_unpartitioned()?;

        let request_instant = Self::current_timestamp();
        let completion_instant = request_instant.clone();
        let layout_two = self.is_layout_two();
        let instant_name = if layout_two {
            format!("{request_instant}_{completion_instant}.commit")
        } else {
            format!("{request_instant}.commit")
        };
        let instant = Instant::from_str(&instant_name)?;
        let timeline_dir = self.timeline_dir();

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
        );
        let commit_relative_path = instant.relative_path_with_base(&timeline_dir)?;
        if let Err(error) = self
            .write_commit_file(&commit_relative_path, &commit_metadata)
            .await
        {
            let _ = self.delete_file(&base_file_path).await;
            return Err(error);
        }

        Ok(BulkInsertResult {
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
            return Err(CoreError::Unsupported(format!(
                "Bulk insert is only supported for append-only tables. Found merge strategy '{strategy}'"
            )));
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
                "Bulk insert currently supports only unpartitioned append-only tables".to_string(),
            ));
        }

        Ok(())
    }

    fn is_layout_two(&self) -> bool {
        let table_version: isize = self.table.hudi_configs.get_or_default(TableVersion).into();
        let layout_version: isize = self
            .table
            .hudi_configs
            .get_or_default(TimelineLayoutVersion)
            .into();
        table_version >= 8 && layout_version == 2
    }

    fn timeline_dir(&self) -> String {
        if self.is_layout_two() {
            let timeline_path: String = self.table.hudi_configs.get_or_default(TimelinePath).into();
            format!("{HUDI_METADATA_DIR}/{timeline_path}")
        } else {
            HUDI_METADATA_DIR.to_string()
        }
    }

    fn current_timestamp() -> String {
        let now = Utc::now().timestamp_millis();

        loop {
            let last = LAST_EPOCH_MILLIS.load(Ordering::Relaxed);
            let next = if now <= last { last + 1 } else { now };

            if LAST_EPOCH_MILLIS
                .compare_exchange(last, next, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                return format!("{next:017}");
            }
        }
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

    async fn delete_file(&self, relative_path: &str) -> Result<()> {
        let storage = self.table.file_system_view.storage.clone();
        let object_url = join_url_segments(&storage.base_url, &[relative_path])?;
        let object_path = Path::from_url_path(object_url.path()).map_err(StorageError::from)?;
        storage
            .object_store
            .delete(&object_path)
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
            partition_to_replace_file_ids: None,
            compacted: Some(false),
            extra_metadata: None,
        }
    }

    async fn write_commit_file(
        &self,
        commit_relative_path: &str,
        metadata: &HoodieCommitMetadata,
    ) -> Result<()> {
        let bytes = if self.is_layout_two() {
            let schema = HoodieCommitMetadata::get_schema();
            let mut writer = AvroWriter::new(&schema, Vec::new());
            writer.append_ser(metadata)?;
            writer.flush()?;
            writer.into_inner()?
        } else {
            serde_json::to_vec(metadata).map_err(|e| {
                CoreError::CommitMetadata(format!("Failed to serialize commit metadata: {e}"))
            })?
        };

        self.put_file(commit_relative_path, bytes).await
    }
}
