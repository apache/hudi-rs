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

use crate::Result;
use crate::file_group::base_file::parquet::ParquetBaseFileReader;
use crate::storage::Storage;
use std::sync::Arc;

/// Cached ratios derived from a single base file sample.
/// Used to estimate `byte_size` and `num_records` for all files.
///
/// TODO: this assumes base file format is Parquet. It should support other base file format as the table supports.
#[derive(Clone, Debug)]
pub(crate) struct FileStatsEstimator {
    /// Average row size on disk in bytes (compressed).
    avg_row_size_on_disk: f64,
    /// Ratio of uncompressed to compressed size.
    compression_ratio: f64,
}

impl FileStatsEstimator {
    pub(crate) fn new(avg_row_size_on_disk: f64, compression_ratio: f64) -> Self {
        Self {
            avg_row_size_on_disk,
            compression_ratio,
        }
    }

    /// Build an estimator by reading a single Parquet footer.
    pub(crate) async fn from_parquet_footer(
        storage: &Arc<Storage>,
        relative_path: &str,
    ) -> Result<Self> {
        let parquet_meta = ParquetBaseFileReader::new(storage.clone())
            .get_parquet_metadata(relative_path)
            .await?;
        let on_disk: i64 = parquet_meta
            .row_groups()
            .iter()
            .map(|rg| rg.compressed_size())
            .sum();
        let uncompressed: i64 = parquet_meta
            .row_groups()
            .iter()
            .map(|rg| rg.total_byte_size())
            .sum();
        let num_rows = parquet_meta.file_metadata().num_rows();

        let compression_ratio = if on_disk > 0 {
            uncompressed as f64 / on_disk as f64
        } else {
            1.0
        };
        let avg_row_size_on_disk = if num_rows > 0 {
            on_disk as f64 / num_rows as f64
        } else {
            0.0
        };

        Ok(Self::new(avg_row_size_on_disk, compression_ratio))
    }

    /// Estimate metadata fields from on-disk size.
    pub(crate) fn estimate(&self, size: u64) -> (i64, i64) {
        let byte_size = (size as f64 * self.compression_ratio) as i64;
        let num_records = if self.avg_row_size_on_disk > 0.0 {
            (size as f64 / self.avg_row_size_on_disk) as i64
        } else {
            0
        };
        (byte_size, num_records)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::File;
    use std::path::Path;
    use std::sync::Arc;

    use super::*;
    use crate::config::HudiConfigs;
    use crate::config::table::HudiTableConfig::BasePath;
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use tempfile::tempdir;
    use url::Url;

    fn write_single_row_parquet_file(path: &Path) {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1_i64]))],
        )
        .unwrap();

        let file = File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn write_empty_parquet_file(path: &Path) {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let file = File::create(path).unwrap();
        let writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.close().unwrap();
    }

    async fn new_storage_at(base_url: &Url) -> Arc<Storage> {
        let hudi_configs = Arc::new(HudiConfigs::new([(BasePath.as_ref(), base_url.as_str())]));
        Storage::new(Arc::new(HashMap::new()), hudi_configs).unwrap()
    }

    #[tokio::test]
    async fn from_parquet_footer_handles_success_and_edge_cases() {
        let temp_dir = tempdir().unwrap();
        let base_url = Url::from_directory_path(temp_dir.path()).unwrap();
        let storage = new_storage_at(&base_url).await;

        let sample_path = temp_dir.path().join("sample.parquet");
        write_single_row_parquet_file(&sample_path);
        let estimator = FileStatsEstimator::from_parquet_footer(&storage, "sample.parquet")
            .await
            .unwrap();
        let (_, rows) = estimator.estimate(1024);
        assert!(rows > 0);

        let empty_path = temp_dir.path().join("empty.parquet");
        write_empty_parquet_file(&empty_path);
        let empty_estimator = FileStatsEstimator::from_parquet_footer(&storage, "empty.parquet")
            .await
            .unwrap();
        let (estimated_size, estimated_rows) = empty_estimator.estimate(1024);
        assert_eq!(estimated_size, 1024);
        assert_eq!(estimated_rows, 0);
    }

    #[test]
    fn test_estimate_normal() {
        // 100 bytes/row on disk, 2.5x compression ratio
        let estimator = FileStatsEstimator::new(100.0, 2.5);
        let (byte_size, num_records) = estimator.estimate(1000);
        assert_eq!(byte_size, 2500); // 1000 * 2.5
        assert_eq!(num_records, 10); // 1000 / 100

        // No compression (ratio = 1.0)
        let estimator = FileStatsEstimator::new(200.0, 1.0);
        let (byte_size, num_records) = estimator.estimate(2000);
        assert_eq!(byte_size, 2000); // same as on-disk
        assert_eq!(num_records, 10); // 2000 / 200
    }

    #[test]
    fn test_estimate_edge_cases() {
        // Zero avg_row_size → num_records should be 0
        let estimator = FileStatsEstimator::new(0.0, 2.0);
        let (byte_size, num_records) = estimator.estimate(500);
        assert_eq!(byte_size, 1000);
        assert_eq!(num_records, 0);

        // Zero size file
        let estimator = FileStatsEstimator::new(100.0, 2.0);
        let (byte_size, num_records) = estimator.estimate(0);
        assert_eq!(byte_size, 0);
        assert_eq!(num_records, 0);
    }
}
