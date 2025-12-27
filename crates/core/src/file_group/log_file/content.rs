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
use crate::avro_to_arrow::arrow_array_reader::AvroArrowArrayReader;
use crate::config::HudiConfigs;
use crate::error::CoreError;
use crate::file_group::log_file::avro::AvroDataBlockContentReader;
use crate::file_group::log_file::log_block::{
    BlockMetadataKey, BlockType, LogBlockContent, LogBlockVersion,
};
use crate::file_group::log_file::log_format::LogFormatVersion;
use crate::file_group::record_batches::RecordBatches;
use crate::hfile::{HFileReader, HFileRecord};
use crate::schema::delete::{avro_schema_for_delete_record, avro_schema_for_delete_record_list};
use crate::Result;
use apache_avro::{from_avro_datum, Schema as AvroSchema};
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use std::collections::HashMap;
use std::io::{Read, Seek};
use std::sync::Arc;

#[allow(dead_code)]
pub struct Decoder {
    batch_size: usize,
    hudi_configs: Arc<HudiConfigs>,
}

impl Decoder {
    pub fn new(hudi_configs: Arc<HudiConfigs>) -> Self {
        Self {
            batch_size: 1024,
            hudi_configs,
        }
    }
    pub fn decode_content(
        &self,
        reader: &mut (impl Read + Seek),
        log_format_version: &LogFormatVersion,
        fallback_length: u64,
        block_type: &BlockType,
        header: &HashMap<BlockMetadataKey, String>,
    ) -> Result<LogBlockContent> {
        let content_length = if log_format_version.has_content_length() {
            let mut content_length_buf = [0u8; 8];
            reader.read_exact(&mut content_length_buf)?;
            u64::from_be_bytes(content_length_buf)
        } else {
            fallback_length
        };

        let reader = reader.by_ref().take(content_length);
        match block_type {
            BlockType::AvroData => self
                .decode_avro_record_content(reader, header)
                .map(LogBlockContent::Records),
            BlockType::ParquetData => self
                .decode_parquet_record_content(reader)
                .map(LogBlockContent::Records),
            BlockType::Delete => self
                .decode_delete_record_content(reader, header)
                .map(LogBlockContent::Records),
            BlockType::HfileData => self
                .decode_hfile_record_content(reader)
                .map(LogBlockContent::HFileRecords),
            BlockType::Command => Ok(LogBlockContent::Empty),
            _ => Err(CoreError::LogBlockError(format!(
                "Unsupported block type: {block_type:?}"
            ))),
        }
    }

    /// Validate the log block version (first 4 bytes of block content).
    ///
    /// This is NOT the same as [`LogFormatVersion`] (read from the file header).
    /// Modern Hudi tables use [`LogBlockVersion::V3`].
    fn validate_log_block_version(mut reader: impl Read) -> Result<()> {
        let mut version_buf = [0u8; 4];
        reader.read_exact(&mut version_buf)?;
        let version = LogBlockVersion::try_from(version_buf)?;
        if version != LogBlockVersion::V3 {
            return Err(CoreError::LogBlockError(format!(
                "Only support log block version {} but got: {:?}",
                LogBlockVersion::V3 as u32,
                version
            )));
        }
        Ok(())
    }

    fn decode_avro_record_content(
        &self,
        mut reader: impl Read,
        header: &HashMap<BlockMetadataKey, String>,
    ) -> Result<RecordBatches> {
        Decoder::validate_log_block_version(&mut reader)?;

        let writer_schema = header.get(&BlockMetadataKey::Schema).ok_or_else(|| {
            CoreError::LogBlockError("Schema not found in block header".to_string())
        })?;
        let writer_schema = Arc::new(AvroSchema::parse_str(writer_schema)?);

        let mut record_count_buf = [0u8; 4];
        reader.read_exact(&mut record_count_buf)?;
        let record_count = u32::from_be_bytes(record_count_buf);

        let record_content_reader =
            AvroDataBlockContentReader::new(reader, writer_schema.as_ref(), record_count);
        let mut avro_arrow_array_reader =
            AvroArrowArrayReader::try_new(record_content_reader, writer_schema.as_ref(), None)?;
        let mut batches =
            RecordBatches::new_with_capacity(record_count as usize / self.batch_size + 1, 0);
        while let Some(batch) = avro_arrow_array_reader.next_batch(self.batch_size) {
            let batch = batch.map_err(CoreError::ArrowError)?;
            batches.push_data_batch(batch);
        }
        Ok(batches)
    }

    fn decode_parquet_record_content(&self, mut reader: impl Read) -> Result<RecordBatches> {
        let mut content_bytes = Vec::new();
        reader.read_to_end(&mut content_bytes)?;
        let content_bytes = Bytes::from(content_bytes);
        let parquet_reader = ParquetRecordBatchReader::try_new(content_bytes, self.batch_size)?;
        let mut batches = RecordBatches::new();
        for item in parquet_reader {
            let batch = item.map_err(CoreError::ArrowError)?;
            batches.push_data_batch(batch);
        }
        Ok(batches)
    }

    fn decode_delete_record_content(
        &self,
        mut reader: impl Read,
        header: &HashMap<BlockMetadataKey, String>,
    ) -> Result<RecordBatches> {
        Decoder::validate_log_block_version(&mut reader)?;

        // Read delete keys byte length
        let mut delete_records_num_bytes = [0u8; 4];
        reader.read_exact(&mut delete_records_num_bytes)?;
        let delete_records_num_bytes = u32::from_be_bytes(delete_records_num_bytes);

        // Read and parse delete keys as Avro
        let mut delete_records_reader = reader.take(delete_records_num_bytes as u64);
        let del_list_schema = avro_schema_for_delete_record_list()?;
        let delete_record_list =
            from_avro_datum(del_list_schema, delete_records_reader.by_ref(), None)
                .map_err(CoreError::AvroError)?;

        // Extract delete records from the parsed Avro value
        let delete_records = {
            let fields = match delete_record_list {
                apache_avro::types::Value::Record(fields) => fields,
                _ => {
                    return Err(CoreError::LogBlockError(
                        "Expected record type for delete record list".to_string(),
                    ))
                }
            };

            if fields.len() != 1 {
                return Err(CoreError::LogBlockError(format!(
                    "Expected one field in delete record list, got {}",
                    fields.len()
                )));
            }

            let (field_name, field_value) = &fields[0];
            if field_name != "deleteRecordList" {
                return Err(CoreError::LogBlockError(format!(
                    "Expected field name 'deleteRecordList', got '{}'",
                    field_name
                )));
            }

            match field_value {
                // TODO make a specialized AvroArrowArrayReader for delete block to take &[Value] so we don't need to clone here
                apache_avro::types::Value::Array(arr) => arr.clone(),
                _ => {
                    return Err(CoreError::LogBlockError(
                        "Expected 'deleteRecordList' to be an array type".to_string(),
                    ))
                }
            }
        };

        if delete_records.is_empty() {
            return Ok(RecordBatches::new());
        }

        // Generate schema based on the first delete record
        let first_record = &delete_records[0];
        let delete_record_schema = avro_schema_for_delete_record(first_record)?;

        let num_delete_batches = delete_records.len() / self.batch_size + 1;
        let mut batches = RecordBatches::new_with_capacity(0, num_delete_batches);
        let mut reader = AvroArrowArrayReader::try_new(
            delete_records.into_iter().map(Ok),
            &delete_record_schema,
            None,
        )?;

        let instant_time = header.get(&BlockMetadataKey::InstantTime).ok_or_else(|| {
            CoreError::LogBlockError("Instant time not found in block header".to_string())
        })?;
        while let Some(batch_result) = reader.next_batch(self.batch_size) {
            let batch = batch_result.map_err(CoreError::ArrowError)?;
            batches.push_delete_batch(batch, instant_time.clone());
        }

        Ok(batches)
    }

    /// Decode HFile data block content into HFile records.
    ///
    /// HFile blocks are used in metadata table log files. Unlike Avro/Parquet blocks,
    /// the content is NOT converted to Arrow RecordBatch because:
    /// - Metadata table operations need key-based lookup/merge
    /// - Values are Avro-serialized payloads decoded on demand
    ///
    /// The HFile content structure:
    /// - Raw HFile data (no version prefix, unlike Avro blocks)
    fn decode_hfile_record_content(&self, mut reader: impl Read) -> Result<Vec<HFileRecord>> {
        // Note: HFile blocks do NOT have the 4-byte log block version prefix
        // that Avro blocks have. The content is raw HFile data.
        let mut hfile_bytes = Vec::new();
        reader.read_to_end(&mut hfile_bytes)?;

        if hfile_bytes.is_empty() {
            return Ok(Vec::new());
        }

        let mut hfile_reader =
            HFileReader::new(hfile_bytes).map_err(|e| CoreError::HFile(e.to_string()))?;

        let mut records = Vec::new();
        let iter = hfile_reader
            .iter()
            .map_err(|e| CoreError::HFile(e.to_string()))?;

        for kv_result in iter {
            let kv = kv_result.map_err(|e| CoreError::HFile(e.to_string()))?;
            records.push(HFileRecord::new(
                kv.key().content().to_vec(),
                kv.value().to_vec(),
            ));
        }

        Ok(records)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::to_avro_datum;
    use apache_avro::types::Record as AvroRecord;
    use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use std::io::{BufReader, Cursor};
    use std::sync::Arc;

    #[test]
    fn test_decode_avro_content() -> Result<()> {
        // Create Avro schema
        let schema_str = r#"{
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": ["null", "string"]}
            ]
        }"#;
        let writer_schema = AvroSchema::parse_str(schema_str)?;

        // Create in-memory buffer and write the data
        let mut buf = Vec::new();

        // Write format version (3)
        buf.extend_from_slice(&3u32.to_be_bytes());

        // Write record count (2)
        buf.extend_from_slice(&2u32.to_be_bytes());

        // Create records
        let mut record1 = AvroRecord::new(&writer_schema).unwrap();
        record1.put("id", 42i64);
        record1.put("name", Some("Alice"));

        let mut record2 = AvroRecord::new(&writer_schema).unwrap();
        record2.put("id", 43i64);
        record2.put("name", None::<String>);

        // Function to write a record with its size
        let write_record = |buf: &mut Vec<u8>, record: AvroRecord| -> Result<()> {
            // Convert record to Avro format
            let record_bytes = to_avro_datum(&writer_schema, record)?;

            // Write record size to buffer
            buf.extend_from_slice(&(record_bytes.len() as u32).to_be_bytes());

            // Write record bytes to buffer
            buf.extend_from_slice(&record_bytes);

            Ok(())
        };

        // Write both records
        write_record(&mut buf, record1)?;
        write_record(&mut buf, record2)?;

        // Create decoder and test
        let hudi_configs = HudiConfigs::empty();
        let decoder = Decoder::new(Arc::new(hudi_configs));
        let reader = Cursor::new(buf);

        let mut header = HashMap::new();
        header.insert(BlockMetadataKey::Schema, schema_str.to_string());
        let batches = decoder.decode_avro_record_content(reader, &header)?;

        // Verify results
        assert_eq!(batches.num_data_batches(), 1, "Should have 1 batch");
        assert_eq!(batches.num_data_rows(), 2, "Batch should have 2 rows");

        // Verify first row values
        let batch = &batches.data_batches[0];
        let id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(id_array.value(0), 42);
        assert_eq!(id_array.value(1), 43);

        // Verify second row values
        let name_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_array.value(0), "Alice");
        assert!(name_array.is_null(1), "Second name value should be null");

        Ok(())
    }

    #[test]
    fn test_decode_parquet_content() -> Result<()> {
        // Create sample parquet bytes
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let ids = Int64Array::from(vec![1, 2, 3]);
        let names = StringArray::from(vec!["a", "b", "c"]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(ids) as ArrayRef, Arc::new(names) as ArrayRef],
        )?;

        let mut buf = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buf, schema, None)?;
            writer.write(&batch)?;
            writer.close()?;
        }

        let hudi_configs = HudiConfigs::empty();
        let decoder = Decoder::new(Arc::new(hudi_configs));
        let bytes = Bytes::from(buf);
        let mut reader = BufReader::with_capacity(bytes.len(), Cursor::new(bytes));

        let batches = decoder.decode_parquet_record_content(&mut reader)?;
        assert_eq!(batches.num_data_batches(), 1);
        assert_eq!(batches.num_data_rows(), 3);

        Ok(())
    }
}
