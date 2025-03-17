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
use crate::file_group::log_file::log_block::{BlockMetadataKey, BlockType};
use crate::file_group::log_file::log_format::LogFormatVersion;
use crate::Result;
use apache_avro::Schema as AvroSchema;
use arrow_array::RecordBatch;
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
    ) -> Result<Vec<RecordBatch>> {
        let content_length = if log_format_version.has_content_length() {
            let mut content_length_buf = [0u8; 8];
            reader.read_exact(&mut content_length_buf)?;
            u64::from_be_bytes(content_length_buf)
        } else {
            fallback_length
        };

        let mut reader = reader.by_ref().take(content_length);
        match block_type {
            BlockType::AvroData => {
                let writer_schema = header.get(&BlockMetadataKey::Schema).ok_or_else(|| {
                    CoreError::LogBlockError("Schema not found in block header".to_string())
                })?;
                let writer_schema = AvroSchema::parse_str(writer_schema)?;
                self.decode_avro_record_content(reader, &writer_schema)
            }
            BlockType::ParquetData => self.decode_parquet_record_content(&mut reader),
            BlockType::Command => Ok(Vec::new()),
            _ => Err(CoreError::LogBlockError(format!(
                "Unsupported block type: {block_type:?}"
            ))),
        }
    }

    fn decode_avro_record_content(
        &self,
        mut reader: impl Read,
        writer_schema: &AvroSchema,
    ) -> Result<Vec<RecordBatch>> {
        let mut format_version = [0u8; 4];
        reader.read_exact(&mut format_version)?;
        let format_version = u32::from_be_bytes(format_version);
        if format_version != 3 {
            return Err(CoreError::LogBlockError(format!(
                "Unsupported delete record format version: {format_version}"
            )));
        }

        let mut record_count = [0u8; 4];
        reader.read_exact(&mut record_count)?;
        let record_count = u32::from_be_bytes(record_count);

        let record_content_reader =
            AvroDataBlockContentReader::new(reader, writer_schema, record_count);
        let mut avro_arrow_array_reader =
            AvroArrowArrayReader::try_new(record_content_reader, writer_schema, None)?;
        let mut batches = Vec::new();
        while let Some(batch) = avro_arrow_array_reader.next_batch(self.batch_size) {
            let batch = batch.map_err(CoreError::ArrowError)?;
            batches.push(batch);
        }
        Ok(batches)
    }

    fn decode_parquet_record_content(&self, mut reader: impl Read) -> Result<Vec<RecordBatch>> {
        let mut content_bytes = Vec::new();
        reader.read_to_end(&mut content_bytes)?;
        let content_bytes = Bytes::from(content_bytes);
        let parquet_reader = ParquetRecordBatchReader::try_new(content_bytes, self.batch_size)?;
        let mut batches = Vec::new();
        for item in parquet_reader {
            let batch = item.map_err(CoreError::ArrowError)?;
            batches.push(batch);
        }
        Ok(batches)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use std::io::{BufReader, Cursor};
    use std::sync::Arc;

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
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);

        Ok(())
    }
}
