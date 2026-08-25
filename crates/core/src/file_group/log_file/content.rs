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
use crate::config::HudiConfigs;
use crate::error::CoreError;
use crate::file_group::log_file::avro::AvroBlockDecoder;
use crate::file_group::log_file::log_block::{
    BlockMetadataKey, BlockType, LogBlockContent, LogBlockVersion,
};
use crate::file_group::log_file::log_format::LogFormatVersion;
use crate::file_group::record_batches::RecordBatches;
use crate::hfile::record_key::fill_empty_entry_keys;
use crate::hfile::{HFileReader, HFileRecord};
use crate::schema::delete::delete_record_list_schema_json;
use crate::schema::extended_promotion::record_needs_rewrite_for_extended_promotion;
use crate::schema::parquet_list_norm::normalize_parquet_metadata;
use crate::schema::resolver::avro_json_to_arrow_schema;
use crate::storage::RowFilterBuilder;
use arrow_array::{Array, ArrayRef, ListArray, RecordBatch, StructArray, UnionArray};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use parquet::file::metadata::ParquetMetaDataReader;
use std::collections::HashMap;
use std::io::{Read, Seek};
use std::sync::Arc;

/// Turn the wrapped ordering values into a plain column.
///
/// Hudi writes `orderingVal` as a union of per-type wrapper records, so a decode
/// against that schema yields a union of one-field structs. The merge wants the
/// value itself.
///
/// A row on the null branch carries no ordering value and decodes as a null
/// cell, which the merge treats as a natural-order delete. A block with any
/// `ArrayWrapper` row (multiple ordering fields, HUDI-9569) keeps the union
/// column as is: only the per-row union decode in
/// `delete_batch_to_keys_with_ordering` can represent a composite ordering
/// value. At most one scalar wrapper branch may appear otherwise: two would
/// collapse different value types into one column, so that mix is rejected.
fn unwrap_ordering_values(ordering: &ArrayRef) -> Result<ArrayRef> {
    let union = ordering
        .as_any()
        .downcast_ref::<UnionArray>()
        .ok_or_else(|| {
            CoreError::LogBlockError(format!(
                "Expected orderingVal to be a union, got {}",
                ordering.data_type()
            ))
        })?;

    // Only ArrayWrapper carries a list; anything else struct-shaped goes down
    // the scalar path, where a malformed wrapper is still refused loudly.
    let is_composite = |child: &ArrayRef| -> bool {
        matches!(child.data_type(), DataType::Struct(fields)
            if fields.len() == 1
                && matches!(fields[0].data_type(), DataType::List(_) | DataType::LargeList(_)))
    };

    let mut scalar: Option<i8> = None;
    let mut composite_rows = 0usize;
    for i in 0..union.len() {
        let type_id = union.type_id(i);
        let child = union.child(type_id);
        if child.data_type() == &DataType::Null {
            continue;
        }
        if is_composite(child) {
            composite_rows += 1;
            continue;
        }
        match scalar {
            None => scalar = Some(type_id),
            Some(seen) if seen == type_id => {}
            Some(seen) => {
                return Err(CoreError::LogBlockError(format!(
                    "Delete block mixes ordering types (union branches {seen} and {type_id})"
                )));
            }
        }
    }
    if composite_rows > 0 {
        return Ok(ordering.clone());
    }

    let Some(scalar) = scalar else {
        // Every row is a natural-order delete; the column type only has to be
        // one an ordering reader accepts, since every cell is null.
        return Ok(arrow_array::new_null_array(&DataType::Int64, union.len()));
    };

    let child = union.child(scalar);
    let values = match child.as_any().downcast_ref::<StructArray>() {
        Some(wrapper) => {
            if wrapper.num_columns() != 1 {
                return Err(CoreError::LogBlockError(format!(
                    "Expected an ordering wrapper with one field, got {}",
                    wrapper.num_columns()
                )));
            }
            wrapper.column(0).clone()
        }
        None => child.clone(),
    };

    // Rows on other branches take a null index, which `take` renders as a
    // null cell; scalar rows take the union's own offset, so this holds for
    // dense offsets in any order (and for sparse unions, where offset == row).
    let indices: arrow_array::Int32Array = (0..union.len())
        .map(|i| (union.type_id(i) == scalar).then(|| union.value_offset(i) as i32))
        .collect();
    arrow::compute::take(values.as_ref(), &indices, None).map_err(CoreError::ArrowError)
}

#[allow(dead_code)]
pub struct Decoder {
    batch_size: usize,
    hudi_configs: Arc<HudiConfigs>,
    /// Predicate to push into a parquet log block, when the caller has decided
    /// it is safe to evaluate before the merge. See
    /// [`Decoder::with_row_filter`].
    row_filter: Option<RowFilterBuilder>,
    /// Schema an Avro block is resolved up to, as Avro JSON. See
    /// [`Decoder::with_reader_schema`].
    reader_schema_json: Option<String>,
    /// Whether an HFile block's records are decoded into Arrow batches rather
    /// than handed back as key-value pairs. See
    /// [`Decoder::with_hfile_as_records`].
    hfile_as_records: bool,
}

impl Decoder {
    /// Push a predicate into parquet log blocks.
    ///
    /// The caller decides whether this is sound: a log record can update a row,
    /// so filtering before the merge is only safe when the merge cannot change
    /// the predicate's outcome. Log blocks exist only on merge-on-read, so in
    /// practice that means a predicate over primary keys, which are immutable
    /// across upserts.
    pub fn with_row_filter(mut self, row_filter: Option<RowFilterBuilder>) -> Self {
        self.row_filter = row_filter;
        self
    }

    /// Resolve Avro blocks up to this schema as they are read.
    ///
    /// A block records the schema it was written with, which may predate a
    /// change to the table. Supplying the current schema lets the decoder fill
    /// added columns from their defaults and deliver promoted columns in the
    /// promoted type, rather than leaving both to be reconciled after the fact.
    ///
    /// Blocks carrying `IsPartial` are excluded — see
    /// [`Self::decode_avro_record_content`].
    /// Decode an HFile block's records into Arrow batches.
    ///
    /// The two consumers of an HFile block want different shapes and neither is
    /// wrong: a merge needs Arrow like every other block type, while the
    /// metadata table wants the raw key and the still-serialized value so it can
    /// decode against its own schema. Only the caller knows which, so it says.
    pub fn with_hfile_as_records(mut self, hfile_as_records: bool) -> Self {
        self.hfile_as_records = hfile_as_records;
        self
    }

    pub fn with_reader_schema(mut self, reader_schema_json: Option<String>) -> Self {
        self.reader_schema_json = reader_schema_json;
        self
    }

    pub fn new(hudi_configs: Arc<HudiConfigs>) -> Self {
        Self {
            batch_size: 1024,
            hudi_configs,
            row_filter: None,
            reader_schema_json: None,
            hfile_as_records: false,
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
            BlockType::HfileData => self.decode_hfile_content(reader, header),
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

    /// A decoder for records written with `writer_schema_json`, resolved up to
    /// the reader schema when the block's header permits it.
    ///
    /// Shared by the Avro and HFile paths: both hold Avro-encoded records and so
    /// need the same schema resolution, and having one place for it is what stops
    /// the two drifting apart on promotions.
    fn avro_decoder_for(
        &self,
        writer_schema_json: &str,
        header: &HashMap<BlockMetadataKey, String>,
    ) -> Result<AvroBlockDecoder> {
        // A partial-update block carries only the columns that were written, and
        // the merge needs to know which those are. Resolving it up to the table
        // schema would fabricate the rest, so it decodes against its own schema.
        //
        // The header's *value*, not its presence: Java reads this as
        // `Boolean.parseBoolean(getOrDefault(IS_PARTIAL, "false"))`, so a writer
        // that emits `IS_PARTIAL=false` on a full block would otherwise have it
        // decoded writer-only, skipping the reader-schema resolution and
        // reintroducing the int-to-long mismatch at merge time on an evolved table.
        let is_partial = header
            .get(&BlockMetadataKey::IsPartial)
            .is_some_and(|v| v.eq_ignore_ascii_case("true"));
        if is_partial {
            log::debug!("partial-update block: decoding at its own schema, not the table's");
        }
        let reader_schema_json = if is_partial {
            None
        } else {
            self.reader_schema_json.as_deref()
        };
        // Avro resolves what it defines as a promotion; Hudi permits more than
        // that (a number, or anything with a logical type, to string) and Avro
        // refuses to build a reader for those at all. Such a block is decoded at
        // the schema it was written with and converted afterwards, which is what
        // the Java reader does when `recordNeedsRewriteForExtendedAvroTypePromotion`
        // says so.
        let (reader_schema_json, rewrite_to) = match reader_schema_json {
            Some(required_json) => {
                let writer = apache_avro::Schema::parse_str(writer_schema_json)?;
                let required = apache_avro::Schema::parse_str(required_json)?;
                if record_needs_rewrite_for_extended_promotion(&writer, &required)? {
                    log::warn!(
                        "log block rewritten rather than resolved: its schema differs from the \
                         table's in a way Avro does not define a promotion for"
                    );
                    let target = avro_json_to_arrow_schema(required_json)?;
                    (None, Some(Arc::new(target)))
                } else {
                    (Some(required_json), None)
                }
            }
            None => (None, None),
        };

        let mut decoder = AvroBlockDecoder::try_new_with_reader(
            writer_schema_json,
            reader_schema_json,
            self.batch_size,
        )?;
        if let Some(rewrite_to) = rewrite_to {
            decoder = decoder.with_rewrite_to(rewrite_to);
        }
        Ok(decoder)
    }

    /// An HFile block, in whichever shape the caller asked for.
    ///
    /// The records are Avro-encoded values under HFile keys, so decoding them to
    /// Arrow is the same resolution the Avro path performs; the only difference is
    /// that each value arrives already framed by the HFile rather than by a
    /// four-byte length.
    fn decode_hfile_content(
        &self,
        reader: impl Read,
        header: &HashMap<BlockMetadataKey, String>,
    ) -> Result<LogBlockContent> {
        let records = self.decode_hfile_record_content(reader)?;
        if !self.hfile_as_records {
            return Ok(LogBlockContent::HFileRecords(records));
        }

        let writer_schema_json = header.get(&BlockMetadataKey::Schema).ok_or_else(|| {
            CoreError::LogBlockError(
                "an HFile block has no schema in its header, so its records cannot be decoded"
                    .to_string(),
            )
        })?;
        let mut decoder = self.avro_decoder_for(writer_schema_json, header)?;
        let mut decoded: Vec<arrow_array::RecordBatch> = Vec::new();
        for record in &records {
            if let Some(batch) = decoder.decode(&record.value)? {
                decoded.push(batch);
            }
        }
        if let Some(batch) = decoder.flush()?
            && batch.num_rows() > 0
        {
            decoded.push(batch);
        }

        // A writer may leave the record's key field empty because the HFile entry
        // key already holds it. The keys are positional across the whole batch
        // sequence, not per batch, so the offset tracks which records produced
        // which batch.
        let entry_keys: Vec<&str> = records
            .iter()
            .map(|r| {
                r.key_as_str().ok_or_else(|| {
                    CoreError::LogBlockError("an HFile record key is not valid UTF-8".to_string())
                })
            })
            .collect::<Result<Vec<&str>>>()?;

        let mut batches = RecordBatches::new_with_capacity(decoded.len(), 0);
        let mut offset = 0usize;
        for batch in decoded {
            let rows = batch.num_rows();
            let keys = entry_keys.get(offset..offset + rows).ok_or_else(|| {
                CoreError::LogBlockError(format!(
                    "{rows} rows decoded at offset {offset} from {} HFile records; the key of \
                     each row cannot be identified",
                    entry_keys.len()
                ))
            })?;
            batches.push_data_batch(
                fill_empty_entry_keys(batch, keys).map_err(|e| CoreError::HFile(e.to_string()))?,
            );
            offset += rows;
        }
        Ok(LogBlockContent::Records(batches))
    }

    fn decode_avro_record_content(
        &self,
        mut reader: impl Read,
        header: &HashMap<BlockMetadataKey, String>,
    ) -> Result<RecordBatches> {
        Decoder::validate_log_block_version(&mut reader)?;

        let writer_schema_json = header.get(&BlockMetadataKey::Schema).ok_or_else(|| {
            CoreError::LogBlockError("Schema not found in block header".to_string())
        })?;

        let mut record_count_buf = [0u8; 4];
        reader.read_exact(&mut record_count_buf)?;
        let record_count = u32::from_be_bytes(record_count_buf);

        let mut decoder = self.avro_decoder_for(writer_schema_json, header)?;
        let mut batches =
            RecordBatches::new_with_capacity(record_count as usize / self.batch_size + 1, 0);

        // Each datum is framed by Hudi with a four-byte length, so the bodies
        // are read here and handed over one at a time.
        let mut body = Vec::new();
        for _ in 0..record_count {
            let mut len_buf = [0u8; 4];
            reader.read_exact(&mut len_buf)?;
            let len = u32::from_be_bytes(len_buf) as usize;
            body.clear();
            body.resize(len, 0);
            reader.read_exact(&mut body)?;
            if let Some(batch) = decoder.decode(&body)? {
                batches.push_data_batch(batch);
            }
        }
        if let Some(batch) = decoder.flush()?
            && batch.num_rows() > 0
        {
            batches.push_data_batch(batch);
        }
        Ok(batches)
    }

    fn decode_parquet_record_content(&self, mut reader: impl Read) -> Result<RecordBatches> {
        let mut content_bytes = Vec::new();
        reader.read_to_end(&mut content_bytes)?;
        let content_bytes = Bytes::from(content_bytes);

        // Same legacy `array<map>` encoding the base file path has to handle:
        // parse the footer, rewrite the 2-level list, then build the reader from
        // that metadata. The Arrow build is what rejects the original, so the
        // rewrite has to land between the two.
        let raw = Arc::new(ParquetMetaDataReader::new().parse_and_finish(&content_bytes)?);
        let normalized = normalize_parquet_metadata(raw);
        let arrow_metadata = ArrowReaderMetadata::try_new(normalized, ArrowReaderOptions::new())?;
        let mut builder =
            ParquetRecordBatchReaderBuilder::new_with_metadata(content_bytes, arrow_metadata)
                .with_batch_size(self.batch_size);

        // Resolved here rather than by the caller because the predicate has to
        // be matched against this block's own schema, which does not exist until
        // its footer is read. A builder that declines reads every row.
        let row_filter = self
            .row_filter
            .as_ref()
            .and_then(|build| build(builder.parquet_schema(), builder.schema().as_ref()));
        if let Some(row_filter) = row_filter {
            builder = builder.with_row_filter(row_filter);
        }
        let parquet_reader = builder.build()?;
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

        let mut datum_len = [0u8; 4];
        reader.read_exact(&mut datum_len)?;
        let datum_len = u32::from_be_bytes(datum_len) as usize;
        let mut datum = vec![0u8; datum_len];
        reader.read_exact(&mut datum)?;

        // The whole list is a single Avro datum, so one record decodes the lot:
        // a one-row batch whose only column holds the delete records.
        let mut decoder =
            AvroBlockDecoder::try_new_with_reader(delete_record_list_schema_json(), None, 1)?;
        let Some(batch) = decoder.decode(&datum)?.or(decoder.flush()?) else {
            return Ok(RecordBatches::new());
        };

        let records = batch
            .column_by_name("deleteRecordList")
            .ok_or_else(|| {
                CoreError::LogBlockError("Delete block has no deleteRecordList".to_string())
            })?
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| CoreError::LogBlockError("deleteRecordList is not a list".to_string()))?
            .value(0);
        let records = records
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                CoreError::LogBlockError("Delete records are not structs".to_string())
            })?;
        if records.len() == 0 {
            return Ok(RecordBatches::new());
        }

        let ordering =
            unwrap_ordering_values(records.column_by_name("orderingVal").ok_or_else(|| {
                CoreError::LogBlockError("Delete record has no orderingVal".to_string())
            })?)?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("recordKey", DataType::Utf8, true),
            Field::new("partitionPath", DataType::Utf8, true),
            Field::new("orderingVal", ordering.data_type().clone(), true),
        ]));
        let columns = vec![
            records
                .column_by_name("recordKey")
                .ok_or_else(|| {
                    CoreError::LogBlockError("Delete record has no recordKey".to_string())
                })?
                .clone(),
            records
                .column_by_name("partitionPath")
                .ok_or_else(|| {
                    CoreError::LogBlockError("Delete record has no partitionPath".to_string())
                })?
                .clone(),
            ordering,
        ];

        let mut batches = RecordBatches::new_with_capacity(0, 1);
        let instant_time = header
            .get(&BlockMetadataKey::InstantTime)
            .cloned()
            .unwrap_or_default();
        batches.push_delete_batch(
            RecordBatch::try_new(schema, columns).map_err(CoreError::ArrowError)?,
            instant_time,
        );
        Ok(batches)
    }

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
    use crate::file_group::log_file::log_block::LogBlockVersion;
    use apache_avro::to_avro_datum;
    use apache_avro::types::Record as AvroRecord;
    use apache_avro::types::Value as AvroValue;
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
        let writer_schema = apache_avro::Schema::parse_str(schema_str)?;

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

    /// A partial-update block carries only the columns that were written, and
    /// must stay that way.
    ///
    /// The merge distinguishes "this column was not in the update" from "this
    /// column was set to null", and the block's own schema is the only place
    /// that signal exists. Decoding is against that schema and never against a
    /// wider table schema, so the narrowness survives.
    ///
    /// If a reader schema is ever supplied here — for Avro resolution or
    /// extended promotion — a block carrying `IsPartial` has to keep decoding
    /// against its own schema, or the absent columns get fabricated and the
    /// signal is gone.
    /// A block written before a column was promoted to string still reads.
    ///
    /// Avro refuses to build a reader for `int -> string`, so without the
    /// extended-promotion path this read fails outright with "Illegal
    /// promotion Int to String". The
    /// block decodes at its own schema and is converted afterwards.
    #[test]
    fn test_extended_promotion_int_to_string_rewrites() -> Result<()> {
        let writer_json =
            r#"{"type":"record","name":"TestRecord","fields":[{"name":"num","type":"int"}]}"#;
        let required_json =
            r#"{"type":"record","name":"TestRecord","fields":[{"name":"num","type":"string"}]}"#;
        let writer_schema = apache_avro::Schema::parse_str(writer_json)?;

        let mut buf = Vec::new();
        buf.extend_from_slice(&3u32.to_be_bytes());
        buf.extend_from_slice(&1u32.to_be_bytes());
        let mut record = AvroRecord::new(&writer_schema).unwrap();
        record.put("num", 42i32);
        let body = to_avro_datum(&writer_schema, record)?;
        buf.extend_from_slice(&(body.len() as u32).to_be_bytes());
        buf.extend_from_slice(&body);

        let header = HashMap::from([(BlockMetadataKey::Schema, writer_json.to_string())]);
        let decoder = Decoder::new(Arc::new(HudiConfigs::empty()))
            .with_reader_schema(Some(required_json.to_string()));
        let batches = decoder.decode_avro_record_content(buf.as_slice(), &header)?;

        let batch = &batches.data_batches[0];
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Utf8);
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .expect("promoted to string");
        assert_eq!(col.value(0), "42");
        Ok(())
    }

    /// A float promoted to string has to read the way Java renders it. A plain
    /// cast of `1.1f32` gives `1.100000023841858`, which is not what the table
    /// says.
    #[test]
    fn test_extended_promotion_float_to_string_matches_java() -> Result<()> {
        let writer_json =
            r#"{"type":"record","name":"TestRecord","fields":[{"name":"f","type":"float"}]}"#;
        let required_json =
            r#"{"type":"record","name":"TestRecord","fields":[{"name":"f","type":"string"}]}"#;
        let writer_schema = apache_avro::Schema::parse_str(writer_json)?;

        let mut buf = Vec::new();
        buf.extend_from_slice(&3u32.to_be_bytes());
        buf.extend_from_slice(&1u32.to_be_bytes());
        let mut record = AvroRecord::new(&writer_schema).unwrap();
        record.put("f", 1.1f32);
        let body = to_avro_datum(&writer_schema, record)?;
        buf.extend_from_slice(&(body.len() as u32).to_be_bytes());
        buf.extend_from_slice(&body);

        let header = HashMap::from([(BlockMetadataKey::Schema, writer_json.to_string())]);
        let decoder = Decoder::new(Arc::new(HudiConfigs::empty()))
            .with_reader_schema(Some(required_json.to_string()));
        let batches = decoder.decode_avro_record_content(buf.as_slice(), &header)?;

        let col = batches.data_batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .expect("promoted to string");
        assert_eq!(
            col.value(0),
            "1.1",
            "a plain f32 cast would give 1.100000023841858"
        );
        Ok(())
    }

    /// The ordering value arrives wrapped in a union of per-type records; the
    /// merge wants the value. One populated branch, unwrapped to its `value`.
    #[test]
    fn test_unwrap_ordering_values_takes_the_populated_branch() {
        use arrow_array::{Int64Array, UnionArray};
        use arrow_buffer::ScalarBuffer;
        use arrow_schema::{Fields, UnionFields};

        let wrapped = StructArray::new(
            Fields::from(vec![Field::new("value", DataType::Int64, false)]),
            vec![Arc::new(Int64Array::from(vec![4000, 3000])) as ArrayRef],
            None,
        );
        let union_fields = UnionFields::try_new(
            vec![3],
            vec![Field::new(
                "LongWrapper",
                wrapped.data_type().clone(),
                false,
            )],
        )
        .unwrap();
        let union = UnionArray::try_new(
            union_fields,
            ScalarBuffer::from(vec![3i8, 3i8]),
            Some(ScalarBuffer::from(vec![0i32, 1i32])),
            vec![Arc::new(wrapped) as ArrayRef],
        )
        .unwrap();

        let out = unwrap_ordering_values(&(Arc::new(union) as ArrayRef)).unwrap();
        let out = out.as_any().downcast_ref::<Int64Array>().expect("i64");
        assert_eq!(out.value(0), 4000);
        assert_eq!(out.value(1), 3000);
    }

    /// A block writes one ordering type. Two in the same block cannot both
    /// become one column, so it is rejected rather than silently reduced to
    /// whichever branch came first.
    #[test]
    fn test_unwrap_ordering_values_rejects_mixed_branches() {
        use arrow_array::{Int32Array, Int64Array, UnionArray};
        use arrow_buffer::ScalarBuffer;
        use arrow_schema::{Fields, UnionFields};

        let ints = StructArray::new(
            Fields::from(vec![Field::new("value", DataType::Int32, false)]),
            vec![Arc::new(Int32Array::from(vec![7])) as ArrayRef],
            None,
        );
        let longs = StructArray::new(
            Fields::from(vec![Field::new("value", DataType::Int64, false)]),
            vec![Arc::new(Int64Array::from(vec![4000])) as ArrayRef],
            None,
        );
        let union_fields = UnionFields::try_new(
            vec![2, 3],
            vec![
                Field::new("IntWrapper", ints.data_type().clone(), false),
                Field::new("LongWrapper", longs.data_type().clone(), false),
            ],
        )
        .unwrap();
        let union = UnionArray::try_new(
            union_fields,
            ScalarBuffer::from(vec![2i8, 3i8]),
            Some(ScalarBuffer::from(vec![0i32, 0i32])),
            vec![Arc::new(ints) as ArrayRef, Arc::new(longs) as ArrayRef],
        )
        .unwrap();

        let err = unwrap_ordering_values(&(Arc::new(union) as ArrayRef)).unwrap_err();
        assert!(
            err.to_string().contains("mixes ordering types"),
            "got: {err}"
        );
    }

    /// The block version is the first four bytes of a block's content, and only
    /// V3 is understood. A block at another version is refused rather than
    /// decoded as if it were V3, which would misread every byte after it.
    #[test]
    fn test_decode_content_rejects_a_non_v3_block_version() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u32.to_be_bytes());
        buf.extend_from_slice(&0u32.to_be_bytes());
        let header = HashMap::from([(BlockMetadataKey::Schema, "{}".to_string())]);
        let err = Decoder::new(Arc::new(HudiConfigs::empty()))
            .decode_avro_record_content(buf.as_slice(), &header)
            .expect_err("a non-V3 block must be refused");
        assert!(
            err.to_string().contains("log block version"),
            "the error must name the version, got: {err}"
        );
    }

    /// An Avro block records the schema it was written with in its header. With
    /// none there is nothing to decode against, so the read stops rather than
    /// guessing at the table's current schema — which would misread a block
    /// written before an evolution.
    #[test]
    fn test_decode_avro_content_without_a_schema_header_is_an_error() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&3u32.to_be_bytes());
        buf.extend_from_slice(&0u32.to_be_bytes());
        let err = Decoder::new(Arc::new(HudiConfigs::empty()))
            .decode_avro_record_content(buf.as_slice(), &HashMap::new())
            .expect_err("a block with no schema header must be refused");
        assert!(err.to_string().contains("Schema not found"), "got: {err}");
    }

    /// `orderingVal` is a union of per-type wrapper records, so a decode yields
    /// a union of one-field structs and the merge wants the value. Anything else
    /// is refused rather than reduced to something that looks like a value.
    #[test]
    fn test_unwrap_ordering_values_rejects_a_non_union() {
        let plain: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 2]));
        let err = unwrap_ordering_values(&plain).expect_err("a non-union must be refused");
        assert!(
            err.to_string().contains("union"),
            "the error must say what it expected, got: {err}"
        );
    }

    /// A union with no rows has no populated branch to unwrap, so it passes
    /// through unchanged rather than erroring on the absence.
    #[test]
    fn test_unwrap_ordering_values_passes_an_empty_union_through() {
        use arrow_array::{Int32Array, UnionArray};
        use arrow_buffer::ScalarBuffer;
        use arrow_schema::{Fields, UnionFields};
        let ints = StructArray::new(
            Fields::from(vec![Field::new("value", DataType::Int32, false)]),
            vec![Arc::new(Int32Array::from(Vec::<i32>::new())) as ArrayRef],
            None,
        );
        let union_fields = UnionFields::try_new(
            vec![2],
            vec![Field::new("IntWrapper", ints.data_type().clone(), false)],
        )
        .unwrap();
        let union = UnionArray::try_new(
            union_fields,
            ScalarBuffer::from(Vec::<i8>::new()),
            Some(ScalarBuffer::from(Vec::<i32>::new())),
            vec![Arc::new(ints) as ArrayRef],
        )
        .unwrap();
        let src: ArrayRef = Arc::new(union);
        let out = unwrap_ordering_values(&src).expect("an empty union is not an error");
        assert_eq!(out.len(), 0);
    }

    /// A wrapper is one field — the value. More than one means the block was not
    /// written by the writer this decode assumes, and picking a field would be a
    /// guess about which one carries the ordering value.
    #[test]
    fn test_unwrap_ordering_values_rejects_a_multi_field_wrapper() {
        use arrow_array::{Int32Array, UnionArray};
        use arrow_buffer::ScalarBuffer;
        use arrow_schema::{Fields, UnionFields};
        let wrapper = StructArray::new(
            Fields::from(vec![
                Field::new("value", DataType::Int32, false),
                Field::new("extra", DataType::Int32, false),
            ]),
            vec![
                Arc::new(Int32Array::from(vec![7])) as ArrayRef,
                Arc::new(Int32Array::from(vec![8])) as ArrayRef,
            ],
            None,
        );
        let union_fields = UnionFields::try_new(
            vec![2],
            vec![Field::new("IntWrapper", wrapper.data_type().clone(), false)],
        )
        .unwrap();
        let union = UnionArray::try_new(
            union_fields,
            ScalarBuffer::from(vec![2i8]),
            Some(ScalarBuffer::from(vec![0i32])),
            vec![Arc::new(wrapper) as ArrayRef],
        )
        .unwrap();
        let src: ArrayRef = Arc::new(union);
        let err = unwrap_ordering_values(&src).expect_err("a two-field wrapper must be refused");
        assert!(err.to_string().contains("one field"), "got: {err}");
    }

    #[test]
    fn test_decode_avro_partial_update_block_keeps_narrow_schema() -> Result<()> {
        // The table has id + name; this block carries only id.
        let partial_schema_str =
            r#"{"type":"record","name":"TestRecord","fields":[{"name":"id","type":"long"}]}"#;
        let writer_schema = apache_avro::Schema::parse_str(partial_schema_str)?;

        let mut buf = Vec::new();
        buf.extend_from_slice(&3u32.to_be_bytes());
        buf.extend_from_slice(&1u32.to_be_bytes());
        let mut record = AvroRecord::new(&writer_schema).unwrap();
        record.put("id", 7i64);
        let record_bytes = to_avro_datum(&writer_schema, record)?;
        buf.extend_from_slice(&(record_bytes.len() as u32).to_be_bytes());
        buf.extend_from_slice(&record_bytes);

        let header = HashMap::from([
            (BlockMetadataKey::Schema, partial_schema_str.to_string()),
            (BlockMetadataKey::IsPartial, "true".to_string()),
        ]);
        let decoder = Decoder::new(Arc::new(HudiConfigs::empty()));
        let batches = decoder.decode_avro_record_content(buf.as_slice(), &header)?;

        assert_eq!(batches.num_data_rows(), 1);
        let batch = &batches.data_batches[0];
        assert_eq!(
            batch.num_columns(),
            1,
            "a partial block must not be widened to the table schema"
        );
        assert_eq!(batch.schema().field(0).name(), "id");
        Ok(())
    }

    /// Regression test: `IS_PARTIAL=false` marks a FULL block.
    ///
    /// Reading the flag by its mere presence would turn every full block from a
    /// writer that spells the negative case out into a partial one — decoded at
    /// its own schema, skipping the resolution up to the reader's, which is
    /// what delivers a promoted column in the promoted type. Java reads the
    /// value (`Boolean.parseBoolean(getOrDefault(IS_PARTIAL, "false"))`), so
    /// this does too.
    #[test]
    fn test_decode_avro_is_partial_false_resolves_to_the_reader_schema() -> Result<()> {
        // Written narrow; the reader schema promotes `id` to a wider type and
        // adds `name`, which only the resolving path can deliver.
        let writer_schema_str =
            r#"{"type":"record","name":"TestRecord","fields":[{"name":"id","type":"int"}]}"#;
        let reader_schema_str = r#"{"type":"record","name":"TestRecord","fields":[{"name":"id","type":"long"},{"name":"name","type":["null","string"],"default":null}]}"#;
        let writer_schema = apache_avro::Schema::parse_str(writer_schema_str)?;

        let mut buf = Vec::new();
        buf.extend_from_slice(&3u32.to_be_bytes());
        buf.extend_from_slice(&1u32.to_be_bytes());
        let mut record = AvroRecord::new(&writer_schema).unwrap();
        record.put("id", 7i32);
        let record_bytes = to_avro_datum(&writer_schema, record)?;
        buf.extend_from_slice(&(record_bytes.len() as u32).to_be_bytes());
        buf.extend_from_slice(&record_bytes);

        let header = HashMap::from([
            (BlockMetadataKey::Schema, writer_schema_str.to_string()),
            (BlockMetadataKey::IsPartial, "false".to_string()),
        ]);
        let decoder = Decoder::new(Arc::new(HudiConfigs::empty()))
            .with_reader_schema(Some(reader_schema_str.to_string()));
        let batches = decoder.decode_avro_record_content(buf.as_slice(), &header)?;

        let batch = &batches.data_batches[0];
        assert_eq!(
            batch.num_columns(),
            2,
            "IS_PARTIAL=false is a full block, so it resolves up to the reader schema"
        );
        assert_eq!(
            batch.schema().field_with_name("id")?.data_type(),
            &DataType::Int64,
            "the promoted column must arrive promoted"
        );
        Ok(())
    }

    /// A parquet log block written by the Hudi Avro write path carries its
    /// `array<map>` column as a legacy 2-level list, which the parquet→arrow
    /// builder rejects with "Map cannot be repeated" unless the schema is
    /// normalized first. The block is unreadable without it, so returning rows
    /// at all is the assertion.
    #[test]
    fn test_decode_parquet_content_accepts_a_legacy_two_level_list() -> Result<()> {
        const LEGACY_2LEVEL: &[u8] =
            include_bytes!("../../../tests/data/i3/legacy_2level_repeated_map.parquet");

        let decoder = Decoder::new(Arc::new(HudiConfigs::empty()));
        let batches = decoder.decode_parquet_record_content(LEGACY_2LEVEL)?;

        assert!(
            batches.num_data_rows() > 0,
            "a legacy 2-level list block must decode"
        );
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

    /// Union positions in `HoodieDeleteRecordList.avsc`: 0 = null,
    /// 3 = `LongWrapper`, 12 = `ArrayWrapper`.
    fn delete_record_with_ordering(key: &str, ordering: AvroValue) -> AvroValue {
        AvroValue::Record(vec![
            (
                "recordKey".to_string(),
                AvroValue::Union(1, Box::new(AvroValue::String(key.to_string()))),
            ),
            (
                "partitionPath".to_string(),
                AvroValue::Union(1, Box::new(AvroValue::String(String::new()))),
            ),
            ("orderingVal".to_string(), ordering),
        ])
    }

    fn long_wrapper(value: i64) -> AvroValue {
        AvroValue::Union(
            3,
            Box::new(AvroValue::Record(vec![(
                "value".to_string(),
                AvroValue::Long(value),
            )])),
        )
    }

    fn null_ordering() -> AvroValue {
        AvroValue::Union(0, Box::new(AvroValue::Null))
    }

    fn array_wrapper() -> AvroValue {
        AvroValue::Union(
            12,
            Box::new(AvroValue::Record(vec![(
                "wrappedValues".to_string(),
                AvroValue::Union(
                    1,
                    Box::new(AvroValue::Array(vec![long_wrapper(9), long_wrapper(1)])),
                ),
            )])),
        )
    }

    /// Serialize delete records into the block-content byte layout
    /// `decode_delete_record_content` reads: version, datum length, datum.
    fn delete_block_bytes(records: Vec<AvroValue>) -> Result<Vec<u8>> {
        let schema = crate::schema::delete::avro_schema_for_delete_record_list()?;
        let value = AvroValue::Record(vec![(
            "deleteRecordList".to_string(),
            AvroValue::Array(records),
        )]);
        let datum = to_avro_datum(schema, value)?;
        let mut buf = Vec::new();
        buf.extend_from_slice(&(LogBlockVersion::V3 as u32).to_be_bytes());
        buf.extend_from_slice(&(datum.len() as u32).to_be_bytes());
        buf.extend_from_slice(&datum);
        Ok(buf)
    }

    fn decode_delete_block(records: Vec<AvroValue>) -> Result<RecordBatches> {
        let buf = delete_block_bytes(records)?;
        let decoder = Decoder::new(Arc::new(HudiConfigs::empty()));
        decoder.decode_delete_record_content(Cursor::new(buf), &HashMap::new())
    }

    fn ordering_column(batches: &RecordBatches) -> ArrayRef {
        batches.delete_batches[0].0.column(2).clone()
    }

    /// Regression test: a delete written without an ordering value shares its block
    /// with typed ones: null is a union branch, not a second ordering type.
    /// The null slot decodes as a null cell, which the merge treats as a
    /// natural-order delete, matching Hudi.
    #[test]
    fn test_delete_block_null_ordering_among_typed_decodes_as_null_slots() -> Result<()> {
        let batches = decode_delete_block(vec![
            delete_record_with_ordering("k0", null_ordering()),
            delete_record_with_ordering("k1", long_wrapper(5)),
            delete_record_with_ordering("k2", null_ordering()),
        ])?;

        let ordering = ordering_column(&batches);
        let ordering = ordering.as_any().downcast_ref::<Int64Array>().unwrap();
        assert!(ordering.is_null(0));
        assert_eq!(ordering.value(1), 5);
        assert!(ordering.is_null(2));
        Ok(())
    }

    /// Regression test: a block whose deletes all carry no ordering value must not
    /// produce a `DataType::Null` column, which no ordering reader accepts.
    #[test]
    fn test_delete_block_all_null_ordering_decodes_readably() -> Result<()> {
        let batches = decode_delete_block(vec![
            delete_record_with_ordering("k0", null_ordering()),
            delete_record_with_ordering("k1", null_ordering()),
        ])?;

        let ordering = ordering_column(&batches);
        assert_ne!(ordering.data_type(), &DataType::Null);
        assert_eq!(ordering.null_count(), 2);
        Ok(())
    }

    /// Regression test: multiple ordering fields serialize to `ArrayWrapper`, whose
    /// composite value only the per-row union decode can represent. The union
    /// passes through whole and decodes to the composite the merge compares,
    /// so a stale composite delete can lose to a newer row instead of the read
    /// failing (or the delete winning unconditionally).
    #[test]
    fn test_delete_block_array_wrapper_ordering_decodes_as_composite() -> Result<()> {
        use crate::file_group::reader_v2::buffered_record::OrderingValue;
        use crate::file_group::reader_v2::record_context::RecordContext;

        let batches = decode_delete_block(vec![
            delete_record_with_ordering("k0", array_wrapper()),
            delete_record_with_ordering("k1", long_wrapper(7)),
        ])?;

        let ordering = ordering_column(&batches);
        assert!(matches!(ordering.data_type(), DataType::Union(_, _)));

        let record_context = RecordContext::new(&HashMap::new(), String::new());
        let entries =
            record_context.delete_batch_to_keys_with_ordering(&batches.delete_batches[0].0)?;
        assert_eq!(
            entries[0],
            (
                "k0".to_string(),
                Some(OrderingValue::Composite(vec![
                    OrderingValue::Long(9),
                    OrderingValue::Long(1),
                ]))
            )
        );
        assert_eq!(entries[1], ("k1".to_string(), Some(OrderingValue::Long(7))));
        Ok(())
    }

    /// Two scalar ordering types in one block are rejected: collapsing them
    /// to either type would compare across types the merge keeps apart.
    #[test]
    fn test_delete_block_mixing_scalar_ordering_types_errors() {
        let int_wrapper = AvroValue::Union(
            2,
            Box::new(AvroValue::Record(vec![(
                "value".to_string(),
                AvroValue::Int(1),
            )])),
        );
        let err = decode_delete_block(vec![
            delete_record_with_ordering("k0", int_wrapper),
            delete_record_with_ordering("k1", long_wrapper(2)),
        ])
        .unwrap_err();
        assert!(err.to_string().contains("mixes"), "got: {err}");
    }
}
