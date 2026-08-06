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
use crate::error::CoreError;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_avro::reader::{Decoder as ArrowAvroDecoder, ReaderBuilder};
use arrow_avro::schema::{AvroSchema as ArrowAvroSchema, SINGLE_OBJECT_MAGIC, SchemaStore};
use arrow_cast::cast;
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;

/// Decodes the bare Avro record bodies in a Hudi log block straight into Arrow.
///
/// A Hudi data block frames each datum with a four-byte length and no Avro
/// framing of its own, while `arrow-avro`'s decoder expects each record to be
/// introduced by a Single Object Encoding prefix — the two-byte marker and the
/// writer schema's fingerprint. The two are reconciled by synthesizing that
/// prefix: the schema is registered once, and the ten bytes it yields are
/// written ahead of every body.
///
/// The alternative is `arrow-avro`'s `AvroBodyDecoder`, which takes bare bodies
/// directly but is not released yet.
pub struct AvroBlockDecoder {
    decoder: ArrowAvroDecoder,
    /// Marker plus fingerprint, identical for every record in the block.
    prefix: [u8; 10],
    /// Reused across records so framing costs one copy, not one allocation.
    framed: Vec<u8>,
}

impl AvroBlockDecoder {
    /// Build a decoder for a block written with `writer_schema_json`.
    pub fn try_new(writer_schema_json: &str, batch_size: usize) -> Result<Self> {
        let mut store = SchemaStore::new();
        let fingerprint = store
            .register(ArrowAvroSchema::new(writer_schema_json.to_string()))
            .map_err(|e| {
                CoreError::LogBlockError(format!("Failed to register block writer schema: {e}"))
            })?;

        let arrow_avro::schema::Fingerprint::Rabin(rabin) = fingerprint else {
            return Err(CoreError::LogBlockError(format!(
                "Expected a Rabin fingerprint for the block writer schema, got {fingerprint:?}"
            )));
        };

        let decoder = ReaderBuilder::new()
            .with_writer_schema_store(store)
            .with_active_fingerprint(fingerprint)
            .with_batch_size(batch_size)
            .build_decoder()
            .map_err(|e| {
                CoreError::LogBlockError(format!("Failed to build the Avro decoder: {e}"))
            })?;

        let mut prefix = [0u8; 10];
        prefix[..2].copy_from_slice(&SINGLE_OBJECT_MAGIC);
        prefix[2..].copy_from_slice(&rabin.to_le_bytes());

        Ok(Self {
            decoder,
            prefix,
            framed: Vec::new(),
        })
    }

    /// Decode one record body, returning a batch once enough rows have accrued.
    pub fn decode(&mut self, body: &[u8]) -> Result<Option<RecordBatch>> {
        self.framed.clear();
        self.framed.reserve(self.prefix.len() + body.len());
        self.framed.extend_from_slice(&self.prefix);
        self.framed.extend_from_slice(body);

        let consumed = self
            .decoder
            .decode(&self.framed)
            .map_err(|e| CoreError::LogBlockError(format!("Failed to decode a log record: {e}")))?;
        if consumed != self.framed.len() {
            return Err(CoreError::LogBlockError(format!(
                "Log record decoded partially: {consumed} of {} bytes",
                self.framed.len()
            )));
        }

        if self.decoder.batch_is_full() {
            return self.flush();
        }
        Ok(None)
    }

    /// Drain whatever rows are held, if any.
    pub fn flush(&mut self) -> Result<Option<RecordBatch>> {
        let batch = self.decoder.flush().map_err(|e| {
            CoreError::LogBlockError(format!("Failed to flush decoded records: {e}"))
        })?;
        batch.map(normalize_utc_timestamps).transpose()
    }
}

/// Spell a UTC timestamp's zone the way the parquet reader does.
///
/// `arrow-avro` writes the zone of an Avro `timestamp-*` as the offset
/// `+00:00`; parquet writes `UTC`. They denote the same zone, but Arrow
/// compares timezones as strings, so a log batch and the base batch it merges
/// with would be judged to have different types and refuse to concatenate.
///
/// Only the field's type label changes — the values are already UTC instants,
/// so this rebinds metadata rather than converting anything.
fn normalize_utc_timestamps(batch: RecordBatch) -> Result<RecordBatch> {
    fn is_utc_alias(tz: &str) -> bool {
        matches!(tz, "+00:00" | "+0000" | "00:00" | "Z" | "z")
    }

    let needs_fix = batch
        .schema()
        .fields()
        .iter()
        .any(|f| matches!(f.data_type(), DataType::Timestamp(_, Some(tz)) if is_utc_alias(tz)));
    if !needs_fix {
        return Ok(batch);
    }

    let mut fields: Vec<Field> = Vec::with_capacity(batch.num_columns());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        match field.data_type() {
            DataType::Timestamp(unit, Some(tz)) if is_utc_alias(tz) => {
                let retimed = cast(column, &DataType::Timestamp(*unit, Some("UTC".into())))
                    .map_err(CoreError::ArrowError)?;
                fields.push(
                    Field::new(
                        field.name(),
                        retimed.data_type().clone(),
                        field.is_nullable(),
                    )
                    .with_metadata(field.metadata().clone()),
                );
                columns.push(retimed);
            }
            _ => {
                fields.push(field.as_ref().clone());
                columns.push(column.clone());
            }
        }
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(CoreError::ArrowError)
}
