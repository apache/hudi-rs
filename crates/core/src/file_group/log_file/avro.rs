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
use arrow_schema::{DataType, Field, Schema, SchemaRef};
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
    /// What the inner decoder is rebuilt from between batches.
    ///
    /// `arrow-avro`'s union decoder drains its offsets on flush but leaves the
    /// per-branch counts that produced them, so a second batch emits offsets past
    /// the children it just emptied and `UnionArray::try_new` rejects them. It has
    /// no reset, so the only way to get a clean decoder is to build one.
    ///
    /// Kept here so the rebuild skips **registration**, which is one of the two
    /// schema-sized halves. It is not free: `build_decoder` parses the writer
    /// schema again, and the reader schema too when one is set, so a rebuild costs
    /// more than the registration it avoids. Measured on an 8 KB union-bearing
    /// schema: register 234us, build with a reader schema 380us. The rebuild fires
    /// once per batch, so it is a real cost on a block with many records, and
    /// removing it needs the upstream fix (apache/arrow-rs#10876) rather than more
    /// caching here.
    registered: RegisteredWriterSchema,
    reader_schema_json: Option<String>,
    batch_size: usize,
    /// Marker plus fingerprint, identical for every record in the block.
    prefix: [u8; 10],
    /// Reused across records so framing costs one copy, not one allocation.
    framed: Vec<u8>,
    /// When set, each batch is projected to this schema after decoding. See
    /// [`AvroBlockDecoder::with_rewrite_to`].
    rewrite_to: Option<SchemaRef>,
}

/// A writer schema that has been parsed and fingerprinted once.
///
/// Holds only data: a `SchemaStore` is a map from fingerprint to schema, and
/// cloning it copies that map rather than reparsing. Kept separate from the
/// decoder because the decoder carries per-stream state and must not be shared,
/// while this can be.
#[derive(Debug, Clone)]
pub struct RegisteredWriterSchema {
    store: SchemaStore,
    fingerprint: arrow_avro::schema::Fingerprint,
}

impl RegisteredWriterSchema {
    /// Parse and fingerprint `writer_schema_json`.
    pub fn new(writer_schema_json: &str) -> Result<Self> {
        let mut store = SchemaStore::new();
        let fingerprint = store
            .register(ArrowAvroSchema::new(writer_schema_json.to_string()))
            .map_err(|e| {
                CoreError::LogBlockError(format!("Failed to register block writer schema: {e}"))
            })?;
        Ok(Self { store, fingerprint })
    }
}

impl AvroBlockDecoder {
    /// Build a decoder that resolves the block up to `reader_schema_json`.
    ///
    /// A log block records the schema it was written with, which may predate a
    /// schema change on the table — an added column, or a column promoted from
    /// `int` to `long`. Handing the reader schema to the decoder resolves the
    /// block as it is read: absent columns are filled from their defaults and
    /// promoted columns arrive in the promoted type.
    ///
    /// Only promotions Avro itself defines are handled this way. A block whose
    /// writer schema differs in a way Avro does not allow is rejected here
    /// rather than silently mis-read.
    pub fn try_new_with_reader(
        writer_schema_json: &str,
        reader_schema_json: Option<&str>,
        batch_size: usize,
    ) -> Result<Self> {
        let registered = RegisteredWriterSchema::new(writer_schema_json)?;
        Self::try_new_with_registered(&registered, reader_schema_json, batch_size)
    }

    /// Build the inner arrow-avro decoder.
    ///
    /// Separate so a decoder can be rebuilt between batches without redoing the
    /// registration, which is the schema-sized half of the cost.
    fn build_inner(
        registered: &RegisteredWriterSchema,
        reader_schema_json: Option<&str>,
        batch_size: usize,
    ) -> Result<ArrowAvroDecoder> {
        let mut builder = ReaderBuilder::new()
            .with_writer_schema_store(registered.store.clone())
            .with_active_fingerprint(registered.fingerprint)
            .with_batch_size(batch_size);
        if let Some(reader_schema_json) = reader_schema_json {
            builder =
                builder.with_reader_schema(ArrowAvroSchema::new(reader_schema_json.to_string()));
        }
        builder
            .build_decoder()
            .map_err(|e| CoreError::LogBlockError(format!("Failed to build the Avro decoder: {e}")))
    }

    /// Build a decoder from a writer schema that is already registered.
    ///
    /// Registering parses the schema to fingerprint it, which is schema-sized work
    /// and measures around 240us on the metadata table's 8 KB record schema. A
    /// caller decoding many blocks that share one writer schema can register once
    /// and pass the result here. The decoder itself is still built per call, since
    /// it carries per-stream state and cannot be shared.
    pub fn try_new_with_registered(
        registered: &RegisteredWriterSchema,
        reader_schema_json: Option<&str>,
        batch_size: usize,
    ) -> Result<Self> {
        let fingerprint = registered.fingerprint;

        let arrow_avro::schema::Fingerprint::Rabin(rabin) = fingerprint else {
            return Err(CoreError::LogBlockError(format!(
                "Expected a Rabin fingerprint for the block writer schema, got {fingerprint:?}"
            )));
        };

        let decoder = Self::build_inner(registered, reader_schema_json, batch_size)?;

        let mut prefix = [0u8; 10];
        prefix[..2].copy_from_slice(&SINGLE_OBJECT_MAGIC);
        prefix[2..].copy_from_slice(&rabin.to_le_bytes());

        Ok(Self {
            decoder,
            registered: registered.clone(),
            reader_schema_json: reader_schema_json.map(str::to_string),
            batch_size,
            prefix,
            framed: Vec::new(),
            rewrite_to: None,
        })
    }

    /// The Arrow schema decoded records will carry.
    ///
    /// Taken from the decoder rather than converted from the Avro JSON, because
    /// `avro_to_arrow` does not handle named-type references and the metadata
    /// table's schema uses them.
    pub fn schema(&self) -> SchemaRef {
        self.decoder.schema()
    }

    /// Project each decoded batch to `schema` instead of resolving during the
    /// read.
    ///
    /// Hudi permits promotions Avro does not — a number, or anything with a
    /// logical type, to string. Avro refuses to build a reader for those, so a
    /// block carrying one is decoded at the schema it was written with and
    /// converted afterwards. Mirrors what the Java reader does when
    /// `recordNeedsRewriteForExtendedAvroTypePromotion` says so: read
    /// writer-to-writer, then promote.
    pub fn with_rewrite_to(mut self, schema: SchemaRef) -> Self {
        self.rewrite_to = Some(schema);
        self
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
            let batch = self.flush()?;
            // A flushed union decoder cannot be decoded into again, so the next
            // batch gets a fresh one. Only reached when a single block holds more
            // records than one batch, which is the case that fails outright
            // without this; a block inside one batch never takes this path.
            self.decoder = Self::build_inner(
                &self.registered,
                self.reader_schema_json.as_deref(),
                self.batch_size,
            )?;
            return Ok(batch);
        }
        Ok(None)
    }

    /// Drain whatever rows are held, if any.
    pub fn flush(&mut self) -> Result<Option<RecordBatch>> {
        let batch = self.decoder.flush().map_err(|e| {
            CoreError::LogBlockError(format!("Failed to flush decoded records: {e}"))
        })?;
        let batch = batch.map(normalize_utc_timestamps).transpose()?;
        match (batch, self.rewrite_to.as_ref()) {
            (Some(batch), Some(target)) => {
                crate::schema::batch_evolution::project_batch_to_schema(&batch, target).map(Some)
            }
            (batch, _) => Ok(batch),
        }
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
///
/// Top-level fields only. A timestamp nested in a struct, list or map keeps
/// arrow-avro's spelling; no fixture in the corpus has one, so recursing here
/// would be untested code standing in for a case nothing can currently
/// exercise. Not yet implemented, deliberately.
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
                // Relabeled, not converted: same instants, different spelling
                // of the same zone.
                let relabeled = cast(column, &DataType::Timestamp(*unit, Some("UTC".into())))
                    .map_err(CoreError::ArrowError)?;
                fields.push(
                    Field::new(
                        field.name(),
                        relabeled.data_type().clone(),
                        field.is_nullable(),
                    )
                    .with_metadata(field.metadata().clone()),
                );
                columns.push(relabeled);
            }
            _ => {
                fields.push(field.as_ref().clone());
                columns.push(column.clone());
            }
        }
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(CoreError::ArrowError)
}

#[cfg(test)]
mod multi_batch_tests {
    use super::*;
    use apache_avro::types::Value;

    /// A schema whose union has two or more non-null branches, which Arrow
    /// represents as a dense union.
    const UNION_SCHEMA: &str = r#"{"type":"record","name":"R","fields":[
        {"name":"id","type":"long"},
        {"name":"v","type":["null","int","string"]}]}"#;

    fn null_union_records(n: usize) -> Vec<Vec<u8>> {
        let schema = apache_avro::Schema::parse_str(UNION_SCHEMA).unwrap();
        (0..n as i64)
            .map(|i| {
                let mut rec = apache_avro::types::Record::new(&schema).unwrap();
                rec.put("id", i);
                rec.put("v", Value::Union(0, Box::new(Value::Null)));
                apache_avro::to_avro_datum(&schema, rec).unwrap()
            })
            .collect()
    }

    /// More records than one batch holds, with a dense union in the schema.
    ///
    /// `arrow-avro`'s union decoder drains its offsets on flush but keeps the
    /// per-branch counts that produced them, so without a fresh decoder the second
    /// batch emits offsets past the children it just emptied and fails with
    /// "Offsets must be non-negative and within the length of the Array". Every
    /// value here is null, because the schema alone is enough to trigger it.
    ///
    /// The batch size is small so the test is fast; the real ceiling was 1024, which
    /// is what a metadata log block exceeds.
    #[test]
    fn a_block_larger_than_one_batch_decodes_with_a_union_in_the_schema() {
        const BATCH: usize = 16;
        const N: usize = 100;
        let mut decoder =
            AvroBlockDecoder::try_new_with_reader(UNION_SCHEMA, Some(UNION_SCHEMA), BATCH).unwrap();

        let mut rows = 0usize;
        let mut batches = 0usize;
        for datum in &null_union_records(N) {
            if let Some(batch) = decoder
                .decode(datum)
                .expect("a record after a full batch must still decode")
            {
                batches += 1;
                rows += batch.num_rows();
            }
        }
        if let Some(batch) = decoder.flush().expect("the final flush must succeed") {
            batches += 1;
            rows += batch.num_rows();
        }
        assert_eq!(rows, N, "every record must come back");
        assert!(
            batches > 1,
            "the batch size must have forced more than one batch, or this proves nothing: \
             {batches} batch(es) for {N} records at a batch size of {BATCH}"
        );
    }

    /// Values survive the batch boundary, not just the row count.
    ///
    /// The all-null test above pins the crash but would still pass if a rebuilt
    /// decoder returned rows carrying the wrong branch or the wrong value, which is
    /// the failure a corrupt offsets buffer actually produces. This alternates the
    /// union's branches so every row's identity is checkable.
    #[test]
    fn values_survive_the_batch_boundary_with_a_union() {
        use arrow_array::cast::AsArray;

        const BATCH: usize = 16;
        const N: i64 = 100;
        let schema = apache_avro::Schema::parse_str(UNION_SCHEMA).unwrap();
        let datums: Vec<Vec<u8>> = (0..N)
            .map(|i| {
                let mut rec = apache_avro::types::Record::new(&schema).unwrap();
                rec.put("id", i);
                // Branch 1 is int, branch 2 is string: alternating them means a
                // decoder that mixed up offsets returns a detectably wrong row.
                let v = if i % 2 == 0 {
                    Value::Union(1, Box::new(Value::Int(i as i32)))
                } else {
                    Value::Union(2, Box::new(Value::String(format!("s{i}"))))
                };
                rec.put("v", v);
                apache_avro::to_avro_datum(&schema, rec).unwrap()
            })
            .collect();

        let mut decoder =
            AvroBlockDecoder::try_new_with_reader(UNION_SCHEMA, Some(UNION_SCHEMA), BATCH).unwrap();
        let mut ids: Vec<i64> = Vec::new();
        let take = |batch: arrow_array::RecordBatch, ids: &mut Vec<i64>| {
            let col = batch
                .column_by_name("id")
                .expect("the id column")
                .as_primitive::<arrow_array::types::Int64Type>();
            ids.extend(col.iter().flatten());
        };
        for datum in &datums {
            if let Some(batch) = decoder.decode(datum).unwrap() {
                take(batch, &mut ids);
            }
        }
        if let Some(batch) = decoder.flush().unwrap() {
            take(batch, &mut ids);
        }
        assert_eq!(
            ids,
            (0..N).collect::<Vec<_>>(),
            "every row must come back once, in order, across the batch boundary"
        );
    }

    /// The same, without a union, so the guard above is not the only thing keeping
    /// multi-batch decoding honest.
    #[test]
    fn a_block_larger_than_one_batch_decodes_without_a_union() {
        const SCHEMA: &str = r#"{"type":"record","name":"R","fields":[
            {"name":"id","type":"long"},
            {"name":"name","type":"string"}]}"#;
        let schema = apache_avro::Schema::parse_str(SCHEMA).unwrap();
        let datums: Vec<Vec<u8>> = (0..100i64)
            .map(|i| {
                let mut rec = apache_avro::types::Record::new(&schema).unwrap();
                rec.put("id", i);
                rec.put("name", format!("n{i}"));
                apache_avro::to_avro_datum(&schema, rec).unwrap()
            })
            .collect();
        let mut decoder = AvroBlockDecoder::try_new_with_reader(SCHEMA, Some(SCHEMA), 16).unwrap();
        let mut rows = 0usize;
        for d in &datums {
            if let Some(b) = decoder.decode(d).unwrap() {
                rows += b.num_rows();
            }
        }
        if let Some(b) = decoder.flush().unwrap() {
            rows += b.num_rows();
        }
        assert_eq!(rows, 100);
    }
}

#[cfg(test)]
mod tests {
    use super::AvroBlockDecoder;

    /// A block written before a column was promoted still reads at the promoted
    /// type. Avro defines int → long as a promotion, so the decoder resolves it
    /// while reading rather than leaving a narrow column to be reconciled later.
    #[test]
    fn test_reader_schema_promotes_int_to_long() {
        let writer = r#"{"type":"record","name":"r","fields":[{"name":"num","type":"int"}]}"#;
        let reader = r#"{"type":"record","name":"r","fields":[{"name":"num","type":"long"}]}"#;

        let mut decoder =
            AvroBlockDecoder::try_new_with_reader(writer, Some(reader), 1024).unwrap();
        decoder.decode(&[0x0E]).unwrap(); // int 7, zigzag encoded
        let batch = decoder.flush().unwrap().expect("a batch");

        assert_eq!(
            batch.schema().field(0).data_type(),
            &arrow_schema::DataType::Int64
        );
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .expect("promoted to i64");
        assert_eq!(col.value(0), 7);
    }

    /// Hudi writes nullable columns as `["null", T]`, so the promotion has to
    /// survive the union wrapper — which is the shape every real table has.
    #[test]
    fn test_reader_schema_promotes_through_a_nullable_union() {
        let writer = r#"{"type":"record","name":"r","fields":[{"name":"num","type":["null","int"],"default":null}]}"#;
        let reader = r#"{"type":"record","name":"r","fields":[{"name":"num","type":["null","long"],"default":null}]}"#;

        let mut decoder =
            AvroBlockDecoder::try_new_with_reader(writer, Some(reader), 1024).unwrap();
        decoder.decode(&[0x02, 0x0E]).unwrap(); // union branch 1, then int 7
        let batch = decoder.flush().unwrap().expect("a batch");

        assert_eq!(
            batch.schema().field(0).data_type(),
            &arrow_schema::DataType::Int64
        );
    }

    /// Without a reader schema the block reads at the schema it was written
    /// with, which is what a partial-update block relies on.
    #[test]
    fn test_no_reader_schema_keeps_the_writer_type() {
        let writer = r#"{"type":"record","name":"r","fields":[{"name":"num","type":"int"}]}"#;

        let mut decoder = AvroBlockDecoder::try_new_with_reader(writer, None, 1024).unwrap();
        decoder.decode(&[0x0E]).unwrap();
        let batch = decoder.flush().unwrap().expect("a batch");

        assert_eq!(
            batch.schema().field(0).data_type(),
            &arrow_schema::DataType::Int32
        );
    }
}
