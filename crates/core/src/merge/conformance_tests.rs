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
use crate::config::HudiConfigs;
use crate::config::table::HudiTableConfig::{
    OrderingFields, PopulatesMetaFields, RecordMergeStrategy,
};
use crate::file_group::record_batches::RecordBatches;
use crate::merge::RecordMerger;
use crate::merge::record_batch_merger::RecordBatchMerger;
use crate::metadata::meta_field::MetaField;
use arrow_array::{Array, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use std::sync::Arc;

#[derive(Debug, Eq, PartialEq)]
struct ConformanceRow {
    record_key: String,
    commit_time: String,
    commit_seqno: String,
    ts: Option<i32>,
    value: i32,
}

type DataRow<'a> = (&'a str, &'a str, &'a str, Option<i32>, i32);

fn create_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(MetaField::CommitTime.as_ref(), DataType::Utf8, false),
        Field::new(MetaField::CommitSeqno.as_ref(), DataType::Utf8, false),
        Field::new(MetaField::RecordKey.as_ref(), DataType::Utf8, false),
        Field::new("ts", DataType::Int32, true),
        Field::new("value", DataType::Int32, false),
    ]))
}

fn append_only_configs() -> Arc<HudiConfigs> {
    Arc::new(HudiConfigs::new([
        (RecordMergeStrategy, "APPEND_ONLY"),
        (PopulatesMetaFields, "false"),
    ]))
}

fn overwrite_with_latest_configs() -> Arc<HudiConfigs> {
    Arc::new(HudiConfigs::new([
        (RecordMergeStrategy, "OVERWRITE_WITH_LATEST"),
        (PopulatesMetaFields, "true"),
        (OrderingFields, "ts"),
    ]))
}

fn data_batch(schema: SchemaRef, rows: &[DataRow<'_>]) -> RecordBatch {
    let commit_times: Vec<&str> = rows.iter().map(|row| row.0).collect();
    let commit_seqnos: Vec<&str> = rows.iter().map(|row| row.1).collect();
    let record_keys: Vec<&str> = rows.iter().map(|row| row.2).collect();
    let ordering_values: Vec<Option<i32>> = rows.iter().map(|row| row.3).collect();
    let values: Vec<i32> = rows.iter().map(|row| row.4).collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(commit_times)),
            Arc::new(StringArray::from(commit_seqnos)),
            Arc::new(StringArray::from(record_keys)),
            Arc::new(Int32Array::from(ordering_values)),
            Arc::new(Int32Array::from(values)),
        ],
    )
    .unwrap()
}

fn delete_batch(rows: &[(&str, i32)]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("recordKey", DataType::Utf8, false),
        Field::new("partitionPath", DataType::Utf8, false),
        Field::new("orderingVal", DataType::Int32, false),
    ]));
    let record_keys: Vec<&str> = rows.iter().map(|row| row.0).collect();
    let partition_paths: Vec<&str> = rows.iter().map(|_| "partition").collect();
    let ordering_values: Vec<i32> = rows.iter().map(|row| row.1).collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(record_keys)),
            Arc::new(StringArray::from(partition_paths)),
            Arc::new(Int32Array::from(ordering_values)),
        ],
    )
    .unwrap()
}

fn normalize_output(batch: &RecordBatch) -> Vec<ConformanceRow> {
    let schema = batch.schema();
    let commit_time_idx = schema.index_of(MetaField::CommitTime.as_ref()).unwrap();
    let commit_seqno_idx = schema.index_of(MetaField::CommitSeqno.as_ref()).unwrap();
    let record_key_idx = schema.index_of(MetaField::RecordKey.as_ref()).unwrap();
    let ts_idx = schema.index_of("ts").unwrap();
    let value_idx = schema.index_of("value").unwrap();

    let commit_times = batch.column(commit_time_idx);
    let commit_times = commit_times.as_any().downcast_ref::<StringArray>().unwrap();
    let commit_seqnos = batch.column(commit_seqno_idx);
    let commit_seqnos = commit_seqnos
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let record_keys = batch.column(record_key_idx);
    let record_keys = record_keys.as_any().downcast_ref::<StringArray>().unwrap();
    let timestamps = batch.column(ts_idx);
    let timestamps = timestamps.as_any().downcast_ref::<Int32Array>().unwrap();
    let values = batch.column(value_idx);
    let values = values.as_any().downcast_ref::<Int32Array>().unwrap();

    let mut rows: Vec<ConformanceRow> = (0..batch.num_rows())
        .map(|idx| ConformanceRow {
            record_key: record_keys.value(idx).to_string(),
            commit_time: commit_times.value(idx).to_string(),
            commit_seqno: commit_seqnos.value(idx).to_string(),
            ts: if timestamps.is_null(idx) {
                None
            } else {
                Some(timestamps.value(idx))
            },
            value: values.value(idx),
        })
        .collect();
    rows.sort_by(|left, right| {
        left.record_key
            .cmp(&right.record_key)
            .then_with(|| left.commit_time.cmp(&right.commit_time))
            .then_with(|| left.commit_seqno.cmp(&right.commit_seqno))
            .then_with(|| left.ts.cmp(&right.ts))
            .then_with(|| left.value.cmp(&right.value))
    });
    rows
}

fn expected_rows(rows: &[DataRow<'_>]) -> Vec<ConformanceRow> {
    let schema = create_schema();
    let batch = data_batch(schema, rows);
    normalize_output(&batch)
}

fn run_conformance_suite<F>(create_merger: F, schema: SchemaRef)
where
    F: Fn(Arc<HudiConfigs>) -> Arc<dyn RecordMerger>,
{
    empty_input(create_merger(overwrite_with_latest_configs()).as_ref());
    append_only(
        create_merger(append_only_configs()).as_ref(),
        schema.clone(),
    );
    overwrite_with_latest(
        create_merger(overwrite_with_latest_configs()).as_ref(),
        schema.clone(),
    );
    deletes_only_remove_matching(
        create_merger(overwrite_with_latest_configs()).as_ref(),
        schema.clone(),
    );
    mixed_insert_update_delete(
        create_merger(overwrite_with_latest_configs()).as_ref(),
        schema.clone(),
    );
    null_ordering_values(
        create_merger(overwrite_with_latest_configs()).as_ref(),
        schema,
    );
}

fn empty_input(merger: &dyn RecordMerger) {
    let merged = merger.merge(RecordBatches::new()).unwrap();

    assert_eq!(merged.num_rows(), 0);
    assert_eq!(merged.schema(), merger.output_schema().clone());
}

fn append_only(merger: &dyn RecordMerger, schema: SchemaRef) {
    let batch1 = data_batch(
        schema.clone(),
        &[
            ("20240101000000", "001", "k1", Some(1), 10),
            ("20240101000000", "002", "k2", Some(2), 20),
        ],
    );
    let batch2 = data_batch(
        schema,
        &[
            ("20240102000000", "003", "k1", Some(3), 30),
            ("20240102000000", "004", "k3", Some(4), 40),
        ],
    );

    let merged = merger
        .merge(RecordBatches::new_with_data_batches([batch1, batch2]))
        .unwrap();

    assert_eq!(
        normalize_output(&merged),
        expected_rows(&[
            ("20240101000000", "001", "k1", Some(1), 10),
            ("20240102000000", "003", "k1", Some(3), 30),
            ("20240101000000", "002", "k2", Some(2), 20),
            ("20240102000000", "004", "k3", Some(4), 40),
        ])
    );
}

fn overwrite_with_latest(merger: &dyn RecordMerger, schema: SchemaRef) {
    let batch1 = data_batch(
        schema.clone(),
        &[
            ("20240101000000", "001", "k1", Some(1), 10),
            ("20240101000000", "002", "k2", Some(5), 20),
            ("20240101000000", "003", "k3", Some(3), 30),
        ],
    );
    let batch2 = data_batch(
        schema,
        &[
            ("20240102000000", "004", "k1", Some(4), 40),
            ("20240102000000", "005", "k2", Some(2), 50),
            ("20240102000000", "006", "k3", Some(3), 60),
        ],
    );

    let merged = merger
        .merge(RecordBatches::new_with_data_batches([batch1, batch2]))
        .unwrap();

    assert_eq!(
        normalize_output(&merged),
        expected_rows(&[
            ("20240102000000", "004", "k1", Some(4), 40),
            ("20240101000000", "002", "k2", Some(5), 20),
            ("20240102000000", "006", "k3", Some(3), 60),
        ])
    );
}

fn deletes_only_remove_matching(merger: &dyn RecordMerger, schema: SchemaRef) {
    let batch = data_batch(
        schema,
        &[
            ("20240101000000", "001", "k1", Some(1), 10),
            ("20240101000000", "002", "k2", Some(2), 20),
            ("20240101000000", "003", "k3", Some(3), 30),
        ],
    );
    let deletes = delete_batch(&[("k2", 5), ("missing", 5)]);
    let mut batches = RecordBatches::new_with_data_batches([batch]);
    batches.push_delete_batch(deletes, "20240102000000".to_string());

    let merged = merger.merge(batches).unwrap();

    assert_eq!(
        normalize_output(&merged),
        expected_rows(&[
            ("20240101000000", "001", "k1", Some(1), 10),
            ("20240101000000", "003", "k3", Some(3), 30),
        ])
    );
}

fn mixed_insert_update_delete(merger: &dyn RecordMerger, schema: SchemaRef) {
    let batch1 = data_batch(
        schema.clone(),
        &[
            ("20240101000000", "001", "k1", Some(1), 10),
            ("20240101000000", "002", "k2", Some(1), 20),
            ("20240101000000", "003", "k3", Some(1), 30),
        ],
    );
    let batch2 = data_batch(
        schema,
        &[
            ("20240102000000", "004", "k1", Some(4), 40),
            ("20240102000000", "005", "k4", Some(2), 50),
        ],
    );
    let deletes = delete_batch(&[("k1", 3), ("k2", 2)]);
    let mut batches = RecordBatches::new_with_data_batches([batch1, batch2]);
    batches.push_delete_batch(deletes, "20240103000000".to_string());

    let merged = merger.merge(batches).unwrap();

    assert_eq!(
        normalize_output(&merged),
        expected_rows(&[
            ("20240102000000", "004", "k1", Some(4), 40),
            ("20240101000000", "003", "k3", Some(1), 30),
            ("20240102000000", "005", "k4", Some(2), 50),
        ])
    );
}

fn null_ordering_values(merger: &dyn RecordMerger, schema: SchemaRef) {
    let batch1 = data_batch(
        schema.clone(),
        &[
            ("20240101000000", "001", "k1", Some(1), 10),
            ("20240101000000", "002", "k2", None, 20),
            ("20240101000000", "003", "k3", Some(3), 30),
        ],
    );
    let batch2 = data_batch(
        schema,
        &[
            ("20240102000000", "004", "k1", None, 40),
            ("20240102000000", "005", "k2", Some(5), 50),
        ],
    );

    let merged = merger
        .merge(RecordBatches::new_with_data_batches([batch1, batch2]))
        .unwrap();

    assert_eq!(
        normalize_output(&merged),
        expected_rows(&[
            ("20240101000000", "001", "k1", Some(1), 10),
            ("20240102000000", "005", "k2", Some(5), 50),
            ("20240101000000", "003", "k3", Some(3), 30),
        ])
    );
}

#[test]
fn record_batch_merger_satisfies_conformance() {
    let schema = create_schema();
    let merger_schema = schema.clone();
    run_conformance_suite(
        |configs| Arc::new(RecordBatchMerger::new(merger_schema.clone(), configs)),
        schema,
    );
}
