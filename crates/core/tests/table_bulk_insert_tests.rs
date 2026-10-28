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

use std::fs;
use std::sync::Arc;

use arrow::array::{Int32Array, StringArray};
use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use hudi_core::error::CoreError;
use hudi_core::error::Result;
use hudi_core::table::{ReadOptions, Table};
use tempfile::tempdir;

fn write_table_properties(base_dir: &std::path::Path) -> Result<()> {
    let hoodie_dir = base_dir.join(".hoodie");
    fs::create_dir_all(&hoodie_dir)?;

    let properties = [
        "hoodie.table.name=test_table",
        "hoodie.table.type=COPY_ON_WRITE",
        "hoodie.table.version=6",
        "hoodie.timeline.layout.version=1",
        "hoodie.populate.meta.fields=false",
        "hoodie.base.file.format=parquet",
    ]
    .join("\n");

    fs::write(hoodie_dir.join("hoodie.properties"), properties)?;
    Ok(())
}

fn write_partitioned_table_properties(base_dir: &std::path::Path) -> Result<()> {
    let hoodie_dir = base_dir.join(".hoodie");
    fs::create_dir_all(&hoodie_dir)?;

    let properties = [
        "hoodie.table.name=test_table",
        "hoodie.table.type=COPY_ON_WRITE",
        "hoodie.table.version=6",
        "hoodie.timeline.layout.version=1",
        "hoodie.populate.meta.fields=false",
        "hoodie.base.file.format=parquet",
        "hoodie.table.partition.fields=region",
    ]
    .join("\n");

    fs::write(hoodie_dir.join("hoodie.properties"), properties)?;
    Ok(())
}

fn write_layout_v2_table_properties(base_dir: &std::path::Path) -> Result<()> {
    let hoodie_dir = base_dir.join(".hoodie");
    fs::create_dir_all(&hoodie_dir)?;

    let properties = [
        "hoodie.table.name=test_table",
        "hoodie.table.type=COPY_ON_WRITE",
        "hoodie.table.version=8",
        "hoodie.timeline.layout.version=2",
        "hoodie.populate.meta.fields=false",
        "hoodie.base.file.format=parquet",
    ]
    .join("\n");

    fs::write(hoodie_dir.join("hoodie.properties"), properties)?;
    Ok(())
}

#[tokio::test]
async fn append_only_writes_records_and_commit() -> Result<()> {
    let temp_dir = tempdir()?;
    write_table_properties(temp_dir.path())?;

    let mut table = Table::new(temp_dir.path().to_str().expect("valid temp path")).await?;

    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]);

    let batch = RecordBatch::try_new(
        schema.into(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["alice", "bob", "carol"])),
        ],
    )?;

    let result = table.append_only([batch]).await?;
    assert_eq!(result.num_rows, 3);
    assert_eq!(
        result.commit_relative_path,
        format!(".hoodie/{}.commit", result.instant)
    );

    let reopened = Table::new(temp_dir.path().to_str().expect("valid temp path")).await?;
    let records = reopened.read(&ReadOptions::new()).await?;
    assert_eq!(records.len(), 1);

    let read_batch = concat_batches(&records[0].schema(), &records)?;
    assert_eq!(read_batch.num_rows(), 3);

    let ids = read_batch
        .column_by_name("id")
        .and_then(|array| array.as_any().downcast_ref::<Int32Array>())
        .expect("expected id column");
    let names = read_batch
        .column_by_name("name")
        .and_then(|array| array.as_any().downcast_ref::<StringArray>())
        .expect("expected name column");

    assert_eq!(ids.values(), &[1, 2, 3]);
    assert_eq!(names.value(0), "alice");
    assert_eq!(names.value(1), "bob");
    assert_eq!(names.value(2), "carol");

    let commit_file = temp_dir.path().join(&result.commit_relative_path);
    assert!(commit_file.exists(), "commit file should exist");

    Ok(())
}

#[tokio::test]
async fn append_only_rejects_partitioned_tables() -> Result<()> {
    let temp_dir = tempdir()?;
    write_partitioned_table_properties(temp_dir.path())?;

    let mut table = Table::new(temp_dir.path().to_str().expect("valid temp path")).await?;

    let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
    let batch = RecordBatch::try_new(schema.into(), vec![Arc::new(Int32Array::from(vec![1]))])?;

    let error = table.append_only([batch]).await.unwrap_err();
    assert!(
        matches!(error, CoreError::Unsupported(message) if message.contains("unpartitioned append-only tables"))
    );

    Ok(())
}

#[tokio::test]
async fn append_only_writes_layout_v2_commit() -> Result<()> {
    let temp_dir = tempdir()?;
    write_layout_v2_table_properties(temp_dir.path())?;

    let mut table = Table::new(temp_dir.path().to_str().expect("valid temp path")).await?;

    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]);

    let batch = RecordBatch::try_new(
        schema.into(),
        vec![
            Arc::new(Int32Array::from(vec![7])),
            Arc::new(StringArray::from(vec!["delta"])),
        ],
    )?;

    let result = table.append_only([batch]).await?;
    assert!(result.commit_relative_path.starts_with(".hoodie/timeline/"));
    assert!(result.commit_relative_path.ends_with(".commit"));

    let commit_file = temp_dir.path().join(&result.commit_relative_path);
    assert!(commit_file.exists(), "layout v2 commit file should exist");

    Ok(())
}

#[tokio::test]
async fn append_only_writes_multiple_batches_as_one_file() -> Result<()> {
    let temp_dir = tempdir()?;
    write_table_properties(temp_dir.path())?;

    let mut table = Table::new(temp_dir.path().to_str().expect("valid temp path")).await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let first = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["alice", "bob"])),
        ],
    )?;
    let second = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![3])),
            Arc::new(StringArray::from(vec!["carol"])),
        ],
    )?;

    let result = table.append_only([first, second]).await?;
    assert_eq!(result.num_rows, 3);

    let reopened = Table::new(temp_dir.path().to_str().expect("valid temp path")).await?;
    let records = reopened.read(&ReadOptions::new()).await?;
    let read_batch = concat_batches(&records[0].schema(), &records)?;
    let ids = read_batch
        .column_by_name("id")
        .and_then(|array| array.as_any().downcast_ref::<Int32Array>())
        .expect("expected id column");
    assert_eq!(ids.values(), &[1, 2, 3]);

    Ok(())
}

#[tokio::test]
async fn append_only_rejects_empty_and_mismatched_batches() -> Result<()> {
    let temp_dir = tempdir()?;
    write_table_properties(temp_dir.path())?;

    let mut table = Table::new(temp_dir.path().to_str().expect("valid temp path")).await?;

    let error = table
        .append_only(Vec::<RecordBatch>::new())
        .await
        .unwrap_err();
    assert!(
        matches!(error, CoreError::Write(message) if message.contains("at least one RecordBatch"))
    );

    let ints = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![1]))],
    )?;
    let strings = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)])),
        vec![Arc::new(StringArray::from(vec!["1"]))],
    )?;
    let error = table.append_only([ints, strings]).await.unwrap_err();
    assert!(
        matches!(error, CoreError::Write(message) if message.contains("share the same schema"))
    );

    Ok(())
}

#[tokio::test]
async fn append_only_rejects_tables_with_meta_fields_or_metadata_table() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))])?;

    let meta_fields_dir = tempdir()?;
    write_table_properties(meta_fields_dir.path())?;
    let props = meta_fields_dir.path().join(".hoodie/hoodie.properties");
    let rewritten = fs::read_to_string(&props)?.replace(
        "hoodie.populate.meta.fields=false",
        "hoodie.populate.meta.fields=true\nhoodie.table.record.merge.strategy=append_only",
    );
    fs::write(&props, rewritten)?;
    let mut table = Table::new(meta_fields_dir.path().to_str().expect("valid temp path")).await?;
    let error = table.append_only([batch.clone()]).await.unwrap_err();
    assert!(
        matches!(error, CoreError::Unsupported(message) if message.contains("hoodie.populate.meta.fields=false"))
    );

    let mdt_dir = tempdir()?;
    write_table_properties(mdt_dir.path())?;
    fs::create_dir_all(mdt_dir.path().join(".hoodie/metadata/.hoodie"))?;
    fs::write(
        mdt_dir
            .path()
            .join(".hoodie/metadata/.hoodie/hoodie.properties"),
        "hoodie.table.name=test_table_metadata\n",
    )?;
    let mut table = Table::new(mdt_dir.path().to_str().expect("valid temp path")).await?;
    let error = table.append_only([batch]).await.unwrap_err();
    assert!(
        matches!(error, CoreError::Unsupported(message) if message.contains("without a metadata table"))
    );
    assert!(
        fs::read_dir(mdt_dir.path().join(".hoodie"))?
            .filter_map(|entry| entry.ok())
            .all(|entry| !entry.file_name().to_string_lossy().ends_with(".commit")),
        "a rejected append must not write a commit"
    );

    Ok(())
}

fn id_name_batch(ids: Vec<i32>, names: Vec<&str>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .expect("valid batch")
}

#[tokio::test]
async fn append_only_rejects_zero_rows_and_table_schema_mismatch() -> Result<()> {
    let temp_dir = tempdir()?;
    write_table_properties(temp_dir.path())?;
    let mut table = Table::new(temp_dir.path().to_str().expect("valid temp path")).await?;

    let error = table
        .append_only([id_name_batch(vec![], vec![])])
        .await
        .unwrap_err();
    assert!(matches!(error, CoreError::Write(message) if message.contains("at least one row")));

    table
        .append_only([id_name_batch(vec![1], vec!["alice"])])
        .await?;
    let ids_only = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![2]))],
    )?;
    let error = table.append_only([ids_only]).await.unwrap_err();
    assert!(
        matches!(error, CoreError::Write(message) if message.contains("does not match table schema"))
    );

    Ok(())
}

#[tokio::test]
async fn append_only_handle_reads_its_own_writes() -> Result<()> {
    let temp_dir = tempdir()?;
    write_table_properties(temp_dir.path())?;
    let mut table = Table::new(temp_dir.path().to_str().expect("valid temp path")).await?;

    table
        .append_only([id_name_batch(vec![1, 2], vec!["alice", "bob"])])
        .await?;
    table
        .append_only([id_name_batch(vec![3], vec!["carol"])])
        .await?;

    let rows: usize = table
        .read(&ReadOptions::new())
        .await?
        .iter()
        .map(RecordBatch::num_rows)
        .sum();
    assert_eq!(rows, 3);

    Ok(())
}

#[tokio::test]
async fn append_only_accepts_cleared_metadata_partitions_property() -> Result<()> {
    let temp_dir = tempdir()?;
    write_table_properties(temp_dir.path())?;
    let props = temp_dir.path().join(".hoodie/hoodie.properties");
    let mut content = fs::read_to_string(&props)?;
    content.push_str("\nhoodie.table.metadata.partitions=\n");
    fs::write(&props, content)?;
    let mut table = Table::new(temp_dir.path().to_str().expect("valid temp path")).await?;

    let result = table
        .append_only([id_name_batch(vec![1], vec!["alice"])])
        .await?;
    assert_eq!(result.num_rows, 1);

    Ok(())
}

#[tokio::test]
async fn append_only_writes_version_6_commit_metadata() -> Result<()> {
    let temp_dir = tempdir()?;
    write_table_properties(temp_dir.path())?;
    let mut table = Table::new(temp_dir.path().to_str().expect("valid temp path")).await?;

    let result = table
        .append_only([id_name_batch(vec![1, 2, 3], vec!["alice", "bob", "carol"])])
        .await?;

    let commit: serde_json::Value = serde_json::from_slice(&fs::read(
        temp_dir.path().join(&result.commit_relative_path),
    )?)
    .expect("v1 commit metadata is JSON");
    assert_eq!(commit["operationType"], "BULK_INSERT");
    let stats = commit["partitionToWriteStats"][""]
        .as_array()
        .expect("write stats under the empty partition path");
    assert_eq!(stats.len(), 1);
    let stat = &stats[0];
    let file_id = stat["fileId"].as_str().expect("fileId");
    assert_eq!(
        result.base_file_path,
        format!("{file_id}_0-0-0_{}.parquet", result.instant)
    );
    assert_eq!(stat["path"], result.base_file_path.as_str());
    assert_eq!(stat["partitionPath"], "");
    assert_eq!(stat["numWrites"], 3);
    assert_eq!(stat["numInserts"], 3);
    let file_size = fs::metadata(temp_dir.path().join(&result.base_file_path))?.len();
    assert_eq!(stat["fileSizeInBytes"], file_size);

    let schema: serde_json::Value = serde_json::from_str(
        commit["extraMetadata"]["schema"]
            .as_str()
            .expect("the commit records the write schema"),
    )
    .expect("the recorded schema is JSON");
    assert_eq!(schema["type"], "record");
    assert_eq!(
        schema["fields"],
        serde_json::json!([
            {"name": "id", "type": "int"},
            {"name": "name", "type": {"type": "string", "avro.java.string": "String"}},
        ])
    );

    Ok(())
}
