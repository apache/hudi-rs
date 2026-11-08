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
use hudi_core::table::{QueryType, ReadOptions, Table};
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

    // Instants other writers mint before this write must order before it.
    let earlier_instant = chrono::Local::now().format("%Y%m%d%H%M%S%3f").to_string();
    let result = table.append([batch]).await?;
    assert_eq!((result.num_writes, result.num_inserts), (3, 3));
    assert_eq!(result.completion_time, None);
    let instant = result.requested_time.as_deref().expect("append commits");
    assert!(
        chrono::NaiveDateTime::parse_from_str(instant, "%Y%m%d%H%M%S%3f").is_ok()
            && instant >= earlier_instant.as_str(),
        "instant {instant} must be a yyyyMMddHHmmssSSS time at or after {earlier_instant}"
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

    let commit_file = temp_dir.path().join(format!(".hoodie/{instant}.commit"));
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

    let error = table.append([batch]).await.unwrap_err();
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

    let result = table.append([batch]).await?;
    let commit_file = temp_dir.path().join(format!(
        ".hoodie/timeline/{}_{}.commit",
        result.requested_time.as_deref().expect("append commits"),
        result
            .completion_time
            .as_deref()
            .expect("layout v2 records completion")
    ));
    assert!(commit_file.exists(), "layout v2 commit file should exist");

    let reopened = Table::new(temp_dir.path().to_str().expect("valid temp path")).await?;
    let records = reopened.read(&ReadOptions::new()).await?;
    let read_batch = concat_batches(&records[0].schema(), &records)?;
    assert_eq!(read_batch.num_rows(), 1);
    let ids = read_batch
        .column_by_name("id")
        .and_then(|array| array.as_any().downcast_ref::<Int32Array>())
        .expect("expected id column");
    let names = read_batch
        .column_by_name("name")
        .and_then(|array| array.as_any().downcast_ref::<StringArray>())
        .expect("expected name column");
    assert_eq!(ids.values(), &[7]);
    assert_eq!(names.value(0), "delta");

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

    let result = table.append([first, second]).await?;
    assert_eq!((result.num_writes, result.num_inserts), (3, 3));

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
    for write_properties in [write_table_properties, write_layout_v2_table_properties] {
        let temp_dir = tempdir()?;
        write_properties(temp_dir.path())?;

        let mut table = Table::new(temp_dir.path().to_str().expect("valid temp path")).await?;

        let error = table.append(Vec::<RecordBatch>::new()).await.unwrap_err();
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
        let error = table.append([ints, strings]).await.unwrap_err();
        assert!(
            matches!(error, CoreError::Write(message) if message.contains("share the same schema"))
        );
    }

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
    let error = table.append([batch.clone()]).await.unwrap_err();
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
    let error = table.append([batch]).await.unwrap_err();
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
    for write_properties in [write_table_properties, write_layout_v2_table_properties] {
        let temp_dir = tempdir()?;
        write_properties(temp_dir.path())?;
        let mut table = Table::new(temp_dir.path().to_str().expect("valid temp path")).await?;

        let error = table
            .append([id_name_batch(vec![], vec![])])
            .await
            .unwrap_err();
        assert!(matches!(error, CoreError::Write(message) if message.contains("at least one row")));

        table
            .append([id_name_batch(vec![1], vec!["alice"])])
            .await?;
        let ids_only = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![2]))],
        )?;
        let error = table.append([ids_only]).await.unwrap_err();
        assert!(
            matches!(error, CoreError::Write(message) if message.contains("does not match table schema"))
        );
    }

    Ok(())
}

#[tokio::test]
async fn append_only_handle_reads_its_own_writes() -> Result<()> {
    let temp_dir = tempdir()?;
    write_table_properties(temp_dir.path())?;
    let mut table = Table::new(temp_dir.path().to_str().expect("valid temp path")).await?;

    table
        .append([id_name_batch(vec![1, 2], vec!["alice", "bob"])])
        .await?;
    table
        .append([id_name_batch(vec![3], vec!["carol"])])
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
        .append([id_name_batch(vec![1], vec!["alice"])])
        .await?;
    assert_eq!((result.num_writes, result.num_inserts), (1, 1));

    Ok(())
}

#[tokio::test]
async fn append_only_writes_version_6_commit_metadata() -> Result<()> {
    let temp_dir = tempdir()?;
    write_table_properties(temp_dir.path())?;
    let mut table = Table::new(temp_dir.path().to_str().expect("valid temp path")).await?;

    let result = table
        .append([id_name_batch(vec![1, 2, 3], vec!["alice", "bob", "carol"])])
        .await?;

    let instant = result.requested_time.as_deref().expect("append commits");
    let commit: serde_json::Value = serde_json::from_slice(&fs::read(
        temp_dir.path().join(format!(".hoodie/{instant}.commit")),
    )?)
    .expect("v1 commit metadata is JSON");
    assert_eq!(commit["operationType"], "BULK_INSERT");
    let stats = commit["partitionToWriteStats"][""]
        .as_array()
        .expect("write stats under the empty partition path");
    assert_eq!(stats.len(), 1);
    let stat = &stats[0];
    let file_id = stat["fileId"].as_str().expect("fileId");
    let base_file = format!("{file_id}_0-0-0_{instant}.parquet");
    assert_eq!(stat["path"], base_file.as_str());
    assert_eq!(stat["partitionPath"], "");
    assert_eq!(stat["numWrites"], 3);
    assert_eq!(stat["numInserts"], 3);
    let file_size = fs::metadata(temp_dir.path().join(&base_file))?.len();
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

fn append_property(base_dir: &std::path::Path, property: &str) -> Result<()> {
    let props = base_dir.join(".hoodie/hoodie.properties");
    let mut content = fs::read_to_string(&props)?;
    content.push('\n');
    content.push_str(property);
    fs::write(&props, content)?;
    Ok(())
}

fn has_completed_commit(base_dir: &std::path::Path) -> Result<bool> {
    Ok(fs::read_dir(base_dir.join(".hoodie"))?
        .filter_map(|entry| entry.ok())
        .any(|entry| entry.file_name().to_string_lossy().ends_with(".commit")))
}

#[tokio::test]
async fn append_requires_append_only_merge_strategy_below_version_8() -> Result<()> {
    for (write_properties, rejected) in [
        (
            write_table_properties as fn(&std::path::Path) -> Result<()>,
            true,
        ),
        (write_layout_v2_table_properties, false),
    ] {
        let temp_dir = tempdir()?;
        write_properties(temp_dir.path())?;
        // A table without meta fields must be append-only, so only one that
        // populates them can declare another merge strategy.
        let props = temp_dir.path().join(".hoodie/hoodie.properties");
        let rewritten = fs::read_to_string(&props)?.replace(
            "hoodie.populate.meta.fields=false",
            "hoodie.populate.meta.fields=true",
        );
        fs::write(&props, rewritten)?;
        append_property(
            temp_dir.path(),
            "hoodie.table.record.merge.strategy=overwrite_with_latest",
        )?;
        let mut table = Table::new(temp_dir.path().to_str().expect("valid temp path")).await?;

        let result = table.append([id_name_batch(vec![1], vec!["alice"])]).await;
        if !rejected {
            // Version 8 and 9 append is an insert, valid on upsertable tables.
            result?;
            continue;
        }
        let error = result.unwrap_err();
        assert!(
            matches!(error, CoreError::Unsupported(message) if message.contains("requires hoodie.table.record.merge.strategy=append_only"))
        );
        assert!(!has_completed_commit(temp_dir.path())?);
        assert!(table.read(&ReadOptions::new()).await?.is_empty());
    }

    Ok(())
}

#[tokio::test]
async fn append_only_rejects_metadata_partitions_property_without_metadata_dir() -> Result<()> {
    let temp_dir = tempdir()?;
    write_table_properties(temp_dir.path())?;
    append_property(temp_dir.path(), "hoodie.table.metadata.partitions=files")?;
    assert!(!temp_dir.path().join(".hoodie/metadata").exists());
    let mut table = Table::new(temp_dir.path().to_str().expect("valid temp path")).await?;

    let error = table
        .append([id_name_batch(vec![1], vec!["alice"])])
        .await
        .unwrap_err();
    assert!(
        matches!(error, CoreError::Unsupported(message) if message.contains("without a metadata table"))
    );
    assert!(!has_completed_commit(temp_dir.path())?);

    Ok(())
}

async fn ids_read_with(base_uri: &str, options: ReadOptions) -> Result<Vec<i32>> {
    let table = Table::new(base_uri).await?;
    let mut ids: Vec<i32> = table
        .read(&options)
        .await?
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name("id")
                .and_then(|array| array.as_any().downcast_ref::<Int32Array>())
                .expect("expected id column")
                .values()
                .to_vec()
        })
        .collect();
    ids.sort();
    Ok(ids)
}

#[tokio::test]
async fn append_only_version_6_commits_support_time_travel_and_incremental_reads() -> Result<()> {
    let temp_dir = tempdir()?;
    write_table_properties(temp_dir.path())?;
    let uri = temp_dir.path().to_str().expect("valid temp path");
    let mut table = Table::new(uri).await?;

    let first = table
        .append([id_name_batch(vec![1, 2], vec!["alice", "bob"])])
        .await?;
    let second = table
        .append([id_name_batch(vec![3], vec!["carol"])])
        .await?;
    let third = table
        .append([id_name_batch(vec![4, 5], vec!["dave", "erin"])])
        .await?;

    let as_of = |instant: &str| ReadOptions::new().with_as_of_timestamp(instant);
    assert_eq!(
        ids_read_with(uri, as_of(first.requested_time.as_deref().unwrap())).await?,
        vec![1, 2]
    );
    assert_eq!(
        ids_read_with(uri, as_of(second.requested_time.as_deref().unwrap())).await?,
        vec![1, 2, 3]
    );
    assert_eq!(
        ids_read_with(uri, as_of(third.requested_time.as_deref().unwrap())).await?,
        vec![1, 2, 3, 4, 5]
    );

    let incremental = |start: &str| {
        ReadOptions::new()
            .with_query_type(QueryType::Incremental)
            .with_start_timestamp(start)
    };
    assert_eq!(
        ids_read_with(
            uri,
            incremental(first.requested_time.as_deref().unwrap())
                .with_end_timestamp(second.requested_time.as_deref().unwrap())
        )
        .await?,
        vec![3]
    );
    assert_eq!(
        ids_read_with(uri, incremental(first.requested_time.as_deref().unwrap())).await?,
        vec![3, 4, 5]
    );
    assert_eq!(
        ids_read_with(uri, incremental(third.requested_time.as_deref().unwrap())).await?,
        Vec::<i32>::new()
    );

    Ok(())
}

fn narrow_types_batch(ids: Vec<i32>) -> RecordBatch {
    use arrow::array::{Int16Array, LargeStringArray, ListArray};
    use arrow::datatypes::Int32Type;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("small", DataType::Int16, false),
        Field::new("label", DataType::LargeUtf8, false),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            false,
        ),
    ]));
    let small: Vec<i16> = ids.iter().map(|id| (*id as i16) * -100).collect();
    let labels: Vec<String> = ids.iter().map(|id| format!("label{id}")).collect();
    let tags = ListArray::from_iter_primitive::<Int32Type, _, _>(
        ids.iter().map(|id| Some(vec![Some(*id), Some(id * 10)])),
    );
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(Int16Array::from(small)),
            Arc::new(LargeStringArray::from(labels)),
            Arc::new(tags),
        ],
    )
    .expect("valid batch")
}

/// Rows rendered as text, sorted, so the check does not depend on the Arrow
/// type each column reads back as.
fn rendered_rows(batches: &[RecordBatch]) -> Vec<String> {
    use arrow::util::display::{ArrayFormatter, FormatOptions};

    let options = FormatOptions::default();
    let mut rows = Vec::new();
    for batch in batches {
        let formatters: Vec<_> = batch
            .columns()
            .iter()
            .map(|column| ArrayFormatter::try_new(column.as_ref(), &options).expect("formatter"))
            .collect();
        for row in 0..batch.num_rows() {
            let cells: Vec<String> = formatters
                .iter()
                .map(|f| f.value(row).to_string())
                .collect();
            rows.push(cells.join("|"));
        }
    }
    rows.sort();
    rows
}

/// Int16, LargeUtf8 and list item nullability do not survive the Avro commit
/// schema, so the table schema differs from the batch that wrote it.
#[tokio::test]
async fn append_only_accepts_types_the_avro_commit_schema_widens() -> Result<()> {
    for write_properties in [write_table_properties, write_layout_v2_table_properties] {
        let temp_dir = tempdir()?;
        write_properties(temp_dir.path())?;
        let uri = temp_dir.path().to_str().expect("valid temp path");
        let mut table = Table::new(uri).await?;

        table.append([narrow_types_batch(vec![1, 2])]).await?;
        table.append([narrow_types_batch(vec![3])]).await?;

        let records = Table::new(uri).await?.read(&ReadOptions::new()).await?;
        assert_eq!(
            rendered_rows(&records),
            vec![
                "1|-100|label1|[1, 10]",
                "2|-200|label2|[2, 20]",
                "3|-300|label3|[3, 30]",
            ]
        );

        let retyped = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("small", DataType::Utf8, false),
                Field::new("label", DataType::LargeUtf8, false),
                Field::new(
                    "tags",
                    DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                    false,
                ),
            ])),
            narrow_types_batch(vec![4])
                .columns()
                .iter()
                .enumerate()
                .map(|(index, column)| {
                    if index == 1 {
                        Arc::new(StringArray::from(vec!["x"])) as _
                    } else {
                        column.clone()
                    }
                })
                .collect(),
        )?;
        let error = table.append([retyped]).await.unwrap_err();
        assert!(
            matches!(error, CoreError::Write(message) if message.contains("does not match table schema"))
        );
    }

    Ok(())
}

#[tokio::test]
async fn append_only_without_table_version_takes_the_version_6_path() -> Result<()> {
    use hudi_core::config::internal::HudiInternalConfig::SkipConfigValidation;

    let temp_dir = tempdir()?;
    write_table_properties(temp_dir.path())?;
    let props = temp_dir.path().join(".hoodie/hoodie.properties");
    let content = fs::read_to_string(&props)?.replace("hoodie.table.version=6\n", "");
    fs::write(&props, content)?;
    let uri = temp_dir.path().to_str().expect("valid temp path");
    let mut table = Table::new_with_options(uri, [(SkipConfigValidation, "true")]).await?;

    let result = table
        .append([id_name_batch(vec![1, 2], vec!["alice", "bob"])])
        .await?;
    let instant = result.requested_time.as_deref().expect("append commits");
    assert!(
        temp_dir
            .path()
            .join(format!(".hoodie/{instant}.commit"))
            .is_file()
    );
    let rows: usize = table
        .read(&ReadOptions::new())
        .await?
        .iter()
        .map(RecordBatch::num_rows)
        .sum();
    assert_eq!(rows, 2);

    Ok(())
}
