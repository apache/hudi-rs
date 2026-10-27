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
        matches!(error, CoreError::InvalidValue(message) if message.contains("at least one RecordBatch"))
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
        matches!(error, CoreError::InvalidValue(message) if message.contains("share the same schema"))
    );

    Ok(())
}
