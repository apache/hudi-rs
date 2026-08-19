use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use hudi_core::table::{ReadOptions, Table};
use std::sync::Arc;
use tempfile::tempdir;

fn b(rows: &[(&str, i64)]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

/// Overwrite exclusions must survive archival: with no cleaner, replaced file
/// groups stay on disk, so archiving their replacecommit would resurrect them.
#[tokio::test]
async fn test_archival_never_passes_replacecommit() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("probe")
        .with_record_key_fields(["id"])
        .with_option("hoodie.keep.min.commits", "4")
        .with_option("hoodie.keep.max.commits", "6")
        .create()
        .await
        .unwrap();
    table
        .append([b(&[("a", 1), ("b", 2), ("c", 3)])])
        .await
        .unwrap();
    table.overwrite([b(&[("z", 0)])]).await.unwrap();
    for i in 0..12 {
        table
            .append([b(&[(format!("n{i}").as_str(), i)])])
            .await
            .unwrap();
    }
    let rows: usize = table
        .read(&ReadOptions::new())
        .await
        .unwrap()
        .iter()
        .map(RecordBatch::num_rows)
        .sum();
    assert_eq!(rows, 13, "same handle");
    let fresh =
        hudi_core::table::builder::TableBuilder::from_base_uri(dir.path().to_str().unwrap())
            .build()
            .await
            .unwrap();
    let rows: usize = fresh
        .read(&ReadOptions::new())
        .await
        .unwrap()
        .iter()
        .map(RecordBatch::num_rows)
        .sum();
    assert_eq!(rows, 13, "fresh handle");
}
