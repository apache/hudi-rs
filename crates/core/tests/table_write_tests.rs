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

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use hudi_core::error::CoreError;
use hudi_core::table::{ReadOptions, Table};
use tempfile::tempdir;

fn sample_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
            Arc::new(Int64Array::from(vec![1, 2, 3])),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn test_create_and_append_then_read() {
    let dir = tempdir().unwrap();
    let base_uri = dir.path().to_str().unwrap();

    let mut table = Table::create(base_uri)
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .create()
        .await
        .unwrap();

    let result = table.append([sample_batch()]).await.unwrap();
    assert_eq!(result.num_rows, 3);
    assert!(
        dir.path().join(&result.base_file_path).is_file(),
        "base file should exist at {}",
        result.base_file_path
    );
    assert!(
        dir.path().join(&result.commit_relative_path).is_file(),
        "commit file should exist at {}",
        result.commit_relative_path
    );

    let batches = table.read(&ReadOptions::new()).await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);
}

#[tokio::test]
async fn test_create_rejects_existing_table() {
    let dir = tempdir().unwrap();
    let base_uri = dir.path().to_str().unwrap();

    Table::create(base_uri)
        .with_table_name("trips")
        .create()
        .await
        .unwrap();

    let err = Table::create(base_uri)
        .with_table_name("trips")
        .create()
        .await
        .unwrap_err();
    assert!(matches!(err, CoreError::Write(_)));
}

#[tokio::test]
async fn test_upsert_overwrite_delete_not_yet_implemented() {
    let dir = tempdir().unwrap();
    let base_uri = dir.path().to_str().unwrap();
    let mut table = Table::create(base_uri)
        .with_table_name("trips")
        .create()
        .await
        .unwrap();

    assert!(matches!(
        table.upsert([sample_batch()]).await.unwrap_err(),
        CoreError::Unsupported(_)
    ));
    assert!(matches!(
        table.overwrite([sample_batch()]).await.unwrap_err(),
        CoreError::Unsupported(_)
    ));
    assert!(matches!(
        table.delete("id = 'a'").await.unwrap_err(),
        CoreError::Unsupported(_)
    ));
}
