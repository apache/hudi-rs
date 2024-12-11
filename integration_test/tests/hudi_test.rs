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

use datafusion::{
    arrow::{
        array::{self, ArrowPrimitiveType, AsArray},
        datatypes::{DataType, Field, Int64Type, Schema, TimeUnit},
    },
    prelude::*,
    scalar::ScalarValue,
};
use hudi::HudiDataSource;
use hudi_tests::TestTable;

#[derive(PartialEq, Debug)]
struct Record {
    id: i32,
    name: String,
    is_active: bool,
}

async fn prepare_session_context(
    path: &str,
    mut options: Vec<(&str, &str)>,
    tablename: &str,
) -> SessionContext {
    let config = SessionConfig::new()
        .set_bool("datafusion.catalog.information_schema", true)
        .set(
            "datafusion.sql_parser.enable_ident_normalization",
            ScalarValue::from(false),
        );
    let ctx = SessionContext::new_with_config(config);

    options.append(&mut vec![("allow_http", "true")]);

    let hudi = HudiDataSource::new_with_options(path, options)
        .await
        .expect("Failed to create HudiDataSource");

    ctx.register_table(tablename, Arc::new(hudi))
        .expect("Failed to register table");
    ctx
}

async fn get_primitive_from_dataframe<T>(df: &DataFrame) -> <T as ArrowPrimitiveType>::Native
where
    T: ArrowPrimitiveType,
{
    let result = df
        .clone()
        .collect()
        .await
        .expect("Failed to collect result");
    let values = result
        .first()
        .unwrap()
        .column(0)
        .as_primitive::<T>()
        .values();

    *values.first().unwrap()
}

#[tokio::test]
async fn test_datafusion_read_tables() {
    for (t, n_cols, n_rows) in &[
        (TestTable::V6ComplexkeygenHivestyle, 21, 4),
        (TestTable::V6Nonpartitioned, 21, 4),
        (TestTable::V6SimplekeygenHivestyleNoMetafields, 21, 4),
        (TestTable::V6SimplekeygenNonhivestyle, 21, 4),
        (TestTable::V6TimebasedkeygenNonhivestyle, 22, 4),
    ] {
        let ctx = prepare_session_context(&t.s3_path(), vec![], t.as_ref()).await;
        let df_rows = ctx
            .sql(&format!("select count(*) from {}", t.as_ref()))
            .await
            .expect("Failed to get number of rows");
        let df_cols = ctx
            .sql(&format!(
                "select count(column_name) from information_schema.columns where table_name = '{}'",
                t.as_ref()
            ))
            .await
            .expect("Failed to get number of columns");

        let rows = get_primitive_from_dataframe::<Int64Type>(&df_rows).await;
        let cols = get_primitive_from_dataframe::<Int64Type>(&df_cols).await;

        assert_eq!(rows, *n_rows);
        assert_eq!(cols, *n_cols);

        let expected_data = vec![
            Record {
                id: 1,
                name: "Alice".to_string(),
                is_active: false,
            },
            Record {
                id: 2,
                name: "Bob".to_string(),
                is_active: false,
            },
            Record {
                id: 3,
                name: "Carol".to_string(),
                is_active: true,
            },
            Record {
                id: 4,
                name: "Diana".to_string(),
                is_active: true,
            },
        ];

        let mut actual_data: Vec<Record> = vec![];
        let df = ctx
            .sql(&format!("select id, name, isActive from {}", t.as_ref()))
            .await
            .unwrap();

        df.collect().await.unwrap().iter().for_each(|rb| {
            let id = rb
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<array::Int32Array>();
            let name = rb
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<array::StringArray>();

            let is_active = rb
                .column_by_name("isActive")
                .unwrap()
                .as_any()
                .downcast_ref::<array::BooleanArray>();

            for ((id, name), is_active) in id
                .unwrap()
                .values()
                .iter()
                .zip(name.unwrap().iter())
                .zip(is_active.unwrap().values().iter())
            {
                actual_data.push(Record {
                    id: *id,
                    name: name.unwrap().to_string(),
                    is_active,
                });
            }
        });

        assert!(actual_data
            .iter()
            .all(|record| expected_data.contains(record)));

        let df_schema = ctx
            .sql(&format!(
                "select 
                    _hoodie_commit_time,
                    _hoodie_commit_seqno,
                    _hoodie_record_key,
                    _hoodie_partition_path,
                    _hoodie_file_name,
                    id,
                    name,
                    isActive,
                    intField,
                    longField,
                    floatField,
                    doubleField,
                    decimalField,
                    dateField,
                    timestampField,
                    binaryField,
                    structField.field2 as structField_field2 
                from {}",
                t.as_ref()
            ))
            .await
            .unwrap();

        assert_eq!(df_schema.schema().as_arrow(), &get_expected_table_schema());
    }
}

fn get_expected_table_schema() -> Schema {
    Schema::new(vec![
        Field::new("_hoodie_commit_time", DataType::Utf8, true),
        Field::new("_hoodie_commit_seqno", DataType::Utf8, true),
        Field::new("_hoodie_record_key", DataType::Utf8, true),
        Field::new("_hoodie_partition_path", DataType::Utf8, true),
        Field::new("_hoodie_file_name", DataType::Utf8, true),
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("isActive", DataType::Boolean, true),
        Field::new("intField", DataType::Int32, true),
        Field::new("longField", DataType::Int64, true),
        Field::new("floatField", DataType::Float32, true),
        Field::new("doubleField", DataType::Float64, true),
        Field::new("decimalField", DataType::Decimal128(10, 5), true),
        Field::new("dateField", DataType::Date32, true),
        Field::new(
            "timestampField",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new("binaryField", DataType::Binary, true),
        Field::new("structField_field2", DataType::Int32, true),
    ])
}
