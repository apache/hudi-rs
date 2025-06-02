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

use datafusion::error::Result;
use datafusion::prelude::{DataFrame, SessionContext};
use hudi::HudiDataSource;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    let hudi = HudiDataSource::new("s3://hudi-demo/cow/v6_complexkeygen_hivestyle").await?;
    ctx.register_table("cow_v6_table", Arc::new(hudi))?;
    let df: DataFrame = ctx.sql("SELECT * from cow_v6_table").await?;
    assert_eq!(
        df.schema()
            .columns()
            .iter()
            .map(|c| c.name())
            .collect::<Vec<_>>(),
        vec![
            "_hoodie_commit_time",
            "_hoodie_commit_seqno",
            "_hoodie_record_key",
            "_hoodie_partition_path",
            "_hoodie_file_name",
            "id",
            "name",
            "isActive",
            "intField",
            "longField",
            "floatField",
            "doubleField",
            "decimalField",
            "dateField",
            "timestampField",
            "binaryField",
            "arrayField",
            "mapField",
            "structField",
            "byteField",
            "shortField",
        ]
    );
    assert_eq!(df.count().await?, 4);

    println!("SQL (DataFusion): read snapshot successfully!");
    Ok(())
}
