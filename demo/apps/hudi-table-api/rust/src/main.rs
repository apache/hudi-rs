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

use arrow::compute::concat_batches;
use hudi::config::util::empty_filters;
use hudi::error::Result;
use hudi::table::builder::TableBuilder as HudiTableBuilder;

#[tokio::main]
async fn main() -> Result<()> {
    for url in [
        "s3://hudi-demo/cow/v6_complexkeygen_hivestyle",
        "s3://hudi-demo/mor/parquet/v6_complexkeygen_hivestyle",
    ] {
        let hudi_table = HudiTableBuilder::from_base_uri(url).build().await?;
        let batches = hudi_table.read_snapshot(empty_filters()).await?;

        let batch = concat_batches(&batches[0].schema(), &batches)?;
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name())
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
        assert_eq!(batch.num_rows(), 4);
    }

    println!("Table API (Rust): read snapshot successfully!");
    Ok(())
}
