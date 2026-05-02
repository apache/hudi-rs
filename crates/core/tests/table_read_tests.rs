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
//! Integration tests for reading Hudi tables.
//!
//! This module contains tests for snapshot and time-travel queries,
//! organized by table version (v6, v8+) and query type.

use arrow::compute::concat_batches;
use arrow_array::RecordBatch;
use futures::StreamExt;
use futures::stream::BoxStream;
use hudi_core::config::read::HudiReadConfig;
use hudi_core::error::Result;
use hudi_core::table::{QueryType, ReadOptions, Table};
use hudi_test::{QuickstartTripsTable, SampleTable};

async fn collect_stream_batches(
    mut stream: BoxStream<'static, Result<RecordBatch>>,
) -> Result<Vec<RecordBatch>> {
    let mut batches = Vec::new();
    while let Some(result) = stream.next().await {
        batches.push(result?);
    }
    Ok(batches)
}

/// Test helper module for v6 tables (pre-1.0 spec)
mod v6_tables {
    use super::*;

    mod snapshot_queries {
        use super::*;

        #[tokio::test]
        async fn test_empty_table() -> Result<()> {
            for base_url in SampleTable::V6Empty.urls() {
                let hudi_table = Table::new(base_url.path()).await?;
                let records = hudi_table.read(&ReadOptions::new()).await?;
                assert!(records.is_empty());
            }
            Ok(())
        }

        #[tokio::test]
        async fn test_non_partitioned() -> Result<()> {
            for base_url in SampleTable::V6Nonpartitioned.urls() {
                let hudi_table = Table::new(base_url.path()).await?;
                let records = hudi_table.read(&ReadOptions::new()).await?;
                let schema = &records[0].schema();
                let records = concat_batches(schema, &records)?;

                let sample_data = SampleTable::sample_data_order_by_id(&records);
                assert_eq!(
                    sample_data,
                    vec![
                        (1, "Alice", false),
                        (2, "Bob", false),
                        (3, "Carol", true),
                        (4, "Diana", true),
                    ]
                );
            }
            Ok(())
        }

        #[tokio::test]
        async fn test_non_partitioned_read_optimized() -> Result<()> {
            let base_url = SampleTable::V6Nonpartitioned.url_to_mor_parquet();
            let hudi_table = Table::new_with_options(
                base_url.path(),
                [(HudiReadConfig::UseReadOptimizedMode.as_ref(), "true")],
            )
            .await?;
            let commit_timestamps = hudi_table
                .timeline
                .completed_commits
                .iter()
                .map(|i| i.timestamp.as_str())
                .collect::<Vec<_>>();
            let latest_commit = commit_timestamps.last().unwrap();
            let records = hudi_table
                .read(&ReadOptions::new().with_as_of_timestamp(latest_commit))
                .await?;
            let schema = &records[0].schema();
            let records = concat_batches(schema, &records)?;

            let sample_data = SampleTable::sample_data_order_by_id(&records);
            assert_eq!(
                sample_data,
                vec![
                    (1, "Alice", true), // this was updated to false in a log file and not to be read out
                    (2, "Bob", false),
                    (3, "Carol", true),
                    (4, "Diana", true), // this was inserted in a base file and should be read out
                ]
            );
            Ok(())
        }

        #[tokio::test]
        async fn test_non_partitioned_rollback() -> Result<()> {
            let base_url = SampleTable::V6NonpartitionedRollback.url_to_mor_parquet();
            let hudi_table = Table::new(base_url.path()).await?;
            let records = hudi_table.read(&ReadOptions::new()).await?;
            let schema = &records[0].schema();
            let records = concat_batches(schema, &records)?;

            let sample_data = SampleTable::sample_data_order_by_id(&records);
            assert_eq!(
                sample_data,
                vec![
                    (1, "Alice", true), // this was updated to false then rolled back to true
                    (2, "Bob", true),   // this was updated to true after rollback
                    (3, "Carol", true),
                ]
            );
            Ok(())
        }

        #[tokio::test]
        async fn test_complex_keygen_hive_style_with_filters() -> Result<()> {
            for base_url in SampleTable::V6ComplexkeygenHivestyle.urls() {
                let hudi_table = Table::new(base_url.path()).await?;

                let filters = vec![
                    ("byteField", ">=", "10"),
                    ("byteField", "<", "20"),
                    ("shortField", "!=", "100"),
                ];
                let records = hudi_table
                    .read(&ReadOptions::new().with_filters(filters)?)
                    .await?;
                let schema = &records[0].schema();
                let records = concat_batches(schema, &records)?;

                let sample_data = SampleTable::sample_data_order_by_id(&records);
                assert_eq!(sample_data, vec![(1, "Alice", false), (3, "Carol", true),]);
            }
            Ok(())
        }

        #[tokio::test]
        async fn test_simple_keygen_hivestyle_no_metafields() -> Result<()> {
            for base_url in SampleTable::V6SimplekeygenHivestyleNoMetafields.urls() {
                let hudi_table = Table::new(base_url.path()).await?;
                let records = hudi_table.read(&ReadOptions::new()).await?;
                let schema = &records[0].schema();
                let records = concat_batches(schema, &records)?;

                let sample_data = SampleTable::sample_data_order_by_id(&records);
                assert_eq!(
                    sample_data,
                    vec![
                        (1, "Alice", false),
                        (2, "Bob", false),
                        (3, "Carol", true),
                        (4, "Diana", true),
                    ]
                )
            }
            Ok(())
        }
    }

    mod time_travel_queries {
        use super::*;

        #[tokio::test]
        async fn test_simple_keygen_nonhivestyle_time_travel() -> Result<()> {
            for base_url in SampleTable::V6SimplekeygenNonhivestyle.urls() {
                let hudi_table = Table::new(base_url.path()).await?;
                let commit_timestamps = hudi_table
                    .timeline
                    .completed_commits
                    .iter()
                    .map(|i| i.timestamp.as_str())
                    .collect::<Vec<_>>();
                let first_commit = commit_timestamps[0];
                let records = hudi_table
                    .read(&ReadOptions::new().with_as_of_timestamp(first_commit))
                    .await?;
                let schema = &records[0].schema();
                let records = concat_batches(schema, &records)?;

                let sample_data = SampleTable::sample_data_order_by_id(&records);
                assert_eq!(
                    sample_data,
                    vec![(1, "Alice", true), (2, "Bob", false), (3, "Carol", true),]
                );
            }
            Ok(())
        }
    }

    mod mor_log_file_queries {
        use super::*;

        #[tokio::test]
        async fn test_quickstart_trips_inserts_updates() -> Result<()> {
            let base_url = QuickstartTripsTable::V6Trips8I1U.url_to_mor_avro();
            let hudi_table = Table::new(base_url.path()).await?;

            let updated_rider = "rider-D";

            // verify updated record as of the latest commit
            let records = hudi_table.read(&ReadOptions::new()).await?;
            let schema = &records[0].schema();
            let records = concat_batches(schema, &records)?;
            let uuid_rider_and_fare = QuickstartTripsTable::uuid_rider_and_fare(&records)
                .into_iter()
                .filter(|(_, rider, _)| rider == updated_rider)
                .collect::<Vec<_>>();
            assert_eq!(uuid_rider_and_fare.len(), 1);
            assert_eq!(
                uuid_rider_and_fare[0].0,
                "9909a8b1-2d15-4d3d-8ec9-efc48c536a00"
            );
            assert_eq!(uuid_rider_and_fare[0].2, 25.0);

            // verify updated record as of the first commit
            let commit_timestamps = hudi_table
                .timeline
                .completed_commits
                .iter()
                .map(|i| i.timestamp.as_str())
                .collect::<Vec<_>>();
            let first_commit = commit_timestamps[0];
            let records = hudi_table
                .read(&ReadOptions::new().with_as_of_timestamp(first_commit))
                .await?;
            let schema = &records[0].schema();
            let records = concat_batches(schema, &records)?;
            let uuid_rider_and_fare = QuickstartTripsTable::uuid_rider_and_fare(&records)
                .into_iter()
                .filter(|(_, rider, _)| rider == updated_rider)
                .collect::<Vec<_>>();
            assert_eq!(uuid_rider_and_fare.len(), 1);
            assert_eq!(
                uuid_rider_and_fare[0].0,
                "9909a8b1-2d15-4d3d-8ec9-efc48c536a00"
            );
            assert_eq!(uuid_rider_and_fare[0].2, 33.9);

            Ok(())
        }

        #[tokio::test]
        async fn test_quickstart_trips_inserts_deletes() -> Result<()> {
            let base_url = QuickstartTripsTable::V6Trips8I3D.url_to_mor_avro();
            let hudi_table = Table::new(base_url.path()).await?;

            let deleted_riders = ["rider-A", "rider-C", "rider-D"];

            // verify deleted record as of the latest commit
            let records = hudi_table.read(&ReadOptions::new()).await?;
            let schema = &records[0].schema();
            let records = concat_batches(schema, &records)?;
            let riders = QuickstartTripsTable::uuid_rider_and_fare(&records)
                .into_iter()
                .map(|(_, rider, _)| rider)
                .collect::<Vec<_>>();
            assert!(
                riders
                    .iter()
                    .all(|rider| { !deleted_riders.contains(&rider.as_str()) })
            );

            // verify deleted record as of the first commit
            let commit_timestamps = hudi_table
                .timeline
                .completed_commits
                .iter()
                .map(|i| i.timestamp.as_str())
                .collect::<Vec<_>>();
            let first_commit = commit_timestamps[0];
            let records = hudi_table
                .read(&ReadOptions::new().with_as_of_timestamp(first_commit))
                .await?;
            let schema = &records[0].schema();
            let records = concat_batches(schema, &records)?;
            let mut uuid_rider_and_fare = QuickstartTripsTable::uuid_rider_and_fare(&records)
                .into_iter()
                .filter(|(_, rider, _)| deleted_riders.contains(&rider.as_str()))
                .collect::<Vec<_>>();
            uuid_rider_and_fare.sort_unstable_by_key(|(_, rider, _)| rider.to_string());
            assert_eq!(uuid_rider_and_fare.len(), 3);
            assert_eq!(uuid_rider_and_fare[0].1, "rider-A");
            assert_eq!(uuid_rider_and_fare[0].2, 19.10);
            assert_eq!(uuid_rider_and_fare[1].1, "rider-C");
            assert_eq!(uuid_rider_and_fare[1].2, 27.70);
            assert_eq!(uuid_rider_and_fare[2].1, "rider-D");
            assert_eq!(uuid_rider_and_fare[2].2, 33.90);

            Ok(())
        }
    }

    mod incremental_queries {
        use super::*;

        #[tokio::test]
        async fn test_empty_table() -> Result<()> {
            for base_url in SampleTable::V6Empty.urls() {
                let hudi_table = Table::new(base_url.path()).await?;
                let records = hudi_table
                    .read(
                        &ReadOptions::new()
                            .with_query_type(QueryType::Incremental)
                            .with_start_timestamp("0"),
                    )
                    .await?;
                assert!(records.is_empty())
            }
            Ok(())
        }

        #[tokio::test]
        async fn test_simplekeygen_nonhivestyle_overwritetable() -> Result<()> {
            for base_url in SampleTable::V6SimplekeygenNonhivestyleOverwritetable.urls() {
                let hudi_table = Table::new(base_url.path()).await?;
                let commit_timestamps = hudi_table
                    .timeline
                    .completed_commits
                    .iter()
                    .map(|i| i.timestamp.as_str())
                    .collect::<Vec<_>>();
                assert_eq!(commit_timestamps.len(), 3);
                let first_commit = commit_timestamps[0];
                let second_commit = commit_timestamps[1];
                let third_commit = commit_timestamps[2];

                // read records changed from the beginning to the 1st commit
                let records = hudi_table
                    .read(
                        &ReadOptions::new()
                            .with_query_type(QueryType::Incremental)
                            .with_start_timestamp("19700101000000")
                            .with_end_timestamp(first_commit),
                    )
                    .await?;
                let schema = &records[0].schema();
                let records = concat_batches(schema, &records)?;
                let sample_data = SampleTable::sample_data_order_by_id(&records);
                assert_eq!(
                    sample_data,
                    vec![(1, "Alice", true), (2, "Bob", false), (3, "Carol", true),],
                    "Should return 3 records inserted in the 1st commit"
                );

                // read records changed from the 1st to the 2nd commit
                let records = hudi_table
                    .read(
                        &ReadOptions::new()
                            .with_query_type(QueryType::Incremental)
                            .with_start_timestamp(first_commit)
                            .with_end_timestamp(second_commit),
                    )
                    .await?;
                let schema = &records[0].schema();
                let records = concat_batches(schema, &records)?;
                let sample_data = SampleTable::sample_data_order_by_id(&records);
                assert_eq!(
                    sample_data,
                    vec![(1, "Alice", false), (4, "Diana", true),],
                    "Should return 2 records inserted or updated in the 2nd commit"
                );

                // read records changed from the 2nd to the 3rd commit
                let records = hudi_table
                    .read(
                        &ReadOptions::new()
                            .with_query_type(QueryType::Incremental)
                            .with_start_timestamp(second_commit)
                            .with_end_timestamp(third_commit),
                    )
                    .await?;
                let schema = &records[0].schema();
                let records = concat_batches(schema, &records)?;
                let sample_data = SampleTable::sample_data_order_by_id(&records);
                assert_eq!(
                    sample_data,
                    vec![(4, "Diana", false),],
                    "Should return 1 record insert-overwritten in the 3rd commit"
                );

                // read records changed from the 1st commit
                let records = hudi_table
                    .read(
                        &ReadOptions::new()
                            .with_query_type(QueryType::Incremental)
                            .with_start_timestamp(first_commit),
                    )
                    .await?;
                let schema = &records[0].schema();
                let records = concat_batches(schema, &records)?;
                let sample_data = SampleTable::sample_data_order_by_id(&records);
                assert_eq!(
                    sample_data,
                    vec![(4, "Diana", false),],
                    "Should return 1 record insert-overwritten in the 3rd commit"
                );

                // read records changed from the 3rd commit
                let records = hudi_table
                    .read(
                        &ReadOptions::new()
                            .with_query_type(QueryType::Incremental)
                            .with_start_timestamp(third_commit),
                    )
                    .await?;
                assert!(
                    records.is_empty(),
                    "Should return 0 record as it's the latest commit"
                );
            }
            Ok(())
        }
    }
}

/// Test helper module for v8 tables (1.0 spec)
mod v8_tables {
    use super::*;

    mod snapshot_queries {
        use super::*;

        #[tokio::test]
        async fn test_empty_table() -> Result<()> {
            let base_url = SampleTable::V8Empty.url_to_cow();
            let hudi_table = Table::new(base_url.path()).await?;
            let records = hudi_table.read(&ReadOptions::new()).await?;
            assert!(records.is_empty());
            Ok(())
        }

        #[tokio::test]
        async fn test_non_partitioned() -> Result<()> {
            let base_url = SampleTable::V8Nonpartitioned.url_to_cow();
            let hudi_table = Table::new(base_url.path()).await?;
            let records = hudi_table.read(&ReadOptions::new()).await?;
            let schema = &records[0].schema();
            let records = concat_batches(schema, &records)?;

            let sample_data = SampleTable::sample_data_order_by_id(&records);
            assert_eq!(
                sample_data,
                vec![
                    (1, "Alice", false),
                    (2, "Bob", false),
                    (3, "Carol", true),
                    (4, "Diana", true),
                ]
            );
            Ok(())
        }

        #[tokio::test]
        async fn test_complex_keygen_hive_style() -> Result<()> {
            let base_url = SampleTable::V8ComplexkeygenHivestyle.url_to_cow();
            let hudi_table = Table::new(base_url.path()).await?;

            let records = hudi_table.read(&ReadOptions::new()).await?;
            let schema = &records[0].schema();
            let records = concat_batches(schema, &records)?;

            let sample_data = SampleTable::sample_data_order_by_id(&records);
            assert_eq!(
                sample_data,
                vec![
                    (1, "Alice", false),
                    (2, "Bob", false),
                    (3, "Carol", true),
                    (4, "Diana", true),
                ]
            );
            Ok(())
        }

        #[tokio::test]
        async fn test_simple_keygen_nonhivestyle() -> Result<()> {
            let base_url = SampleTable::V8SimplekeygenNonhivestyle.url_to_cow();
            let hudi_table = Table::new(base_url.path()).await?;

            let records = hudi_table.read(&ReadOptions::new()).await?;
            let schema = &records[0].schema();
            let records = concat_batches(schema, &records)?;

            let sample_data = SampleTable::sample_data_order_by_id(&records);
            assert_eq!(
                sample_data,
                vec![
                    (1, "Alice", false),
                    (2, "Bob", false),
                    (3, "Carol", true),
                    (4, "Diana", true),
                ]
            );
            Ok(())
        }

        #[tokio::test]
        async fn test_simple_keygen_hivestyle_no_metafields() -> Result<()> {
            let base_url = SampleTable::V8SimplekeygenHivestyleNoMetafields.url_to_cow();
            let hudi_table = Table::new(base_url.path()).await?;

            let records = hudi_table.read(&ReadOptions::new()).await?;
            let schema = &records[0].schema();
            let records = concat_batches(schema, &records)?;

            let sample_data = SampleTable::sample_data_order_by_id(&records);
            assert_eq!(
                sample_data,
                vec![
                    (1, "Alice", false),
                    (2, "Bob", false),
                    (3, "Carol", true),
                    (4, "Diana", true),
                ]
            );
            Ok(())
        }
    }

    /// MOR log file tests for v8 tables
    mod mor_log_file_queries {
        use super::*;

        #[tokio::test]
        async fn test_quickstart_trips_inserts_updates_deletes() -> Result<()> {
            // V8Trips8I3U1D: 8 inserts, 3 updates (A, J, G fare=0), 2 deletes (F, J)
            let base_url = QuickstartTripsTable::V8Trips8I3U1D.url_to_mor_avro();
            let hudi_table = Table::new(base_url.path()).await?;

            let deleted_riders = ["rider-F", "rider-J"];

            // verify deleted records are not present in latest snapshot
            let records = hudi_table.read(&ReadOptions::new()).await?;
            let schema = &records[0].schema();
            let records = concat_batches(schema, &records)?;
            let uuid_rider_and_fare = QuickstartTripsTable::uuid_rider_and_fare(&records);
            let riders: Vec<_> = uuid_rider_and_fare
                .iter()
                .map(|(_, rider, _)| rider.as_str())
                .collect();

            // Deleted riders should not be present
            assert!(
                riders
                    .iter()
                    .all(|rider| { !deleted_riders.contains(rider) })
            );

            // Should have 6 active riders (8 - 2 deleted)
            assert_eq!(riders.len(), 6);

            // Verify updated fares (rider-A and rider-G have fare=0)
            let rider_a = uuid_rider_and_fare
                .iter()
                .find(|(_, r, _)| r == "rider-A")
                .expect("rider-A should exist");
            assert_eq!(rider_a.2, 0.0, "rider-A fare should be updated to 0");

            let rider_g = uuid_rider_and_fare
                .iter()
                .find(|(_, r, _)| r == "rider-G")
                .expect("rider-G should exist");
            assert_eq!(rider_g.2, 0.0, "rider-G fare should be updated to 0");

            // verify deleted records were present in first commit (before updates/deletes)
            let commit_timestamps = hudi_table
                .timeline
                .completed_commits
                .iter()
                .map(|i| i.timestamp.as_str())
                .collect::<Vec<_>>();
            let first_commit = commit_timestamps[0];
            let records = hudi_table
                .read(&ReadOptions::new().with_as_of_timestamp(first_commit))
                .await?;
            let schema = &records[0].schema();
            let records = concat_batches(schema, &records)?;
            let mut uuid_rider_and_fare = QuickstartTripsTable::uuid_rider_and_fare(&records)
                .into_iter()
                .filter(|(_, rider, _)| deleted_riders.contains(&rider.as_str()))
                .collect::<Vec<_>>();
            uuid_rider_and_fare.sort_unstable_by_key(|(_, rider, _)| rider.to_string());

            // Both deleted riders should be present before delete
            assert_eq!(uuid_rider_and_fare.len(), 2);
            assert_eq!(uuid_rider_and_fare[0].1, "rider-F");
            assert_eq!(uuid_rider_and_fare[0].2, 34.15);
            assert_eq!(uuid_rider_and_fare[1].1, "rider-J");
            assert_eq!(uuid_rider_and_fare[1].2, 17.85);

            Ok(())
        }

        #[tokio::test]
        async fn test_file_group_reader_read_file_slice_mor_applies_filters_and_projection_after_merge()
        -> Result<()> {
            // Direct FileGroupReader path on a MOR slice with log files. The
            // reader merges base + log records first; only afterwards does
            // `apply_eager_options` apply `filters` (as a row mask) and
            // `projection` (column narrowing). Verifies the post-merge
            // hand-off is wired correctly: schema narrows, rows are filtered,
            // and the count matches the table-level read with the same options.
            let base_url = QuickstartTripsTable::V8Trips8I3U1D.url_to_mor_avro();
            let hudi_table = Table::new(base_url.path()).await?;
            let all_slices = hudi_table.get_file_slices(&ReadOptions::new()).await?;
            let mor_slice = all_slices
                .iter()
                .find(|fs| fs.has_log_file())
                .expect("V8Trips8I3U1D MOR fixture should have at least one slice with log files");

            let fg_reader = hudi_table.create_file_group_reader_with_options(
                None,
                std::iter::empty::<(&str, &str)>(),
                std::iter::empty::<(&str, &str)>(),
            )?;
            // Sanity: read the merged slice unfiltered so we can pick a rider
            // present in this slice and assert the filter actually narrows it.
            let unfiltered = fg_reader
                .read_file_slice(mor_slice, &ReadOptions::new())
                .await?;
            assert!(unfiltered.num_rows() > 0);
            let unfiltered_riders = unfiltered
                .column_by_name("rider")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();
            let target_rider = unfiltered_riders.value(0).to_string();

            let options = ReadOptions::new()
                .with_filters([("rider", "=", target_rider.as_str())])?
                .with_projection(["rider", "fare"]);
            let merged = fg_reader.read_file_slice(mor_slice, &options).await?;

            // Schema narrowed to projection.
            let field_names: Vec<_> = merged
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
            assert_eq!(field_names, vec!["rider", "fare"]);
            // Filter applied post-merge: only the target rider's row survives,
            // and there's exactly one row per rider in the merged result.
            assert_eq!(merged.num_rows(), 1);
            let riders = merged
                .column_by_name("rider")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();
            assert_eq!(riders.value(0), target_rider);
            Ok(())
        }
    }

    /// Streaming query tests for v8 tables
    mod streaming_queries {
        use super::*;
        use hudi_core::table::ReadOptions;

        #[tokio::test]
        async fn test_read_snapshot_stream_empty_table() -> Result<()> {
            let base_url = SampleTable::V8Empty.url_to_cow();
            let hudi_table = Table::new(base_url.path()).await?;
            let options = ReadOptions::new().with_projection(["txn_id", "txn_type", "txn_ts"]);
            let mut stream = hudi_table.read_stream(&options).await?;

            // Collect all batches from stream
            let mut batches = Vec::new();
            while let Some(result) = stream.next().await {
                batches.push(result?);
            }
            assert!(batches.is_empty(), "Empty table should produce no batches");
            Ok(())
        }

        #[tokio::test]
        async fn test_read_snapshot_stream_basic() -> Result<()> {
            let base_url = SampleTable::V8Nonpartitioned.url_to_cow();
            let hudi_table = Table::new(base_url.path()).await?;
            let options = ReadOptions::new();
            let mut stream = hudi_table.read_stream(&options).await?;

            // Collect all batches from stream
            let mut batches = Vec::new();
            while let Some(result) = stream.next().await {
                batches.push(result?);
            }

            assert!(!batches.is_empty(), "Should produce at least one batch");

            // Concatenate batches and verify data
            let schema = &batches[0].schema();
            let records = concat_batches(schema, &batches)?;

            let sample_data = SampleTable::sample_data_order_by_id(&records);
            assert_eq!(
                sample_data,
                vec![
                    (1, "Alice", false),
                    (2, "Bob", false),
                    (3, "Carol", true),
                    (4, "Diana", true),
                ]
            );
            Ok(())
        }

        #[tokio::test]
        async fn test_read_snapshot_stream_with_batch_size() -> Result<()> {
            let base_url = SampleTable::V8Nonpartitioned.url_to_cow();
            let hudi_table = Table::new(base_url.path()).await?;

            // Request small batch size
            let options = ReadOptions::new().with_batch_size(1)?;
            let mut stream = hudi_table.read_stream(&options).await?;

            // Collect all batches from stream
            let mut batches = Vec::new();
            while let Some(result) = stream.next().await {
                batches.push(result?);
            }

            // With batch_size=1 and 4 rows, we expect multiple batches, but the
            // exact number depends on both the batch_size setting and the Parquet
            // file's internal row group structure.
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total_rows, 4, "Total rows should match expected count");
            Ok(())
        }

        #[tokio::test]
        async fn test_read_snapshot_stream_with_partition_filters() -> Result<()> {
            let base_url = SampleTable::V8ComplexkeygenHivestyle.url_to_cow();
            let hudi_table = Table::new(base_url.path()).await?;

            let options = ReadOptions::new().with_filters([
                ("byteField", ">=", "10"),
                ("byteField", "<", "20"),
                ("shortField", "!=", "100"),
            ])?;
            let mut stream = hudi_table.read_stream(&options).await?;

            // Collect all batches from stream
            let mut batches = Vec::new();
            while let Some(result) = stream.next().await {
                batches.push(result?);
            }

            assert!(
                !batches.is_empty(),
                "Should produce at least one batch for the given partition filters"
            );
            let schema = &batches[0].schema();
            let records = concat_batches(schema, &batches)?;

            let sample_data = SampleTable::sample_data_order_by_id(&records);
            assert_eq!(sample_data, vec![(1, "Alice", false), (3, "Carol", true),]);
            Ok(())
        }

        #[tokio::test]
        async fn test_read_file_slice_stream_basic() -> Result<()> {
            let base_url = SampleTable::V8Nonpartitioned.url_to_cow();
            let hudi_table = Table::new(base_url.path()).await?;

            // Get file slices first
            let file_slices = hudi_table.get_file_slices(&ReadOptions::new()).await?;
            assert!(
                !file_slices.is_empty(),
                "Should have at least one file slice"
            );

            let fg_reader = hudi_table.create_file_group_reader_with_options(
                None,
                std::iter::empty::<(&str, &str)>(),
                std::iter::empty::<(&str, &str)>(),
            )?;
            let options = ReadOptions::new();
            let file_slice = &file_slices[0];
            let mut stream = fg_reader
                .read_file_slice_stream(file_slice, &options)
                .await?;

            // Collect all batches from stream
            let mut batches = Vec::new();
            while let Some(result) = stream.next().await {
                batches.push(result?);
            }

            assert!(!batches.is_empty(), "Should produce at least one batch");

            // Verify we got records
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert!(total_rows > 0, "Should read at least one row");
            Ok(())
        }

        #[tokio::test]
        async fn test_read_file_slice_stream_with_batch_size() -> Result<()> {
            let base_url = SampleTable::V8Nonpartitioned.url_to_cow();
            let hudi_table = Table::new(base_url.path()).await?;

            let file_slices = hudi_table.get_file_slices(&ReadOptions::new()).await?;
            let file_slice = &file_slices[0];

            let fg_reader = hudi_table.create_file_group_reader_with_options(
                None,
                std::iter::empty::<(&str, &str)>(),
                std::iter::empty::<(&str, &str)>(),
            )?;
            // Test with small batch size
            let options = ReadOptions::new().with_batch_size(1)?;
            let mut stream = fg_reader
                .read_file_slice_stream(file_slice, &options)
                .await?;

            let mut batches = Vec::new();
            while let Some(result) = stream.next().await {
                batches.push(result?);
            }

            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total_rows, 4, "Should read all 4 rows");
            Ok(())
        }

        #[tokio::test]
        async fn test_read_snapshot_stream_mor_with_log_files() -> Result<()> {
            // Test MOR table with log files - should still work (falls back to collect+merge)
            // V8Trips8I3U1D: 8 inserts, 3 updates (A, J, G fare=0), 2 deletes (F, J)
            let base_url = QuickstartTripsTable::V8Trips8I3U1D.url_to_mor_avro();
            let hudi_table = Table::new(base_url.path()).await?;

            let options = ReadOptions::new();
            let mut stream = hudi_table.read_stream(&options).await?;

            let mut batches = Vec::new();
            while let Some(result) = stream.next().await {
                batches.push(result?);
            }

            assert!(!batches.is_empty(), "Should produce batches from MOR table");

            // Verify total row count - should have 6 rows (8 inserts - 2 deletes)
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total_rows, 6, "Should have 6 rows (8 inserts - 2 deleted)");

            // Verify deleted riders are not present
            let schema = &batches[0].schema();
            let records = concat_batches(schema, &batches)?;
            let uuid_rider_and_fare = QuickstartTripsTable::uuid_rider_and_fare(&records);
            let riders: Vec<_> = uuid_rider_and_fare
                .iter()
                .map(|(_, rider, _)| rider.as_str())
                .collect();

            let deleted_riders = ["rider-F", "rider-J"];
            assert!(
                riders.iter().all(|rider| !deleted_riders.contains(rider)),
                "Deleted riders should not be present in streaming results"
            );

            Ok(())
        }
    }
}

/// Test helper module for v9 tables (1.1 spec)
mod v9_tables {
    use super::*;
    use arrow_array::{Int64Array, StringArray};

    fn txn_rows(records: &[RecordBatch]) -> Vec<(String, String, i64)> {
        let mut rows = Vec::new();
        for record_batch in records {
            let txn_ids = record_batch
                .column_by_name("txn_id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let txn_types = record_batch
                .column_by_name("txn_type")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let txn_timestamps = record_batch
                .column_by_name("txn_ts")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();

            for i in 0..record_batch.num_rows() {
                rows.push((
                    txn_ids.value(i).to_string(),
                    txn_types.value(i).to_string(),
                    txn_timestamps.value(i),
                ));
            }
        }
        rows.sort_unstable();
        rows
    }

    async fn read_txn_rows_from_snapshot(hudi_table: &Table) -> Result<Vec<(String, String, i64)>> {
        let records = hudi_table.read(&ReadOptions::new()).await?;
        Ok(txn_rows(&records))
    }

    async fn read_txn_rows_as_of(
        hudi_table: &Table,
        timestamp: &str,
    ) -> Result<Vec<(String, String, i64)>> {
        let records = hudi_table
            .read(&ReadOptions::new().with_as_of_timestamp(timestamp))
            .await?;
        Ok(txn_rows(&records))
    }

    async fn open_table(path: &str, use_read_optimized: bool) -> Result<Table> {
        if use_read_optimized {
            Table::new_with_options(
                path,
                [(HudiReadConfig::UseReadOptimizedMode.as_ref(), "true")],
            )
            .await
        } else {
            Table::new(path).await
        }
    }

    mod snapshot_queries {
        use super::*;

        #[tokio::test]
        async fn test_timebasedkeygen_epochmillis_cow_snapshot() -> Result<()> {
            let base_url = SampleTable::V9TimebasedkeygenEpochmillis.url_to_cow();
            let hudi_table = open_table(base_url.path(), false).await?;

            let rows = read_txn_rows_from_snapshot(&hudi_table).await?;
            assert_eq!(
                rows,
                vec![
                    ("TXN-001".to_string(), "debit".to_string(), 1700000000001),
                    ("TXN-001".to_string(), "reversal".to_string(), 1700100000001),
                    ("TXN-002".to_string(), "debit".to_string(), 1700000000002),
                    ("TXN-003".to_string(), "debit".to_string(), 1700200000003),
                    ("TXN-005".to_string(), "debit".to_string(), 1700100000005),
                    ("TXN-006".to_string(), "transfer".to_string(), 1700100000006),
                ]
            );
            Ok(())
        }

        #[tokio::test]
        async fn test_timebasedkeygen_nonhivestyle_cow_snapshot() -> Result<()> {
            let base_url = SampleTable::V9TimebasedkeygenNonhivestyle.url_to_cow();
            let hudi_table = open_table(base_url.path(), false).await?;

            let rows = read_txn_rows_from_snapshot(&hudi_table).await?;
            assert_eq!(
                rows,
                vec![
                    ("TXN-001".to_string(), "debit".to_string(), 1700000000001),
                    ("TXN-001".to_string(), "reversal".to_string(), 1700100000001),
                    ("TXN-003".to_string(), "debit".to_string(), 1700200000003),
                    ("TXN-004".to_string(), "transfer".to_string(), 1700000000004),
                    ("TXN-005".to_string(), "debit".to_string(), 1700100000005),
                ]
            );
            Ok(())
        }

        #[tokio::test]
        async fn test_timebasedkeygen_unixtimestamp_cow_snapshot() -> Result<()> {
            let base_url = SampleTable::V9TimebasedkeygenUnixtimestamp.url_to_cow();
            let hudi_table = open_table(base_url.path(), false).await?;

            let rows = read_txn_rows_from_snapshot(&hudi_table).await?;
            assert_eq!(
                rows,
                vec![
                    ("TXN-001".to_string(), "debit".to_string(), 1700000000001),
                    ("TXN-001".to_string(), "reversal".to_string(), 1700100000001),
                    ("TXN-002".to_string(), "debit".to_string(), 1700000000002),
                    ("TXN-003".to_string(), "debit".to_string(), 1700200000003),
                    ("TXN-005".to_string(), "debit".to_string(), 1700100000005),
                    ("TXN-006".to_string(), "transfer".to_string(), 1700100000006),
                ]
            );
            Ok(())
        }

        #[tokio::test]
        async fn test_txns_simple_overwrite_cow_snapshot() -> Result<()> {
            let base_url = SampleTable::V9TxnsSimpleOverwrite.url_to_cow();
            let hudi_table = open_table(base_url.path(), false).await?;

            let rows = read_txn_rows_from_snapshot(&hudi_table).await?;
            assert_eq!(
                rows,
                vec![
                    ("TXN-101".to_string(), "debit".to_string(), 1700500000001),
                    ("TXN-102".to_string(), "debit".to_string(), 1700500000002),
                    ("TXN-103".to_string(), "debit".to_string(), 1700500000003),
                ]
            );
            Ok(())
        }

        #[tokio::test]
        async fn test_timebasedkeygen_nonhivestyle_mor_snapshot() -> Result<()> {
            let base_url = SampleTable::V9TimebasedkeygenNonhivestyle.url_to_mor_avro();
            let hudi_table = open_table(base_url.path(), true).await?;

            let rows = read_txn_rows_from_snapshot(&hudi_table).await?;
            assert_eq!(
                rows,
                vec![
                    ("TXN-001".to_string(), "debit".to_string(), 1700000000001),
                    ("TXN-001".to_string(), "reversal".to_string(), 1700100000001),
                    ("TXN-002".to_string(), "debit".to_string(), 1700000000002),
                    ("TXN-003".to_string(), "debit".to_string(), 1700000000003),
                    ("TXN-004".to_string(), "transfer".to_string(), 1700000000004),
                    ("TXN-005".to_string(), "debit".to_string(), 1700100000005),
                    ("TXN-006".to_string(), "debit".to_string(), 1700300000006),
                ]
            );
            Ok(())
        }

        #[tokio::test]
        async fn test_txns_simple_overwrite_mor_snapshot() -> Result<()> {
            let base_url = SampleTable::V9TxnsSimpleOverwrite.url_to_mor_avro();
            let hudi_table = open_table(base_url.path(), true).await?;

            let rows = read_txn_rows_from_snapshot(&hudi_table).await?;
            assert_eq!(
                rows,
                vec![
                    ("TXN-101".to_string(), "debit".to_string(), 1700500000001),
                    ("TXN-102".to_string(), "debit".to_string(), 1700500000002),
                    ("TXN-103".to_string(), "debit".to_string(), 1700500000003),
                ]
            );
            Ok(())
        }

        #[tokio::test]
        async fn test_nonpartitioned_rollback_mor_snapshot() -> Result<()> {
            let base_url = SampleTable::V9NonpartitionedRollback.url_to_mor_avro();
            let hudi_table = open_table(base_url.path(), false).await?;

            let rows = read_txn_rows_from_snapshot(&hudi_table).await?;
            assert_eq!(
                rows,
                vec![
                    ("TXN-001".to_string(), "debit".to_string(), 1700000000001),
                    ("TXN-002".to_string(), "debit".to_string(), 1700000000002),
                    ("TXN-002".to_string(), "debit".to_string(), 1700200000002),
                    ("TXN-003".to_string(), "debit".to_string(), 1700000000003),
                ]
            );
            Ok(())
        }
    }

    mod time_travel_queries {
        use super::*;

        #[tokio::test]
        async fn test_txns_simple_overwrite_cow_time_travel() -> Result<()> {
            let base_url = SampleTable::V9TxnsSimpleOverwrite.url_to_cow();
            let hudi_table = open_table(base_url.path(), false).await?;

            let commits = hudi_table.timeline.get_completed_commits(false).await?;
            let replace_commits = hudi_table
                .timeline
                .get_completed_replacecommits(false)
                .await?;
            assert_eq!(
                commits.len(),
                2,
                "Expected two commit instants before overwrite"
            );
            assert_eq!(
                replace_commits.len(),
                1,
                "Expected one replacecommit instant for full-table overwrite"
            );

            let rows_before_overwrite =
                read_txn_rows_as_of(&hudi_table, &commits[1].timestamp).await?;
            assert_eq!(
                rows_before_overwrite,
                vec![
                    ("TXN-001".to_string(), "debit".to_string(), 1700000000001),
                    ("TXN-002".to_string(), "debit".to_string(), 1700000000002),
                    ("TXN-003".to_string(), "debit".to_string(), 1700000000003),
                    ("TXN-004".to_string(), "credit".to_string(), 1700000000004),
                    ("TXN-005".to_string(), "debit".to_string(), 1700000000005),
                    ("TXN-006".to_string(), "debit".to_string(), 1700000000006),
                    ("TXN-007".to_string(), "debit".to_string(), 1700100000007),
                    ("TXN-008".to_string(), "debit".to_string(), 1700100000008),
                ]
            );

            let rows_as_of_replace =
                read_txn_rows_as_of(&hudi_table, &replace_commits[0].timestamp).await?;
            assert_eq!(
                rows_as_of_replace,
                vec![
                    ("TXN-101".to_string(), "debit".to_string(), 1700500000001),
                    ("TXN-102".to_string(), "debit".to_string(), 1700500000002),
                    ("TXN-103".to_string(), "debit".to_string(), 1700500000003),
                ]
            );
            Ok(())
        }

        #[tokio::test]
        async fn test_nonpartitioned_rollback_mor_time_travel() -> Result<()> {
            let base_url = SampleTable::V9NonpartitionedRollback.url_to_mor_avro();
            let hudi_table = open_table(base_url.path(), false).await?;

            let deltacommits = hudi_table
                .timeline
                .get_completed_deltacommits(false)
                .await?;
            assert_eq!(
                deltacommits.len(),
                2,
                "Expected two completed deltacommit instants after rollback flow"
            );

            let rows_as_of_first_commit =
                read_txn_rows_as_of(&hudi_table, &deltacommits[0].timestamp).await?;
            assert_eq!(
                rows_as_of_first_commit,
                vec![
                    ("TXN-001".to_string(), "debit".to_string(), 1700000000001),
                    ("TXN-002".to_string(), "debit".to_string(), 1700000000002),
                    ("TXN-003".to_string(), "debit".to_string(), 1700000000003),
                ]
            );

            let latest_rows = read_txn_rows_from_snapshot(&hudi_table).await?;
            assert_eq!(
                latest_rows,
                vec![
                    ("TXN-001".to_string(), "debit".to_string(), 1700000000001),
                    ("TXN-002".to_string(), "debit".to_string(), 1700000000002),
                    ("TXN-002".to_string(), "debit".to_string(), 1700200000002),
                    ("TXN-003".to_string(), "debit".to_string(), 1700000000003),
                ]
            );
            Ok(())
        }
    }

    mod incremental_queries {
        use super::*;

        #[tokio::test]
        async fn test_txns_simple_overwrite_cow_incremental() -> Result<()> {
            let base_url = SampleTable::V9TxnsSimpleOverwrite.url_to_cow();
            let hudi_table = open_table(base_url.path(), false).await?;

            let commit_timestamps = hudi_table
                .timeline
                .completed_commits
                .iter()
                .map(|i| i.timestamp.as_str())
                .collect::<Vec<_>>();
            assert_eq!(commit_timestamps.len(), 3);
            let first_commit = commit_timestamps[0];
            let second_commit = commit_timestamps[1];
            let third_commit = commit_timestamps[2];

            let records = hudi_table
                .read(
                    &ReadOptions::new()
                        .with_query_type(QueryType::Incremental)
                        .with_start_timestamp("19700101000000000")
                        .with_end_timestamp(first_commit),
                )
                .await?;
            let rows = txn_rows(&records);
            assert_eq!(
                rows,
                vec![
                    ("TXN-001".to_string(), "debit".to_string(), 1700000000001),
                    ("TXN-002".to_string(), "debit".to_string(), 1700000000002),
                    ("TXN-003".to_string(), "debit".to_string(), 1700000000003),
                    ("TXN-004".to_string(), "credit".to_string(), 1700000000004),
                    ("TXN-005".to_string(), "debit".to_string(), 1700000000005),
                    ("TXN-006".to_string(), "debit".to_string(), 1700000000006),
                ],
                "Should return rows inserted in the first commit"
            );

            let records = hudi_table
                .read(
                    &ReadOptions::new()
                        .with_query_type(QueryType::Incremental)
                        .with_start_timestamp(first_commit)
                        .with_end_timestamp(second_commit),
                )
                .await?;
            let rows = txn_rows(&records);
            assert_eq!(
                rows,
                vec![
                    ("TXN-007".to_string(), "debit".to_string(), 1700100000007),
                    ("TXN-008".to_string(), "debit".to_string(), 1700100000008),
                ],
                "Should return rows inserted in the second commit"
            );

            let records = hudi_table
                .read(
                    &ReadOptions::new()
                        .with_query_type(QueryType::Incremental)
                        .with_start_timestamp(second_commit)
                        .with_end_timestamp(third_commit),
                )
                .await?;
            let rows = txn_rows(&records);
            assert_eq!(
                rows,
                vec![
                    ("TXN-101".to_string(), "debit".to_string(), 1700500000001),
                    ("TXN-102".to_string(), "debit".to_string(), 1700500000002),
                    ("TXN-103".to_string(), "debit".to_string(), 1700500000003),
                ],
                "Should return rows produced by replacecommit"
            );

            let records = hudi_table
                .read(
                    &ReadOptions::new()
                        .with_query_type(QueryType::Incremental)
                        .with_start_timestamp(first_commit),
                )
                .await?;
            let rows = txn_rows(&records);
            assert_eq!(
                rows,
                vec![
                    ("TXN-101".to_string(), "debit".to_string(), 1700500000001),
                    ("TXN-102".to_string(), "debit".to_string(), 1700500000002),
                    ("TXN-103".to_string(), "debit".to_string(), 1700500000003),
                ],
                "Should return latest states since first commit, after replacecommit pruning"
            );

            let records = hudi_table
                .read(
                    &ReadOptions::new()
                        .with_query_type(QueryType::Incremental)
                        .with_start_timestamp(third_commit),
                )
                .await?;
            assert!(
                records.is_empty(),
                "Should return 0 records as third commit is latest"
            );

            Ok(())
        }

        #[tokio::test]
        async fn test_nonpartitioned_rollback_mor_incremental() -> Result<()> {
            let base_url = SampleTable::V9NonpartitionedRollback.url_to_mor_avro();
            let hudi_table = open_table(base_url.path(), false).await?;

            let deltacommits = hudi_table
                .timeline
                .get_completed_deltacommits(false)
                .await?;
            assert_eq!(deltacommits.len(), 2);
            let first_commit = &deltacommits[0].timestamp;
            let second_commit = &deltacommits[1].timestamp;

            let records = hudi_table
                .read(
                    &ReadOptions::new()
                        .with_query_type(QueryType::Incremental)
                        .with_start_timestamp("19700101000000000")
                        .with_end_timestamp(first_commit),
                )
                .await?;
            let rows = txn_rows(&records);
            assert_eq!(
                rows,
                vec![
                    ("TXN-001".to_string(), "debit".to_string(), 1700000000001),
                    ("TXN-002".to_string(), "debit".to_string(), 1700000000002),
                    ("TXN-003".to_string(), "debit".to_string(), 1700000000003),
                ],
                "Should return rows inserted in the first deltacommit"
            );

            let records = hudi_table
                .read(
                    &ReadOptions::new()
                        .with_query_type(QueryType::Incremental)
                        .with_start_timestamp(first_commit)
                        .with_end_timestamp(second_commit),
                )
                .await?;
            let rows = txn_rows(&records);
            assert_eq!(
                rows,
                vec![("TXN-002".to_string(), "debit".to_string(), 1700200000002),],
                "Should return rows changed after rollback in the second deltacommit"
            );

            let records = hudi_table
                .read(
                    &ReadOptions::new()
                        .with_query_type(QueryType::Incremental)
                        .with_start_timestamp(first_commit),
                )
                .await?;
            let rows = txn_rows(&records);
            assert_eq!(
                rows,
                vec![("TXN-002".to_string(), "debit".to_string(), 1700200000002),],
                "Should return latest rows changed since first deltacommit"
            );

            let records = hudi_table
                .read(
                    &ReadOptions::new()
                        .with_query_type(QueryType::Incremental)
                        .with_start_timestamp(second_commit),
                )
                .await?;
            assert!(
                records.is_empty(),
                "Should return 0 records as second deltacommit is latest"
            );

            Ok(())
        }
    }

    mod streaming_queries {
        use super::*;
        use hudi_core::table::ReadOptions;

        #[tokio::test]
        async fn test_read_snapshot_stream_basic() -> Result<()> {
            let base_url = SampleTable::V9TxnsSimpleOverwrite.url_to_cow();
            let hudi_table = open_table(base_url.path(), false).await?;

            let options = ReadOptions::new();
            let stream = hudi_table.read_stream(&options).await?;
            let batches = collect_stream_batches(stream).await?;

            assert!(!batches.is_empty(), "Should produce at least one batch");

            let rows = txn_rows(&batches);
            assert_eq!(
                rows,
                vec![
                    ("TXN-101".to_string(), "debit".to_string(), 1700500000001),
                    ("TXN-102".to_string(), "debit".to_string(), 1700500000002),
                    ("TXN-103".to_string(), "debit".to_string(), 1700500000003),
                ]
            );
            Ok(())
        }

        #[tokio::test]
        async fn test_read_snapshot_stream_with_batch_size() -> Result<()> {
            let base_url = SampleTable::V9TxnsSimpleOverwrite.url_to_cow();
            let hudi_table = open_table(base_url.path(), false).await?;

            let options = ReadOptions::new().with_batch_size(1)?;
            let stream = hudi_table.read_stream(&options).await?;
            let batches = collect_stream_batches(stream).await?;
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

            assert_eq!(total_rows, 3, "Total rows should match expected count");
            Ok(())
        }

        #[tokio::test]
        async fn test_read_snapshot_stream_with_partition_filters() -> Result<()> {
            let base_url = SampleTable::V9TxnsSimpleOverwrite.url_to_cow();
            let hudi_table = open_table(base_url.path(), false).await?;

            let options = ReadOptions::new().with_filters([("region", "=", "us")])?;
            let stream = hudi_table.read_stream(&options).await?;
            let batches = collect_stream_batches(stream).await?;

            assert!(
                !batches.is_empty(),
                "Should produce at least one batch for the partition filter"
            );
            let rows = txn_rows(&batches);
            assert_eq!(
                rows,
                vec![("TXN-101".to_string(), "debit".to_string(), 1700500000001)]
            );
            Ok(())
        }

        #[tokio::test]
        async fn test_read_snapshot_stream_with_non_matching_partition_filter() -> Result<()> {
            let base_url = SampleTable::V9TxnsSimpleOverwrite.url_to_cow();
            let hudi_table = open_table(base_url.path(), false).await?;

            let options = ReadOptions::new().with_filters([("region", "=", "latam")])?;
            let stream = hudi_table.read_stream(&options).await?;
            let batches = collect_stream_batches(stream).await?;

            assert!(
                batches.is_empty(),
                "Non-matching filter should return no batches"
            );
            Ok(())
        }

        #[tokio::test]
        async fn test_read_file_slice_stream_basic() -> Result<()> {
            let base_url = SampleTable::V9TxnsSimpleOverwrite.url_to_cow();
            let hudi_table = open_table(base_url.path(), false).await?;

            let file_slices = hudi_table.get_file_slices(&ReadOptions::new()).await?;
            assert!(
                !file_slices.is_empty(),
                "Should have at least one file slice"
            );

            let fg_reader = hudi_table.create_file_group_reader_with_options(
                None,
                std::iter::empty::<(&str, &str)>(),
                std::iter::empty::<(&str, &str)>(),
            )?;
            let options = ReadOptions::new();
            let file_slice = &file_slices[0];
            let stream = fg_reader
                .read_file_slice_stream(file_slice, &options)
                .await?;
            let batches = collect_stream_batches(stream).await?;
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

            assert!(total_rows > 0, "Should read at least one row");
            Ok(())
        }

        #[tokio::test]
        async fn test_read_file_slice_stream_with_batch_size() -> Result<()> {
            let base_url = SampleTable::V9TxnsSimpleOverwrite.url_to_cow();
            let hudi_table = open_table(base_url.path(), false).await?;

            let file_slices = hudi_table.get_file_slices(&ReadOptions::new()).await?;
            let file_slice = &file_slices[0];

            let fg_reader = hudi_table.create_file_group_reader_with_options(
                None,
                std::iter::empty::<(&str, &str)>(),
                std::iter::empty::<(&str, &str)>(),
            )?;
            let options = ReadOptions::new().with_batch_size(1)?;
            let stream = fg_reader
                .read_file_slice_stream(file_slice, &options)
                .await?;
            let batches = collect_stream_batches(stream).await?;
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

            assert!(total_rows > 0, "Should read at least one row");
            Ok(())
        }

        #[tokio::test]
        async fn test_read_snapshot_stream_mor_with_log_files() -> Result<()> {
            // Test MOR table with log files in snapshot mode (non read-optimized)
            // so streaming falls back to collect+merge.
            let base_url = SampleTable::V9TimebasedkeygenNonhivestyle.url_to_mor_avro();
            let hudi_table = open_table(base_url.path(), false).await?;

            let file_slices = hudi_table.get_file_slices(&ReadOptions::new()).await?;
            assert!(
                file_slices
                    .iter()
                    .any(|file_slice| file_slice.has_log_file()),
                "Expected at least one MOR file slice with log files"
            );

            let options = ReadOptions::new();
            let stream = hudi_table.read_stream(&options).await?;
            let err = match collect_stream_batches(stream).await {
                Ok(_) => panic!("Expected MOR streaming read with decimal log records to fail"),
                Err(err) => err,
            };
            let err_message = err.to_string();
            assert!(
                err_message.contains("Decimal128(15, 2) not supported"),
                "Unexpected error for MOR log-file streaming path: {err_message}"
            );
            Ok(())
        }

        #[tokio::test]
        async fn test_read_snapshot_stream_mor_read_optimized() -> Result<()> {
            let base_url = SampleTable::V9TimebasedkeygenNonhivestyle.url_to_mor_avro();
            let hudi_table = open_table(base_url.path(), true).await?;

            let options = ReadOptions::new();
            let stream = hudi_table.read_stream(&options).await?;
            let batches = collect_stream_batches(stream).await?;

            assert!(!batches.is_empty(), "Should produce batches from MOR table");
            let rows = txn_rows(&batches);
            assert_eq!(rows.len(), 7, "Should have 7 rows in read-optimized mode");
            assert!(
                rows.iter().any(|(txn_id, _, _)| txn_id == "TXN-006"),
                "Expected record inserted after compaction to be present"
            );
            Ok(())
        }
    }
}

/// Test module for streaming read APIs.
/// These tests verify the streaming versions of snapshot and file slice reads.
mod streaming_queries {
    use super::*;
    use futures::StreamExt;
    use hudi_core::table::ReadOptions;

    #[tokio::test]
    async fn test_read_snapshot_stream_empty_table() -> Result<()> {
        for base_url in SampleTable::V6Empty.urls() {
            let hudi_table = Table::new(base_url.path()).await?;
            let options = ReadOptions::new();
            let stream = hudi_table.read_stream(&options).await?;
            let batches = collect_stream_batches(stream).await?;
            assert!(batches.is_empty(), "Empty table should produce no batches");
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_read_snapshot_stream_basic() -> Result<()> {
        for base_url in SampleTable::V6Nonpartitioned.urls() {
            let hudi_table = Table::new(base_url.path()).await?;
            let options = ReadOptions::new();
            let stream = hudi_table.read_stream(&options).await?;
            let batches = collect_stream_batches(stream).await?;

            assert!(!batches.is_empty(), "Should produce at least one batch");

            // Concatenate batches and verify data
            let schema = &batches[0].schema();
            let records = concat_batches(schema, &batches)?;

            let sample_data = SampleTable::sample_data_order_by_id(&records);
            assert_eq!(
                sample_data,
                vec![
                    (1, "Alice", false),
                    (2, "Bob", false),
                    (3, "Carol", true),
                    (4, "Diana", true),
                ]
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_read_snapshot_stream_with_batch_size() -> Result<()> {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await?;

        // Request small batch size
        let options = ReadOptions::new().with_batch_size(1)?;
        let stream = hudi_table.read_stream(&options).await?;
        let batches = collect_stream_batches(stream).await?;

        // With batch_size=1 and 4 rows, we expect multiple batches, but the
        // exact number depends on both the batch_size setting and the Parquet
        // file's internal row group structure.
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 4, "Total rows should match expected count");
        Ok(())
    }

    #[tokio::test]
    async fn test_read_snapshot_stream_with_partition_filters() -> Result<()> {
        let base_url = SampleTable::V6ComplexkeygenHivestyle.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await?;

        let options = ReadOptions::new().with_filters([
            ("byteField", ">=", "10"),
            ("byteField", "<", "20"),
            ("shortField", "!=", "100"),
        ])?;
        let stream = hudi_table.read_stream(&options).await?;
        let batches = collect_stream_batches(stream).await?;

        assert!(
            !batches.is_empty(),
            "Should produce at least one batch for the given partition filters"
        );
        let schema = &batches[0].schema();
        let records = concat_batches(schema, &batches)?;

        let sample_data = SampleTable::sample_data_order_by_id(&records);
        assert_eq!(sample_data, vec![(1, "Alice", false), (3, "Carol", true),]);
        Ok(())
    }

    #[tokio::test]
    async fn test_read_file_slice_stream_basic() -> Result<()> {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await?;

        // Get file slices first
        let file_slices = hudi_table.get_file_slices(&ReadOptions::new()).await?;
        assert!(
            !file_slices.is_empty(),
            "Should have at least one file slice"
        );

        let fg_reader = hudi_table.create_file_group_reader_with_options(
            None,
            std::iter::empty::<(&str, &str)>(),
            std::iter::empty::<(&str, &str)>(),
        )?;
        let options = ReadOptions::new();
        let file_slice = &file_slices[0];
        let stream = fg_reader
            .read_file_slice_stream(file_slice, &options)
            .await?;
        let batches = collect_stream_batches(stream).await?;

        assert!(!batches.is_empty(), "Should produce at least one batch");

        // Verify we got records
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(total_rows > 0, "Should read at least one row");
        Ok(())
    }

    #[tokio::test]
    async fn test_read_file_slice_stream_with_batch_size() -> Result<()> {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await?;

        let file_slices = hudi_table.get_file_slices(&ReadOptions::new()).await?;
        let file_slice = &file_slices[0];

        let fg_reader = hudi_table.create_file_group_reader_with_options(
            None,
            std::iter::empty::<(&str, &str)>(),
            std::iter::empty::<(&str, &str)>(),
        )?;
        // Test with small batch size
        let options = ReadOptions::new().with_batch_size(1)?;
        let stream = fg_reader
            .read_file_slice_stream(file_slice, &options)
            .await?;
        let batches = collect_stream_batches(stream).await?;

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 4, "Should read all 4 rows");
        Ok(())
    }

    #[tokio::test]
    async fn test_read_snapshot_stream_mor_with_log_files() -> Result<()> {
        // Test MOR table with log files - should still work (falls back to collect+merge)
        let base_url = QuickstartTripsTable::V6Trips8I1U.url_to_mor_avro();
        let hudi_table = Table::new(base_url.path()).await?;

        let options = ReadOptions::new();
        let stream = hudi_table.read_stream(&options).await?;
        let batches = collect_stream_batches(stream).await?;

        assert!(!batches.is_empty(), "Should produce batches from MOR table");

        // Verify total row count
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 8, "Should have 8 rows (8 inserts)");
        Ok(())
    }

    #[tokio::test]
    async fn test_read_snapshot_stream_with_projection() -> Result<()> {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await?;

        // Only request id and name columns (not isActive)
        let options = ReadOptions::new().with_projection(["id", "name"]);
        let stream = hudi_table.read_stream(&options).await?;
        let batches = collect_stream_batches(stream).await?;

        assert!(!batches.is_empty(), "Should produce at least one batch");

        // Verify only projected columns are returned
        let schema = &batches[0].schema();
        assert_eq!(schema.fields().len(), 2, "Should only have 2 columns");
        assert!(
            schema.field_with_name("id").is_ok(),
            "Should have id column"
        );
        assert!(
            schema.field_with_name("name").is_ok(),
            "Should have name column"
        );
        assert!(
            schema.field_with_name("isActive").is_err(),
            "Should NOT have isActive column"
        );

        // Verify row count is still correct
        let records = concat_batches(schema, &batches)?;
        assert_eq!(records.num_rows(), 4, "Should have all 4 rows");
        Ok(())
    }

    #[tokio::test]
    async fn test_read_snapshot_stream_with_non_partition_filter() -> Result<()> {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await?;

        // Filter on a non-partition column. The filter applies as a row mask
        // during reading even though file pruning can't use it.
        let options = ReadOptions::new().with_filters([("isActive", "=", "true")])?;

        let stream = hudi_table.read_stream(&options).await?;
        let batches = collect_stream_batches(stream).await?;

        assert!(!batches.is_empty(), "Should produce at least one batch");

        let schema = &batches[0].schema();
        let records = concat_batches(schema, &batches)?;

        // Should only have Carol and Diana (isActive = true)
        let sample_data = SampleTable::sample_data_order_by_id(&records);
        assert_eq!(sample_data, vec![(3, "Carol", true), (4, "Diana", true)]);
        Ok(())
    }

    #[tokio::test]
    async fn test_read_snapshot_stream_with_projection_and_filter() -> Result<()> {
        use arrow::array::Int32Array;

        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await?;

        // Project only id and isActive, filter where isActive = true
        let options = ReadOptions::new()
            .with_projection(["id", "isActive"])
            .with_filters([("isActive", "=", "true")])?;

        let stream = hudi_table.read_stream(&options).await?;
        let batches = collect_stream_batches(stream).await?;

        assert!(!batches.is_empty(), "Should produce at least one batch");

        // Verify only projected columns
        let schema = &batches[0].schema();
        assert_eq!(schema.fields().len(), 2, "Should only have 2 columns");
        assert!(schema.field_with_name("id").is_ok());
        assert!(schema.field_with_name("isActive").is_ok());
        assert!(schema.field_with_name("name").is_err());

        // Verify filtered rows (only active users: Carol=3, Diana=4)
        let records = concat_batches(schema, &batches)?;
        assert_eq!(records.num_rows(), 2, "Should have 2 rows");

        let ids = records
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let id_values: Vec<i32> = ids.iter().flatten().collect();
        assert!(id_values.contains(&3) && id_values.contains(&4));
        Ok(())
    }

    #[tokio::test]
    async fn test_read_snapshot_stream_filter_column_not_in_projection() -> Result<()> {
        use arrow::array::Int32Array;

        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await?;

        // Filter on `isActive` but project only `id` (filter column NOT projected).
        // The filter must still apply at row level — implementation augments the
        // read projection internally with filter columns, then projects down.
        let options = ReadOptions::new()
            .with_projection(["id"])
            .with_filters([("isActive", "=", "true")])?;

        let stream = hudi_table.read_stream(&options).await?;
        let batches = collect_stream_batches(stream).await?;

        let schema = &batches[0].schema();
        assert_eq!(schema.fields().len(), 1);
        assert!(schema.field_with_name("id").is_ok());
        assert!(schema.field_with_name("isActive").is_err());

        let records = concat_batches(schema, &batches)?;
        assert_eq!(
            records.num_rows(),
            2,
            "filter must drop rows even when filter column is not projected"
        );

        let ids = records
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let id_values: Vec<i32> = ids.iter().flatten().collect();
        assert!(id_values.contains(&3) && id_values.contains(&4));
        Ok(())
    }

    #[tokio::test]
    async fn test_read_snapshot_stream_with_as_of_timestamp() -> Result<()> {
        // Cross-validate streaming time-travel against the eager API: with the
        // same `as_of_timestamp`, both must return the same row count. (Before
        // the fix, streaming silently used the latest commit and could diverge.)
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await?;
        let first_commit = hudi_table
            .get_timeline()
            .get_completed_commits(false)
            .await?
            .first()
            .map(|i| i.timestamp.to_string())
            .expect("table must have at least one commit");
        let options = ReadOptions::new().with_as_of_timestamp(&first_commit);

        let eager = hudi_table.read(&options).await?;
        let eager_rows: usize = eager.iter().map(|b| b.num_rows()).sum();

        let stream = hudi_table.read_stream(&options).await?;
        let stream_batches = collect_stream_batches(stream).await?;
        let stream_rows: usize = stream_batches.iter().map(|b| b.num_rows()).sum();

        assert_eq!(eager_rows, stream_rows);
        Ok(())
    }

    #[tokio::test]
    async fn test_filter_on_unknown_column_errors() -> Result<()> {
        // A typo on a filter column should error rather than silently no-op,
        // across all three dispatch paths (eager snapshot, eager incremental,
        // streaming snapshot). Streaming surfaces the error synchronously,
        // before the stream is constructed, so callers don't have to poll.
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await?;
        let options = ReadOptions::new().with_filters([("rider_idd", "=", "x")])?;

        let snapshot_err = hudi_table.read(&options).await.unwrap_err();
        assert!(
            snapshot_err.to_string().contains("rider_idd"),
            "snapshot error should mention the bad column, got: {snapshot_err}"
        );

        let incremental_err = hudi_table
            .read(&options.clone().with_query_type(QueryType::Incremental))
            .await
            .unwrap_err();
        assert!(
            incremental_err.to_string().contains("rider_idd"),
            "incremental error should mention the bad column, got: {incremental_err}"
        );

        match hudi_table.read_stream(&options).await {
            Ok(_) => panic!("read_stream must surface the typo synchronously"),
            Err(e) => assert!(
                e.to_string().contains("rider_idd"),
                "stream error should mention the bad column, got: {e}"
            ),
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_file_group_reader_filter_on_unknown_column_errors() -> Result<()> {
        // FileGroupReader-direct reads bypass table-level filter validation; the
        // reader must reject typoed filter fields itself rather than silently
        // skipping them in `filters_to_row_mask`.
        use hudi_core::file_group::reader::FileGroupReader;
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await?;
        let file_slice = hudi_table.get_file_slices(&ReadOptions::new()).await?[0].clone();
        let reader =
            FileGroupReader::new_with_options(base_url.path(), std::iter::empty::<(&str, &str)>())
                .await?;
        let options = ReadOptions::new().with_filters([("rider_idd", "=", "x")])?;

        let eager_err = reader
            .read_file_slice(&file_slice, &options)
            .await
            .unwrap_err();
        assert!(
            eager_err.to_string().contains("rider_idd"),
            "eager error should mention the bad column, got: {eager_err}"
        );

        // Streaming validates lazily on the first batch.
        let stream = reader.read_file_slice_stream(&file_slice, &options).await?;
        let mut stream_err = None;
        let mut s = stream;
        while let Some(result) = s.next().await {
            if let Err(e) = result {
                stream_err = Some(e);
                break;
            }
        }
        let stream_err = stream_err.expect("streaming must surface the typo as an error");
        assert!(
            stream_err.to_string().contains("rider_idd"),
            "stream error should mention the bad column, got: {stream_err}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_file_group_reader_stream_projection_widens_for_commit_time_filter() -> Result<()>
    {
        // Regression: when commit-time filtering is active (StartTimestamp set
        // and PopulatesMetaFields true), the streaming path must widen the parquet read
        // projection to include `_hoodie_commit_time` even if the user's projection
        // omits it. The widened column is dropped by the existing final-projection step
        // so the user-visible schema still matches `options.projection`.
        use hudi_core::file_group::reader::FileGroupReader;
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await?;
        let file_slice = hudi_table.get_file_slices(&ReadOptions::new()).await?[0].clone();
        let reader = FileGroupReader::new_with_options(
            base_url.path(),
            [(HudiReadConfig::StartTimestamp.as_ref(), "0")],
        )
        .await?;
        let options = ReadOptions::new().with_projection(["id"]);

        let stream = reader.read_file_slice_stream(&file_slice, &options).await?;
        let batches = collect_stream_batches(stream).await?;

        assert!(!batches.is_empty(), "should produce at least one batch");
        let schema = batches[0].schema();
        assert_eq!(
            schema.fields().len(),
            1,
            "projection should narrow to one column"
        );
        assert_eq!(schema.field(0).name(), "id");
        Ok(())
    }

    #[tokio::test]
    async fn test_read_snapshot_stream_projection_invalid_column() -> Result<()> {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await?;

        // Request a non-existent column
        let options = ReadOptions::new().with_projection(["id", "nonexistent_column"]);
        let mut stream = hudi_table.read_stream(&options).await?;

        // Error occurs when polling the stream (lazy evaluation)
        let mut found_error = false;
        while let Some(result) = stream.next().await {
            match result {
                Ok(_) => {}
                Err(err) => {
                    assert!(
                        err.to_string().contains("nonexistent_column"),
                        "Error should mention the invalid column name, got: {err}"
                    );
                    found_error = true;
                    break;
                }
            }
        }
        assert!(
            found_error,
            "Should have encountered an error for non-existent column"
        );
        Ok(())
    }

    /// Regression: a stray `hoodie.read.start.timestamp` left in
    /// `options.hudi_options` must NOT silently activate commit-time filtering on
    /// a snapshot read. Snapshot dispatch + a future-dated start timestamp would
    /// otherwise filter out every row at the FG-reader layer.
    #[tokio::test]
    async fn test_snapshot_ignores_stale_start_timestamp_in_options() -> Result<()> {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await?;

        let baseline = hudi_table.read(&ReadOptions::new()).await?;
        let baseline_rows: usize = baseline.iter().map(|b| b.num_rows()).sum();
        assert!(baseline_rows > 0);

        // Snapshot dispatch (default), but with a future-dated start_timestamp
        // sitting in the bag — a stray incremental knob the user might have set
        // before flipping to snapshot. Snapshot must ignore it.
        let polluted = ReadOptions::new().with_start_timestamp("99999999999999999");
        let result = hudi_table.read(&polluted).await?;
        let result_rows: usize = result.iter().map(|b| b.num_rows()).sum();

        assert_eq!(
            baseline_rows, result_rows,
            "snapshot read must not honor an incremental-only start_timestamp"
        );
        Ok(())
    }

    /// Same regression for the streaming snapshot path.
    #[tokio::test]
    async fn test_snapshot_stream_ignores_stale_start_timestamp_in_options() -> Result<()> {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await?;

        let baseline_stream = hudi_table.read_stream(&ReadOptions::new()).await?;
        let baseline = collect_stream_batches(baseline_stream).await?;
        let baseline_rows: usize = baseline.iter().map(|b| b.num_rows()).sum();
        assert!(baseline_rows > 0);

        let polluted = ReadOptions::new().with_start_timestamp("99999999999999999");
        let polluted_stream = hudi_table.read_stream(&polluted).await?;
        let result = collect_stream_batches(polluted_stream).await?;
        let result_rows: usize = result.iter().map(|b| b.num_rows()).sum();

        assert_eq!(
            baseline_rows, result_rows,
            "snapshot stream must not honor an incremental-only start_timestamp"
        );
        Ok(())
    }

    /// Regression: the public `create_file_group_reader_with_options` API must
    /// strip the four `Table`-owned read keys from `read_options.hudi_options`
    /// before forwarding them to the FG reader. Otherwise a stray
    /// `StartTimestamp` activates commit-time filtering at the physical layer
    /// and silently drops every row.
    #[tokio::test]
    async fn test_create_fg_reader_strips_table_owned_keys() -> Result<()> {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await?;
        let file_slices = hudi_table.get_file_slices(&ReadOptions::new()).await?;
        assert!(!file_slices.is_empty());
        let file_slice = &file_slices[0];

        // Bag pre-populated with all four Table-owned keys, including a
        // future-dated StartTimestamp that would zero out commit-time filtering.
        let polluted = ReadOptions::new()
            .with_query_type(QueryType::Incremental)
            .with_as_of_timestamp("19700101000000000")
            .with_start_timestamp("99999999999999999")
            .with_end_timestamp("19700101000000000");

        let fg_reader = hudi_table.create_file_group_reader_with_options(
            Some(&polluted),
            std::iter::empty::<(&str, &str)>(),
            std::iter::empty::<(&str, &str)>(),
        )?;
        let batch = fg_reader
            .read_file_slice(file_slice, &ReadOptions::new())
            .await?;
        assert!(
            batch.num_rows() > 0,
            "Table-owned keys must not leak into the FG reader; got 0 rows"
        );
        Ok(())
    }

    /// Regression: filters on Hudi meta fields (e.g. `_hoodie_record_key`) must
    /// be accepted by the table-level validation. Returned batches include meta
    /// fields, and the row-level mask applies the filter against them.
    #[tokio::test]
    async fn test_table_read_accepts_meta_field_filter() -> Result<()> {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await?;

        // _hoodie_record_key is in the returned batch but not in the data
        // schema. A typo'd column name should still error.
        let options = ReadOptions::new().with_filters([(
            hudi_core::metadata::meta_field::MetaField::RecordKey.as_ref(),
            "!=",
            "",
        )])?;
        let batches = hudi_table.read(&options).await?;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(total_rows > 0, "meta-field filter should match real keys");

        let typo = ReadOptions::new().with_filters([("_hoodie_record_keyy", "!=", "")])?;
        let err = hudi_table.read(&typo).await.unwrap_err();
        assert!(
            err.to_string().contains("_hoodie_record_keyy"),
            "typo'd filter should still fail validation; got: {err}"
        );
        Ok(())
    }

    /// Regression: `with_batch_size(0)` must error at the builder so callers
    /// catch the misuse synchronously, not when the parquet stream reader
    /// silently yields zero rows.
    #[tokio::test]
    async fn test_batch_size_zero_errors() -> Result<()> {
        let err = ReadOptions::new().with_batch_size(0).unwrap_err();
        assert!(
            err.to_string().contains("must be > 0"),
            "expected validation error for batch_size=0, got: {err}"
        );
        Ok(())
    }
}

/// Test module for tables with metadata table (MDT) enabled.
/// These tests verify MDT-accelerated file listing and partition normalization.
mod mdt_enabled_tables {
    use super::*;
    use hudi_core::table::partition::PartitionPruner;

    mod snapshot_queries {
        use super::*;

        /// Test reading a V9 MOR non-partitioned table with MDT enabled.
        /// Verifies:
        /// 1. Table can be read correctly via MDT file listing
        /// 2. MDT partition key normalization ("." -> "") works correctly
        /// 3. File slices are retrieved correctly from MDT
        #[tokio::test]
        async fn test_v9_nonpartitioned_with_mdt() -> Result<()> {
            let base_url = SampleTable::V9TxnsNonpartMeta.url_to_mor_avro();
            let hudi_table = Table::new(base_url.path()).await?;

            // Verify MDT is enabled
            assert!(
                hudi_table.is_metadata_table_enabled(),
                "Metadata table should be enabled"
            );

            // Get file slices - this uses MDT file listing
            let file_slices = hudi_table.get_file_slices(&ReadOptions::new()).await?;

            // Should have file slices for the non-partitioned table
            assert!(
                !file_slices.is_empty(),
                "Should have file slices from MDT listing"
            );

            // All file slices should be in the root partition (empty string)
            for fs in &file_slices {
                assert_eq!(
                    &fs.partition_path, "",
                    "Non-partitioned table should have files in root partition"
                );
            }

            Ok(())
        }

        /// Test MDT partition key normalization for non-partitioned tables.
        /// The metadata table stores "." as partition key, but external API should see "".
        /// For non-partitioned tables, we use a fast path that directly fetches "." without
        /// going through __all_partitions__ lookup.
        #[tokio::test]
        async fn test_v9_nonpartitioned_mdt_partition_normalization() -> Result<()> {
            let base_url = SampleTable::V9TxnsNonpartMeta.url_to_mor_avro();
            let hudi_table = Table::new(base_url.path()).await?;

            // Read MDT files partition records
            let partition_pruner = PartitionPruner::empty();
            let records = hudi_table
                .read_metadata_table_files_partition(&partition_pruner)
                .await?;

            // For non-partitioned tables, the fast path only fetches the files record.
            // __all_partitions__ is not fetched to avoid redundant HFile lookup.
            assert_eq!(
                records.len(),
                1,
                "Non-partitioned table fast path should only fetch files record"
            );

            // The files record should be keyed by "" (empty string)
            // not "." (which is the internal MDT representation)
            assert!(
                records.contains_key(""),
                "Non-partitioned table should have files record with empty string key"
            );
            assert!(
                !records.contains_key("."),
                "Non-partitioned table should NOT have files record with '.' key after normalization"
            );

            // Verify the files record has actual file entries
            let files_record = records.get("").unwrap();
            assert!(
                !files_record.files.is_empty(),
                "Files record should contain file entries"
            );

            Ok(())
        }
    }
}
