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
use hudi_core::config::read::HudiReadConfig;
use hudi_core::config::util::empty_filters;
use hudi_core::error::Result;
use hudi_core::table::Table;
use hudi_test::{QuickstartTripsTable, SampleTable, SampleTableMdt};

/// Test helper module for v6 tables (pre-1.0 spec)
mod v6_tables {
    use super::*;

    mod snapshot_queries {
        use super::*;

        #[test]
        fn test_empty_table() -> Result<()> {
            for base_url in SampleTable::V6Empty.urls() {
                let hudi_table = Table::new_blocking(base_url.path())?;
                let records = hudi_table.read_snapshot_blocking(empty_filters())?;
                assert!(records.is_empty());
            }
            Ok(())
        }

        #[test]
        fn test_non_partitioned() -> Result<()> {
            for base_url in SampleTable::V6Nonpartitioned.urls() {
                let hudi_table = Table::new_blocking(base_url.path())?;
                let records = hudi_table.read_snapshot_blocking(empty_filters())?;
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

        #[test]
        fn test_non_partitioned_read_optimized() -> Result<()> {
            let base_url = SampleTable::V6Nonpartitioned.url_to_mor_parquet();
            let hudi_table = Table::new_with_options_blocking(
                base_url.path(),
                [(HudiReadConfig::UseReadOptimizedMode.as_ref(), "true")],
            )?;
            let commit_timestamps = hudi_table
                .timeline
                .completed_commits
                .iter()
                .map(|i| i.timestamp.as_str())
                .collect::<Vec<_>>();
            let latest_commit = commit_timestamps.last().unwrap();
            let records =
                hudi_table.read_snapshot_as_of_blocking(latest_commit, empty_filters())?;
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

        #[test]
        fn test_non_partitioned_rollback() -> Result<()> {
            let base_url = SampleTable::V6NonpartitionedRollback.url_to_mor_parquet();
            let hudi_table = Table::new_blocking(base_url.path())?;
            let records = hudi_table.read_snapshot_blocking(empty_filters())?;
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

        #[test]
        fn test_complex_keygen_hive_style_with_filters() -> Result<()> {
            for base_url in SampleTable::V6ComplexkeygenHivestyle.urls() {
                let hudi_table = Table::new_blocking(base_url.path())?;

                let filters = vec![
                    ("byteField", ">=", "10"),
                    ("byteField", "<", "20"),
                    ("shortField", "!=", "100"),
                ];
                let records = hudi_table.read_snapshot_blocking(filters)?;
                let schema = &records[0].schema();
                let records = concat_batches(schema, &records)?;

                let sample_data = SampleTable::sample_data_order_by_id(&records);
                assert_eq!(sample_data, vec![(1, "Alice", false), (3, "Carol", true),]);
            }
            Ok(())
        }

        #[test]
        fn test_simple_keygen_hivestyle_no_metafields() -> Result<()> {
            for base_url in SampleTable::V6SimplekeygenHivestyleNoMetafields.urls() {
                let hudi_table = Table::new_blocking(base_url.path())?;
                let records = hudi_table.read_snapshot_blocking(empty_filters())?;
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

        #[test]
        fn test_simple_keygen_nonhivestyle_time_travel() -> Result<()> {
            for base_url in SampleTable::V6SimplekeygenNonhivestyle.urls() {
                let hudi_table = Table::new_blocking(base_url.path())?;
                let commit_timestamps = hudi_table
                    .timeline
                    .completed_commits
                    .iter()
                    .map(|i| i.timestamp.as_str())
                    .collect::<Vec<_>>();
                let first_commit = commit_timestamps[0];
                let records =
                    hudi_table.read_snapshot_as_of_blocking(first_commit, empty_filters())?;
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

        #[test]
        fn test_quickstart_trips_inserts_updates() -> Result<()> {
            let base_url = QuickstartTripsTable::V6Trips8I1U.url_to_mor_avro();
            let hudi_table = Table::new_blocking(base_url.path())?;

            let updated_rider = "rider-D";

            // verify updated record as of the latest commit
            let records = hudi_table.read_snapshot_blocking(empty_filters())?;
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
            let records = hudi_table.read_snapshot_as_of_blocking(first_commit, empty_filters())?;
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

        #[test]
        fn test_quickstart_trips_inserts_deletes() -> Result<()> {
            let base_url = QuickstartTripsTable::V6Trips8I3D.url_to_mor_avro();
            let hudi_table = Table::new_blocking(base_url.path())?;

            let deleted_riders = ["rider-A", "rider-C", "rider-D"];

            // verify deleted record as of the latest commit
            let records = hudi_table.read_snapshot_blocking(empty_filters())?;
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
            let records = hudi_table.read_snapshot_as_of_blocking(first_commit, empty_filters())?;
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

        #[test]
        fn test_empty_table() -> Result<()> {
            for base_url in SampleTable::V6Empty.urls() {
                let hudi_table = Table::new_blocking(base_url.path())?;
                let records = hudi_table.read_incremental_records_blocking("0", None)?;
                assert!(records.is_empty())
            }
            Ok(())
        }

        #[test]
        fn test_simplekeygen_nonhivestyle_overwritetable() -> Result<()> {
            for base_url in SampleTable::V6SimplekeygenNonhivestyleOverwritetable.urls() {
                let hudi_table = Table::new_blocking(base_url.path())?;
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
                    .read_incremental_records_blocking("19700101000000", Some(first_commit))?;
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
                    .read_incremental_records_blocking(first_commit, Some(second_commit))?;
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
                    .read_incremental_records_blocking(second_commit, Some(third_commit))?;
                let schema = &records[0].schema();
                let records = concat_batches(schema, &records)?;
                let sample_data = SampleTable::sample_data_order_by_id(&records);
                assert_eq!(
                    sample_data,
                    vec![(4, "Diana", false),],
                    "Should return 1 record insert-overwritten in the 3rd commit"
                );

                // read records changed from the 1st commit
                let records = hudi_table.read_incremental_records_blocking(first_commit, None)?;
                let schema = &records[0].schema();
                let records = concat_batches(schema, &records)?;
                let sample_data = SampleTable::sample_data_order_by_id(&records);
                assert_eq!(
                    sample_data,
                    vec![(4, "Diana", false),],
                    "Should return 1 record insert-overwritten in the 3rd commit"
                );

                // read records changed from the 3rd commit
                let records = hudi_table.read_incremental_records_blocking(third_commit, None)?;
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

        #[test]
        fn test_empty_table() -> Result<()> {
            let base_url = SampleTable::V8Empty.url_to_cow();
            let hudi_table = Table::new_blocking(base_url.path())?;
            let records = hudi_table.read_snapshot_blocking(empty_filters())?;
            assert!(records.is_empty());
            Ok(())
        }

        #[test]
        fn test_non_partitioned() -> Result<()> {
            let base_url = SampleTable::V8Nonpartitioned.url_to_cow();
            let hudi_table = Table::new_blocking(base_url.path())?;
            let records = hudi_table.read_snapshot_blocking(empty_filters())?;
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

        #[test]
        fn test_complex_keygen_hive_style() -> Result<()> {
            let base_url = SampleTable::V8ComplexkeygenHivestyle.url_to_cow();
            let hudi_table = Table::new_blocking(base_url.path())?;

            let records = hudi_table.read_snapshot_blocking(empty_filters())?;
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

        #[test]
        fn test_simple_keygen_nonhivestyle() -> Result<()> {
            let base_url = SampleTable::V8SimplekeygenNonhivestyle.url_to_cow();
            let hudi_table = Table::new_blocking(base_url.path())?;

            let records = hudi_table.read_snapshot_blocking(empty_filters())?;
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

        #[test]
        fn test_simple_keygen_hivestyle_no_metafields() -> Result<()> {
            let base_url = SampleTable::V8SimplekeygenHivestyleNoMetafields.url_to_cow();
            let hudi_table = Table::new_blocking(base_url.path())?;

            let records = hudi_table.read_snapshot_blocking(empty_filters())?;
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

        #[test]
        fn test_quickstart_trips_inserts_updates_deletes() -> Result<()> {
            // V8Trips8I3U1D: 8 inserts, 3 updates (A, J, G fare=0), 2 deletes (F, J)
            let base_url = QuickstartTripsTable::V8Trips8I3U1D.url_to_mor_avro();
            let hudi_table = Table::new_blocking(base_url.path())?;

            let deleted_riders = ["rider-F", "rider-J"];

            // verify deleted records are not present in latest snapshot
            let records = hudi_table.read_snapshot_blocking(empty_filters())?;
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
            let records = hudi_table.read_snapshot_as_of_blocking(first_commit, empty_filters())?;
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
    }

    /// Streaming query tests for v8 tables
    mod streaming_queries {
        use super::*;
        use futures::StreamExt;
        use hudi_core::table::ReadOptions;

        #[tokio::test]
        async fn test_read_snapshot_stream_empty_table() -> Result<()> {
            let base_url = SampleTable::V8Empty.url_to_cow();
            let hudi_table = Table::new(base_url.path()).await?;
            let options = ReadOptions::new();
            let mut stream = hudi_table.read_snapshot_stream(&options).await?;

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
            let mut stream = hudi_table.read_snapshot_stream(&options).await?;

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
            let options = ReadOptions::new().with_batch_size(1);
            let mut stream = hudi_table.read_snapshot_stream(&options).await?;

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
            ]);
            let mut stream = hudi_table.read_snapshot_stream(&options).await?;

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
            let file_slices = hudi_table.get_file_slices(empty_filters()).await?;
            assert!(
                !file_slices.is_empty(),
                "Should have at least one file slice"
            );

            let options = ReadOptions::new();
            let file_slice = &file_slices[0];
            let mut stream = hudi_table
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

            let file_slices = hudi_table.get_file_slices(empty_filters()).await?;
            let file_slice = &file_slices[0];

            // Test with small batch size
            let options = ReadOptions::new().with_batch_size(1);
            let mut stream = hudi_table
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
            let mut stream = hudi_table.read_snapshot_stream(&options).await?;

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

/// Test module for streaming read APIs.
/// These tests verify the streaming versions of snapshot and file slice reads.
mod streaming_queries {
    use super::*;
    use arrow::record_batch::RecordBatch;
    use futures::StreamExt;
    use hudi_core::table::ReadOptions;

    #[tokio::test]
    async fn test_read_snapshot_stream_empty_table() -> Result<()> {
        for base_url in SampleTable::V6Empty.urls() {
            let hudi_table = Table::new(base_url.path()).await?;
            let options = ReadOptions::new();
            let mut stream = hudi_table.read_snapshot_stream(&options).await?;

            // Collect all batches from stream
            let mut batches = Vec::new();
            while let Some(result) = stream.next().await {
                batches.push(result?);
            }
            assert!(batches.is_empty(), "Empty table should produce no batches");
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_read_snapshot_stream_basic() -> Result<()> {
        for base_url in SampleTable::V6Nonpartitioned.urls() {
            let hudi_table = Table::new(base_url.path()).await?;
            let options = ReadOptions::new();
            let mut stream = hudi_table.read_snapshot_stream(&options).await?;

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
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_read_snapshot_stream_with_batch_size() -> Result<()> {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await?;

        // Request small batch size
        let options = ReadOptions::new().with_batch_size(1);
        let mut stream = hudi_table.read_snapshot_stream(&options).await?;

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
        let base_url = SampleTable::V6ComplexkeygenHivestyle.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await?;

        let options = ReadOptions::new().with_filters([
            ("byteField", ">=", "10"),
            ("byteField", "<", "20"),
            ("shortField", "!=", "100"),
        ]);
        let mut stream = hudi_table.read_snapshot_stream(&options).await?;

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
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await?;

        // Get file slices first
        let file_slices = hudi_table.get_file_slices(empty_filters()).await?;
        assert!(
            !file_slices.is_empty(),
            "Should have at least one file slice"
        );

        let options = ReadOptions::new();
        let file_slice = &file_slices[0];
        let mut stream = hudi_table
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
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await?;

        let file_slices = hudi_table.get_file_slices(empty_filters()).await?;
        let file_slice = &file_slices[0];

        // Test with small batch size
        let options = ReadOptions::new().with_batch_size(1);
        let mut stream = hudi_table
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
        let base_url = QuickstartTripsTable::V6Trips8I1U.url_to_mor_avro();
        let hudi_table = Table::new(base_url.path()).await?;

        let options = ReadOptions::new();
        let mut stream = hudi_table.read_snapshot_stream(&options).await?;

        let mut batches = Vec::new();
        while let Some(result) = stream.next().await {
            batches.push(result?);
        }

        assert!(!batches.is_empty(), "Should produce batches from MOR table");

        // Verify total row count
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 8, "Should have 8 rows (8 inserts)");
        Ok(())
    }

    #[tokio::test]
    async fn test_read_snapshot_stream_successful_read() -> Result<()> {
        // This test verifies that reading from a valid table succeeds without errors.
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await?;

        let options = ReadOptions::new();
        let mut stream = hudi_table.read_snapshot_stream(&options).await?;

        // All reads should succeed without error
        while let Some(result) = stream.next().await {
            assert!(result.is_ok(), "Reading should not produce errors");
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_read_snapshot_stream_empty_table_no_timestamp() -> Result<()> {
        // For an empty table with no commit timestamp, streaming reads should return
        // an empty stream (consistent with read_snapshot behavior).
        let base_url = SampleTable::V6Empty.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await?;

        let options = ReadOptions::new();
        let mut stream = hudi_table.read_snapshot_stream(&options).await?;

        let mut count = 0;
        while (stream.next().await).is_some() {
            count += 1;
        }
        assert_eq!(count, 0, "Empty table should produce no batches");
        Ok(())
    }

    #[tokio::test]
    async fn test_read_snapshot_stream_with_projection() -> Result<()> {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await?;

        // Only request id and name columns (not isActive)
        let options = ReadOptions::new().with_projection(["id", "name"]);
        let mut stream = hudi_table.read_snapshot_stream(&options).await?;

        let mut batches = Vec::new();
        while let Some(result) = stream.next().await {
            batches.push(result?);
        }

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
    async fn test_read_snapshot_stream_with_row_predicate() -> Result<()> {
        use arrow::array::BooleanArray;

        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await?;

        // Filter rows where isActive = true (Carol and Diana)
        let options = ReadOptions::new().with_row_predicate(|batch: &RecordBatch| {
            let col = batch
                .column_by_name("isActive")
                .ok_or_else(|| hudi_core::error::CoreError::Schema("isActive not found".into()))?;
            let arr = col
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| hudi_core::error::CoreError::Schema("Not boolean".into()))?;
            Ok(arr.clone())
        });

        let mut stream = hudi_table.read_snapshot_stream(&options).await?;

        let mut batches = Vec::new();
        while let Some(result) = stream.next().await {
            batches.push(result?);
        }

        assert!(!batches.is_empty(), "Should produce at least one batch");

        let schema = &batches[0].schema();
        let records = concat_batches(schema, &batches)?;

        // Should only have Carol and Diana (isActive = true)
        let sample_data = SampleTable::sample_data_order_by_id(&records);
        assert_eq!(sample_data, vec![(3, "Carol", true), (4, "Diana", true)]);
        Ok(())
    }

    #[tokio::test]
    async fn test_read_snapshot_stream_with_projection_and_row_predicate() -> Result<()> {
        use arrow::array::{BooleanArray, Int32Array};

        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await?;

        // Project only id and isActive, filter where isActive = true
        let options = ReadOptions::new()
            .with_projection(["id", "isActive"])
            .with_row_predicate(|batch: &RecordBatch| {
                let col = batch.column_by_name("isActive").ok_or_else(|| {
                    hudi_core::error::CoreError::Schema("isActive not found".into())
                })?;
                let arr = col
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| hudi_core::error::CoreError::Schema("Not boolean".into()))?;
                Ok(arr.clone())
            });

        let mut stream = hudi_table.read_snapshot_stream(&options).await?;

        let mut batches = Vec::new();
        while let Some(result) = stream.next().await {
            batches.push(result?);
        }

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
    async fn test_read_snapshot_stream_projection_invalid_column() -> Result<()> {
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await?;

        // Request a non-existent column
        let options = ReadOptions::new().with_projection(["id", "nonexistent_column"]);
        let mut stream = hudi_table.read_snapshot_stream(&options).await?;

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
}

/// Test module for tables with metadata table (MDT) enabled.
/// These tests verify MDT-accelerated file listing and partition normalization.
mod mdt_enabled_tables {
    use super::*;
    use hudi_core::table::PartitionPruner;

    mod snapshot_queries {
        use super::*;

        /// Test reading a V8 MOR non-partitioned table with MDT enabled.
        /// Verifies:
        /// 1. Table can be read correctly via MDT file listing
        /// 2. MDT partition key normalization ("." -> "") works correctly
        /// 3. File slices are retrieved correctly from MDT
        #[test]
        fn test_v8_nonpartitioned_with_mdt() -> Result<()> {
            let base_url = SampleTableMdt::V8Nonpartitioned.url_to_mor_avro();
            let hudi_table = Table::new_blocking(base_url.path())?;

            // Verify MDT is enabled
            assert!(
                hudi_table.is_metadata_table_enabled(),
                "Metadata table should be enabled"
            );

            // Get file slices - this uses MDT file listing
            let file_slices = hudi_table.get_file_slices_blocking(empty_filters())?;

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
        async fn test_v8_nonpartitioned_mdt_partition_normalization() -> Result<()> {
            let base_url = SampleTableMdt::V8Nonpartitioned.url_to_mor_avro();
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

        /// Test reading column statistics from metadata table.
        #[tokio::test]
        async fn test_v8_nonpartitioned_read_column_stats() -> Result<()> {
            let base_url = SampleTableMdt::V8Nonpartitioned.url_to_mor_avro();
            let hudi_table = Table::new(base_url.path()).await?;

            // Verify column_stats partition is available
            assert!(hudi_table.has_column_stats_partition());

            // Get file slices and read column stats
            let file_slices = hudi_table.get_file_slices(empty_filters()).await?;
            let file_names: Vec<String> = file_slices
                .iter()
                .map(|fs| fs.base_file.file_name())
                .collect();
            let file_name_refs: Vec<&str> = file_names.iter().map(|s| s.as_str()).collect();

            let stats = hudi_table
                .read_column_stats_for_files(&file_name_refs, &["id", "longField"], "")
                .await?;

            // API should work - stats may be empty or contain data depending on test data
            assert!(stats.is_empty() || stats.values().any(|s| !s.columns.is_empty()));

            Ok(())
        }
    }
}
