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
            let hudi_table = Table::new(base_url.path()).await?;
            let commit_timestamps = hudi_table
                .timeline
                .completed_commits
                .iter()
                .map(|i| i.timestamp.as_str())
                .collect::<Vec<_>>();
            let latest_commit = commit_timestamps.last().unwrap();
            let records = hudi_table
                .read(
                    &ReadOptions::new()
                        .with_as_of_timestamp(latest_commit)
                        .with_hudi_option(HudiReadConfig::UseReadOptimizedMode.as_ref(), "true"),
                )
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
        async fn test_read_optimized_file_slices_have_no_log_files() -> Result<()> {
            let base_url = SampleTable::V6Nonpartitioned.url_to_mor_parquet();
            let hudi_table = Table::new(base_url.path()).await?;
            let ro_opts = ReadOptions::new()
                .with_hudi_option(HudiReadConfig::UseReadOptimizedMode.as_ref(), "true");
            let file_slices = hudi_table.get_file_slices(&ro_opts).await?;
            assert!(!file_slices.is_empty());
            for fs in &file_slices {
                assert!(
                    fs.log_files.is_empty(),
                    "RO mode should strip log files from file slices"
                );
            }
            Ok(())
        }

        #[tokio::test]
        async fn test_read_optimized_via_read_options() -> Result<()> {
            let base_url = SampleTable::V6Nonpartitioned.url_to_mor_parquet();
            let hudi_table = Table::new(base_url.path()).await?;
            let ro_options = ReadOptions::new()
                .with_hudi_option(HudiReadConfig::UseReadOptimizedMode.as_ref(), "true");
            let file_slices = hudi_table.get_file_slices(&ro_options).await?;
            assert!(!file_slices.is_empty());
            for fs in &file_slices {
                assert!(
                    fs.log_files.is_empty(),
                    "RO via ReadOptions should strip log files"
                );
            }

            let default_slices = hudi_table.get_file_slices(&ReadOptions::new()).await?;
            assert!(
                default_slices.iter().any(|fs| fs.has_log_file()),
                "Without RO, MOR slices should have log files"
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

            let fg_reader = hudi_table
                .create_file_group_reader_with_options(None, std::iter::empty::<(&str, &str)>())?;
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

            let fg_reader = hudi_table
                .create_file_group_reader_with_options(None, std::iter::empty::<(&str, &str)>())?;
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

            let fg_reader = hudi_table
                .create_file_group_reader_with_options(None, std::iter::empty::<(&str, &str)>())?;
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

    fn read_optimized_options() -> ReadOptions {
        ReadOptions::new().with_hudi_option(HudiReadConfig::UseReadOptimizedMode.as_ref(), "true")
    }

    async fn read_txn_rows_from_snapshot(hudi_table: &Table) -> Result<Vec<(String, String, i64)>> {
        let records = hudi_table.read(&ReadOptions::new()).await?;
        Ok(txn_rows(&records))
    }

    async fn read_txn_rows_from_snapshot_ro(
        hudi_table: &Table,
    ) -> Result<Vec<(String, String, i64)>> {
        let records = hudi_table.read(&read_optimized_options()).await?;
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

    async fn open_table(path: &str) -> Result<Table> {
        Table::new(path).await
    }

    mod snapshot_queries {
        use super::*;

        #[tokio::test]
        async fn test_timebasedkeygen_epochmillis_cow_snapshot() -> Result<()> {
            let base_url = SampleTable::V9TimebasedkeygenEpochmillis.url_to_cow();
            let hudi_table = open_table(base_url.path()).await?;

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
            let hudi_table = open_table(base_url.path()).await?;

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
            let hudi_table = open_table(base_url.path()).await?;

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
            let hudi_table = open_table(base_url.path()).await?;

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
            let hudi_table = open_table(base_url.path()).await?;

            let rows = read_txn_rows_from_snapshot_ro(&hudi_table).await?;
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
            let hudi_table = open_table(base_url.path()).await?;

            let rows = read_txn_rows_from_snapshot_ro(&hudi_table).await?;
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
            let hudi_table = open_table(base_url.path()).await?;

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
            let hudi_table = open_table(base_url.path()).await?;

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
            let hudi_table = open_table(base_url.path()).await?;

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
            let hudi_table = open_table(base_url.path()).await?;

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
            let hudi_table = open_table(base_url.path()).await?;

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
            let hudi_table = open_table(base_url.path()).await?;

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
            let hudi_table = open_table(base_url.path()).await?;

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
            let hudi_table = open_table(base_url.path()).await?;

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
            let hudi_table = open_table(base_url.path()).await?;

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
            let hudi_table = open_table(base_url.path()).await?;

            let file_slices = hudi_table.get_file_slices(&ReadOptions::new()).await?;
            assert!(
                !file_slices.is_empty(),
                "Should have at least one file slice"
            );

            let fg_reader = hudi_table
                .create_file_group_reader_with_options(None, std::iter::empty::<(&str, &str)>())?;
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
            let hudi_table = open_table(base_url.path()).await?;

            let file_slices = hudi_table.get_file_slices(&ReadOptions::new()).await?;
            let file_slice = &file_slices[0];

            let fg_reader = hudi_table
                .create_file_group_reader_with_options(None, std::iter::empty::<(&str, &str)>())?;
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
            let hudi_table = open_table(base_url.path()).await?;

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
            let hudi_table = open_table(base_url.path()).await?;

            let options = read_optimized_options();
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

        let fg_reader = hudi_table
            .create_file_group_reader_with_options(None, std::iter::empty::<(&str, &str)>())?;
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

        let fg_reader = hudi_table
            .create_file_group_reader_with_options(None, std::iter::empty::<(&str, &str)>())?;
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

/// Regression tests: manual `get_file_slices` + `create_file_group_reader_with_options`
/// + `read_file_slice` must produce the same results as `table.read()`.
///
/// Before the fix, `create_file_group_reader_with_options` passed caller options
/// directly to `FileGroupReader` without resolving `AsOfTimestamp` → `EndTimestamp`
/// (snapshot) or filling default `StartTimestamp` / `EndTimestamp` (incremental).
/// For MOR non-read-optimized time-travel, log files could be read without the
/// intended upper bound.
mod manual_reader_matches_table_read {
    use super::*;
    use arrow::compute::concat_batches;

    async fn read_via_manual_reader(
        table: &Table,
        options: &ReadOptions,
    ) -> Result<Vec<RecordBatch>> {
        let file_slices = table.get_file_slices(options).await?;
        let fg_reader = table.create_file_group_reader_with_options(
            Some(options),
            std::iter::empty::<(&str, &str)>(),
        )?;
        let batches = futures::future::try_join_all(
            file_slices
                .iter()
                .map(|f| fg_reader.read_file_slice(f, options)),
        )
        .await?;
        Ok(batches)
    }

    #[tokio::test]
    async fn test_mor_time_travel_manual_reader_matches_table_read() -> Result<()> {
        let base_url = QuickstartTripsTable::V6Trips8I1U.url_to_mor_avro();
        let hudi_table = Table::new(base_url.path()).await?;

        let first_commit = hudi_table
            .timeline
            .completed_commits
            .iter()
            .map(|i| i.timestamp.as_str())
            .next()
            .expect("V6Trips8I1U should have at least one commit");

        let options = ReadOptions::new().with_as_of_timestamp(first_commit);

        let expected = hudi_table.read(&options).await?;
        let actual = read_via_manual_reader(&hudi_table, &options).await?;

        let expected_schema = &expected[0].schema();
        let expected_batch = concat_batches(expected_schema, &expected)?;
        let actual_schema = &actual[0].schema();
        let actual_batch = concat_batches(actual_schema, &actual)?;

        assert_eq!(
            expected_batch.num_rows(),
            actual_batch.num_rows(),
            "MOR time-travel: manual reader should return same row count as table.read()"
        );

        let mut expected_riders = QuickstartTripsTable::uuid_rider_and_fare(&expected_batch);
        let mut actual_riders = QuickstartTripsTable::uuid_rider_and_fare(&actual_batch);
        expected_riders.sort_unstable_by_key(|(uuid, _, _)| uuid.clone());
        actual_riders.sort_unstable_by_key(|(uuid, _, _)| uuid.clone());
        assert_eq!(
            expected_riders, actual_riders,
            "MOR time-travel: manual reader data should match table.read() data"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_incremental_with_explicit_range_manual_reader_matches_table_read() -> Result<()> {
        let base_url = SampleTable::V9NonpartitionedRollback.url_to_mor_avro();
        let hudi_table = Table::new(base_url.path()).await?;

        let deltacommits: Vec<_> = hudi_table
            .timeline
            .get_completed_deltacommits(false)
            .await?;
        assert!(deltacommits.len() >= 2);
        let first_commit = &deltacommits[0].timestamp;
        let second_commit = &deltacommits[1].timestamp;

        let options = ReadOptions::new()
            .with_query_type(QueryType::Incremental)
            .with_start_timestamp("19700101000000000")
            .with_end_timestamp(first_commit.as_str());

        let expected = hudi_table.read(&options).await?;
        let actual = read_via_manual_reader(&hudi_table, &options).await?;

        let expected_rows: usize = expected.iter().map(|b| b.num_rows()).sum();
        let actual_rows: usize = actual.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            expected_rows, actual_rows,
            "incremental (explicit range up to first commit): row counts must match"
        );

        let options = ReadOptions::new()
            .with_query_type(QueryType::Incremental)
            .with_start_timestamp(first_commit.as_str())
            .with_end_timestamp(second_commit.as_str());

        let expected = hudi_table.read(&options).await?;
        let actual = read_via_manual_reader(&hudi_table, &options).await?;

        let expected_rows: usize = expected.iter().map(|b| b.num_rows()).sum();
        let actual_rows: usize = actual.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            expected_rows, actual_rows,
            "incremental (first..second commit): row counts must match"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_incremental_with_start_only_manual_reader_matches_table_read() -> Result<()> {
        let base_url = SampleTable::V9NonpartitionedRollback.url_to_mor_avro();
        let hudi_table = Table::new(base_url.path()).await?;

        let deltacommits: Vec<_> = hudi_table
            .timeline
            .get_completed_deltacommits(false)
            .await?;
        let first_commit = &deltacommits[0].timestamp;

        let options = ReadOptions::new()
            .with_query_type(QueryType::Incremental)
            .with_start_timestamp(first_commit.as_str());

        let expected = hudi_table.read(&options).await?;
        let actual = read_via_manual_reader(&hudi_table, &options).await?;

        let expected_rows: usize = expected.iter().map(|b| b.num_rows()).sum();
        let actual_rows: usize = actual.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            expected_rows, actual_rows,
            "incremental (start only, end defaults to latest): row counts must match"
        );

        Ok(())
    }
}

mod lance_tables {
    use super::*;
    use arrow_array::{Float64Array, Int32Array, Int64Array, StringArray};
    use std::collections::HashMap;
    use url::Url;

    async fn read_lance_table(
        base_url: Url,
        projection: impl IntoIterator<Item = &'static str>,
    ) -> Result<RecordBatch> {
        let hudi_table = Table::new(base_url.path()).await?;
        let batches = hudi_table
            .read(&ReadOptions::new().with_projection(projection))
            .await?;
        let schema = batches[0].schema();
        Ok(concat_batches(&schema, &batches)?)
    }

    fn string_column<'a>(batch: &'a RecordBatch, name: &str) -> &'a StringArray {
        batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
    }

    fn int64_column<'a>(batch: &'a RecordBatch, name: &str) -> &'a Int64Array {
        batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
    }

    fn float64_column<'a>(batch: &'a RecordBatch, name: &str) -> &'a Float64Array {
        batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
    }

    fn sorted_string_values(batch: &RecordBatch, name: &str) -> Vec<String> {
        let values = string_column(batch, name);
        let mut strings = (0..batch.num_rows())
            .map(|row| values.value(row).to_string())
            .collect::<Vec<_>>();
        strings.sort_unstable();
        strings
    }

    fn assert_float_eq(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 1e-9,
            "expected {expected}, got {actual}"
        );
    }

    fn txn_rows(batch: &RecordBatch) -> HashMap<String, (String, i64, Option<String>)> {
        let txn_ids = string_column(batch, "txn_id");
        let txn_types = string_column(batch, "txn_type");
        let txn_timestamps = int64_column(batch, "txn_ts");
        let regions = batch
            .column_by_name("region")
            .map(|column| column.as_any().downcast_ref::<StringArray>().unwrap());

        let mut rows = HashMap::new();
        for row_idx in 0..batch.num_rows() {
            rows.insert(
                txn_ids.value(row_idx).to_string(),
                (
                    txn_types.value(row_idx).to_string(),
                    txn_timestamps.value(row_idx),
                    regions.map(|column| column.value(row_idx).to_string()),
                ),
            );
        }
        rows
    }

    fn assert_lance_txn_table_rows(batch: &RecordBatch, partitioned: bool) {
        let rows = txn_rows(batch);
        let mut actual_ids = rows.keys().cloned().collect::<Vec<_>>();
        actual_ids.sort_unstable();
        assert_eq!(
            actual_ids,
            [
                "TXN-001", "TXN-003", "TXN-004", "TXN-006", "TXN-007", "TXN-008", "TXN-009",
                "TXN-010", "TXN-011", "TXN-012", "TXN-013", "TXN-014", "TXN-015", "TXN-016",
            ]
        );
        assert!(
            !rows.contains_key("TXN-002"),
            "deleted TXN-002 must be absent"
        );
        assert!(
            !rows.contains_key("TXN-005"),
            "deleted TXN-005 must be absent"
        );
        assert_eq!(rows.get("TXN-001").unwrap().0, "reversal");
        assert_eq!(rows.get("TXN-001").unwrap().1, 1700100000001);
        assert_eq!(rows.get("TXN-007").unwrap().1, 1700300000007);
        assert_eq!(rows.get("TXN-016").unwrap().0, "debit");

        if partitioned {
            let region_rows = rows
                .iter()
                .map(|(txn_id, (_, _, region))| {
                    (txn_id.as_str(), region.as_ref().unwrap().as_str())
                })
                .collect::<HashMap<_, _>>();
            assert_eq!(region_rows.get("TXN-001"), Some(&"us"));
            assert_eq!(region_rows.get("TXN-004"), Some(&"eu"));
            assert_eq!(region_rows.get("TXN-007"), Some(&"apac"));
            assert_eq!(region_rows.get("TXN-016"), Some(&"apac"));
        }
    }

    fn trips_rows(batch: &RecordBatch) -> HashMap<String, (String, f64, i64)> {
        let riders = string_column(batch, "rider");
        let drivers = string_column(batch, "driver");
        let fares = float64_column(batch, "fare");
        let timestamps = int64_column(batch, "ts");

        let mut rows = HashMap::new();
        for row_idx in 0..batch.num_rows() {
            rows.insert(
                riders.value(row_idx).to_string(),
                (
                    drivers.value(row_idx).to_string(),
                    fares.value(row_idx),
                    timestamps.value(row_idx),
                ),
            );
        }
        rows
    }

    #[tokio::test]
    async fn test_v9_lance_nonpartitioned_cow_snapshot_applies_hudi_updates_deletes_and_inserts()
    -> Result<()> {
        let batch = read_lance_table(
            SampleTable::V9LanceNonpartitioned.url_to_cow(),
            ["id", "name", "score", "updated_at"],
        )
        .await?;

        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let names = batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let scores = batch
            .column_by_name("score")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let updated_at = batch
            .column_by_name("updated_at")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let mut rows = HashMap::new();
        for row_idx in 0..batch.num_rows() {
            rows.insert(
                ids.value(row_idx),
                (
                    names.value(row_idx).to_string(),
                    scores.value(row_idx),
                    updated_at.value(row_idx),
                ),
            );
        }

        let mut actual_ids = rows.keys().copied().collect::<Vec<_>>();
        actual_ids.sort_unstable();
        assert_eq!(actual_ids, vec![1, 2, 3, 5, 6, 7, 8, 9, 10]);
        assert!(!rows.contains_key(&4), "deleted id 4 must be absent");
        assert!(
            (rows.get(&1).unwrap().1 - 0.96).abs() < 1e-9,
            "id 1 should reflect the score update"
        );
        assert_eq!(rows.get(&1).unwrap().2, 1700100000000);
        assert!(
            (rows.get(&2).unwrap().1 - 0.93).abs() < 1e-9,
            "id 2 should reflect the score update"
        );
        assert_eq!(rows.get(&2).unwrap().2, 1700100000001);
        assert_eq!(rows.get(&9).unwrap().0, "feature-set-iota");
        assert_eq!(rows.get(&10).unwrap().0, "feature-set-kappa");

        Ok(())
    }

    #[tokio::test]
    async fn test_v9_lance_txns_nonpart_cow_snapshot_applies_updates_deletes_and_inserts()
    -> Result<()> {
        let batch = read_lance_table(
            SampleTable::V9LanceTxnsNonpart.url_to_cow(),
            ["txn_id", "txn_type", "txn_ts"],
        )
        .await?;
        assert_lance_txn_table_rows(&batch, false);
        Ok(())
    }

    #[tokio::test]
    async fn test_v9_lance_txns_simple_cow_snapshot_applies_updates_deletes_and_inserts()
    -> Result<()> {
        let batch = read_lance_table(
            SampleTable::V9LanceTxnsSimple.url_to_cow(),
            ["txn_id", "txn_type", "txn_ts", "region"],
        )
        .await?;
        assert_lance_txn_table_rows(&batch, true);
        Ok(())
    }

    #[tokio::test]
    async fn test_v9_trips_lance_cow_snapshot_applies_updates_deletes_and_inserts() -> Result<()> {
        let batch = read_lance_table(
            QuickstartTripsTable::V9TripsLance.url_to_cow(),
            ["rider", "driver", "fare", "ts"],
        )
        .await?;
        let rows = trips_rows(&batch);
        let mut riders = rows.keys().cloned().collect::<Vec<_>>();
        riders.sort_unstable();
        assert_eq!(
            riders,
            [
                "rider-A", "rider-C", "rider-D", "rider-E", "rider-G", "rider-I", "rider-J",
                "rider-K", "rider-L", "rider-M", "rider-N",
            ]
        );
        assert!(
            !rows.contains_key("rider-F"),
            "deleted rider-F must be absent"
        );
        assert_float_eq(rows.get("rider-A").unwrap().1, 0.0);
        assert_eq!(rows.get("rider-A").unwrap().2, 1695200000000);
        assert_float_eq(rows.get("rider-G").unwrap().1, 0.0);
        assert_eq!(rows.get("rider-K").unwrap().0, "driver-U");
        assert_eq!(rows.get("rider-N").unwrap().0, "driver-X");
        Ok(())
    }

    #[tokio::test]
    async fn test_v9_lance_nonhivestyle_mor_snapshot_merges_available_log_update_and_base_files()
    -> Result<()> {
        let batch = read_lance_table(
            SampleTable::V9LanceNonhivestyle.url_to_mor_avro(),
            ["event_id", "user_id", "payload", "event_ts", "event_date"],
        )
        .await?;
        let event_ids = sorted_string_values(&batch, "event_id");
        assert_eq!(
            event_ids,
            [
                "evt-001", "evt-002", "evt-003", "evt-004", "evt-005", "evt-006", "evt-007",
                "evt-008", "evt-009", "evt-010", "evt-011", "evt-012", "evt-013", "evt-014",
            ]
        );

        let event_id_col = string_column(&batch, "event_id");
        let user_id_col = string_column(&batch, "user_id");
        let payload_col = string_column(&batch, "payload");
        let event_ts_col = int64_column(&batch, "event_ts");
        let mut rows = HashMap::new();
        for row_idx in 0..batch.num_rows() {
            rows.insert(
                event_id_col.value(row_idx).to_string(),
                (
                    user_id_col.value(row_idx).to_string(),
                    payload_col.value(row_idx).to_string(),
                    event_ts_col.value(row_idx),
                ),
            );
        }

        assert_eq!(
            rows.get("evt-001").unwrap().1,
            r#"{"page": "/home", "session": "sess-abc123"}"#
        );
        assert_eq!(rows.get("evt-001").unwrap().2, 1700000000001);
        assert_eq!(rows.get("evt-002").unwrap().1, r#"{"button": "signup"}"#);
        assert_eq!(rows.get("evt-013").unwrap().0, "user-100");
        assert_eq!(rows.get("evt-014").unwrap().0, "user-101");
        Ok(())
    }

    #[tokio::test]
    async fn test_v9_trips_lance_mor_snapshot_merges_available_log_update_and_base_files()
    -> Result<()> {
        let batch = read_lance_table(
            QuickstartTripsTable::V9TripsLance.url_to_mor_avro(),
            ["rider", "driver", "fare", "ts"],
        )
        .await?;
        let rows = trips_rows(&batch);
        let mut riders = rows.keys().cloned().collect::<Vec<_>>();
        riders.sort_unstable();
        assert_eq!(
            riders,
            [
                "rider-A", "rider-C", "rider-D", "rider-E", "rider-F", "rider-G", "rider-I",
                "rider-J", "rider-M", "rider-N", "rider-O", "rider-P",
            ]
        );
        assert_float_eq(rows.get("rider-A").unwrap().1, 0.0);
        assert_eq!(rows.get("rider-A").unwrap().2, 1695200000000);
        assert_float_eq(rows.get("rider-C").unwrap().1, 27.70);
        assert_float_eq(rows.get("rider-G").unwrap().1, 43.40);
        assert_eq!(rows.get("rider-O").unwrap().0, "driver-Y");
        assert_eq!(rows.get("rider-P").unwrap().0, "driver-Z");
        Ok(())
    }

    #[tokio::test]
    async fn test_v9_lance_nonpartitioned_cow_read_uses_extension_fallback_without_format_config()
    -> Result<()> {
        // Safe because each `path_to_cow()` call extracts the zip into its own
        // tempdir (see `hudi_test::extract_test_table`); the in-place edit does
        // not leak into other tests.
        let table_path = SampleTable::V9LanceNonpartitioned.path_to_cow();
        let props_path = std::path::Path::new(&table_path).join(".hoodie/hoodie.properties");
        let props = std::fs::read_to_string(&props_path).unwrap();
        let props_without_format = props
            .lines()
            .filter(|line| !line.starts_with("hoodie.table.base.file.format="))
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(&props_path, props_without_format).unwrap();

        let hudi_table = Table::new(&table_path).await?;
        let file_slices = hudi_table.get_file_slices(&ReadOptions::new()).await?;
        assert!(
            file_slices.iter().any(|slice| slice
                .base_file_relative_path()
                .is_ok_and(|path| path.ends_with(".lance"))),
            "table listing should discover .lance base files without an explicit format config"
        );

        let batches = hudi_table
            .read(&ReadOptions::new().with_projection(["id"]))
            .await?;
        let total_rows = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
        assert_eq!(total_rows, 9);

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
