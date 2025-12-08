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
use hudi_test::{QuickstartTripsTable, SampleTable};

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
            assert!(riders
                .iter()
                .all(|rider| { !deleted_riders.contains(&rider.as_str()) }));

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
        fn test_quickstart_trips_inserts_updates() -> Result<()> {
            let base_url = QuickstartTripsTable::V8Trips8I1U.url_to_mor_avro();
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
            // Updated fare should be 25.0 (after the update)
            assert_eq!(uuid_rider_and_fare[0].2, 25.0);

            // verify updated record as of the first commit (before update)
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
            // Original fare should be 33.9 (before the update)
            assert_eq!(uuid_rider_and_fare[0].2, 33.9);

            Ok(())
        }

        #[test]
        fn test_quickstart_trips_inserts_deletes() -> Result<()> {
            let base_url = QuickstartTripsTable::V8Trips8I3D.url_to_mor_avro();
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
            // All deleted riders should not be present
            assert!(riders
                .iter()
                .all(|rider| { !deleted_riders.contains(&rider.as_str()) }));

            // verify deleted record as of the first commit (before delete)
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
            // All 3 riders should be present before delete
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
}
