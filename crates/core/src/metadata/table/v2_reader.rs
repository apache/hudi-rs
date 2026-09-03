// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Reads the metadata table's `files` partition through the version two file
//! group reader.
//!
//! Every metadata-table concern lives here and none of it reaches `reader_v2`,
//! mirroring how `HoodieBackedTableMetadata` sits above Java's generic file group
//! reader. This layer knows the partition, the record shape and the decoded type;
//! the reader below is handed a slice, a schema and a predicate.
//!
//! Behaviour matches [`super::reader::MetadataTableFileGroupReader`] deliberately,
//! down to the `.` to empty-string key normalisation, because the two are compared
//! record for record and value for value in the tests. Where the two differ, this
//! one is wrong.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, RecordBatch};

use crate::Result;
use crate::config::HudiConfigs;
use crate::error::CoreError;
use crate::file_group::base_file::reader::KeyPredicate;
use crate::file_group::file_slice::FileSlice;
use crate::file_group::reader_v2::engine::HoodieFileGroupReader;
use crate::file_group::reader_v2::input_split::InputSplit;
use crate::file_group::reader_v2::reader_parameters::ReaderParameters;
use crate::file_group::reader_v2::resolver::resolve_reader_context;
use crate::metadata::table::records::{
    FilesPartitionRecord, HoodieMetadataFileInfo, MetadataRecordType,
};
use crate::storage::Storage;

/// Column names in `HoodieMetadataRecord`, as the metadata table's schema spells
/// them. Named here rather than in `reader_v2`, which must stay unaware of them.
const KEY_COLUMN: &str = "key";
const TYPE_COLUMN: &str = "type";
const FILESYSTEM_METADATA_COLUMN: &str = "filesystemMetadata";
const FILE_SIZE_FIELD: &str = "size";
const FILE_IS_DELETED_FIELD: &str = "isDeleted";

/// Reads a metadata-table file slice through the version two reader.
pub(crate) struct MetadataTableV2Reader {
    hudi_configs: Arc<HudiConfigs>,
    storage: Arc<Storage>,
}

impl MetadataTableV2Reader {
    pub(crate) fn new(hudi_configs: Arc<HudiConfigs>, storage: Arc<Storage>) -> Self {
        Self {
            hudi_configs,
            storage,
        }
    }

    /// Read the `files` partition's records from one file slice.
    ///
    /// `keys` empty reads every record; otherwise only those keys, pushed into the
    /// base file reader so the read seeks rather than scans — which is what the
    /// reader this replaces does through `HFileReader::lookup_records`.
    /// Read the `files` partition's records from one file slice.
    ///
    /// Decodes the merged batch into the records the caller expects. Callers that
    /// want the batch itself, rather than decoded structs, use
    /// [`Self::read_files_partition_batch`]; this is that plus the decode.
    pub(crate) async fn read_files_partition(
        &self,
        file_slice: &FileSlice,
        keys: &[&str],
    ) -> Result<HashMap<String, FilesPartitionRecord>> {
        let batch = self.read_files_partition_batch(file_slice, keys).await?;
        Self::records_from_batch(&batch, keys)
    }

    /// The merged `files` partition batch, before it is decoded into records.
    ///
    /// Separate because the batch is what an Arrow consumer wants and decoding it
    /// only to re-encode would be work in both directions. Nothing about the read
    /// differs between the two; this is the read, and the other is this plus a
    /// decode.
    pub(crate) async fn read_files_partition_batch(
        &self,
        file_slice: &FileSlice,
        keys: &[&str],
    ) -> Result<RecordBatch> {
        let base_file_path = file_slice.base_file_relative_path()?;
        let log_file_paths = if file_slice.has_log_file() {
            file_slice
                .log_files
                .iter()
                .map(|log_file| file_slice.log_file_relative_path(log_file))
                .collect::<Result<Vec<String>>>()?
        } else {
            vec![]
        };

        let mut reader_context = resolve_reader_context(
            &self.hudi_configs,
            !log_file_paths.is_empty(),
            base_file_path.as_deref(),
        )?;
        // The metadata table sets `hoodie.populate.meta.fields=false`, so the record
        // key is its own `key` column rather than `_hoodie_record_key`. Without this
        // the merge keys every record on an empty string and collapses them into one.
        reader_context.rebuild_record_context(FilesPartitionRecord::PARTITION_NAME.to_string());

        // A named-key read is a point lookup, which is the normal shape for the
        // metadata table; an empty key list is the full scan the `files` partition
        // is allowed.
        let key_predicate = (!keys.is_empty())
            .then(|| KeyPredicate::Keys(keys.iter().map(|k| (*k).to_string()).collect()));

        reader_context.key_predicate = key_predicate;

        let base_file_commit_time = file_slice
            .base_file
            .as_ref()
            .map(|base_file| base_file.commit_timestamp.clone());

        let mut reader = HoodieFileGroupReader::new(
            Arc::new(reader_context),
            self.storage.clone(),
            InputSplit::new(
                base_file_path,
                base_file_commit_time,
                log_file_paths,
                FilesPartitionRecord::PARTITION_NAME.to_string(),
            ),
            ReaderParameters::default(),
            None,
            None,
        )?;

        let batch = reader.read().await?;
        log::debug!(
            "metadata read of '{}' with {} named key(s): merge map peaked at {} entries",
            FilesPartitionRecord::PARTITION_NAME,
            keys.len(),
            reader.read_stats().merge_map_peak_entries
        );
        Ok(batch)
    }

    /// Convert the merged Arrow batch into the decoded records the caller expects.
    ///
    /// This is the whole of this layer's new behaviour, and the reason the tests
    /// compare values rather than key sets: everything else is delegation.
    fn records_from_batch(
        batch: &RecordBatch,
        keys: &[&str],
    ) -> Result<HashMap<String, FilesPartitionRecord>> {
        let missing = |name: &str| {
            CoreError::MetadataTable(format!(
                "A metadata record must carry a '{name}' column; the metadata table's \
                 schema has changed or the wrong partition was read"
            ))
        };
        let key_column = batch
            .column_by_name(KEY_COLUMN)
            .ok_or_else(|| missing(KEY_COLUMN))?
            .as_string_opt::<i32>()
            .ok_or_else(|| missing(KEY_COLUMN))?;
        let types = batch
            .column_by_name(TYPE_COLUMN)
            .ok_or_else(|| missing(TYPE_COLUMN))?
            .as_primitive_opt::<arrow_array::types::Int32Type>()
            .ok_or_else(|| missing(TYPE_COLUMN))?;
        let maps = batch
            .column_by_name(FILESYSTEM_METADATA_COLUMN)
            .ok_or_else(|| missing(FILESYSTEM_METADATA_COLUMN))?
            .as_map_opt()
            .ok_or_else(|| missing(FILESYSTEM_METADATA_COLUMN))?;

        // Hoisted: this runs per row, and the pruned-keys branch of
        // `fetch_files_partition_records` passes one key per surviving partition, so a
        // linear scan here is rows times keys. The reader this mirrors builds a set
        // once, and the two other filter sites in this crate already do the same
        // (`decode_window`, and the log-block decoder). This was the one that did not.
        let wanted: Option<std::collections::HashSet<&str>> =
            (!keys.is_empty()).then(|| keys.iter().copied().collect());

        let mut out = HashMap::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            if key_column.is_null(row) {
                continue;
            }
            // The predicate narrows which *blocks* the base file reader touches; it
            // does not filter log records, which arrive through the log scanner and
            // reach the merge whatever their key. So a named-key read still has to
            // drop what it did not ask for, which is what the reader this replaces
            // does when it filters the merged result by key.
            //
            // Matched against the key as stored, before the `.` normalisation below,
            // because that is the form the caller passes: `fetch_files_partition_records`
            // asks for `NON_PARTITIONED_NAME` and expects `""` back.
            if let Some(wanted) = wanted.as_ref()
                && !wanted.contains(key_column.value(row))
            {
                continue;
            }
            // An unrecognised type becomes `Unknown` rather than an error, because
            // that is what the reader this replaces does (`get_record_type` falls
            // back to `Unknown`) and parity is the goal. Refusing would be a
            // divergence, and a stricter reader belongs with whoever decides that
            // an unknown metadata type should fail a read.
            let record_type = MetadataRecordType::from(types.value(row));
            // `.` is how a non-partitioned table's partition is spelled on disk; the
            // caller works in empty strings. The reader this replaces normalises at
            // decode time, so this has to as well or the two disagree on every
            // non-partitioned table.
            let key = normalise_partition(key_column.value(row));
            let mut files = Self::file_map(maps, row)?;
            if record_type == MetadataRecordType::AllPartitions
                && let Some(mut info) = files.remove(FilesPartitionRecord::NON_PARTITIONED_NAME)
            {
                info.name = String::new();
                files.insert(String::new(), info);
            }
            out.insert(
                key.clone(),
                FilesPartitionRecord {
                    key,
                    record_type,
                    files,
                },
            );
        }
        Ok(out)
    }

    /// One row's `filesystemMetadata` map, as file name to file info.
    fn file_map(
        maps: &arrow_array::MapArray,
        row: usize,
    ) -> Result<HashMap<String, HoodieMetadataFileInfo>> {
        let mut files = HashMap::new();
        if maps.is_null(row) {
            return Ok(files);
        }
        let entries = maps.value(row);
        let names = entries.column(0).as_string_opt::<i32>().ok_or_else(|| {
            CoreError::MetadataTable(
                "'filesystemMetadata' keys must be Utf8 file names".to_string(),
            )
        })?;
        let values = entries.column(1).as_struct_opt().ok_or_else(|| {
            CoreError::MetadataTable("'filesystemMetadata' values must be structs".to_string())
        })?;
        let sizes = values
            .column_by_name(FILE_SIZE_FIELD)
            .and_then(|c| c.as_primitive_opt::<arrow_array::types::Int64Type>())
            .ok_or_else(|| {
                CoreError::MetadataTable(format!("'{FILE_SIZE_FIELD}' must be Int64"))
            })?;
        let deleted = values
            .column_by_name(FILE_IS_DELETED_FIELD)
            .and_then(|c| c.as_boolean_opt())
            .ok_or_else(|| {
                CoreError::MetadataTable(format!("'{FILE_IS_DELETED_FIELD}' must be Boolean"))
            })?;

        for i in 0..names.len() {
            // A null key or a null value entry is omitted, as the reader this mirrors
            // omits it (`extract_file_info` returns `None` for a non-record value).
            // Defaulting instead would invent a zero-sized, undeleted file and send a
            // read at a path that does not exist — the one divergence here that
            // corrupts rather than errors.
            if names.is_null(i) || values.is_null(i) {
                continue;
            }
            // Taken as written. In a `Files` record these keys are file names, and
            // normalising them as if they were partitions would be wrong in intent
            // even though no file is called `.`. The one place a `.` genuinely means
            // a partition here is an all-partitions record's map, and that is
            // rewritten by the caller, as the reader this replaces does.
            let name = names.value(i).to_string();
            files.insert(
                name.clone(),
                HoodieMetadataFileInfo {
                    name,
                    size: if sizes.is_null(i) { 0 } else { sizes.value(i) },
                    is_deleted: !deleted.is_null(i) && deleted.value(i),
                },
            );
        }
        Ok(files)
    }
}

/// `.` is the non-partitioned table's on-disk partition name; callers use `""`.
fn normalise_partition(raw: &str) -> String {
    if raw == FilesPartitionRecord::NON_PARTITIONED_NAME {
        String::new()
    } else {
        raw.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_group::FileGroup;
    use crate::metadata::table::reader::MetadataTableFileGroupReader;
    use hudi_test::QuickstartTripsTable;
    use std::fs::canonicalize;
    use std::path::PathBuf;
    use url::Url;

    const FILE_GROUP: &str = "files-0000-0";
    /// The `files` partition before the compaction at 20251220210130942: the
    /// initial base file plus every log file written after it. The compacted slice
    /// has no log files, so it would compare the two readers over a base file only
    /// and never exercise the merge.
    const PRECOMPACT_BASE: &str = "files-0000-0_0-955-2690_00000000000000000.hfile";
    const PRECOMPACT_LOGS: &[&str] = &[
        ".files-0000-0_00000000000000000.log.1_0-0-0",
        ".files-0000-0_20251220210108078.log.1_10-999-2838",
        ".files-0000-0_20251220210123755.log.1_3-1032-2950",
        ".files-0000-0_20251220210125441.log.1_5-1057-3024",
        ".files-0000-0_20251220210127080.log.1_3-1082-3100",
        ".files-0000-0_20251220210128625.log.1_5-1107-3174",
        ".files-0000-0_20251220210129235.log.1_3-1118-3220",
        ".files-0000-0_20251220210130911.log.1_3-1149-3338",
    ];

    fn metadata_configs() -> Arc<HudiConfigs> {
        let table_path = QuickstartTripsTable::V8Trips8I3U1D.path_to_mor_avro();
        let mdt = PathBuf::from(table_path).join(".hoodie").join("metadata");
        let uri = Url::from_file_path(canonicalize(&mdt).unwrap())
            .unwrap()
            .as_ref()
            .to_string();
        crate::metadata::table::test_support::metadata_table_configs(&uri)
    }

    fn precompact_slice() -> crate::Result<FileSlice> {
        let mut fg = FileGroup::new(
            FILE_GROUP.to_string(),
            FilesPartitionRecord::PARTITION_NAME.to_string(),
        );
        fg.add_base_file_from_name(PRECOMPACT_BASE)?;
        fg.add_log_files_from_names(PRECOMPACT_LOGS.iter().copied())?;
        Ok(fg
            .get_file_slice_as_of(crate::file_group::reader_v2::MAX_INSTANT_TIME)
            .expect("the file group has a slice")
            .clone())
    }

    use crate::metadata::table::test_support::comparable;

    /// A slice of just the base file, so the base side's cost can be read apart
    /// from the log side's.
    fn base_only_slice() -> crate::Result<FileSlice> {
        let mut fg = FileGroup::new(
            FILE_GROUP.to_string(),
            FilesPartitionRecord::PARTITION_NAME.to_string(),
        );
        fg.add_base_file_from_name(PRECOMPACT_BASE)?;
        Ok(fg
            .get_file_slice_as_of(crate::file_group::reader_v2::MAX_INSTANT_TIME)
            .expect("the file group has a slice")
            .clone())
    }

    /// What each reader costs on the same metadata slice, which decides whether the
    /// seam can move.
    ///
    /// Parity is not only values: a replacement that agrees on every record while
    /// doing more work is a regression every equality test above passes. So this
    /// asserts agreement first, and only then times, which also means a fast wrong
    /// answer fails the run instead of printing a good number.
    ///
    /// `#[ignore]`d because a wall time is a measurement, not an assertion: it moves
    /// with the machine and would make CI flaky. Run it deliberately, in release,
    /// since the two readers' work is different in kind (Avro decode against Arrow
    /// kernels) and a debug build penalises them differently:
    ///
    /// ```text
    /// cargo test --release -p hudi-core --lib reader_cost_on_a_metadata_slice \
    ///     -- --ignored --nocapture
    /// ```
    #[tokio::test]
    #[ignore]
    async fn reader_cost_on_a_metadata_slice() -> crate::Result<()> {
        use std::time::Instant;

        let configs = metadata_configs();
        let storage = Storage::new(Arc::new(HashMap::new()), configs.clone())?;
        let v1 = MetadataTableFileGroupReader::new(configs.clone(), storage.clone());
        let v2 = MetadataTableV2Reader::new(configs.clone(), storage.clone());
        let full = precompact_slice()?;
        let base_only = base_only_slice()?;

        const WARM: usize = 20;
        const ITERS: usize = 60;
        println!("{:28} {:>10} {:>10} {:>8}", "slice", "v1", "v2", "ratio");
        for (label, slice, keys) in [
            ("base + 8 logs, all keys", &full, &[][..]),
            ("base + 8 logs, one key", &full, &["city=chennai"][..]),
            ("base only, all keys", &base_only, &[][..]),
        ] {
            let a = v1.read_files_partition(slice, keys).await?;
            let b = v2.read_files_partition(slice, keys).await?;
            assert_eq!(
                comparable(&a),
                comparable(&b),
                "{label}: the readers must agree before a timing means anything"
            );

            for _ in 0..WARM {
                v1.read_files_partition(slice, keys).await?;
                v2.read_files_partition(slice, keys).await?;
            }
            let t = Instant::now();
            for _ in 0..ITERS {
                v1.read_files_partition(slice, keys).await?;
            }
            let d1 = t.elapsed() / ITERS as u32;
            let t = Instant::now();
            for _ in 0..ITERS {
                v2.read_files_partition(slice, keys).await?;
            }
            let d2 = t.elapsed() / ITERS as u32;
            println!(
                "{label:28} {:>8}us {:>8}us {:>7.2}x",
                d1.as_micros(),
                d2.as_micros(),
                d2.as_secs_f64() / d1.as_secs_f64()
            );
        }
        Ok(())
    }

    /// The v2-backed reader returns what the existing reader returns, record for
    /// record and value for value.
    ///
    /// This is the deliverable rather than a check on it: the task is parity, so an
    /// equality test that passes *is* the result. Values and not just keys, because
    /// a key-set comparison passes while the merge is wrong — which is how the fold
    /// bug in ENG-47455 stayed hidden behind ENG-47456's key-set assertion.
    #[tokio::test]
    async fn it_matches_the_existing_reader_value_for_value() -> crate::Result<()> {
        let configs = metadata_configs();
        let storage = Storage::new(Arc::new(HashMap::new()), configs.clone())?;
        let slice = precompact_slice()?;

        let existing = MetadataTableFileGroupReader::new(configs.clone(), storage.clone())
            .read_files_partition(&slice, &[])
            .await?;
        assert!(
            comparable(&existing)
                .iter()
                .any(|(_, _, files)| files.len() > 1),
            "the oracle must fold several file entries into at least one record, or \
             the comparison says nothing about the merge"
        );

        let through_v2 = MetadataTableV2Reader::new(configs, storage)
            .read_files_partition(&slice, &[])
            .await?;

        assert_eq!(comparable(&through_v2), comparable(&existing));
        Ok(())
    }

    /// A named-keys read returns the same records the existing reader returns for
    /// those keys, and nothing else.
    #[tokio::test]
    async fn a_named_keys_read_matches_the_existing_reader() -> crate::Result<()> {
        let configs = metadata_configs();
        let storage = Storage::new(Arc::new(HashMap::new()), configs.clone())?;
        let slice = precompact_slice()?;

        let all = MetadataTableFileGroupReader::new(configs.clone(), storage.clone())
            .read_files_partition(&slice, &[])
            .await?;
        let mut names: Vec<&str> = all.keys().map(String::as_str).collect();
        names.sort();
        assert!(names.len() > 1, "the fixture must hold several keys");
        let wanted = vec![names[names.len() - 1]];

        let existing = MetadataTableFileGroupReader::new(configs.clone(), storage.clone())
            .read_files_partition(&slice, &wanted)
            .await?;
        let through_v2 = MetadataTableV2Reader::new(configs, storage)
            .read_files_partition(&slice, &wanted)
            .await?;

        assert_eq!(
            existing.len(),
            1,
            "the oracle must return just the named key"
        );
        assert_eq!(comparable(&through_v2), comparable(&existing));
        Ok(())
    }

    /// A one-row metadata record, built rather than read.
    ///
    /// Three of this module's rules have no fixture behind them: the fixture's
    /// metadata table is partitioned, so no `.` key exists, and it holds no
    /// tombstones. `v9_mor_nonpart_3commits` has no metadata table at all, so a
    /// non-partitioned `.` key cannot be read from anywhere in this repo. These
    /// build the input instead, and say so rather than leaving the rules untested.
    fn one_record(key: &str, record_type: i32, entries: &[(&str, i64, bool)]) -> RecordBatch {
        use arrow_array::builder::{
            BooleanBuilder, Int64Builder, MapBuilder, StringBuilder, StructBuilder,
        };
        use arrow_schema::{DataType, Field, Fields, Schema};

        let value_fields = Fields::from(vec![
            Field::new(FILE_SIZE_FIELD, DataType::Int64, false),
            Field::new(FILE_IS_DELETED_FIELD, DataType::Boolean, false),
        ]);
        let mut builder = MapBuilder::new(
            None,
            StringBuilder::new(),
            StructBuilder::new(
                value_fields,
                vec![
                    Box::new(Int64Builder::new()),
                    Box::new(BooleanBuilder::new()),
                ],
            ),
        );
        for (name, size, is_deleted) in entries {
            builder.keys().append_value(name);
            let values = builder.values();
            values
                .field_builder::<Int64Builder>(0)
                .unwrap()
                .append_value(*size);
            values
                .field_builder::<BooleanBuilder>(1)
                .unwrap()
                .append_value(*is_deleted);
            values.append(true);
        }
        builder.append(true).unwrap();
        let map = builder.finish();

        let schema = Arc::new(Schema::new(vec![
            Field::new(KEY_COLUMN, DataType::Utf8, false),
            Field::new(TYPE_COLUMN, DataType::Int32, false),
            Field::new(FILESYSTEM_METADATA_COLUMN, map.data_type().clone(), false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow_array::StringArray::from(vec![key])),
                Arc::new(arrow_array::Int32Array::from(vec![record_type])),
                Arc::new(map),
            ],
        )
        .unwrap()
    }

    /// A non-partitioned table's partition is `.` on disk and `""` to the caller.
    ///
    /// `fetch_files_partition_records` asks for `.` and every consumer above it
    /// works in empty strings, so getting this wrong makes a non-partitioned table
    /// list no files at all while a partitioned one is unaffected.
    #[test]
    fn a_non_partitioned_key_normalises_to_the_empty_string() {
        let batch = one_record(
            ".",
            MetadataRecordType::Files as i32,
            &[("a.parquet", 10, false)],
        );
        let records = MetadataTableV2Reader::records_from_batch(&batch, &[]).unwrap();
        assert_eq!(records.keys().collect::<Vec<_>>(), vec![""]);
        assert_eq!(records[""].key, "");
    }

    /// The same `.` inside an all-partitions record's file map, where it names a
    /// partition rather than a file, including its `name` field.
    #[test]
    fn a_non_partitioned_entry_inside_all_partitions_normalises_too() {
        let batch = one_record(
            "__all_partitions__",
            MetadataRecordType::AllPartitions as i32,
            &[(".", 0, false)],
        );
        let records = MetadataTableV2Reader::records_from_batch(&batch, &[]).unwrap();
        let record = &records["__all_partitions__"];
        assert_eq!(record.files.keys().collect::<Vec<_>>(), vec![""]);
        assert_eq!(
            record.files[""].name, "",
            "the entry's own name field has to be cleared as well as its map key"
        );
    }

    /// A null map value is omitted, not defaulted.
    ///
    /// The reader this mirrors omits it (`extract_file_info` returns `None` for a
    /// non-record value). Defaulting would invent a zero-sized, undeleted file and
    /// send a read at a path that does not exist, so this is the one divergence in
    /// the conversion that would corrupt rather than error. The fixture's schema
    /// makes map values non-nullable, so nothing can produce this today and only a
    /// built input can pin it.
    #[test]
    fn a_null_map_value_is_omitted_rather_than_defaulted() {
        use arrow_array::builder::{
            BooleanBuilder, Int64Builder, MapBuilder, StringBuilder, StructBuilder,
        };
        use arrow_schema::{DataType, Field, Fields, Schema};

        let value_fields = Fields::from(vec![
            Field::new(FILE_SIZE_FIELD, DataType::Int64, true),
            Field::new(FILE_IS_DELETED_FIELD, DataType::Boolean, true),
        ]);
        let mut builder = MapBuilder::new(
            None,
            StringBuilder::new(),
            StructBuilder::new(
                value_fields,
                vec![
                    Box::new(Int64Builder::new()),
                    Box::new(BooleanBuilder::new()),
                ],
            ),
        );
        // A real entry, then one whose value struct is null.
        for (name, present) in [("live.parquet", true), ("null-valued.parquet", false)] {
            builder.keys().append_value(name);
            let values = builder.values();
            let size = values.field_builder::<Int64Builder>(0).unwrap();
            if present {
                size.append_value(10);
            } else {
                size.append_null();
            }
            let deleted = values.field_builder::<BooleanBuilder>(1).unwrap();
            if present {
                deleted.append_value(false);
            } else {
                deleted.append_null();
            }
            values.append(present);
        }
        builder.append(true).unwrap();
        let map = builder.finish();

        let schema = Arc::new(Schema::new(vec![
            Field::new(KEY_COLUMN, DataType::Utf8, false),
            Field::new(TYPE_COLUMN, DataType::Int32, false),
            Field::new(FILESYSTEM_METADATA_COLUMN, map.data_type().clone(), false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow_array::StringArray::from(vec!["city=x"])),
                Arc::new(arrow_array::Int32Array::from(vec![
                    MetadataRecordType::Files as i32,
                ])),
                Arc::new(map),
            ],
        )
        .unwrap();

        let records = MetadataTableV2Reader::records_from_batch(&batch, &[]).unwrap();
        let files = &records["city=x"].files;
        assert_eq!(
            files.keys().collect::<Vec<_>>(),
            vec!["live.parquet"],
            "a null-valued entry must be dropped, not turned into a phantom active file"
        );
    }

    /// A tombstone is reported as deleted.
    ///
    /// No fixture here has one — the metadata table's `files` records are all live —
    /// so without this the flag could be hardcoded false and every test would pass.
    #[test]
    fn a_tombstone_is_reported_as_deleted() {
        let batch = one_record(
            "city=x",
            MetadataRecordType::Files as i32,
            &[("live.parquet", 10, false), ("gone.parquet", 0, true)],
        );
        let records = MetadataTableV2Reader::records_from_batch(&batch, &[]).unwrap();
        let files = &records["city=x"].files;
        assert!(!files["live.parquet"].is_deleted);
        assert!(files["gone.parquet"].is_deleted);
        assert_eq!(files["live.parquet"].size, 10);
    }

    /// An unrecognised partition type decodes to `Unknown`, matching the reader
    /// this replaces rather than refusing.
    ///
    /// Written as a test because it is a deliberate choice and not an oversight:
    /// `get_record_type` in the existing reader falls back to `Unknown`, so
    /// refusing here would make the two readers disagree on a malformed record and
    /// break the parity this task exists to establish. Whether an unknown type
    /// *should* fail a read is a separate question with a separate owner.
    #[test]
    fn an_unknown_record_type_decodes_to_unknown_as_the_existing_reader_does() {
        let batch = one_record("k", 99, &[]);
        let records = MetadataTableV2Reader::records_from_batch(&batch, &[]).unwrap();
        assert_eq!(records["k"].record_type, MetadataRecordType::Unknown);
    }

    /// How many log records reached the merge, for a read of `slice` with `keys`.
    ///
    /// `merge_map_peak_entries` is the instrument because it counts exactly the work
    /// the pushdown avoids: every log record that reaches the merge has already been
    /// Avro-decoded and buffered. Bytes cannot show it — the log file's blocks are
    /// fetched whole before decode either way.
    async fn merge_peak(
        configs: Arc<HudiConfigs>,
        storage: Arc<Storage>,
        slice: &FileSlice,
        keys: &[&str],
    ) -> crate::Result<u64> {
        let base_file_path = slice.base_file_relative_path()?;
        let mut context =
            resolve_reader_context(&configs, slice.has_log_file(), base_file_path.as_deref())?;
        context.rebuild_record_context(FilesPartitionRecord::PARTITION_NAME.to_string());
        context.key_predicate = (!keys.is_empty())
            .then(|| KeyPredicate::Keys(keys.iter().map(|k| (*k).to_string()).collect()));
        let logs = slice
            .log_files
            .iter()
            .map(|f| slice.log_file_relative_path(f))
            .collect::<crate::Result<Vec<String>>>()?;
        let mut reader = HoodieFileGroupReader::new(
            Arc::new(context),
            storage,
            InputSplit::new(
                slice.base_file_relative_path()?,
                slice.base_file.as_ref().map(|b| b.commit_timestamp.clone()),
                logs,
                FilesPartitionRecord::PARTITION_NAME.to_string(),
            ),
            ReaderParameters::default(),
            None,
            None,
        )?;
        reader.read().await?;
        Ok(reader.read_stats().merge_map_peak_entries as u64)
    }

    /// A named-keys read does less work than a full read, not just less output.
    ///
    /// Parity is performance as well as values: a reader that returns the right
    /// record while decoding and merging every other one is a regression that every
    /// equality test passes. The predicate reaches the HFile log block read, so the
    /// records for keys nobody asked for are never decoded and never enter the merge.
    #[tokio::test]
    async fn a_named_keys_read_merges_less_than_a_full_read() -> crate::Result<()> {
        let configs = metadata_configs();
        let storage = Storage::new(Arc::new(HashMap::new()), configs.clone())?;
        let slice = precompact_slice()?;

        let full = merge_peak(configs.clone(), storage.clone(), &slice, &[]).await?;
        assert!(
            full > 1,
            "the slice must put several records through the merge, or there is no work \
             to avoid; peaked at {full}"
        );

        let all = MetadataTableFileGroupReader::new(configs.clone(), storage.clone())
            .read_files_partition(&slice, &[])
            .await?;
        let mut names: Vec<&str> = all.keys().map(String::as_str).collect();
        names.sort();
        let one = merge_peak(configs, storage, &slice, &[names[names.len() - 1]]).await?;

        assert!(
            one < full,
            "a one-key read must merge fewer records than a full read; both peaked at \
             {one} against {full}, so the predicate is not reaching the log read"
        );
        Ok(())
    }
}
