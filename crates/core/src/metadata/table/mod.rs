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

//! Metadata table APIs for the Hudi Table.
//!
//! This module provides methods for interacting with Hudi's metadata table,
//! which stores file listings and other metadata for efficient table operations.

pub mod records;

// The reader the metadata read used before `v2_reader` replaced it. Kept, and kept
// out of the production build, because it is the oracle every parity test compares
// against: an Avro-decoding implementation of the same semantics, written
// independently, is worth more as a check on the new reader than as one fewer file.
// Deleting it is a separate step, once the parity tests have something else to
// compare against or no longer need to.
#[cfg(test)]
pub(crate) mod reader;
pub(crate) mod routing;

pub(crate) mod v2_reader;

#[cfg(test)]
pub(crate) mod test_support;

use crate::config::HudiConfigs;
use crate::storage::Storage;
use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;

use crate::Result;
use crate::config::table::HudiTableConfig::{
    MetadataTableEnabled, MetadataTablePartitions, PartitionFields, TableVersion,
};
use crate::error::CoreError;
use crate::expr::filter::from_str_tuples;
use crate::metadata::METADATA_TABLE_PARTITION_FIELD;
use crate::storage::util::join_url_segments;
use crate::table::ReadOptions;
use crate::table::Table;
use crate::table::file_pruner::FilePruner;
use crate::table::partition::PartitionPruner;

use records::FilesPartitionRecord;

impl Table {
    /// Check if this table is a metadata table.
    ///
    /// Detection is based on the base path ending with `.hoodie/metadata`.
    pub fn is_metadata_table(&self) -> bool {
        let base_path: String = self
            .hudi_configs
            .get_or_default(crate::config::table::HudiTableConfig::BasePath)
            .into();
        crate::util::path::is_metadata_table_path(&base_path)
    }

    /// Get the list of available metadata table partitions for this table.
    ///
    /// Returns the partitions configured in [`MetadataTablePartitions`].
    pub fn get_metadata_table_partitions(&self) -> Vec<String> {
        self.hudi_configs
            .get_or_default(MetadataTablePartitions)
            .into()
    }

    /// Check if the metadata table is enabled.
    ///
    /// Returns `true` if:
    /// 1. Table version is >= 8 (metadata table support is only for v8+ tables), AND
    /// 2. Table is not a metadata table itself, AND
    /// 3. Either:
    ///    - `hoodie.metadata.enable` is explicitly true, OR
    ///    - `files` is in the configured [`MetadataTablePartitions`]
    ///
    /// # Note
    /// The metadata table is considered active when partitions are configured,
    /// even without explicit `hoodie.metadata.enable=true`. When metadata table
    /// is enabled, it must have at least the `files` partition enabled.
    pub fn is_metadata_table_enabled(&self) -> bool {
        // TODO: drop v6 support then no need to check table version here
        let table_version: isize = self
            .hudi_configs
            .get(TableVersion)
            .map(|v| v.into())
            .unwrap_or(0);

        if table_version < 8 {
            return false;
        }

        if self.is_metadata_table() {
            return false;
        }

        // Check if "files" partition is configured
        let has_files_partition = self
            .get_metadata_table_partitions()
            .contains(&FilesPartitionRecord::PARTITION_NAME.to_string());

        // Explicit check for hoodie.metadata.enable
        let metadata_explicitly_enabled: bool = self
            .hudi_configs
            .get_or_default(MetadataTableEnabled)
            .into();

        metadata_explicitly_enabled || has_files_partition
    }

    /// Create a metadata table instance for this data table.
    ///
    /// TODO: support more partitions. Only "files" is used currently.
    ///
    /// # Errors
    ///
    /// Returns an error if the metadata table cannot be created or if there are
    /// no metadata table partitions configured.
    ///
    /// # Note
    /// Must be called on a DATA table, not a METADATA table.
    pub async fn new_metadata_table(&self) -> Result<Table> {
        if self.is_metadata_table() {
            return Err(CoreError::MetadataTable(
                "Cannot create metadata table from another metadata table".to_string(),
            ));
        }

        let mdt_partitions = self.get_metadata_table_partitions();
        if mdt_partitions.is_empty() {
            return Err(CoreError::MetadataTable(
                "No metadata table partitions configured".to_string(),
            ));
        }

        let mdt_url = join_url_segments(&self.base_url(), &[".hoodie", "metadata"])?;
        Table::new_with_options(
            mdt_url.as_str(),
            [(PartitionFields.as_ref(), METADATA_TABLE_PARTITION_FIELD)],
        )
        .await
    }

    /// Fetch records from the `files` partition of metadata table
    /// with optional data table partition pruning.
    ///
    /// Records are returned with normalized partition keys. For non-partitioned tables,
    /// the key is "" (empty string) instead of the internal "." representation.
    /// Normalization happens at decode time in [`decode_files_partition_record_with_schema`].
    ///
    /// # Note
    /// Must be called on a DATA table, not a METADATA table.
    pub async fn read_metadata_table_files_partition(
        &self,
        partition_pruner: &PartitionPruner,
    ) -> Result<HashMap<String, FilesPartitionRecord>> {
        let metadata_table = self.get_or_init_metadata_table().await?;
        metadata_table
            .fetch_files_partition_records(partition_pruner)
            .await
    }

    /// The `files` partition as an Arrow batch, undecoded.
    ///
    /// Same read as [`Self::read_metadata_table_files_partition`], stopping one
    /// step earlier. A consumer that wants Arrow — a JVM caller importing through
    /// the C Data Interface, or an engine that merges this with other batches —
    /// would otherwise take decoded structs and re-encode them, paying for a
    /// decode it did not want and an encode that loses nothing but time.
    ///
    /// `keys` empty reads every record; otherwise only those keys. Unlike the
    /// decoded form, keys are matched **as stored**, so a non-partitioned table's
    /// record is asked for as `"."` and arrives with that key rather than `""`:
    /// normalisation happens during decode, which this skips.
    ///
    /// # Note
    /// Must be called on a DATA table, not a METADATA table.
    pub async fn read_metadata_table_files_partition_arrow(
        &self,
        keys: &[&str],
    ) -> Result<arrow_array::RecordBatch> {
        let metadata_table = self.get_or_init_metadata_table().await?;
        metadata_table.read_files_partition_batch(keys).await
    }

    /// Fetch records from the `files` partition with optional partition pruning.
    ///
    /// For non-partitioned tables, directly fetches the "." record.
    /// For partitioned tables with filters, performs partition pruning via `__all_partitions__`.
    ///
    /// # Arguments
    /// * `partition_pruner` - Data table's partition pruner to filter partitions.
    ///
    /// # Note
    /// Must be called on a METADATA table instance.
    pub async fn fetch_files_partition_records(
        &self,
        partition_pruner: &PartitionPruner,
    ) -> Result<HashMap<String, FilesPartitionRecord>> {
        // Non-partitioned table: directly fetch "." record
        if !partition_pruner.is_table_partitioned() {
            return self
                .read_files_partition(&[FilesPartitionRecord::NON_PARTITIONED_NAME])
                .await;
        }

        // Partitioned table without filters: read all records
        if partition_pruner.is_empty() {
            return self.read_files_partition(&[]).await;
        }

        // Partitioned table with filters: prune partitions first, then read only matching records
        let all_partitions_records = self
            .read_files_partition(&[FilesPartitionRecord::ALL_PARTITIONS_KEY])
            .await?;

        let partition_names: Vec<&str> = all_partitions_records
            .get(FilesPartitionRecord::ALL_PARTITIONS_KEY)
            .map(|r| r.partition_names())
            .unwrap_or_default();

        let pruned: Vec<&str> = partition_names
            .into_iter()
            .filter(|p| partition_pruner.should_include(p))
            .collect();

        if pruned.is_empty() {
            return Ok(HashMap::new());
        }

        self.read_files_partition(&pruned).await
    }

    /// Read records from the `files` partition.
    ///
    /// If keys is empty, reads all records. Otherwise, reads only the specified keys.
    ///
    /// # Note
    /// Must be called on a METADATA table instance.
    async fn read_files_partition(
        &self,
        keys: &[&str],
    ) -> Result<HashMap<String, FilesPartitionRecord>> {
        let Some((reader, file_slices)) = self.files_partition_reader().await? else {
            return Ok(HashMap::new());
        };
        // Each slice holds a disjoint set of keys -- a metadata partition shards
        // by hashing the record key -- so the per-slice maps combine by insertion
        // with no merge rule needed. Bounded fan-out, the same ceiling the data
        // read uses, so a sharded partition cannot open more readers at once than
        // a table scan would.
        // Only the shards the keys route to. On a partition with ten file
        // groups a single-key lookup opens one slice, not ten.
        let targets = routing::slices_for_keys(&file_slices, keys);
        let per_slice = crate::util::concurrency::bounded_in_order(
            &targets,
            self.file_slice_read_concurrency(),
            |file_slice| reader.read_files_partition(file_slice, keys),
        )
        .await?;
        Ok(per_slice.into_iter().flatten().collect())
    }

    /// The `files` partition's merged batch, for a caller that wants Arrow.
    ///
    /// An empty batch when the metadata table has no commits, matching the empty
    /// map the decoded form returns in that case.
    ///
    /// # Note
    /// Must be called on a METADATA table instance.
    pub(crate) async fn read_files_partition_batch(
        &self,
        keys: &[&str],
    ) -> Result<arrow_array::RecordBatch> {
        let Some((reader, file_slices)) = self.files_partition_reader().await? else {
            return Ok(arrow_array::RecordBatch::new_empty(std::sync::Arc::new(
                Schema::empty(),
            )));
        };
        let targets = routing::slices_for_keys(&file_slices, keys);
        let batches = crate::util::concurrency::bounded_in_order(
            &targets,
            self.file_slice_read_concurrency(),
            |file_slice| reader.read_files_partition_batch(file_slice, keys),
        )
        .await?;
        concat_metadata_batches(batches)
    }

    /// Resolve the `files` partition's single file slice and build a reader for it.
    ///
    /// `None` when the metadata table has no commits, which both callers treat as
    /// an empty result rather than an error.
    ///
    /// # Note
    /// Must be called on a METADATA table instance.
    /// The reader and slices for the `files` partition.
    ///
    /// A thin wrapper over [`Self::partition_reader`]: `files` is the only
    /// partition with a decoded record type and a caller today. The others are
    /// reachable through `partition_reader` for tests, which is what lets shard
    /// routing be checked against real file ids rather than synthetic ones.
    async fn files_partition_reader(
        &self,
    ) -> Result<
        Option<(
            v2_reader::MetadataTableV2Reader,
            Vec<crate::file_group::file_slice::FileSlice>,
        )>,
    > {
        self.partition_reader(FilesPartitionRecord::PARTITION_NAME)
            .await
    }

    /// The reader and every file slice of one metadata partition.
    ///
    /// Parameterised rather than pinned to `files` because the partitions that
    /// shard -- record index, secondary index -- differ from it only in how many
    /// slices come back and how their records decode. Slice discovery does not
    /// care which partition it is listing.
    async fn partition_reader(
        &self,
        partition: &str,
    ) -> Result<
        Option<(
            v2_reader::MetadataTableV2Reader,
            Vec<crate::file_group::file_slice::FileSlice>,
        )>,
    > {
        let Some(timestamp) = self.timeline.get_latest_commit_timestamp_as_option() else {
            return Ok(None);
        };

        let timeline_view = self.timeline.create_view_as_of(timestamp).await?;

        let filters = from_str_tuples([(METADATA_TABLE_PARTITION_FIELD, "=", partition)])?;
        let partition_schema = self.get_partition_schema().await?;
        let partition_pruner =
            PartitionPruner::new(&filters, &partition_schema, self.hudi_configs.as_ref())?;

        // Use empty file pruner for metadata table - no column stats pruning needed
        // Use empty schema since the pruner is empty and won't use the schema
        let file_pruner = FilePruner::empty();
        let table_schema = Schema::empty();

        // MDT itself uses HFile base files; no estimator applies here.
        let file_slices = self
            .file_system_view
            .get_file_slices_by_storage_listing(
                &partition_pruner,
                &file_pruner,
                &table_schema,
                &timeline_view,
                None,
            )
            .await?;

        // No refusal on the slice count. `files` is one file group in practice,
        // but the partitions that shard -- record index above all -- are many by
        // design, hashing keys across file groups. Refusing anything but one made
        // them unreadable; the reader itself never cared how many there were.
        if file_slices.is_empty() {
            return Ok(None);
        }
        let opts = ReadOptions::new().with_end_timestamp(timestamp);
        let configs = Arc::new(HudiConfigs::new(
            self.hudi_configs
                .as_options()
                .into_iter()
                .chain(opts.hudi_options.clone()),
        ));
        let storage = Storage::new(Arc::new(self.storage_options()), configs.clone())?;

        // About 2.1x the cost of the reader it replaces on this table's own `files`
        // partition, measured by `v2_reader::tests::reader_cost_on_a_metadata_slice`,
        // and accepted: roughly a quarter of that is `arrow_avro` re-parsing the
        // writer schema's JSON on every decoder construction, which no change here
        // can remove while a flushed decoder cannot be reused (arrow-rs#10876).
        //
        // The fixture is also this reader's worst case, holding 2.6 records per block,
        // where per-block fixed cost cannot amortise; on larger blocks it overtakes
        // the reader it replaces. So the ratio above is not the production ratio, and
        // nothing measured here establishes what that is.
        Ok(Some((
            v2_reader::MetadataTableV2Reader::new(configs, storage),
            file_slices,
        )))
    }
}

/// Concatenate the per-slice batches of one metadata partition.
///
/// A sharded partition is read slice by slice, and a caller wants one batch. The
/// slices share a schema -- they are the same partition -- so this is a
/// concatenation, not a union: a schema mismatch here means the read assembled
/// slices from different partitions and should fail rather than coerce.
fn concat_metadata_batches(batches: Vec<RecordBatch>) -> Result<RecordBatch> {
    match batches.len() {
        // The single-slice case, which is every `files` partition in practice,
        // returns its batch untouched: no concat, no copy, nothing added to the
        // path that existed before sharding was supported.
        1 => Ok(batches.into_iter().next().expect("length checked")),
        0 => Ok(RecordBatch::new_empty(Arc::new(Schema::empty()))),
        _ => {
            let schema = batches[0].schema();
            arrow::compute::concat_batches(&schema, &batches).map_err(CoreError::ArrowError)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::table::HudiTableConfig::TableVersion;
    use crate::table::partition::PartitionPruner;
    use hudi_test::{QuickstartTripsTable, SampleTable};
    use records::{FilesPartitionRecord, MetadataRecordType};
    use std::collections::HashSet;

    async fn get_data_table() -> Table {
        let table_path = QuickstartTripsTable::V8Trips8I3U1D.path_to_mor_avro();
        Table::new(&table_path).await.unwrap()
    }

    #[tokio::test]
    async fn hudi_table_read_metadata_table_files_partition() {
        let data_table = get_data_table().await;
        let partition_schema = data_table.get_partition_schema().await.unwrap();
        let partition_pruner =
            PartitionPruner::new(&[], &partition_schema, data_table.hudi_configs.as_ref()).unwrap();

        let records = data_table
            .read_metadata_table_files_partition(&partition_pruner)
            .await
            .unwrap();

        // Should have 4 records: __all_partitions__ + 3 city partitions
        assert_eq!(records.len(), 4);

        // Validate __all_partitions__ record
        let all_partitions = records
            .get(FilesPartitionRecord::ALL_PARTITIONS_KEY)
            .unwrap();
        assert_eq!(
            all_partitions.record_type,
            MetadataRecordType::AllPartitions
        );
        let partition_names: HashSet<&str> = all_partitions.partition_names().into_iter().collect();
        assert_eq!(
            partition_names,
            HashSet::from(["city=chennai", "city=san_francisco", "city=sao_paulo"])
        );

        // Validate city=chennai record with actual file names
        let chennai = records.get("city=chennai").unwrap();
        assert_eq!(chennai.record_type, MetadataRecordType::Files);
        let chennai_files: HashSet<_> = chennai.active_file_names().into_iter().collect();
        assert_eq!(
            chennai_files,
            HashSet::from([
                "6e1d5cc4-c487-487d-abbe-fe9b30b1c0cc-0_2-986-2794_20251220210108078.parquet",
                "6e1d5cc4-c487-487d-abbe-fe9b30b1c0cc-0_0-1112-3190_20251220210129235.parquet",
                ".6e1d5cc4-c487-487d-abbe-fe9b30b1c0cc-0_20251220210127080.log.1_0-1072-3078",
                ".6e1d5cc4-c487-487d-abbe-fe9b30b1c0cc-0_20251220210128625.log.1_0-1097-3150",
            ])
        );
        assert!(chennai.total_size() > 0);
    }

    /// An HFile block inside a Hudi log file. Framing mirrors `memory_bench`'s
    /// `build_block`, with the block type set to HFile and the content being the
    /// HFile's own bytes rather than length-prefixed Avro data. Built here rather
    /// than committed, so the framing the tests below depend on stays readable.
    fn hfile_log_block(hfile: &[u8], schema: &str, instant: &str) -> Vec<u8> {
        const MAGIC: &[u8] = b"#HUDI#";
        let mut header = Vec::new();
        header.extend_from_slice(&2u32.to_be_bytes());
        for (key, value) in [(0u32, instant), (2u32, schema)] {
            header.extend_from_slice(&key.to_be_bytes());
            header.extend_from_slice(&(value.len() as u32).to_be_bytes());
            header.extend_from_slice(value.as_bytes());
        }
        let mut body = Vec::new();
        body.extend_from_slice(&1u32.to_be_bytes()); // log format version
        body.extend_from_slice(&4u32.to_be_bytes()); // BlockType::HfileData
        body.extend_from_slice(&header);
        body.extend_from_slice(&(hfile.len() as u64).to_be_bytes());
        body.extend_from_slice(hfile);
        body.extend_from_slice(&0u32.to_be_bytes()); // empty footer
        let block_length = (body.len() + 8) as u64;
        let mut out = Vec::new();
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&block_length.to_be_bytes());
        out.extend_from_slice(&body);
        out.extend_from_slice(&(block_length + MAGIC.len() as u64).to_be_bytes());
        out
    }

    /// The committed HFiles under `tests/data/metadata_slices`, whose contents and
    /// provenance that directory's `README.md` states.
    fn slice_fixture(name: &str) -> Vec<u8> {
        std::fs::read(std::path::Path::new("tests/data/metadata_slices").join(name)).unwrap()
    }

    /// A one-file-group `files` partition on disk: the base HFile, and when one is
    /// given, a log file whose single block holds the other HFile.
    ///
    /// Returns the temp dir so the caller keeps it alive for the length of the read.
    fn metadata_slice_on_disk(
        base_hfile: &[u8],
        log_hfile: Option<&[u8]>,
    ) -> (
        tempfile::TempDir,
        Arc<HudiConfigs>,
        Arc<Storage>,
        crate::file_group::file_slice::FileSlice,
    ) {
        use crate::file_group::FileGroup;

        let schema_json = String::from_utf8(slice_fixture("metadata-record.avsc")).unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let part = tmp.path().join("files");
        std::fs::create_dir_all(&part).unwrap();
        let base_name = "files-0000-0_0-1-1_00000000000000000.hfile";
        let log_name = ".files-0000-0_00000000000000000.log.1_0-2-2";
        std::fs::write(part.join(base_name), base_hfile).unwrap();
        if let Some(log_hfile) = log_hfile {
            std::fs::write(
                part.join(log_name),
                hfile_log_block(log_hfile, &schema_json, "20250101000000000"),
            )
            .unwrap();
        }

        let base_url =
            url::Url::from_directory_path(std::fs::canonicalize(tmp.path()).unwrap()).unwrap();
        let configs = test_support::metadata_table_configs(base_url.as_str());
        let storage = Storage::new(Arc::new(HashMap::new()), configs.clone()).unwrap();

        let mut fg = FileGroup::new("files-0000-0".to_string(), "files".to_string());
        fg.add_base_file_from_name(base_name).unwrap();
        if log_hfile.is_some() {
            fg.add_log_files_from_names(vec![log_name]).unwrap();
        }
        let slice = fg
            .get_file_slice_as_of(crate::file_group::reader_v2::MAX_INSTANT_TIME)
            .expect("a slice")
            .clone();

        (tmp, configs, storage, slice)
    }

    /// Read one metadata slice through both readers, requiring they agree.
    ///
    /// Agreement is half the point: v2 is intended to replace v1 at the seam, so any
    /// value the two disagree on is a regression regardless of which one is right.
    /// The other half is the caller's, which asserts the absolute values, since two
    /// readers can agree and both be wrong.
    async fn read_both(
        configs: Arc<HudiConfigs>,
        storage: Arc<Storage>,
        slice: &crate::file_group::file_slice::FileSlice,
        keys: &[&str],
    ) -> Vec<test_support::ComparableRecord> {
        let v1 = reader::MetadataTableFileGroupReader::new(configs.clone(), storage.clone())
            .read_files_partition(slice, keys)
            .await;
        let v2 = v2_reader::MetadataTableV2Reader::new(configs.clone(), storage.clone())
            .read_files_partition(slice, keys)
            .await;

        match (&v1, &v2) {
            (Ok(a), Ok(b)) => {
                let (a, b) = (test_support::comparable(a), test_support::comparable(b));
                assert_eq!(a, b, "the two readers must agree record for record");
                a
            }
            (a, b) => panic!("v1: {a:?}\nv2: {b:?}"),
        }
    }

    /// Does v2 fold an entry-level tombstone the way v1 does?
    ///
    /// A cleaned file arrives in the metadata table as a `filesystemMetadata` entry
    /// with `size = 0, isDeleted = true`, written by
    /// `HoodieMetadataPayload.DELETE_FILE_METADATA`. No fixture table in this repo
    /// carries one, because none of them was ever cleaned, so this builds the slice:
    /// a base file naming a file live and a log block naming the same file deleted.
    ///
    /// The key survives with an empty file map, so a tombstone cancels the entry
    /// rather than lingering as a deleted one.
    #[tokio::test]
    async fn an_entry_level_tombstone_folds_the_same_in_both_readers() {
        let (_tmp, configs, storage, slice) = metadata_slice_on_disk(
            &slice_fixture("files-live.hfile"),
            Some(&slice_fixture("files-tombstone.hfile")),
        );
        let agreed = read_both(configs, storage, &slice, &[]).await;

        assert_eq!(
            agreed,
            vec![(
                "city=p00000000".to_string(),
                MetadataRecordType::Files as i32,
                vec![]
            )],
            "a tombstone over the only live entry must cancel it, leaving the key \
             with no files"
        );
    }

    /// Does a non-partitioned table's `.` record read the same through both readers?
    ///
    /// The `files` partition of a non-partitioned table holds one record keyed `.`,
    /// which both readers must hand back as the empty string, since that is the form
    /// `fetch_files_partition_records` and every caller above it work in. No fixture
    /// in this repo is a non-partitioned table with a metadata table
    /// (`v9_mor_nonpart_3commits` has none), so this builds the slice from two
    /// `.`-keyed HFiles naming two different files.
    ///
    /// A base file plus a log block, rather than a base file alone, for two reasons.
    /// The union of the two file maps is what catches a merge that overwrites the
    /// record instead of folding its `filesystemMetadata`. And the read is a keyed
    /// one, where the predicate narrows only the base file and never the log
    /// records, so a predicate compared against the normalised key rather than the
    /// key as stored drops the base record while the log record survives, which a
    /// base-only slice would not show. Asking for `""` instead of `.` finds nothing
    /// in either reader.
    #[tokio::test]
    async fn a_non_partitioned_record_reads_as_an_empty_key_in_both_readers() {
        let (_tmp, configs, storage, slice) = metadata_slice_on_disk(
            &slice_fixture("nonpartitioned-base.hfile"),
            Some(&slice_fixture("nonpartitioned-log.hfile")),
        );
        let agreed = read_both(
            configs,
            storage,
            &slice,
            &[FilesPartitionRecord::NON_PARTITIONED_NAME],
        )
        .await;

        assert_eq!(
            agreed,
            vec![(
                String::new(),
                MetadataRecordType::Files as i32,
                vec![
                    (
                        "f00000000-0_0-1-1_20250101000000000.parquet".to_string(),
                        1024,
                        false
                    ),
                    (
                        "f00000001-0_0-1-1_20250101000000000.parquet".to_string(),
                        1024,
                        false
                    ),
                ]
            )],
            "the one `.` record must come back keyed by the empty string, with the \
             log block's file merged into the base file's"
        );
    }

    /// The other `.`: inside the all-partitions record's map, where it names a
    /// partition rather than a file.
    ///
    /// This one has a production consumer the record key does not, `partition_names`,
    /// which feeds the pruner. A `.` left unnormalised there makes the pruner compare
    /// the data table's empty partition path against `.` and prune the table's only
    /// partition away, so both the map key and the entry's own `name` field have to
    /// be cleared.
    #[tokio::test]
    async fn a_non_partitioned_all_partitions_entry_reads_as_empty_in_both_readers() {
        let (_tmp, configs, storage, slice) =
            metadata_slice_on_disk(&slice_fixture("allpartitions-dot.hfile"), None);
        let agreed = read_both(
            configs,
            storage,
            &slice,
            &[FilesPartitionRecord::ALL_PARTITIONS_KEY],
        )
        .await;

        assert_eq!(
            agreed,
            vec![(
                FilesPartitionRecord::ALL_PARTITIONS_KEY.to_string(),
                MetadataRecordType::AllPartitions as i32,
                vec![(String::new(), 0, false)]
            )],
            "the `.` partition entry must come back named by the empty string"
        );
    }

    /// The pruned-keys branch of `fetch_files_partition_records`: read
    /// `__all_partitions__` first, prune, then read only the surviving partitions.
    ///
    /// The three branches are the reason this matters. The unfiltered branch is
    /// covered by `hudi_table_read_metadata_table_files_partition` above; this
    /// covers the filtered one; and the keys the non-partitioned branch asks for are
    /// covered by `a_non_partitioned_record_reads_as_an_empty_key_in_both_readers`,
    /// which reads a `.` record through both readers.
    ///
    /// What none of the three exercises is the branch *selection* itself, since no
    /// fixture here is a non-partitioned table carrying a metadata table
    /// (`v9_mor_nonpart_3commits` has none). That selection sits above the reader
    /// seam and so is the same code whichever reader runs underneath, which is why
    /// covering the keys it passes was the part worth building a fixture for.
    #[tokio::test]
    async fn hudi_table_read_metadata_table_files_partition_with_pruning() {
        let data_table = get_data_table().await;
        let partition_schema = data_table.get_partition_schema().await.unwrap();
        let filters = crate::expr::filter::from_str_tuples([("city", "=", "chennai")]).unwrap();
        let partition_pruner = PartitionPruner::new(
            &filters,
            &partition_schema,
            data_table.hudi_configs.as_ref(),
        )
        .unwrap();

        let records = data_table
            .read_metadata_table_files_partition(&partition_pruner)
            .await
            .unwrap();

        // Only the surviving partition, and not the `__all_partitions__` record the
        // pruning read to find it: that read is an intermediate step, and returning
        // it would make a pruned listing include a record no caller asked for.
        assert_eq!(
            records.keys().collect::<Vec<_>>(),
            vec!["city=chennai"],
            "a pruned read must return the matching partition and nothing else"
        );

        // Same values as the unfiltered read gives for that partition, so pruning
        // changes which records come back and not what they say.
        let chennai = records.get("city=chennai").unwrap();
        assert_eq!(chennai.record_type, MetadataRecordType::Files);
        let names: HashSet<_> = chennai.active_file_names().into_iter().collect();
        assert_eq!(
            names,
            HashSet::from([
                "6e1d5cc4-c487-487d-abbe-fe9b30b1c0cc-0_2-986-2794_20251220210108078.parquet",
                "6e1d5cc4-c487-487d-abbe-fe9b30b1c0cc-0_0-1112-3190_20251220210129235.parquet",
                ".6e1d5cc4-c487-487d-abbe-fe9b30b1c0cc-0_20251220210127080.log.1_0-1072-3078",
                ".6e1d5cc4-c487-487d-abbe-fe9b30b1c0cc-0_20251220210128625.log.1_0-1097-3150",
            ])
        );
    }

    #[tokio::test]
    async fn hudi_table_get_metadata_table_partitions() {
        let data_table = get_data_table().await;

        // Verify we can get the metadata table partitions from the data table
        let partitions = data_table.get_metadata_table_partitions();

        // The test table has 5 metadata table partitions configured
        assert_eq!(
            partitions.len(),
            5,
            "Should have 5 metadata table partitions, got: {partitions:?}"
        );

        // Verify all expected partitions are present
        let expected = [
            "column_stats",
            "files",
            "partition_stats",
            "record_index",
            "secondary_index_rider_idx",
        ];
        for partition in &expected {
            assert!(
                partitions.contains(&partition.to_string()),
                "Should contain '{partition}' partition, got: {partitions:?}"
            );
        }
    }

    #[tokio::test]
    async fn hudi_table_is_metadata_table_enabled() {
        // V8 table with files partition configured should enable metadata table
        // even without explicit hoodie.metadata.enable=true
        let data_table = get_data_table().await;

        // Verify it's a v8 table
        let table_version: isize = data_table
            .hudi_configs
            .get(TableVersion)
            .map(|v| v.into())
            .unwrap_or(0);
        assert_eq!(table_version, 8, "Test table should be v8");

        // Verify files partition is configured
        let partitions = data_table.get_metadata_table_partitions();
        assert!(
            partitions.contains(&"files".to_string()),
            "Should have 'files' partition configured"
        );

        // Verify is_metadata_table_enabled returns true (implicit enablement)
        assert!(
            data_table.is_metadata_table_enabled(),
            "is_metadata_table_enabled should return true for v8 table with files partition"
        );
    }

    #[tokio::test]
    async fn hudi_table_v6_metadata_table_not_enabled() {
        // V6 tables should NOT have metadata table enabled, even with explicit setting
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await.unwrap();

        // Verify it's a v6 table
        let table_version: isize = hudi_table
            .hudi_configs
            .get(TableVersion)
            .map(|v| v.into())
            .unwrap_or(0);
        assert_eq!(table_version, 6, "Test table should be v6");

        // V6 tables should not have metadata table enabled
        assert!(
            !hudi_table.is_metadata_table_enabled(),
            "is_metadata_table_enabled should return false for v6 table"
        );
    }

    #[tokio::test]
    async fn hudi_table_is_not_metadata_table() {
        // A regular data table should not be a metadata table
        let base_url = SampleTable::V6Nonpartitioned.url_to_cow();
        let hudi_table = Table::new(base_url.path()).await.unwrap();
        assert!(
            !hudi_table.is_metadata_table(),
            "Regular data table should not be a metadata table"
        );
    }

    #[tokio::test]
    async fn hudi_table_metadata_table_is_metadata_table() {
        // Create a metadata table and verify it's recognized as such
        let data_table = get_data_table().await;
        let metadata_table = data_table.new_metadata_table().await.unwrap();
        assert!(
            metadata_table.is_metadata_table(),
            "Metadata table should be recognized as a metadata table"
        );
    }

    #[tokio::test]
    async fn hudi_table_new_metadata_table_from_metadata_table_errors() {
        // Trying to create a metadata table from a metadata table should fail
        let data_table = get_data_table().await;
        let metadata_table = data_table.new_metadata_table().await.unwrap();

        let result = metadata_table.new_metadata_table().await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string()
                .contains("Cannot create metadata table from another metadata table"),
            "Error message should indicate cannot create from metadata table"
        );
    }

    /// A `files` partition spread over more than one file slice reads every
    /// slice, rather than being refused for having more than one.
    ///
    /// The fixture's own `files` partition is a single file group, as it is in
    /// practice, so a second slice is made by copying the base file under a new
    /// file id. Slice discovery is a storage listing, so the copy is discovered
    /// without touching the timeline -- which on table version 8 is Avro-encoded
    /// and not something a test should have to write.
    ///
    /// The assertion is on **row count, doubled**. Three outcomes are then
    /// distinguishable: the old refusal errors, a one-slice read returns N, and a
    /// correct two-slice read returns 2N. Asserting merely that the read
    /// succeeded would pass on the middle case, which is the one worth catching.
    #[tokio::test]
    async fn a_files_partition_of_several_slices_reads_all_of_them() -> Result<()> {
        let src = QuickstartTripsTable::V8Trips8I3U1D.path_to_mor_avro();
        let src_root = std::path::Path::new(src.trim_start_matches("file://"));

        let tmp = tempfile::tempdir().unwrap();
        let dst_root = tmp.path().join("table");
        copy_tree(src_root, &dst_root);

        let files_dir = dst_root.join(".hoodie").join("metadata").join("files");
        // The partition holds two base files, both in file group `files-0000-0`
        // at different instants -- two slices of one group, of which discovery
        // takes the latest. Copying the latest under a second file id is what
        // makes a second *group*, and so a second slice in the result.
        let mut base: Vec<std::path::PathBuf> = std::fs::read_dir(&files_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|x| x == "hfile"))
            .collect();
        base.sort();
        assert!(
            base.iter().all(|p| p
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("files-0000-0")),
            "the fixture must start with a single file group, or this test proves nothing"
        );

        let latest = base.last().expect("a base file");
        let original = latest.file_name().unwrap().to_string_lossy().to_string();
        // Same shape, different file id: `files-0001-0` beside `files-0000-0`.
        let twin = original.replacen("files-0000-0", "files-0001-0", 1);
        assert_ne!(
            twin, original,
            "the copy must land in a different file group"
        );
        std::fs::copy(latest, files_dir.join(&twin)).unwrap();

        let table = Table::new(dst_root.to_str().unwrap()).await?;
        let metadata = table.get_or_init_metadata_table().await?;

        let one = QuickstartTripsTable::V8Trips8I3U1D.path_to_mor_avro();
        let baseline_table = Table::new(&one).await?;
        let baseline = baseline_table
            .get_or_init_metadata_table()
            .await?
            .read_files_partition_batch(&[])
            .await?;

        let doubled = metadata.read_files_partition_batch(&[]).await?;
        assert_eq!(
            doubled.num_rows(),
            baseline.num_rows() * 2,
            "both slices must be read and concatenated: one slice gives {}, two give {}",
            baseline.num_rows(),
            doubled.num_rows()
        );
        Ok(())
    }

    /// Recursive directory copy, so the test can modify a fixture without
    /// touching the checked-in one.
    fn copy_tree(from: &std::path::Path, to: &std::path::Path) {
        std::fs::create_dir_all(to).unwrap();
        for entry in std::fs::read_dir(from).unwrap().filter_map(|e| e.ok()) {
            let target = to.join(entry.file_name());
            match entry.file_type() {
                Ok(t) if t.is_dir() => copy_tree(&entry.path(), &target),
                Ok(t) if t.is_file() => {
                    std::fs::copy(entry.path(), target).unwrap();
                }
                _ => {}
            }
        }
    }

    /// Shard routing, checked against the fixture's **real** record index: ten
    /// file groups, written by Hudi, with Hudi's own file-id naming.
    ///
    /// The routing tests elsewhere use synthetic `FileSlice`s, so they prove the
    /// selection logic but not that it survives real file ids -- where the sort
    /// that recovers shard order has to work on names like
    /// `record-index-0003-0` rather than ones a test chose.
    ///
    /// What this does *not* do is read the records: `record_index` has no decoded
    /// type yet, so the keys each shard holds are unknown here. It pins
    /// discovery and selection, and says so.
    #[tokio::test]
    async fn routing_selects_one_of_the_record_index_s_real_shards() -> Result<()> {
        let data_table = get_data_table().await;
        let metadata = data_table.get_or_init_metadata_table().await?;

        let Some((_reader, slices)) = metadata.partition_reader("record_index").await? else {
            panic!("the fixture must have a record_index partition, or this test proves nothing");
        };
        assert_eq!(
            slices.len(),
            10,
            "the fixture's record index is sharded across ten file groups; a different \
             number means discovery changed and this test's premise is stale"
        );

        let key = "some-record-key";
        let picked = routing::slices_for_keys(&slices, &[key]);
        assert_eq!(picked.len(), 1, "one key opens one shard, not ten");

        // The shard the hash names, computed over the real ids sorted into shard
        // order -- not merely "some shard", which a broken sort would also give.
        let mut ordered: Vec<&str> = slices.iter().map(|s| s.file_id()).collect();
        ordered.sort();
        let expected = ordered[routing::file_group_index(key, ordered.len())];
        assert_eq!(
            picked[0].file_id(),
            expected,
            "selection must follow shard order recovered from Hudi's own file ids"
        );
        Ok(())
    }
}
