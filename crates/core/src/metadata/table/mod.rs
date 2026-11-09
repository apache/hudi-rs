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

pub mod column_stats;
pub mod encode;
pub mod hash;
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
use std::collections::{HashMap, HashSet};
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
use crate::metadata::rollback::{RestoreMetadata, RollbackMetadata, RollbackPlan};
use crate::storage::util::join_url_segments;
use crate::table::ReadOptions;
use crate::table::Table;
use crate::table::file_pruner::FilePruner;
use crate::table::partition::PartitionPruner;
use crate::timeline::instant::{Action, Instant, State};
use crate::timeline::selector::TimelineSelector;

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
    /// Normalization happens at decode time in `decode_files_partition_record_with_schema`.
    ///
    /// # Note
    /// Must be called on a DATA table, not a METADATA table.
    pub async fn read_metadata_table_files_partition(
        &self,
        partition_pruner: &PartitionPruner,
    ) -> Result<HashMap<String, FilesPartitionRecord>> {
        let metadata_table = self.get_or_init_metadata_table().await?;
        // Built here because it needs both timelines: the data table's completed
        // and pending instants, and the metadata table's own.
        let valid = self.valid_instant_timestamps(metadata_table).await?;
        metadata_table
            .fetch_files_partition_records(partition_pruner, &valid)
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
        let valid = self.valid_instant_timestamps(metadata_table).await?;
        metadata_table
            .read_files_partition_batch(keys, &valid)
            .await
    }

    /// Read one metadata-table partition as an Arrow batch, undecoded.
    ///
    /// The generalisation of [`Self::read_metadata_table_files_partition_arrow`]:
    /// `partition` is the metadata table partition to read — one of `files`,
    /// `column_stats`, `partition_stats`, `bloom_filters`, `record_index`, or a
    /// `secondary_index_<name>` value (see
    /// [`Self::get_metadata_table_partitions`]). `files` is the only partition
    /// with a decoded record type; the others come back as raw Arrow in their
    /// stored schema, so callers that want them should consume the batch
    /// directly.
    ///
    /// `keys` empty reads every record; otherwise only those keys, matched
    /// **as stored**.
    ///
    /// An empty batch when the partition has no commits yet.
    ///
    /// # Errors
    /// [`CoreError::MetadataTable`] when `partition` is not one the table
    /// declares in `hoodie.table.metadata.partitions`. An empty batch there
    /// would be indistinguishable from a declared partition that holds no
    /// records, and an undeclared name is most likely a typo.
    ///
    /// A file group that is log-only and holds nothing but tombstones fails in
    /// the merge with "No schema available for merge output", since no record
    /// survives to lend the output its schema. Not yet handled.
    ///
    /// # Note
    /// Must be called on a DATA table, not a METADATA table.
    pub async fn read_metadata_table_partition_arrow(
        &self,
        partition: &str,
        keys: &[&str],
    ) -> Result<arrow_array::RecordBatch> {
        let declared = self.get_metadata_table_partitions();
        if !declared.iter().any(|p| p == partition) {
            return Err(CoreError::MetadataTable(format!(
                "Metadata table partition '{partition}' is not declared on this table; \
                 declared partitions: {declared:?}"
            )));
        }
        let metadata_table = self.get_or_init_metadata_table().await?;
        let valid = self.valid_instant_timestamps(metadata_table).await?;
        metadata_table
            .read_metadata_table_partition_batch(partition, keys, &valid)
            .await
    }

    /// Fetch records from the `files` partition with optional partition pruning.
    ///
    /// For non-partitioned tables, directly fetches the "." record.
    /// For partitioned tables with filters, performs partition pruning via `__all_partitions__`.
    ///
    /// # Arguments
    /// * `partition_pruner` - Data table's partition pruner to filter partitions.
    /// * `valid_instants` - Data-timeline-fenced MDT instants; `None` trusts all
    ///   log blocks (only for callers without data-table access, e.g. tests).
    ///
    /// # Note
    /// Must be called on a METADATA table instance.
    pub async fn fetch_files_partition_records(
        &self,
        partition_pruner: &PartitionPruner,
        valid_instants: &HashSet<String>,
    ) -> Result<HashMap<String, FilesPartitionRecord>> {
        // Non-partitioned table: directly fetch "." record
        if !partition_pruner.is_table_partitioned() {
            return self
                .read_files_partition(
                    &[FilesPartitionRecord::NON_PARTITIONED_NAME],
                    valid_instants,
                )
                .await;
        }

        // Partitioned table without filters: read all records
        if partition_pruner.is_empty() {
            return self.read_files_partition(&[], valid_instants).await;
        }

        // Partitioned table with filters: prune partitions first, then read only matching records
        let all_partitions_records = self
            .read_files_partition(&[FilesPartitionRecord::ALL_PARTITIONS_KEY], valid_instants)
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

        self.read_files_partition(&pruned, valid_instants).await
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
        valid_instants: &HashSet<String>,
    ) -> Result<HashMap<String, FilesPartitionRecord>> {
        let Some((reader, file_slices)) = self.files_partition_reader().await? else {
            return Ok(HashMap::new());
        };
        let reader = reader.with_valid_instants(valid_instants.clone());
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
            self.bounded_read_concurrency_for(&targets),
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
        valid_instants: &HashSet<String>,
    ) -> Result<arrow_array::RecordBatch> {
        self.read_metadata_table_partition_batch(
            FilesPartitionRecord::PARTITION_NAME,
            keys,
            valid_instants,
        )
        .await
    }

    /// The merged batch of one metadata partition, for a caller that wants Arrow.
    ///
    /// An empty batch when the partition is not enabled or has no commits,
    /// matching the empty map the decoded form returns in that case.
    ///
    /// # Note
    /// Must be called on a METADATA table instance.
    pub(crate) async fn read_metadata_table_partition_batch(
        &self,
        partition: &str,
        keys: &[&str],
        valid_instants: &HashSet<String>,
    ) -> Result<arrow_array::RecordBatch> {
        let Some((reader, file_slices)) = self.partition_reader(partition).await? else {
            return Ok(arrow_array::RecordBatch::new_empty(std::sync::Arc::new(
                Schema::empty(),
            )));
        };
        let reader = reader.with_valid_instants(valid_instants.clone());
        let targets = routing::slices_for_keys(&file_slices, keys);
        let batches = crate::util::concurrency::bounded_in_order(
            &targets,
            self.bounded_read_concurrency_for(&targets),
            |file_slice| reader.read_partition_batch(file_slice, keys, partition),
        )
        .await?;
        concat_metadata_batches(batches)
    }

    /// The reader and every file slice of the `files` partition.
    ///
    /// `None` when the metadata table has no commits, which both callers treat as
    /// an empty result rather than an error.
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
        // NOTE: the reader comes back without a valid-instant set. Each read
        // attaches its own with `MetadataTableV2Reader::with_valid_instants`,
        // because the set needs the data table's timeline and this constructor
        // has only the metadata table's.
    }
}

/// Hudi's sentinel prefix for a metadata delta commit written outside the data
/// timeline (`HoodieTableMetadata.SOLO_COMMIT_TIMESTAMP`).
const SOLO_COMMIT_TIMESTAMP: &str = "00000000000000";

/// How many rollback instants to read at once when assembling the valid-instant
/// set.
///
/// Each read is one small GET of an instant file, so this bounds request
/// concurrency rather than memory -- unlike the file-slice fan-out, nothing here
/// retains a batch. Fixed rather than configurable because it is not a
/// user-facing trade-off: the alternative is the sequential walk this replaces.
const ROLLBACK_READ_CONCURRENCY: usize = 8;

impl Table {
    /// The valid-instant set as of the data timeline in storage now rather than
    /// as of this handle's snapshot, for a write and for the record index, which
    /// fences its own reads to a snapshot.
    pub(crate) async fn valid_metadata_instants(&self) -> Result<HashSet<String>> {
        // The handle's metadata table may refuse to open for this snapshot; the
        // latest set does not depend on the snapshot.
        match self.get_or_init_metadata_table().await {
            Ok(mdt) => self.latest_valid_instant_timestamps(mdt).await,
            Err(_) if self.metadata_table_refused() => {
                let mdt = self.new_metadata_table().await?;
                self.latest_valid_instant_timestamps(&mdt).await
            }
            Err(e) => Err(e),
        }
    }

    /// The instants whose metadata log blocks a read of this handle may read:
    /// those of the data snapshot the handle opened on.
    ///
    /// Built from the listing the handle's timeline was loaded from, so a
    /// pending instant that has since completed stays excluded, as Java builds
    /// the set from the data meta client's cached timeline. Capped at the latest
    /// instant time in that listing, which Java does not do: Java loads the
    /// metadata table's timeline with the data one, while this handle opens the
    /// metadata table at its first read, and an instant above the cap was
    /// created after the snapshot.
    pub(crate) async fn valid_instant_timestamps(&self, mdt: &Table) -> Result<HashSet<String>> {
        let listed = &self.timeline.listed;
        let mut valid = self
            .collect_valid_instants(
                mdt,
                &listed.pending,
                &listed.rollbacks_and_restores,
                &mdt.timeline.listed.rollbacks_and_restores,
            )
            .await?;
        if let Some(latest) = listed.latest.as_deref() {
            valid.retain(|instant| instant.as_str() <= latest);
        }
        Ok(valid)
    }

    /// [`Self::valid_instant_timestamps`] for a write, from the data and
    /// metadata timelines as they are in storage now.
    pub(crate) async fn latest_valid_instant_timestamps(
        &self,
        mdt: &Table,
    ) -> Result<HashSet<String>> {
        let data_pending = self.timeline.all_pending_instant_times().await?;
        let data_rollbacks = self.rollback_and_restore_instants().await?;
        let mdt_rollbacks = mdt.rollback_and_restore_instants().await?;
        self.collect_valid_instants(mdt, &data_pending, &data_rollbacks, &mdt_rollbacks)
            .await
    }

    /// The instants whose metadata log blocks may be read.
    ///
    /// Mirrors Java's `HoodieTableMetadataUtil.getValidInstantTimestamps` (:2081).
    /// This is a **set, not a window**: it has holes -- a pending data instant is
    /// excluded while instants either side of it are included -- and members from
    /// outside the data timeline entirely. See
    /// [`InstantRange::exact_match`](crate::timeline::selector::InstantRange::exact_match)
    /// for why a bounded range cannot stand in for it.
    ///
    /// `self` is the data table; `mdt` its metadata table. Both timelines are
    /// needed, which is why this lives here rather than on either one alone.
    /// `data_pending` is Java's `datasetPendingInstants`: every action, not only
    /// the commit actions the timeline loads by default.
    async fn collect_valid_instants(
        &self,
        mdt: &Table,
        data_pending: &HashSet<String>,
        data_rollbacks: &[Instant],
        mdt_rollbacks: &[Instant],
    ) -> Result<HashSet<String>> {
        let mut valid: HashSet<String> = self.valid_from_completed_data_instants();
        valid.extend(self.valid_from_mdt_delta_commits(mdt, data_pending));
        valid.extend(Self::valid_from_sentinel_commits(mdt));

        // 3. Commits rolled back by the data table's rollbacks and restores.
        //    Their log blocks were written, rolled back, and re-applied, so
        //    excluding them drops records that are genuinely present.
        //
        //    Only rollbacks newer than the earliest valid instant can have
        //    rolled back anything we hold a log block for; Java bounds the scan
        //    the same way (`getValidInstantTimestamps`, HoodieTableMetadataUtil
        //    :2101), falling back to the sentinel when the set is empty.
        let earliest = valid
            .iter()
            .min()
            .cloned()
            .unwrap_or_else(|| SOLO_COMMIT_TIMESTAMP.to_string());
        let in_scope: Vec<&Instant> = data_rollbacks
            .iter()
            .filter(|instant| instant.timestamp > earliest)
            .collect();
        // Each of these is an awaited GET, and the bound above is the only thing
        // limiting how many there are -- a long-lived table with many rollbacks
        // pays all of them on every metadata read. Reading them concurrently
        // keeps that cost off the critical path without narrowing the set.
        let rolled_back = crate::util::concurrency::bounded_in_order(
            &in_scope,
            ROLLBACK_READ_CONCURRENCY,
            |instant| self.commits_rolled_back_by(instant),
        )
        .await?;
        valid.extend(rolled_back.into_iter().flatten());

        // 4. The metadata table's own rollback and restore instants.
        for instant in mdt_rollbacks {
            valid.insert(instant.timestamp.clone());
        }

        Ok(valid)
    }

    /// Hold `mdt` to this handle's snapshot: drop from its timeline every
    /// compaction that may have folded in a metadata instant the snapshot
    /// cannot see, so its reads take the file slices from before it, as a
    /// metadata table opened together with the snapshot would.
    ///
    /// No instant filter can take a later instant's changes back out of a
    /// compacted base file, so the slice before it has to be readable: on
    /// storage, which the metadata table's cleaner may already have changed,
    /// and with every instant since its base still on the timeline. Returns
    /// why not when it is not, and the handle then lists storage instead.
    pub(crate) async fn pin_metadata_table_to_snapshot(
        &self,
        mdt: &mut Table,
    ) -> Result<Option<String>> {
        let Some(bound) = self.metadata_compaction_bound(mdt) else {
            return Ok(None);
        };
        let dropped: HashSet<String> = mdt
            .timeline
            .completed_commits
            .iter()
            .filter(|instant| instant.action == Action::Commit && instant.timestamp > bound)
            .map(|instant| instant.timestamp.clone())
            .collect();
        if dropped.is_empty() {
            return Ok(None);
        }
        let timeline = &mdt.timeline;
        let base = timeline
            .completed_commits
            .iter()
            .filter(|instant| {
                instant.action == Action::Commit && !dropped.contains(&instant.timestamp)
            })
            .map(|instant| instant.timestamp.as_str())
            .max();
        // Without an earlier compaction, the base is the one written when the
        // metadata table was initialized, at a sentinel instant.
        let archived_past = match (timeline.earliest_active_instant.as_deref(), base) {
            (None, _) => false,
            (Some(earliest), Some(base)) => earliest > base,
            (Some(earliest), None) => !earliest.starts_with(SOLO_COMMIT_TIMESTAMP),
        };
        if archived_past {
            return Ok(Some(format!(
                "The metadata table was compacted after {bound} and has since archived the \
                 instants this table's snapshot needs"
            )));
        }
        if let Some(group) = Self::file_group_missing_earlier_slice(mdt, &dropped).await? {
            return Ok(Some(format!(
                "The metadata table was compacted after {bound} and no longer holds the file \
                 slice of {group} from before that"
            )));
        }
        mdt.timeline
            .completed_commits
            .retain(|instant| !dropped.contains(&instant.timestamp));
        Ok(None)
    }

    /// The first metadata file group, as `partition/file id`, that a dropped
    /// compaction rewrote and that has nothing left on storage of its slice
    /// from before it: no committed earlier base, and no log older than the
    /// dropped base.
    ///
    /// The metadata table's cleaner removes a slice's base and logs together,
    /// so a slice it has removed leaves neither.
    async fn file_group_missing_earlier_slice(
        mdt: &Table,
        dropped: &HashSet<String>,
    ) -> Result<Option<String>> {
        use crate::file_group::base_file::BaseFile;
        use crate::file_group::log_file::LogFile;
        use std::str::FromStr;

        let timeline = &mdt.timeline;
        let completed: HashSet<&str> = timeline
            .completed_commits
            .iter()
            .map(|instant| instant.timestamp.as_str())
            .collect();
        let committed_earlier = |timestamp: &str| {
            !dropped.contains(timestamp)
                && (completed.contains(timestamp)
                    || timeline
                        .earliest_active_instant
                        .as_deref()
                        .is_some_and(|earliest| timestamp < earliest))
        };
        let storage = &mdt.file_system_view.storage;
        for partition in storage.list_dirs(None).await? {
            if partition.starts_with('.') {
                continue;
            }
            #[derive(Default)]
            struct Group {
                earliest_dropped_base: Option<String>,
                has_earlier_base: bool,
                earliest_log: Option<String>,
            }
            let mut groups: HashMap<String, Group> = HashMap::new();
            for file in storage.list_files(Some(&partition)).await? {
                let name = file.name.as_str();
                if name.ends_with(".hfile") && !name.starts_with('.') {
                    let Ok(base) = BaseFile::from_str(name) else {
                        continue;
                    };
                    let group = groups.entry(base.file_id).or_default();
                    if dropped.contains(&base.commit_timestamp) {
                        if group
                            .earliest_dropped_base
                            .as_ref()
                            .is_none_or(|earliest| base.commit_timestamp < *earliest)
                        {
                            group.earliest_dropped_base = Some(base.commit_timestamp);
                        }
                    } else if committed_earlier(&base.commit_timestamp) {
                        group.has_earlier_base = true;
                    }
                } else if LogFile::is_log_file_name(name)
                    && let Ok(log) = LogFile::from_str(name)
                {
                    let group = groups.entry(log.file_id).or_default();
                    if group
                        .earliest_log
                        .as_ref()
                        .is_none_or(|earliest| log.timestamp < *earliest)
                    {
                        group.earliest_log = Some(log.timestamp);
                    }
                }
            }
            for (file_id, group) in groups {
                if let Some(dropped_base) = group.earliest_dropped_base
                    && !group.has_earlier_base
                    && group.earliest_log.is_none_or(|log| log >= dropped_base)
                {
                    return Ok(Some(format!("{partition}/{file_id}")));
                }
            }
        }
        Ok(None)
    }

    /// Whether `mdt` holds instants this handle's snapshot cannot see.
    pub(crate) fn metadata_table_past_snapshot(&self, mdt: &Table) -> bool {
        self.metadata_compaction_bound(mdt).is_some()
    }

    /// The instant above which a metadata compaction may have folded in an
    /// instant this handle's snapshot cannot see, or `None` when the metadata
    /// table holds no such instant.
    ///
    /// A compaction folds in only what completed before it, so the bound is the
    /// earliest completion among the unseen instants. The compaction's own
    /// instant cannot decide this alone: hudi-rs compacts after the data commit
    /// that triggers it completes, so a compaction that folded in nothing new
    /// often sorts above every instant the snapshot listed.
    fn metadata_compaction_bound(&self, mdt: &Table) -> Option<String> {
        let listed = &self.timeline.listed;
        let latest = listed.latest.as_deref()?;
        // An unseen instant the metadata table has archived is not on its
        // timeline, and completed no earlier than it was requested.
        let floor = listed
            .pending
            .iter()
            .map(String::as_str)
            .fold(latest, |floor, pending| floor.min(pending));
        if mdt
            .timeline
            .earliest_active_instant
            .as_deref()
            .is_some_and(|earliest| earliest > floor)
        {
            return Some(floor.to_string());
        }
        mdt.timeline
            .completed_commits
            .iter()
            .filter(|instant| instant.action == Action::DeltaCommit)
            .chain(&mdt.timeline.listed.rollbacks_and_restores)
            .filter(|instant| {
                let timestamp = instant.timestamp.as_str();
                !timestamp.starts_with(SOLO_COMMIT_TIMESTAMP)
                    && (timestamp > latest || listed.pending.contains(timestamp))
            })
            .map(|instant| {
                instant
                    .completion_timestamp
                    .as_deref()
                    .unwrap_or(&instant.timestamp)
            })
            .min()
            .map(str::to_string)
    }

    /// Source 1 — every completed data instant.
    ///
    /// The metadata table is written before the data instant commits, so a log
    /// block whose instant never completed must not be read.
    fn valid_from_completed_data_instants(&self) -> HashSet<String> {
        self.timeline
            .completed_commits
            .iter()
            .map(|instant| instant.timestamp.clone())
            .collect()
    }

    /// Source 2 — completed metadata delta commits with no *pending* data
    /// instant of the same name.
    ///
    /// This source is also what keeps a data instant archived off the data
    /// timeline readable: an archived instant is never pending, and the
    /// metadata table keeps its delta commits active until compaction folds
    /// their log blocks into base files.
    ///
    /// An indexing delta commit has no data instant at all and is valid. One
    /// whose data instant is still pending is the case where the data write
    /// failed after the metadata write committed, and must not be read — that
    /// exclusion is the whole content of this source.
    ///
    /// `data_pending` is the data table's pending instants across **every**
    /// action, not only the commit actions. Passed in rather
    /// than read from `self.timeline.pending_instants`, which covers only
    /// commit, delta commit and replace commit: a metadata delta commit written
    /// for a compaction or a clean that then crashed mid-commit leaves a
    /// `.compaction.inflight` or `.clean.inflight` on the data timeline, and the
    /// narrow set would admit it where Java excludes it.
    fn valid_from_mdt_delta_commits(
        &self,
        mdt: &Table,
        data_pending: &HashSet<String>,
    ) -> HashSet<String> {
        mdt.timeline
            .completed_commits
            .iter()
            .filter(|i| i.action == Action::DeltaCommit && !data_pending.contains(&i.timestamp))
            .map(|i| i.timestamp.clone())
            .collect()
    }

    /// Source 5 — metadata delta commits written outside the data timeline.
    fn valid_from_sentinel_commits(mdt: &Table) -> HashSet<String> {
        mdt.timeline
            .completed_commits
            .iter()
            .filter(|i| i.timestamp.starts_with(SOLO_COMMIT_TIMESTAMP))
            .map(|i| i.timestamp.clone())
            .collect()
    }

    /// Completed rollback and restore instants on this table's timeline.
    ///
    /// Loaded with a selector naming those actions, because they are not in
    /// `DEFAULT_LOADING_ACTIONS` -- a rollback is not a commit, and putting it
    /// there would change every existing read.
    async fn rollback_and_restore_instants(&self) -> Result<Vec<Instant>> {
        let selector = TimelineSelector::actions_in_range(
            &[Action::Rollback, Action::Restore],
            &[State::Completed],
            self.hudi_configs.clone(),
            None,
            None,
        )?;
        self.timeline.load_instants(&selector, false).await
    }

    /// The commits one rollback or restore instant rolled back.
    ///
    /// A rollback names them in its own metadata, falling back to its
    /// `.requested` plan when the completed file is empty -- Java does the same
    /// (`getRollbackedCommits`, HoodieTableMetadataUtil:2158). A restore is
    /// several rollbacks, so its commits are the union of theirs.
    ///
    /// An instant that cannot be read fails the read, as Java's does. Yielding
    /// an empty list instead would drop the commits that rollback re-applied
    /// from the valid set, and the read would then serve listings missing
    /// their log blocks -- wrong results returned as if they were right, which
    /// is worse than a read that stops. The one tolerated failure is the same
    /// one Java tolerates: an unreadable *completed* rollback file, which falls
    /// back to the plan.
    async fn commits_rolled_back_by(&self, instant: &Instant) -> Result<Vec<String>> {
        let bytes = self
            .timeline
            .load_instant_bytes(instant)
            .await
            .map_err(|e| Self::rollback_error(instant, e))?;

        match instant.action {
            Action::Restore => Ok(RestoreMetadata::from_avro_bytes(&bytes)
                .map_err(|e| Self::rollback_error(instant, e))?
                .commits_rolled_back()),
            Action::Rollback => {
                match RollbackMetadata::from_avro_bytes(&bytes) {
                    Ok(rollback) if !rollback.commits_rollback.is_empty() => {
                        return Ok(rollback.commits_rollback);
                    }
                    // Java falls back for exactly these two: a completed file
                    // that will not parse, and one that parsed to nothing.
                    Ok(_) => log::warn!(
                        "rollback {} names no commits; reading its plan instead",
                        instant.timestamp
                    ),
                    Err(e) => log::warn!(
                        "rollback {} is unreadable ({e}); reading its plan instead",
                        instant.timestamp
                    ),
                }
                let plan = Instant {
                    state: State::Requested,
                    ..instant.clone()
                };
                let plan_bytes = self
                    .timeline
                    .load_instant_bytes(&plan)
                    .await
                    .map_err(|e| Self::rollback_error(instant, e))?;
                let plan = RollbackPlan::from_avro_bytes(&plan_bytes)
                    .map_err(|e| Self::rollback_error(instant, e))?;
                // A plan that names no instant is a rollback of nothing, not a
                // failure -- there is simply no commit to re-admit.
                Ok(plan
                    .commit_rolled_back()
                    .map(|c| vec![c.to_string()])
                    .unwrap_or_default())
            }
            _ => Ok(Vec::new()),
        }
    }

    /// Java raises `HoodieMetadataException` here; this is its counterpart.
    fn rollback_error(instant: &Instant, source: impl std::fmt::Display) -> CoreError {
        CoreError::MetadataTable(format!(
            "Error retrieving the commits rolled back by {} {}: {source}",
            instant.action.as_ref(),
            instant.timestamp
        ))
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
        let baseline_mdt = baseline_table.get_or_init_metadata_table().await?;
        let baseline_valid = baseline_table
            .valid_instant_timestamps(baseline_mdt)
            .await?;
        let baseline = baseline_mdt
            .read_files_partition_batch(&[], &baseline_valid)
            .await?;

        let doubled_valid = table.valid_instant_timestamps(metadata).await?;
        let doubled = metadata
            .read_files_partition_batch(&[], &doubled_valid)
            .await?;
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

    /// Each source is tested on its **own** output, not on the union.
    ///
    /// Testing the union does not work on this fixture and the reason is worth
    /// recording: sources 1, 2 and 5 overlap almost entirely — a data commit and
    /// the metadata delta commit that records it share a timestamp — so every
    /// member survives dropping any single source, and a membership assertion
    /// over the union cannot fail. Three mutations, one per source, all passed
    /// against exactly such a test before it was replaced by this one.
    #[tokio::test]
    async fn each_source_contributes_what_it_claims() -> Result<()> {
        let data_table = get_data_table().await;
        let mdt = data_table.get_or_init_metadata_table().await?;

        // Source 1 — exactly the completed data instants, no more.
        let from_data = data_table.valid_from_completed_data_instants();
        let expected: HashSet<String> = data_table
            .timeline
            .completed_commits
            .iter()
            .map(|i| i.timestamp.clone())
            .collect();
        assert!(
            !expected.is_empty(),
            "the fixture must have completed data instants"
        );
        assert_eq!(
            from_data, expected,
            "source 1 is exactly the completed data instants"
        );

        // Source 5 — exactly the sentinel-prefixed metadata commits, and every
        // one of them really carries the prefix.
        let from_sentinel = Table::valid_from_sentinel_commits(mdt);
        assert!(
            !from_sentinel.is_empty(),
            "the fixture must have sentinel-prefixed metadata commits"
        );
        assert!(
            from_sentinel
                .iter()
                .all(|t| t.starts_with(SOLO_COMMIT_TIMESTAMP)),
            "source 5 must contain only sentinel-prefixed instants"
        );

        // Source 2 — every metadata delta commit whose data instant is not
        // pending, and none whose data instant is.
        let data_pending = data_table.timeline.all_pending_instant_times().await?;
        let from_mdt = data_table.valid_from_mdt_delta_commits(mdt, &data_pending);
        for instant in &mdt.timeline.completed_commits {
            let pending = data_pending.contains(&instant.timestamp);
            let is_delta = instant.action == Action::DeltaCommit;
            assert_eq!(
                from_mdt.contains(&instant.timestamp),
                is_delta && !pending,
                "source 2 disagreed on {} (delta={is_delta}, pending={pending})",
                instant.timestamp
            );
        }
        Ok(())
    }

    /// Source 2's exclusion, on a case the fixture does not contain.
    ///
    /// The fixture has **zero** pending data instants, so its exclusion branch is
    /// never exercised by real data — which is precisely why dropping the
    /// exclusion passed every test written against the fixture alone. The
    /// pending set is injected here so the branch has an input that reaches it.
    #[tokio::test]
    async fn source_two_excludes_a_metadata_commit_whose_data_instant_is_pending() -> Result<()> {
        let data_table = get_data_table().await;
        let mdt_owned = data_table.new_metadata_table().await?;

        let victim = mdt_owned
            .timeline
            .completed_commits
            .iter()
            .find(|i| i.action == Action::DeltaCommit)
            .map(|i| i.timestamp.clone())
            .expect("the fixture must have a completed metadata delta commit");

        let none_pending = HashSet::new();
        assert!(
            data_table
                .valid_from_mdt_delta_commits(&mdt_owned, &none_pending)
                .contains(&victim),
            "with nothing pending, {victim} must be included — or the next assertion proves nothing"
        );

        let pending = HashSet::from([victim.clone()]);
        assert!(
            !data_table
                .valid_from_mdt_delta_commits(&mdt_owned, &pending)
                .contains(&victim),
            "{victim} has a pending data instant and must be excluded"
        );
        Ok(())
    }

    /// The exclusion must see a data instant pending under an action the
    /// timeline does not load by default.
    ///
    /// This is the case the narrow `Timeline::pending_instants` misses. The
    /// metadata table commits a delta commit for a data compaction; the
    /// compaction then crashes, leaving `{ts}.compaction.inflight` and no
    /// completed file on the data timeline. Java's `filterInflightsAndRequested()`
    /// runs over the whole active timeline and excludes that delta commit. A
    /// view built from commit/deltacommit/replacecommit alone cannot represent a
    /// `compaction` at all, so it sees nothing pending and admits it.
    ///
    /// The instant is one the fixture does not have, and is written only as a
    /// *metadata* delta commit — so no other source can supply it and the
    /// exclusion is the only thing the assertion can be measuring.
    #[tokio::test]
    async fn source_two_excludes_a_data_instant_pending_under_a_non_commit_action() -> Result<()> {
        let src = QuickstartTripsTable::V8Trips8I3U1D.path_to_mor_avro();
        let src_root = std::path::Path::new(src.trim_start_matches("file://"));
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("table");
        copy_tree(src_root, &root);

        // A metadata delta commit for a data compaction, at an instant the
        // fixture does not otherwise use. Below the fixture's latest data
        // instant, since a handle reads none above it.
        const COMPACTION_TS: &str = "20251220000000000";
        std::fs::write(
            root.join(format!(
                ".hoodie/metadata/.hoodie/timeline/{COMPACTION_TS}_{COMPACTION_TS}.deltacommit"
            )),
            b"",
        )
        .unwrap();

        let table = Table::new(root.to_str().unwrap()).await?;
        let mdt = table.get_or_init_metadata_table().await?;

        // The premise: with no pending data instant, source 2 admits it. Without
        // this the exclusion below could be passing for any other reason.
        assert!(
            table
                .valid_instant_timestamps(mdt)
                .await?
                .contains(COMPACTION_TS),
            "{COMPACTION_TS} must start out valid, or the assertion below proves nothing"
        );

        // Now the data-side compaction that never completed.
        std::fs::write(
            root.join(format!(
                ".hoodie/timeline/{COMPACTION_TS}.compaction.inflight"
            )),
            b"",
        )
        .unwrap();

        let table = Table::new(root.to_str().unwrap()).await?;
        let mdt = table.get_or_init_metadata_table().await?;
        assert!(
            table
                .timeline
                .all_pending_instant_times()
                .await?
                .contains(COMPACTION_TS),
            "a .compaction.inflight must register as pending"
        );
        assert!(
            !table.timeline.pending_instants.contains(COMPACTION_TS),
            "the commit-action-only set must NOT see it — that is the gap this covers"
        );
        assert!(
            !table
                .valid_instant_timestamps(mdt)
                .await?
                .contains(COMPACTION_TS),
            "{COMPACTION_TS} has a pending data compaction and must be excluded"
        );
        Ok(())
    }

    /// Sources 3 and 4, on a timeline the fixture does not have.
    ///
    /// The fixture carries zero rollback and zero restore instants, so both
    /// sources run on every metadata read today while being exercised by
    /// nothing. A rollback instant is written into a copy of the table — real
    /// Avro `HoodieRollbackMetadata`, written against Hudi's own schema — so the
    /// paths have an input that reaches them.
    ///
    /// The rolled-back commit is a timestamp that appears **nowhere else on
    /// either timeline**. That is what makes the assertion attributable: no
    /// other source can supply it, so its presence in the set can only have come
    /// from source 3. The same trap as the union test — overlapping sources make
    /// membership prove nothing about origin.
    #[tokio::test]
    async fn sources_three_and_four_admit_rolled_back_and_rollback_instants() -> Result<()> {
        use crate::metadata::rollback::tests::container_bytes;

        let src = QuickstartTripsTable::V8Trips8I3U1D.path_to_mor_avro();
        let src_root = std::path::Path::new(src.trim_start_matches("file://"));
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("table");
        copy_tree(src_root, &root);

        // Later than every instant in the fixture, so the "newer than the
        // earliest valid instant" bound admits it.
        const ROLLBACK_TS: &str = "20260101000000000";
        const ROLLED_BACK: &str = "20259999000000000";
        // At or below the data timeline's latest instant; one above it is
        // past every snapshot of this timeline.
        const MDT_ROLLBACK_TS: &str = "20251231000000000";
        const MDT_ROLLBACK_PAST_SNAPSHOT_TS: &str = "20260102000000000";

        let write_rollback = |dir: &std::path::Path, ts: &str, rolls: &[&str]| {
            std::fs::write(
                dir.join(format!("{ts}_{ts}.rollback")),
                container_bytes(ts, rolls),
            )
            .unwrap();
        };
        write_rollback(&root.join(".hoodie/timeline"), ROLLBACK_TS, &[ROLLED_BACK]);
        for ts in [MDT_ROLLBACK_TS, MDT_ROLLBACK_PAST_SNAPSHOT_TS] {
            write_rollback(&root.join(".hoodie/metadata/.hoodie/timeline"), ts, &[]);
        }

        let table = Table::new(root.to_str().unwrap()).await?;
        let mdt = table.get_or_init_metadata_table().await?;

        // The premise: neither synthetic timestamp is otherwise present, or the
        // assertions below would pass without either source doing anything.
        let data_pending = table.timeline.all_pending_instant_times().await?;
        for ts in [ROLLED_BACK, MDT_ROLLBACK_TS] {
            assert!(
                !table.valid_from_completed_data_instants().contains(ts)
                    && !table
                        .valid_from_mdt_delta_commits(mdt, &data_pending)
                        .contains(ts)
                    && !Table::valid_from_sentinel_commits(mdt).contains(ts),
                "{ts} must not be reachable from sources 1, 2 or 5, or this test proves nothing"
            );
        }

        let valid = table.valid_instant_timestamps(mdt).await?;
        assert!(
            valid.contains(ROLLED_BACK),
            "source 3: a commit rolled back by a data-table rollback must be valid"
        );
        assert!(
            valid.contains(MDT_ROLLBACK_TS),
            "source 4: the metadata table's own rollback instant must be valid"
        );
        assert!(
            !valid.contains(MDT_ROLLBACK_PAST_SNAPSHOT_TS),
            "a metadata rollback newer than the data timeline is past the snapshot"
        );
        assert!(
            table
                .latest_valid_instant_timestamps(mdt)
                .await?
                .contains(MDT_ROLLBACK_PAST_SNAPSHOT_TS),
            "a write reads the metadata table as it is now"
        );
        Ok(())
    }

    /// A rollback that cannot be read fails the read instead of shrinking the
    /// valid-instant set behind the caller's back.
    ///
    /// Source 3 exists to re-admit commits a rollback rolled back and re-applied.
    /// Swallowing a read failure drops exactly those commits, and the listing
    /// that follows is missing their log blocks -- a wrong answer returned as if
    /// it were right. Java raises `HoodieMetadataException` here for the same
    /// reason, so the assertion is on the error, not on a degraded set.
    ///
    /// The corrupt file is the *completed* rollback with no `.requested` plan
    /// beside it, which is the one case Java cannot fall back from either.
    #[tokio::test]
    async fn an_unreadable_rollback_fails_the_read_rather_than_shrinking_the_set() -> Result<()> {
        let src = QuickstartTripsTable::V8Trips8I3U1D.path_to_mor_avro();
        let src_root = std::path::Path::new(src.trim_start_matches("file://"));
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("table");
        copy_tree(src_root, &root);

        // Newer than every fixture instant, so the scan's lower bound admits it.
        const ROLLBACK_TS: &str = "20260101000000000";
        std::fs::write(
            root.join(format!(
                ".hoodie/timeline/{ROLLBACK_TS}_{ROLLBACK_TS}.rollback"
            )),
            b"not avro, and no .requested plan to fall back to",
        )
        .unwrap();

        let table = Table::new(root.to_str().unwrap()).await?;
        let mdt = table.get_or_init_metadata_table().await?;

        let err = table
            .valid_instant_timestamps(mdt)
            .await
            .expect_err("an unreadable rollback must fail the read");
        assert!(
            matches!(err, CoreError::MetadataTable(_)),
            "expected a metadata-table error, got {err:?}"
        );
        assert!(
            err.to_string().contains(ROLLBACK_TS),
            "the error must name the instant that could not be read, got {err}"
        );
        Ok(())
    }

    /// The record index through the public Arrow API, on a table whose index is
    /// sharded across file groups. A keyed read opens one shard and comes back
    /// keyed as stored; an unknown key is empty rather than an error; a full
    /// scan holds one entry per live record.
    #[tokio::test]
    async fn read_metadata_table_partition_arrow_reads_the_record_index() -> Result<()> {
        use arrow_array::{Array, StringArray};

        let table = Table::new(&SampleTable::V9TxnsSimpleMeta.path_to_cow()).await?;

        let batch = table
            .read_metadata_table_partition_arrow("record_index", &["TXN-011"])
            .await?;
        assert_eq!(batch.num_rows(), 1, "one live key is one index entry");
        let keys = batch
            .column_by_name("key")
            .expect("the record index keys on a `key` column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("`key` is a string column");
        assert_eq!(keys.value(0), "TXN-011");

        let batch = table
            .read_metadata_table_partition_arrow("record_index", &["no-such-key"])
            .await?;
        assert_eq!(
            batch.num_rows(),
            0,
            "a missing key is an empty batch, not an error"
        );

        let live_rows: usize = table
            .read(&crate::table::ReadOptions::new())
            .await?
            .iter()
            .map(|batch| batch.num_rows())
            .sum();
        let batch = table
            .read_metadata_table_partition_arrow("record_index", &[])
            .await?;
        assert_eq!(
            batch.num_rows(),
            live_rows,
            "a full scan holds one entry per live record"
        );
        Ok(())
    }

    #[tokio::test]
    async fn read_metadata_table_partition_arrow_rejects_an_undeclared_partition() -> Result<()> {
        let table = Table::new(&SampleTable::V9TxnsSimpleMeta.path_to_cow()).await?;
        let err = table
            .read_metadata_table_partition_arrow("secondary_idx_merchant", &[])
            .await
            .unwrap_err();
        assert!(matches!(err, CoreError::MetadataTable(_)), "got {err}");
        assert!(
            err.to_string().contains("secondary_idx_merchant"),
            "the error must name the partition, got {err}"
        );
        Ok(())
    }

    /// A handle reads the metadata table as of the data snapshot it opened on,
    /// not as of the moment it plans.
    mod snapshot_fence {
        use super::*;
        use crate::table::ReadOptions;
        use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema as ArrowSchema};

        const MAX_DELTA_COMMITS: &str = "hoodie.metadata.compact.max.delta.commits";

        fn rows(city: &str, rows: &[(&str, i64)]) -> RecordBatch {
            let schema = Arc::new(ArrowSchema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("city", DataType::Utf8, false),
                Field::new("ts", DataType::Int64, false),
            ]));
            let ids: Vec<&str> = rows.iter().map(|r| r.0).collect();
            let cities: Vec<&str> = rows.iter().map(|_| city).collect();
            let ts: Vec<i64> = rows.iter().map(|r| r.1).collect();
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(StringArray::from(ids)) as ArrayRef,
                    Arc::new(StringArray::from(cities)),
                    Arc::new(Int64Array::from(ts)),
                ],
            )
            .unwrap()
        }

        /// A copy-on-write table with one file group in `sf`, written at c5.
        async fn table_at_c5(path: &std::path::Path) -> Table {
            let mut table = Table::create(path.to_str().unwrap())
                .with_table_name("t")
                .with_record_key_fields(["id"])
                .with_partition_fields(["city"])
                .with_ordering_fields(["ts"])
                .create()
                .await
                .unwrap();
            table
                .append([rows("sf", &[("a", 1), ("b", 1)])])
                .await
                .unwrap();
            table
        }

        /// `(partition, base file name)` of every planned slice, sorted.
        async fn planned(table: &Table) -> Vec<(String, String)> {
            let mut planned: Vec<(String, String)> = table
                .get_file_slices(&ReadOptions::new())
                .await
                .unwrap()
                .iter()
                .map(|fs| {
                    let base = fs.base_file.as_ref().expect("a copy-on-write slice");
                    (fs.partition_path.clone(), base.file_name())
                })
                .collect();
            planned.sort();
            planned
        }

        /// Write the metadata delta commit `instant` with `files` entries, the
        /// way a data instant records its file changes.
        async fn metadata_delta(
            table: &Table,
            instant: &str,
            files: &[(String, String, i64, bool)],
        ) {
            let storage = table.file_system_view.storage.clone();
            let stats = crate::write::metadata::update_files_partition_entries(
                storage.as_ref(),
                instant,
                files,
            )
            .await
            .unwrap();
            crate::write::metadata::write_metadata_commit(
                storage.as_ref(),
                instant,
                HashMap::from([(FilesPartitionRecord::PARTITION_NAME.to_string(), stats)]),
            )
            .await
            .unwrap();
        }

        async fn put_timeline_file(table: &Table, name: &str) {
            table
                .file_system_view
                .storage
                .put_file(&format!(".hoodie/timeline/{name}"), Vec::<u8>::new())
                .await
                .unwrap();
        }

        /// A clean that deletes `file` from `partition`: its metadata delta
        /// commit, then its completed instant on the data timeline. The file
        /// itself is left for the caller to delete or keep.
        async fn clean(table: &Table, partition: &str, file: &str) -> String {
            let instant = crate::write::append::generate_instant_time().await;
            put_timeline_file(table, &format!("{instant}.clean.requested")).await;
            put_timeline_file(table, &format!("{instant}.clean.inflight")).await;
            metadata_delta(
                table,
                &instant,
                &[(partition.to_string(), file.to_string(), 0, true)],
            )
            .await;
            let completion = crate::write::append::generate_instant_time().await;
            put_timeline_file(table, &format!("{instant}_{completion}.clean")).await;
            instant
        }

        /// A handle opened at c5 plans c5's slice after a later commit c6
        /// rewrites the file group and a clean c7 removes c5's base file from
        /// the metadata table. Admitting c6 and c7 left the file group with
        /// only a slice the handle cannot see, and its rows vanished.
        #[tokio::test]
        async fn a_stale_handle_plans_its_own_snapshot_after_a_commit_and_clean() {
            let dir = tempfile::tempdir().unwrap();
            let mut writer = table_at_c5(dir.path()).await;

            // Not planned until after the writes: the first plan opens the
            // metadata table.
            let handle = Table::new(dir.path().to_str().unwrap()).await.unwrap();
            let at_c5 = planned(&Table::new(dir.path().to_str().unwrap()).await.unwrap()).await;
            assert_eq!(at_c5.len(), 1, "one file group: {at_c5:?}");
            let (partition, c5_base) = at_c5[0].clone();

            writer.upsert([rows("sf", &[("a", 2)])]).await.unwrap();
            let c6 = writer.timeline.get_latest_commit_timestamp().unwrap();
            let fresh = planned(&Table::new(dir.path().to_str().unwrap()).await.unwrap()).await;
            assert_eq!(fresh.len(), 1);
            assert_ne!(fresh[0].1, c5_base, "c6 must rewrite the file group");
            clean(&writer, &partition, &c5_base).await;
            writer
                .file_system_view
                .storage
                .delete_file(&format!("{partition}/{c5_base}"))
                .await
                .unwrap();

            assert_eq!(
                planned(&handle).await,
                at_c5,
                "the handle must plan the slice of the snapshot it opened on"
            );
            let err = handle
                .read(&ReadOptions::new())
                .await
                .expect_err("c5's base file is gone, so the read must fail, not come back short");
            assert!(err.to_string().contains(&c5_base), "got {err}");

            // A write on this handle, and the record index, read the metadata
            // table as it is now.
            let mdt = handle.get_or_init_metadata_table().await.unwrap();
            assert!(
                !handle
                    .valid_instant_timestamps(mdt)
                    .await
                    .unwrap()
                    .contains(&c6)
            );
            assert!(
                handle
                    .valid_metadata_instants()
                    .await
                    .unwrap()
                    .contains(&c6)
            );
        }

        /// A metadata base file compacted after the snapshot folds in changes
        /// the snapshot cannot see, so every read of the handle takes the slice
        /// from before that compaction.
        #[tokio::test]
        async fn a_stale_handle_does_not_read_a_metadata_base_compacted_after_its_snapshot() {
            let dir = tempfile::tempdir().unwrap();
            table_at_c5(dir.path()).await;
            let handle = Table::new(dir.path().to_str().unwrap()).await.unwrap();
            let at_c5 = planned(&Table::new(dir.path().to_str().unwrap()).await.unwrap()).await;
            let (partition, c5_base) = at_c5[0].clone();

            let mut writer =
                Table::new_with_options(dir.path().to_str().unwrap(), [(MAX_DELTA_COMMITS, "1")])
                    .await
                    .unwrap();
            writer.upsert([rows("sf", &[("a", 2)])]).await.unwrap();
            clean(&writer, &partition, &c5_base).await;
            writer
                .file_system_view
                .storage
                .delete_file(&format!("{partition}/{c5_base}"))
                .await
                .unwrap();
            // The next write compacts the metadata table, folding the clean in.
            writer.append([rows("ny", &[("c", 3)])]).await.unwrap();
            let storage = writer.file_system_view.storage.clone();
            let files_slices = crate::write::metadata::mdt_partition_latest_slices(
                storage.as_ref(),
                FilesPartitionRecord::PARTITION_NAME,
            )
            .await
            .unwrap();
            assert!(
                files_slices
                    .iter()
                    .all(|(_, base, logs)| base.is_some() && logs.is_empty()),
                "the files partition must be compacted after the clean: {files_slices:?}"
            );

            let pruner = PartitionPruner::new(
                &[],
                &handle.get_partition_schema().await.unwrap(),
                handle.hudi_configs.as_ref(),
            )
            .unwrap();
            let records = handle
                .read_metadata_table_files_partition(&pruner)
                .await
                .unwrap();
            assert_eq!(
                records[&partition].active_file_names(),
                vec![c5_base.as_str()],
                "the public read sees the snapshot's files partition"
            );
            assert_eq!(planned(&handle).await, at_c5);
            let err = handle
                .read(&ReadOptions::new())
                .await
                .expect_err("c5's base file is gone, so the read must fail, not come back short");
            assert!(err.to_string().contains(&c5_base), "got {err}");
        }

        /// A metadata table that archived the instants after the slice a stale
        /// handle would read cannot reproduce the snapshot: its public reads
        /// fail, and planning lists storage instead.
        #[tokio::test]
        async fn a_stale_handle_refuses_a_metadata_table_archived_past_its_snapshot() {
            let dir = tempfile::tempdir().unwrap();
            table_at_c5(dir.path()).await;
            let handle = Table::new(dir.path().to_str().unwrap()).await.unwrap();
            let at_c5 = planned(&Table::new(dir.path().to_str().unwrap()).await.unwrap()).await;
            let (partition, c5_base) = at_c5[0].clone();

            let mut writer =
                Table::new_with_options(dir.path().to_str().unwrap(), [(MAX_DELTA_COMMITS, "1")])
                    .await
                    .unwrap();
            writer.upsert([rows("sf", &[("a", 2)])]).await.unwrap();
            clean(&writer, &partition, &c5_base).await;
            writer.append([rows("ny", &[("c", 3)])]).await.unwrap();
            // Archive the metadata table's initializing instants.
            let mdt_timeline = dir.path().join(".hoodie/metadata/.hoodie/timeline");
            for entry in std::fs::read_dir(&mdt_timeline).unwrap() {
                let path = entry.unwrap().path();
                let name = path.file_name().unwrap().to_string_lossy().to_string();
                if name.starts_with(SOLO_COMMIT_TIMESTAMP) {
                    std::fs::remove_file(path).unwrap();
                }
            }

            let err = handle
                .read_metadata_table_files_partition_arrow(&[])
                .await
                .expect_err("the metadata table cannot reproduce the snapshot");
            assert!(
                matches!(err, CoreError::MetadataTable(_)) && err.to_string().contains("archived"),
                "got {err}"
            );
            assert!(handle.metadata_table_refused());
            // c5's base is still on storage, which the listing finds.
            assert_eq!(planned(&handle).await, at_c5);
        }

        /// Remove every metadata file slice older than the two newest base
        /// versions of its file group, as the metadata table's cleaner does
        /// with `KEEP_LATEST_FILE_VERSIONS` and two versions.
        fn clean_metadata_keeping_two_versions(root: &std::path::Path) {
            use crate::file_group::base_file::BaseFile;
            use crate::file_group::log_file::LogFile;
            use std::str::FromStr;
            let mdt = root.join(".hoodie/metadata");
            for entry in std::fs::read_dir(&mdt).unwrap() {
                let dir = entry.unwrap().path();
                if !dir.is_dir() || dir.file_name().unwrap().to_string_lossy().starts_with('.') {
                    continue;
                }
                let mut groups: HashMap<String, Vec<(String, String, bool)>> = HashMap::new();
                for file in std::fs::read_dir(&dir).unwrap() {
                    let name = file.unwrap().file_name().to_string_lossy().to_string();
                    if let Ok(base) = BaseFile::from_str(&name) {
                        groups.entry(base.file_id).or_default().push((
                            name,
                            base.commit_timestamp,
                            true,
                        ));
                    } else if let Ok(log) = LogFile::from_str(&name) {
                        groups
                            .entry(log.file_id)
                            .or_default()
                            .push((name, log.timestamp, false));
                    }
                }
                for files in groups.values() {
                    let mut bases: Vec<&String> =
                        files.iter().filter(|f| f.2).map(|f| &f.1).collect();
                    bases.sort();
                    bases.dedup();
                    if bases.len() < 3 {
                        continue;
                    }
                    let keep_from = bases[bases.len() - 2].clone();
                    for (name, timestamp, _) in files {
                        if *timestamp < keep_from {
                            std::fs::remove_file(dir.join(name)).unwrap();
                        }
                    }
                }
            }
        }

        /// A metadata table whose cleaner removed the slice a stale handle
        /// would read cannot reproduce the snapshot, and the handle lists
        /// storage instead.
        #[tokio::test]
        async fn a_stale_handle_refuses_a_metadata_table_cleaned_past_its_snapshot() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().to_str().unwrap();
            table_at_c5(dir.path()).await;
            let handle = Table::new(path).await.unwrap();
            let at_c5 = planned(&Table::new(path).await.unwrap()).await;

            let mut writer = Table::new_with_options(path, [(MAX_DELTA_COMMITS, "1")])
                .await
                .unwrap();
            // Each write compacts the metadata table after it commits.
            writer.upsert([rows("sf", &[("a", 2)])]).await.unwrap();
            writer.append([rows("ny", &[("c", 3)])]).await.unwrap();
            clean_metadata_keeping_two_versions(dir.path());

            let err = handle
                .read_metadata_table_files_partition_arrow(&[])
                .await
                .expect_err("the metadata table no longer holds the snapshot's slice");
            assert!(
                matches!(err, CoreError::MetadataTable(_))
                    && err.to_string().contains("no longer holds"),
                "got {err}"
            );
            assert_eq!(planned(&handle).await, at_c5);
            let rows: usize = handle
                .read(&ReadOptions::new())
                .await
                .unwrap()
                .iter()
                .map(|batch| batch.num_rows())
                .sum();
            assert_eq!(rows, 2, "the snapshot's two rows, read by storage listing");
        }

        /// A metadata table whose cleaner removes the slice a stale handle
        /// pinned after the pin fails the pinned read, and planning lists
        /// storage instead.
        #[tokio::test]
        async fn a_stale_handle_lists_storage_when_its_pinned_slice_is_cleaned_later() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().to_str().unwrap();
            table_at_c5(dir.path()).await;
            let handle = Table::new(path).await.unwrap();
            let at_c5 = planned(&Table::new(path).await.unwrap()).await;

            let mut writer = Table::new_with_options(path, [(MAX_DELTA_COMMITS, "1")])
                .await
                .unwrap();
            // Each write compacts the metadata table after it commits.
            writer.upsert([rows("sf", &[("a", 2)])]).await.unwrap();
            writer.append([rows("ny", &[("c", 3)])]).await.unwrap();
            assert_eq!(planned(&handle).await, at_c5);
            let mdt = handle.get_or_init_metadata_table().await.unwrap();
            assert!(handle.metadata_table_past_snapshot(mdt));

            clean_metadata_keeping_two_versions(dir.path());
            handle
                .read_metadata_table_files_partition_arrow(&[])
                .await
                .expect_err("the pinned slice is gone");
            assert_eq!(planned(&handle).await, at_c5);
            let rows: usize = handle
                .read(&ReadOptions::new())
                .await
                .unwrap()
                .iter()
                .map(|batch| batch.num_rows())
                .sum();
            assert_eq!(rows, 2, "the snapshot's two rows, read by storage listing");
        }

        /// A handle opened while a write is in flight keeps a metadata
        /// compaction that ran before the write's metadata commit completed:
        /// the compaction could not have folded that commit in.
        #[tokio::test]
        async fn a_compaction_before_an_unseen_commit_completed_is_kept() {
            let dir = tempfile::tempdir().unwrap();
            table_at_c5(dir.path()).await;
            let in_flight = crate::write::append::generate_instant_time().await;
            let mut writer =
                Table::new_with_options(dir.path().to_str().unwrap(), [(MAX_DELTA_COMMITS, "1")])
                    .await
                    .unwrap();
            // Compacts the metadata table after the commit completes.
            writer.upsert([rows("sf", &[("a", 2)])]).await.unwrap();
            put_timeline_file(&writer, &format!("{in_flight}.commit.requested")).await;
            put_timeline_file(&writer, &format!("{in_flight}.commit.inflight")).await;
            let partition = planned(&writer).await[0].0.clone();
            metadata_delta(
                &writer,
                &in_flight,
                &[(
                    partition,
                    "unseen-file_0-0-0_0.parquet".to_string(),
                    1,
                    false,
                )],
            )
            .await;

            let handle = Table::new(dir.path().to_str().unwrap()).await.unwrap();
            let mdt = handle.get_or_init_metadata_table().await.unwrap();
            let unpinned = handle.new_metadata_table().await.unwrap();
            let compactions = |mdt: &Table| -> Vec<String> {
                mdt.timeline
                    .completed_commits
                    .iter()
                    .filter(|instant| instant.action == Action::Commit)
                    .map(|instant| instant.timestamp.clone())
                    .collect()
            };
            assert!(
                !compactions(&unpinned).is_empty(),
                "the writer must compact"
            );
            assert_eq!(compactions(mdt), compactions(&unpinned));
        }

        /// A handle opened after the same history plans the latest snapshot
        /// from the metadata table, including after a metadata compaction that
        /// sorts above every data instant it listed.
        #[tokio::test]
        async fn a_handle_opened_after_the_writes_reads_the_latest_metadata() {
            let dir = tempfile::tempdir().unwrap();
            table_at_c5(dir.path()).await;
            let c5 = planned(&Table::new(dir.path().to_str().unwrap()).await.unwrap()).await;
            let (partition, c5_base) = c5[0].clone();
            let mut writer =
                Table::new_with_options(dir.path().to_str().unwrap(), [(MAX_DELTA_COMMITS, "1")])
                    .await
                    .unwrap();
            writer.upsert([rows("sf", &[("a", 2)])]).await.unwrap();
            clean(&writer, &partition, &c5_base).await;
            writer.append([rows("ny", &[("c", 3)])]).await.unwrap();

            let handle = Table::new(dir.path().to_str().unwrap()).await.unwrap();
            let mdt = handle.get_or_init_metadata_table().await.unwrap();
            assert_eq!(
                mdt.timeline.completed_commits.len(),
                handle
                    .new_metadata_table()
                    .await
                    .unwrap()
                    .timeline
                    .completed_commits
                    .len(),
                "nothing in the metadata table is past this snapshot"
            );
            assert_eq!(
                handle.valid_instant_timestamps(mdt).await.unwrap(),
                handle.latest_valid_instant_timestamps(mdt).await.unwrap(),
                "a snapshot that is the latest admits what a write would"
            );
            let latest = planned(&handle).await;
            assert_eq!(latest.len(), 2, "{latest:?}");
            let file_id = c5_base.split('_').next().unwrap();
            assert!(
                latest.iter().any(|(p, base)| *p == partition
                    && *base != c5_base
                    && base.starts_with(file_id)),
                "the file group's c6 slice must be planned: {latest:?}"
            );
        }

        /// A data instant pending when the handle opened stays out of the
        /// valid-instant set after it completes, under a commit action and
        /// under one the timeline does not load by default.
        #[tokio::test]
        async fn a_data_instant_pending_at_open_is_not_admitted_once_it_completes() {
            for action in ["commit", "clean"] {
                let dir = tempfile::tempdir().unwrap();
                let writer = table_at_c5(dir.path()).await;
                let pending = crate::write::append::generate_instant_time().await;
                put_timeline_file(&writer, &format!("{pending}.{action}.requested")).await;
                put_timeline_file(&writer, &format!("{pending}.{action}.inflight")).await;

                let handle = Table::new(dir.path().to_str().unwrap()).await.unwrap();
                let partition = planned(&writer).await[0].0.clone();

                let unseen = "unseen-file_0-0-0_0.parquet";
                metadata_delta(
                    &writer,
                    &pending,
                    &[(partition.clone(), unseen.to_string(), 1, false)],
                )
                .await;
                let completion = crate::write::append::generate_instant_time().await;
                put_timeline_file(&writer, &format!("{pending}_{completion}.{action}")).await;

                let mdt = handle.get_or_init_metadata_table().await.unwrap();
                let valid = handle.valid_instant_timestamps(mdt).await.unwrap();
                assert!(
                    !valid.contains(&pending),
                    "[{action}] {pending} was pending at open and must stay out"
                );
                let partition_pruner = PartitionPruner::new(
                    &[],
                    &handle.get_partition_schema().await.unwrap(),
                    handle.hudi_configs.as_ref(),
                )
                .unwrap();
                let records = mdt
                    .fetch_files_partition_records(&partition_pruner, &valid)
                    .await
                    .unwrap();
                let listed: Vec<(&str, u64)> =
                    records[&partition].active_files_with_sizes().collect();
                assert!(
                    listed.iter().all(|(name, _)| *name != unseen),
                    "[{action}] the handle listed {unseen} from an instant it saw pending: {listed:?}"
                );
            }
        }
    }
}
