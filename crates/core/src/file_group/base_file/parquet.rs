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
//! Parquet implementation of [`BaseFileReader`].

use std::collections::BTreeSet;
use std::ops::Range;
use std::sync::{Arc, OnceLock};

use futures::StreamExt;
use futures::future::BoxFuture;
use object_store::ObjectStoreExt;
use object_store::path::Path as ObjPath;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
// `ParquetObjectReader` is deprecated as of parquet 59 in favour of hand-rolling
// an `AsyncFileReader` over `object_store`. Replacing it is its own change
// (apache/hudi-rs#843): it has to carry the counting wrapper and the metadata
// normalization below, so the deprecated reader is kept, allowed at each use
// site rather than for the whole file.
#[allow(deprecated)]
use parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};
use parquet::arrow::{ParquetRecordBatchStreamBuilder, RowNumber, parquet_to_arrow_schema};
use parquet::file::metadata::{ColumnChunkMetaData, ParquetMetaData};

use super::reader::{BaseFileReadOptions, BaseFileReader, BaseFileStream};
use crate::schema::parquet_list_norm::normalize_parquet_metadata;
use crate::statistics::StatisticsContainer;
use crate::storage::ReadVolume;
use crate::storage::Storage;
use crate::storage::error::{Result, StorageError};
use crate::storage::file_metadata::FileMetadata;
use crate::storage::util::join_url_segments;
use crate::util::concurrency::AbortOnDrop;

/// `AsyncFileReader` wrapper that accumulates read volume into a shared
/// [`ReadVolume`].
///
/// Delegates everything and changes no behaviour. Counting happens here rather
/// than inside the object store because the store is shared between readers and
/// therefore cannot carry per-read counters.
#[derive(Clone)]
struct CountingReader<R: AsyncFileReader> {
    inner: R,
    volume: Arc<ReadVolume>,
}

impl<R: AsyncFileReader> AsyncFileReader for CountingReader<R> {
    fn get_bytes(
        &mut self,
        range: std::ops::Range<u64>,
    ) -> BoxFuture<'_, parquet::errors::Result<bytes::Bytes>> {
        let volume = self.volume.clone();
        let fut = self.inner.get_bytes(range);
        Box::pin(async move {
            let bytes = fut.await?;
            volume.add_bytes(bytes.len() as u64);
            Ok(bytes)
        })
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<std::ops::Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<bytes::Bytes>>> {
        let volume = self.volume.clone();
        let fut = self.inner.get_byte_ranges(ranges);
        Box::pin(async move {
            let chunks = fut.await?;
            // One call, many ranges: count the call once and every byte it
            // returned, so `io_calls` stays a count of round trips.
            let total: u64 = chunks.iter().map(|b| b.len() as u64).sum();
            volume.add_bytes(total);
            Ok(chunks)
        })
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        self.inner.get_metadata(options)
    }
}

/// What the prefetch needs to know about a read, set once its options are
/// applied; until then no prefetch is issued.
struct ReadPlan {
    /// The row groups the stream will read, in its read order.
    row_groups: Vec<usize>,
    /// Whether a row filter is installed.
    filtered: bool,
}

type ReadPlanCell = Arc<OnceLock<ReadPlan>>;

/// `AsyncFileReader` wrapper that fetches the next row group's column chunks
/// while the current one decodes.
///
/// The parquet stream asks for a row group's chunks only once it is done with
/// the previous one, so on its own it never overlaps a fetch with decoding.
/// Here, each request is mapped to the row group and leaf columns it falls in,
/// and the full chunks of the leaf columns requested so far are fetched in a
/// spawned task for the next row group in the [`ReadPlan`]. Learning the
/// columns from the requests themselves covers both the row-filter and the
/// projection columns without having to resolve either mask.
///
/// With a row filter, only the columns of each row group's first request are
/// learned: that is the filter stage, which every group fetches. The stream
/// fetches the later stages only for groups where some row survives, so
/// prefetching them would read data the stream skips.
///
/// A request is served from a prefetched chunk only when the chunk holds the
/// whole range, so sub-chunk (page-level) requests stay correct. A prefetch
/// that fails is dropped and the range is fetched normally, so its error can
/// only surface through the real fetch. Buffers are kept for the current and
/// the next row group only, and dropping the reader aborts any task still in
/// flight.
struct PrefetchingReader<R> {
    inner: R,
    metadata: Arc<ParquetMetaData>,
    plan: ReadPlanCell,
    /// Leaf columns to prefetch, learned from the stream's requests.
    columns: BTreeSet<usize>,
    /// Position in the read order of the row group the stream is on, once it
    /// has asked for one.
    current: Option<usize>,
    prefetched: Vec<PrefetchPart>,
}

/// One background fetch: full column chunks of one row group.
struct PrefetchPart {
    position: usize,
    /// Leaf column and its chunk's byte range, in fetch order.
    chunks: Vec<(usize, Range<u64>)>,
    state: PrefetchState,
}

enum PrefetchState {
    InFlight(AbortOnDrop<parquet::errors::Result<Vec<bytes::Bytes>>>),
    Ready(Vec<bytes::Bytes>),
    Failed,
}

/// The byte range of a column chunk, or `None` for metadata that does not
/// describe one.
fn chunk_range(column: &ColumnChunkMetaData) -> Option<Range<u64>> {
    let start = column
        .dictionary_page_offset()
        .unwrap_or_else(|| column.data_page_offset());
    let start = u64::try_from(start).ok()?;
    let len = u64::try_from(column.compressed_size()).ok()?;
    (len > 0).then(|| start..start + len)
}

fn contains(outer: &Range<u64>, inner: &Range<u64>) -> bool {
    outer.start <= inner.start && inner.end <= outer.end
}

impl PrefetchPart {
    fn covers(&self, range: &Range<u64>) -> bool {
        self.chunks.iter().any(|(_, chunk)| contains(chunk, range))
    }

    async fn resolve(&mut self) {
        if let PrefetchState::InFlight(task) = &mut self.state {
            let result = task.await;
            // A short or mis-sized answer is treated as a failure, so that the
            // slicing below can never go out of bounds.
            self.state = match result {
                Ok(Ok(buffers))
                    if buffers.len() == self.chunks.len()
                        && self
                            .chunks
                            .iter()
                            .zip(&buffers)
                            .all(|((_, r), b)| b.len() as u64 == r.end - r.start) =>
                {
                    PrefetchState::Ready(buffers)
                }
                _ => PrefetchState::Failed,
            };
        }
    }

    fn slice(&self, range: &Range<u64>) -> Option<bytes::Bytes> {
        let PrefetchState::Ready(buffers) = &self.state else {
            return None;
        };
        self.chunks
            .iter()
            .zip(buffers)
            .find(|((_, chunk), _)| contains(chunk, range))
            .map(|((_, chunk), buffer)| {
                buffer
                    .slice((range.start - chunk.start) as usize..(range.end - chunk.start) as usize)
            })
    }
}

impl<R> PrefetchingReader<R>
where
    R: AsyncFileReader + Clone + 'static,
{
    fn new(inner: R, metadata: Arc<ParquetMetaData>, plan: ReadPlanCell) -> Self {
        Self {
            inner,
            metadata,
            plan,
            columns: BTreeSet::new(),
            current: None,
            prefetched: Vec::new(),
        }
    }

    /// Position in `order` and leaf column of the chunk holding `range`,
    /// searched from position `from` onward first since the stream moves
    /// forward.
    fn locate(&self, order: &[usize], from: usize, range: &Range<u64>) -> Option<(usize, usize)> {
        let from = from.min(order.len());
        (from..order.len()).chain(0..from).find_map(|position| {
            let row_group = self.metadata.row_groups().get(order[position])?;
            row_group
                .columns()
                .iter()
                .position(|c| chunk_range(c).is_some_and(|chunk| contains(&chunk, range)))
                .map(|column| (position, column))
        })
    }

    /// Track where the stream is and which columns it reads, drop buffers it
    /// has moved past, and start fetching what the next row group will need.
    fn observe(&mut self, ranges: &[Range<u64>]) {
        let plan = self.plan.clone();
        let Some(ReadPlan {
            row_groups: order,
            filtered,
        }) = plan.get()
        else {
            return;
        };
        let mut position = None;
        let mut requested = BTreeSet::new();
        for range in ranges {
            let from = position.or(self.current).unwrap_or(0);
            if let Some((p, column)) = self.locate(order, from, range) {
                position.get_or_insert(p);
                requested.insert(column);
            }
        }
        let Some(position) = position else {
            return;
        };
        if !filtered || self.current != Some(position) {
            self.columns.extend(requested);
        }
        self.current = Some(position);
        self.prefetched.retain(|part| part.position >= position);

        let next = position + 1;
        let Some(row_group) = order
            .get(next)
            .and_then(|&rg| self.metadata.row_groups().get(rg))
        else {
            return;
        };
        let covered: BTreeSet<usize> = self
            .prefetched
            .iter()
            .filter(|part| part.position == next)
            .flat_map(|part| part.chunks.iter().map(|(column, _)| *column))
            .collect();
        let chunks: Vec<(usize, Range<u64>)> = self
            .columns
            .difference(&covered)
            .filter_map(|&column| Some((column, chunk_range(row_group.columns().get(column)?)?)))
            .collect();
        if chunks.is_empty() {
            return;
        }
        // Spawned rather than stored so that it makes progress while the stream
        // decodes; on a current-thread runtime that is only while the consumer
        // yields, which limits the overlap there. Without a runtime the read
        // proceeds unprefetched.
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let mut reader = self.inner.clone();
        let ranges = chunks.iter().map(|(_, r)| r.clone()).collect();
        let task = runtime.spawn(async move { reader.get_byte_ranges(ranges).await });
        self.prefetched.push(PrefetchPart {
            position: next,
            chunks,
            state: PrefetchState::InFlight(AbortOnDrop(task)),
        });
    }

    async fn read_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> parquet::errors::Result<Vec<bytes::Bytes>> {
        self.observe(&ranges);

        let mut out: Vec<Option<bytes::Bytes>> = vec![None; ranges.len()];
        for part in &mut self.prefetched {
            if ranges.iter().any(|r| part.covers(r)) {
                part.resolve().await;
                for (slot, range) in out.iter_mut().zip(&ranges) {
                    if slot.is_none() {
                        *slot = part.slice(range);
                    }
                }
            }
        }

        let misses: Vec<Range<u64>> = out
            .iter()
            .zip(&ranges)
            .filter(|(slot, _)| slot.is_none())
            .map(|(_, r)| r.clone())
            .collect();
        if misses.is_empty() {
            return Ok(out.into_iter().flatten().collect());
        }
        let mut fetched = self.inner.get_byte_ranges(misses).await?.into_iter();
        out.into_iter()
            .map(|slot| {
                slot.or_else(|| fetched.next()).ok_or_else(|| {
                    parquet::errors::ParquetError::General(
                        "reader returned fewer byte ranges than requested".to_string(),
                    )
                })
            })
            .collect()
    }
}

impl<R> AsyncFileReader for PrefetchingReader<R>
where
    R: AsyncFileReader + Clone + 'static,
{
    fn get_bytes(
        &mut self,
        range: Range<u64>,
    ) -> BoxFuture<'_, parquet::errors::Result<bytes::Bytes>> {
        Box::pin(async move {
            self.read_ranges(vec![range]).await?.pop().ok_or_else(|| {
                parquet::errors::ParquetError::General("reader returned no bytes".to_string())
            })
        })
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<bytes::Bytes>>> {
        Box::pin(self.read_ranges(ranges))
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        self.inner.get_metadata(options)
    }
}

/// The builder every read here is driven from: a parquet object reader with the
/// read-volume counters and the row-group prefetch wrapped around it.
#[allow(deprecated)]
type CountedBuilder =
    ParquetRecordBatchStreamBuilder<PrefetchingReader<CountingReader<ParquetObjectReader>>>;

/// Parquet implementation of [`BaseFileReader`].
///
/// Reads Parquet files directly via `object_store` and the `parquet` crate.
pub struct ParquetBaseFileReader {
    storage: Arc<Storage>,
}

impl ParquetBaseFileReader {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self { storage }
    }

    async fn object_path_and_size(&self, relative_path: &str) -> Result<(ObjPath, u64)> {
        let obj_url = join_url_segments(&self.storage.base_url, &[relative_path])?;
        let obj_path = ObjPath::from_url_path(obj_url.path())?;
        let meta = self.storage.object_store.head(&obj_path).await?;
        Ok((obj_path, meta.size))
    }

    /// Reader options carrying the caller's request for a row-position column.
    ///
    /// The column is a parquet-rs *virtual* column: it is not in the file, so
    /// parquet-rs materializes it from each row's physical index while decoding.
    /// That is why it stays correct under row-group and page pruning, where
    /// counting emitted rows would not.
    fn arrow_reader_options(row_index_column: Option<&str>) -> Result<ArrowReaderOptions> {
        let options = ArrowReaderOptions::new();
        let Some(name) = row_index_column else {
            return Ok(options);
        };
        let row_number = Arc::new(
            arrow_schema::Field::new(name, arrow_schema::DataType::Int64, false)
                .with_extension_type(RowNumber),
        );
        Ok(options.with_virtual_columns(vec![row_number])?)
    }

    async fn open_builder_with_size(
        &self,
        obj_path: ObjPath,
        file_size: u64,
        row_index_column: Option<&str>,
    ) -> Result<(CountedBuilder, ReadPlanCell)> {
        #[allow(deprecated)]
        let mut reader = ParquetObjectReader::new(self.storage.object_store.clone(), obj_path)
            .with_file_size(file_size);

        // A parquet-avro writer with `write-old-list-structure=true` encodes an
        // `array<map>` as a legacy 2-level list whose element is a REPEATED map
        // group, which parquet-rs rejects outright when it builds the Arrow
        // schema — "Map cannot be repeated". The footer parse itself does not,
        // so the schema is rewritten in between and every reader built from this
        // metadata accepts the file. The column chunks are untouched.
        let raw = reader.get_metadata(None).await?;
        let normalized = normalize_parquet_metadata(raw);
        let arrow_metadata = ArrowReaderMetadata::try_new(
            normalized,
            Self::arrow_reader_options(row_index_column)?,
        )?;

        let reader = CountingReader {
            inner: reader,
            volume: self.storage.read_volume.clone(),
        };
        let plan = ReadPlanCell::default();
        let reader =
            PrefetchingReader::new(reader, arrow_metadata.metadata().clone(), plan.clone());
        Ok((
            ParquetRecordBatchStreamBuilder::new_with_metadata(reader, arrow_metadata),
            plan,
        ))
    }

    async fn open_builder(
        &self,
        relative_path: &str,
        row_index_column: Option<&str>,
    ) -> Result<(CountedBuilder, ReadPlanCell)> {
        let (obj_path, file_size) = self.object_path_and_size(relative_path).await?;
        self.open_builder_with_size(obj_path, file_size, row_index_column)
            .await
    }

    fn apply_options(
        &self,
        mut builder: CountedBuilder,
        options: &BaseFileReadOptions,
        plan: &ReadPlanCell,
    ) -> Result<CountedBuilder> {
        // What the file holds, taken from the footer that has already been
        // fetched, so it costs no extra IO. Recorded here rather than at every
        // builder open so it counts once per read OF THE DATA: opening the same
        // file again for its schema alone would otherwise inflate the
        // denominator that `row_groups_read` and `rows_out` are read against.
        let metadata = builder.metadata();
        self.storage.read_volume.record_file_shape(
            metadata.num_row_groups() as u64,
            metadata.file_metadata().num_rows().max(0) as u64,
        );

        if let Some(batch_size) = options.batch_size {
            builder = builder.with_batch_size(batch_size);
        }

        // Handle projection: convert column names to indices using builder's schema.
        if let Some(ref column_names) = options.projection {
            let arrow_schema = builder.schema();
            let projection: Vec<usize> = column_names
                .iter()
                // The row-position column is virtual — it has no parquet column
                // to select, and it is emitted whatever the projection says. A
                // caller that names it alongside real columns gets it once, not
                // an out-of-range projection index.
                .filter(|name| Some(name.as_str()) != options.row_index_column.as_deref())
                .map(|name| {
                    arrow_schema.index_of(name).map_err(|_| {
                        let available = arrow_schema
                            .fields()
                            .iter()
                            .map(|f| f.name().as_str())
                            .collect::<Vec<_>>()
                            .join(", ");
                        StorageError::InvalidColumn(format!(
                            "Column '{name}' not found in parquet file schema. Available columns: [{available}]"
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            let projection_mask = parquet::arrow::ProjectionMask::roots(
                builder.parquet_schema(),
                projection.iter().copied(),
            );
            builder = builder.with_projection(projection_mask);
        }

        // Prune row groups from footer statistics, BEFORE the row filter is
        // installed: a group excluded here is never fetched, so the filter only
        // ever sees groups that survived.
        let volume = &self.storage.read_volume;
        let total_row_groups = builder.metadata().num_row_groups();
        let mut row_groups: Vec<usize> = (0..total_row_groups).collect();
        if let Some(keep) = options.row_group_selector.as_ref().and_then(|select| {
            // Count the CALL, not just a successful prune, so "ran and found
            // nothing" stays distinguishable from "never installed".
            volume.record_selector_call();
            select(builder.metadata())
        }) {
            // A selector that names a row group the file does not have is a bug
            // in the selector. parquet-rs does not validate the indices; the
            // read panics when the stream reaches the bad index, release builds
            // included. The `debug_assert!` surfaces the bug at this choke
            // point with a clear message in tests; no release check is added
            // because the failure is already loud, a panic rather than lost
            // rows.
            debug_assert!(
                keep.iter().all(|&i| i < total_row_groups),
                "selector returned an out-of-range row-group index"
            );
            row_groups = keep.clone();
            builder = builder.with_row_groups(keep);
        }
        volume.add_row_groups_read(row_groups.len() as u64);

        // Built here rather than by the caller because the predicate has to be
        // resolved against the file's own schema, which only exists once the
        // footer is open. A builder that returns `None` — typically because the
        // file does not have a column the predicate names — means no filter,
        // which reads every row rather than guessing at the predicate.
        let row_filter = options
            .row_filter
            .as_ref()
            .and_then(|build| build(builder.parquet_schema(), builder.schema().as_ref()));
        let _ = plan.set(ReadPlan {
            row_groups,
            filtered: row_filter.is_some(),
        });
        if let Some(row_filter) = row_filter {
            builder = builder.with_row_filter(row_filter);
        }

        Ok(builder)
    }

    /// Put the virtual row-position column back into a projected stream's
    /// reported schema.
    ///
    /// parquet-rs 57.3.0 builds that schema by keeping the leaves the projection
    /// mask selects, and the mask only has entries for the file's own leaves —
    /// so a virtual column, whose leaf index sits past the end of the mask, is
    /// cut. The batches still carry it (the array-reader builder returns a
    /// virtual reader regardless of the mask), leaving the advertised schema one
    /// column short of the data. `full_schema` is the builder's own schema,
    /// which is where the field with its extension metadata comes from — a
    /// hand-built replacement would compare unequal to the batches'.
    fn schema_with_row_index(
        stream_schema: &arrow_schema::SchemaRef,
        full_schema: &arrow_schema::SchemaRef,
        row_index_column: Option<&str>,
    ) -> Result<arrow_schema::SchemaRef> {
        let Some(name) = row_index_column else {
            return Ok(stream_schema.clone());
        };
        if stream_schema.column_with_name(name).is_some() {
            return Ok(stream_schema.clone());
        }
        let field = full_schema.field_with_name(name).map_err(|_| {
            StorageError::InvalidColumn(format!(
                "Row-position column '{name}' was requested but the parquet reader did not \
                 produce it"
            ))
        })?;
        let mut fields = stream_schema.fields().to_vec();
        fields.push(Arc::new(field.clone()));
        Ok(Arc::new(arrow_schema::Schema::new(fields)))
    }

    /// Read the raw Parquet footer metadata.
    ///
    /// Exposed for callers that need format-specific details such as row group
    /// compressed sizes for statistics estimation.
    pub async fn get_parquet_metadata(&self, relative_path: &str) -> Result<ParquetMetaData> {
        let (builder, _) = self.open_builder(relative_path, None).await?;
        Ok(builder.metadata().as_ref().clone())
    }

    /// Get the Arrow schema from a Parquet file's footer.
    pub async fn get_schema(&self, relative_path: &str) -> Result<arrow_schema::Schema> {
        let (builder, _) = self.open_builder(relative_path, None).await?;
        let parquet_meta = builder.metadata();
        Ok(parquet_to_arrow_schema(
            parquet_meta.file_metadata().schema_descr(),
            None,
        )?)
    }
}

impl BaseFileReader for ParquetBaseFileReader {
    fn read_stream<'a>(
        &'a self,
        relative_path: &'a str,
        options: BaseFileReadOptions,
    ) -> BoxFuture<'a, Result<BaseFileStream>> {
        Box::pin(async move {
            let (builder, plan) = self
                .open_builder(relative_path, options.row_index_column.as_deref())
                .await?;
            let builder = self.apply_options(builder, &options, &plan)?;
            let full_schema = builder.schema().clone();
            let stream = builder.build()?;
            let schema = Self::schema_with_row_index(
                stream.schema(),
                &full_schema,
                options.row_index_column.as_deref(),
            )?;
            // Rows the stream actually yields — after any row filter. Against
            // `file_rows` this is the read's selectivity; against `bytes_read`,
            // what that selectivity cost.
            let volume = self.storage.read_volume.clone();
            let mapped_stream = stream
                .map(move |result| {
                    let batch = result.map_err(StorageError::from)?;
                    volume.add_rows_out(batch.num_rows() as u64);
                    Ok(batch)
                })
                .boxed();

            Ok(BaseFileStream::new(schema, mapped_stream))
        })
    }

    /// Answered from the footer alone: no stream is built, and no read-volume
    /// counter moves for a call that reads no data.
    fn read_schema<'a>(
        &'a self,
        relative_path: &'a str,
    ) -> BoxFuture<'a, Result<arrow_schema::SchemaRef>> {
        Box::pin(async move { Ok(Arc::new(self.get_schema(relative_path).await?)) })
    }

    fn get_metadata_and_stats<'a>(
        &'a self,
        relative_path: &'a str,
        table_schema: &'a arrow_schema::Schema,
    ) -> BoxFuture<'a, Result<(FileMetadata, StatisticsContainer)>> {
        Box::pin(async move {
            let (obj_path, file_size) = self.object_path_and_size(relative_path).await?;
            let (builder, _) = self
                .open_builder_with_size(obj_path, file_size, None)
                .await?;
            let parquet_meta = builder.metadata().as_ref();

            let name = std::path::Path::new(relative_path)
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or(relative_path)
                .to_string();

            let num_records = parquet_meta.file_metadata().num_rows().max(0);
            let byte_size: i64 = parquet_meta
                .row_groups()
                .iter()
                .map(|rg| rg.total_byte_size())
                .sum::<i64>()
                .max(0);

            let file_metadata = FileMetadata {
                name,
                size: file_size,
                byte_size,
                num_records,
            };

            let col_stats = StatisticsContainer::from_parquet_metadata(parquet_meta, table_schema);

            Ok((file_metadata, col_stats))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::canonicalize;
    use std::path::Path;
    use url::Url;

    fn test_storage() -> Arc<Storage> {
        let base_url =
            Url::from_directory_path(canonicalize(Path::new("tests/data")).unwrap()).unwrap();
        Storage::new_with_base_url(base_url).unwrap()
    }

    /// A base file written by parquet-avro with `write-old-list-structure=true`
    /// must read. Its `array<map>` column is encoded as a legacy 2-level list
    /// whose element is a REPEATED map group, which the parquet→arrow builder
    /// rejects with "Map cannot be repeated" unless the schema is normalized
    /// first. Without that step this read fails outright rather than returning
    /// wrong data, so the assertion is that it returns rows at all.
    #[tokio::test]
    async fn test_read_data_accepts_a_legacy_two_level_list() {
        let reader = ParquetBaseFileReader::new(test_storage());
        let batch = reader
            .read_data(
                "i3/legacy_2level_repeated_map.parquet",
                BaseFileReadOptions::default(),
            )
            .await
            .expect("a legacy 2-level list encoding must be readable");

        assert!(batch.num_rows() > 0);
        let field = batch
            .schema()
            .field_with_name("obj_ids")
            .expect("the array<map> column")
            .clone();
        assert!(
            matches!(field.data_type(), arrow_schema::DataType::List(_)),
            "obj_ids should surface as a List, got {:?}",
            field.data_type()
        );
    }

    #[tokio::test]
    async fn test_read_data_returns_all_rows() {
        let reader = ParquetBaseFileReader::new(test_storage());
        let batch = reader
            .read_data("a.parquet", BaseFileReadOptions::default())
            .await
            .unwrap();
        assert_eq!(batch.num_rows(), 5);
        assert!(batch.num_columns() > 1);
    }

    #[tokio::test]
    async fn test_read_data_with_projection() {
        let reader = ParquetBaseFileReader::new(test_storage());

        let full = reader
            .read_data("a.parquet", BaseFileReadOptions::default())
            .await
            .unwrap();

        let first_col = full.schema().field(0).name().clone();
        let opts = BaseFileReadOptions::default().with_projection([&first_col]);
        let projected = reader.read_data("a.parquet", opts).await.unwrap();

        assert_eq!(projected.num_columns(), 1);
        assert_eq!(projected.schema().field(0).name(), &first_col);
        assert_eq!(projected.num_rows(), full.num_rows());
    }

    /// The filter reaches the reader and prunes. Asserted with an always-false
    /// predicate so the result is unambiguous whatever the fixture holds — a
    /// filter that was silently dropped would return every row instead.
    #[tokio::test]
    async fn test_read_data_applies_the_row_filter() {
        use arrow_array::BooleanArray;
        use parquet::arrow::ProjectionMask;
        use parquet::arrow::arrow_reader::{ArrowPredicateFn, RowFilter};

        let reader = ParquetBaseFileReader::new(test_storage());
        let opts = BaseFileReadOptions::default().with_row_filter(Arc::new(|descr, _| {
            let mask = ProjectionMask::roots(descr, [0]);
            Some(RowFilter::new(vec![Box::new(ArrowPredicateFn::new(
                mask,
                |batch| Ok(BooleanArray::from(vec![false; batch.num_rows()])),
            ))]))
        }));

        let batch = reader.read_data("a.parquet", opts).await.unwrap();
        assert_eq!(batch.num_rows(), 0, "the predicate rejected every row");
    }

    /// The volume counters describe a real read: what the file held, what was
    /// fetched to read it, and what came back.
    #[tokio::test]
    async fn read_volume_counts_what_the_read_actually_moved() {
        use std::sync::atomic::Ordering::Relaxed;

        let storage = test_storage();
        let volume = storage.read_volume();
        let reader = ParquetBaseFileReader::new(storage);

        let batch = reader
            .read_data("a.parquet", BaseFileReadOptions::default())
            .await
            .unwrap();

        assert_eq!(batch.num_rows(), 5);
        assert_eq!(volume.file_rows.load(Relaxed), 5, "footer row count");
        assert_eq!(volume.rows_out.load(Relaxed), 5, "every row was yielded");
        let file_row_groups = volume.file_row_groups.load(Relaxed);
        assert!(file_row_groups >= 1, "the file has at least one row group");
        assert_eq!(
            volume.row_groups_read.load(Relaxed),
            file_row_groups,
            "nothing prunes, so every row group is scanned"
        );
        assert!(volume.bytes_read.load(Relaxed) > 0, "bytes were fetched");
        assert!(volume.io_calls.load(Relaxed) > 0, "round trips were made");
    }

    /// Why the counters exist. A `RowFilter` decides per row *after* the
    /// predicate columns are decoded, so a filter that rejects everything still
    /// reads the file: `rows_out` collapses to zero while `bytes_read` does not,
    /// and `row_groups_read` still covers the whole file. A disposition flag
    /// ("was a predicate pushed?") reports the same thing here as it would for a
    /// read that skipped the file entirely.
    #[tokio::test]
    async fn a_row_filter_that_rejects_everything_still_reads_the_file() {
        use arrow_array::BooleanArray;
        use parquet::arrow::ProjectionMask;
        use parquet::arrow::arrow_reader::{ArrowPredicateFn, RowFilter};
        use std::sync::atomic::Ordering::Relaxed;

        let storage = test_storage();
        let volume = storage.read_volume();
        let reader = ParquetBaseFileReader::new(storage);

        let opts = BaseFileReadOptions::default().with_row_filter(Arc::new(|descr, _| {
            let mask = ProjectionMask::roots(descr, [0]);
            Some(RowFilter::new(vec![Box::new(ArrowPredicateFn::new(
                mask,
                |batch| Ok(BooleanArray::from(vec![false; batch.num_rows()])),
            ))]))
        }));

        let batch = reader.read_data("a.parquet", opts).await.unwrap();

        assert_eq!(batch.num_rows(), 0);
        assert_eq!(volume.rows_out.load(Relaxed), 0, "no row survived");
        assert_eq!(volume.file_rows.load(Relaxed), 5, "the file still held 5");
        assert_eq!(
            volume.row_groups_read.load(Relaxed),
            volume.file_row_groups.load(Relaxed),
            "a row filter skips no row group"
        );
        assert!(
            volume.bytes_read.load(Relaxed) > 0,
            "rejecting every row still cost IO"
        );
    }

    /// A builder that declines — typically because the file has none of the
    /// columns the predicate names — reads every row. Returning no rows would
    /// silently drop data on a file the predicate cannot speak about.
    #[tokio::test]
    async fn test_row_filter_builder_that_declines_reads_every_row() {
        let reader = ParquetBaseFileReader::new(test_storage());
        let opts = BaseFileReadOptions::default().with_row_filter(Arc::new(|_, _| None));

        let batch = reader.read_data("a.parquet", opts).await.unwrap();
        assert_eq!(batch.num_rows(), 5);
    }

    /// The row-position column is appended after the file's own columns and
    /// numbers the rows from zero.
    #[tokio::test]
    async fn test_read_data_appends_the_row_index_column() {
        use arrow_array::Int64Array;

        let reader = ParquetBaseFileReader::new(test_storage());
        let opts = BaseFileReadOptions::default().with_row_index_column("_row_pos");
        let batch = reader.read_data("a.parquet", opts).await.unwrap();

        let idx = batch.schema().index_of("_row_pos").unwrap();
        assert_eq!(
            idx,
            batch.num_columns() - 1,
            "the row-position column goes after the file's own columns"
        );
        let positions = batch
            .column(idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("row positions are Int64");
        assert_eq!(positions.values(), &[0, 1, 2, 3, 4]);
    }

    /// Positions are the row's index in the *file*, not in the result. A filter
    /// that skips rows must leave gaps: position-based merge matches log records
    /// against positions recorded over the unfiltered base file, so renumbering
    /// would silently pair a log record with the wrong row.
    #[tokio::test]
    async fn test_row_index_survives_the_row_filter_as_physical_positions() {
        use arrow_array::{BooleanArray, Int64Array};
        use parquet::arrow::ProjectionMask;
        use parquet::arrow::arrow_reader::{ArrowPredicateFn, RowFilter};

        let reader = ParquetBaseFileReader::new(test_storage());
        // Keep only odd rows, decided by position within the predicate's own
        // batch — the fixture's column values are irrelevant to the assertion.
        let opts = BaseFileReadOptions::default()
            .with_row_index_column("_row_pos")
            .with_row_filter(Arc::new(|descr, _| {
                let mask = ProjectionMask::roots(descr, [0]);
                Some(RowFilter::new(vec![Box::new(ArrowPredicateFn::new(
                    mask,
                    |batch| {
                        Ok(BooleanArray::from(
                            (0..batch.num_rows())
                                .map(|i| i % 2 == 1)
                                .collect::<Vec<_>>(),
                        ))
                    },
                ))]))
            }));

        let batch = reader.read_data("a.parquet", opts).await.unwrap();

        let positions = batch
            .column(batch.schema().index_of("_row_pos").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("row positions are Int64");
        assert_eq!(
            positions.values(),
            &[1, 3],
            "surviving rows keep their position in the file, not in the output"
        );
    }

    /// A projection selects the file's own columns; the row-position column is
    /// virtual and comes back regardless. Naming it in the projection is not an
    /// error and does not duplicate it.
    #[tokio::test]
    async fn test_row_index_column_is_returned_alongside_a_projection() {
        let reader = ParquetBaseFileReader::new(test_storage());

        let full = reader
            .read_data("a.parquet", BaseFileReadOptions::default())
            .await
            .unwrap();
        let first_col = full.schema().field(0).name().clone();

        let opts = BaseFileReadOptions::default()
            .with_row_index_column("_row_pos")
            .with_projection([first_col.as_str(), "_row_pos"]);
        let projected = reader.read_data("a.parquet", opts).await.unwrap();

        assert_eq!(
            projected
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>(),
            vec![first_col.as_str(), "_row_pos"]
        );
        assert_eq!(projected.num_rows(), full.num_rows());
    }

    #[tokio::test]
    async fn test_read_stream_matches_read_data() {
        let reader = ParquetBaseFileReader::new(test_storage());

        let eager = reader
            .read_data("a.parquet", BaseFileReadOptions::default())
            .await
            .unwrap();

        let opts = BaseFileReadOptions::default().with_batch_size(2);
        let mut stream = reader.read_stream("a.parquet", opts).await.unwrap();

        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch.unwrap());
        }

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, eager.num_rows());
        assert_eq!(batches[0].schema(), eager.schema());
    }

    #[tokio::test]
    async fn test_get_metadata_and_stats() {
        let reader = ParquetBaseFileReader::new(test_storage());

        let schema = reader.get_schema("a.parquet").await.unwrap();
        let (metadata, stats) = reader
            .get_metadata_and_stats("a.parquet", &schema)
            .await
            .unwrap();

        assert_eq!(metadata.name, "a.parquet");
        assert!(metadata.size > 0);
        assert_eq!(metadata.num_records, 5);
        assert!(!stats.columns.is_empty());
    }

    #[tokio::test]
    async fn test_get_schema() {
        let reader = ParquetBaseFileReader::new(test_storage());
        let schema = reader.get_schema("a.parquet").await.unwrap();
        assert!(!schema.fields().is_empty());
    }

    /// The row-position column is re-added to a projected stream's schema from
    /// the builder's full schema; a full schema that lacks it is a loud error,
    /// not a schema that silently disagrees with the batches.
    #[test]
    fn test_schema_with_row_index_requires_the_column_in_the_full_schema() {
        use arrow_schema::{DataType, Field, Schema};
        let stream_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let full_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("_row_pos", DataType::Int64, false),
        ]));

        // No column requested: the stream schema passes through.
        let out = ParquetBaseFileReader::schema_with_row_index(&stream_schema, &full_schema, None)
            .unwrap();
        assert_eq!(out, stream_schema);

        // Requested and present in the full schema: appended after the
        // stream's own columns.
        let out = ParquetBaseFileReader::schema_with_row_index(
            &stream_schema,
            &full_schema,
            Some("_row_pos"),
        )
        .unwrap();
        assert_eq!(out.fields().len(), 2);
        assert_eq!(out.field(1).name(), "_row_pos");

        // Requested but absent from the full schema: an error naming the column.
        let err = ParquetBaseFileReader::schema_with_row_index(
            &stream_schema,
            &stream_schema,
            Some("_row_pos"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("_row_pos"), "got: {err}");
    }

    #[tokio::test]
    async fn test_get_parquet_metadata() {
        let reader = ParquetBaseFileReader::new(test_storage());
        let meta = reader.get_parquet_metadata("a.parquet").await.unwrap();
        assert_eq!(meta.file_metadata().num_rows(), 5);
        assert!(!meta.row_groups().is_empty());
    }

    /// Parquet has no key seek, so it ignores a key predicate and returns every
    /// row. That is the fallback Java takes when a reader reports no key-predicate
    /// support, and it is asserted rather than argued from the source: a silent
    /// change here would drop rows from every parquet read that carries one.
    #[tokio::test]
    async fn a_key_predicate_is_ignored_rather_than_applied() {
        use super::super::reader::KeyPredicate;
        let reader = ParquetBaseFileReader::new(test_storage());

        let all = reader
            .read_data("a.parquet", BaseFileReadOptions::default())
            .await
            .unwrap();
        assert!(all.num_rows() > 0, "the fixture must have rows");

        let with_predicate = reader
            .read_data(
                "a.parquet",
                BaseFileReadOptions {
                    key_predicate: Some(KeyPredicate::Keys(vec!["no-such-key".to_string()])),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(
            with_predicate.num_rows(),
            all.num_rows(),
            "a format that cannot seek by key must return every row, not fewer"
        );
    }

    mod prefetch {
        use super::*;
        use arrow_array::{BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
        use bytes::Bytes;
        use parquet::arrow::arrow_reader::{
            ArrowPredicateFn, RowFilter, RowSelection, RowSelector,
        };
        use parquet::arrow::{ArrowWriter, ProjectionMask};
        use parquet::file::properties::WriterProperties;
        use std::collections::HashMap;
        use std::sync::Mutex;
        use std::sync::atomic::{AtomicUsize, Ordering};

        const ROWS_PER_GROUP: usize = 10;

        /// `groups` row groups of three columns, in two-row data pages so that a
        /// row selection can skip pages once the page index is loaded.
        fn parquet_bytes(groups: usize) -> Bytes {
            use arrow_schema::{DataType, Field, Schema};
            let n = (groups * ROWS_PER_GROUP) as i64;
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("value", DataType::Float64, false),
            ]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from_iter_values(0..n)),
                    Arc::new(StringArray::from_iter_values(
                        (0..n).map(|i| format!("name{i}")),
                    )),
                    Arc::new(Float64Array::from_iter_values(
                        (0..n).map(|i| i as f64 / 2.0),
                    )),
                ],
            )
            .unwrap();
            let props = WriterProperties::builder()
                .set_max_row_group_row_count(Some(ROWS_PER_GROUP))
                .set_data_page_row_count_limit(2)
                .set_write_batch_size(2)
                .build();
            let mut buf = Vec::new();
            let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props)).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
            Bytes::from(buf)
        }

        fn metadata(data: &Bytes, page_index: bool) -> ArrowReaderMetadata {
            let options = ArrowReaderOptions::new()
                .with_page_index_policy(parquet::file::metadata::PageIndexPolicy::from(page_index));
            ArrowReaderMetadata::load(data, options).unwrap()
        }

        fn chunk(meta: &ParquetMetaData, row_group: usize, column: usize) -> Range<u64> {
            chunk_range(meta.row_group(row_group).column(column)).unwrap()
        }

        #[derive(Debug, Clone)]
        enum Event {
            /// What the parquet stream asked the prefetching reader for.
            Ask(Vec<Range<u64>>),
            /// What reached the underlying file.
            Fetch(Vec<Range<u64>>),
        }

        type Log = Arc<Mutex<Vec<Event>>>;

        fn fetched(log: &Log) -> Vec<Range<u64>> {
            log.lock()
                .unwrap()
                .iter()
                .filter_map(|e| match e {
                    Event::Fetch(r) => Some(r.clone()),
                    Event::Ask(_) => None,
                })
                .flatten()
                .collect()
        }

        /// Decrements the live count when the stalled fetch holding it is dropped.
        struct StallGuard(Arc<AtomicUsize>);

        impl Drop for StallGuard {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }

        /// An in-memory file that logs every fetch, and can fail or stall the
        /// fetches that include a range starting at a given offset.
        #[derive(Clone)]
        struct MemReader {
            data: Bytes,
            log: Log,
            fail_at: Option<u64>,
            failures_left: Arc<AtomicUsize>,
            stall_at: Option<u64>,
            stalled: Arc<AtomicUsize>,
        }

        impl MemReader {
            fn new(data: Bytes) -> Self {
                Self {
                    data,
                    log: Log::default(),
                    fail_at: None,
                    failures_left: Arc::new(AtomicUsize::new(0)),
                    stall_at: None,
                    stalled: Arc::new(AtomicUsize::new(0)),
                }
            }
        }

        impl AsyncFileReader for MemReader {
            fn get_bytes(
                &mut self,
                range: Range<u64>,
            ) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
                Box::pin(async move { Ok(self.get_byte_ranges(vec![range]).await?.remove(0)) })
            }

            fn get_byte_ranges(
                &mut self,
                ranges: Vec<Range<u64>>,
            ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
                self.log.lock().unwrap().push(Event::Fetch(ranges.clone()));
                let hits =
                    |at: Option<u64>| at.is_some_and(|at| ranges.iter().any(|r| r.start == at));
                if hits(self.fail_at)
                    && self
                        .failures_left
                        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                        .is_ok()
                {
                    return Box::pin(futures::future::ready(Err(
                        parquet::errors::ParquetError::General("injected failure".to_string()),
                    )));
                }
                if hits(self.stall_at) {
                    self.stalled.fetch_add(1, Ordering::SeqCst);
                    let guard = StallGuard(self.stalled.clone());
                    return Box::pin(async move {
                        let _guard = guard;
                        futures::future::pending().await
                    });
                }
                let out = ranges
                    .iter()
                    .map(|r| self.data.slice(r.start as usize..r.end as usize))
                    .collect();
                Box::pin(futures::future::ready(Ok(out)))
            }

            fn get_metadata<'a>(
                &'a mut self,
                _options: Option<&'a ArrowReaderOptions>,
            ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
                Box::pin(futures::future::ready(Err(
                    parquet::errors::ParquetError::General("not used".to_string()),
                )))
            }
        }

        /// Logs what the stream asks for before passing it on.
        struct AskLog<R> {
            inner: R,
            log: Log,
        }

        impl<R: AsyncFileReader> AsyncFileReader for AskLog<R> {
            fn get_bytes(
                &mut self,
                range: Range<u64>,
            ) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
                self.log
                    .lock()
                    .unwrap()
                    .push(Event::Ask(vec![range.clone()]));
                self.inner.get_bytes(range)
            }

            fn get_byte_ranges(
                &mut self,
                ranges: Vec<Range<u64>>,
            ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
                self.log.lock().unwrap().push(Event::Ask(ranges.clone()));
                self.inner.get_byte_ranges(ranges)
            }

            fn get_metadata<'a>(
                &'a mut self,
                options: Option<&'a ArrowReaderOptions>,
            ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
                self.inner.get_metadata(options)
            }
        }

        fn prefetching(
            inner: MemReader,
            meta: &ArrowReaderMetadata,
            row_groups: Vec<usize>,
            filtered: bool,
        ) -> AskLog<PrefetchingReader<MemReader>> {
            let log = inner.log.clone();
            let plan = ReadPlanCell::new(OnceLock::from(ReadPlan {
                row_groups,
                filtered,
            }));
            AskLog {
                inner: PrefetchingReader::new(inner, meta.metadata().clone(), plan),
                log,
            }
        }

        #[derive(Clone, Default)]
        struct Scan {
            projection: Option<Vec<usize>>,
            row_groups: Option<Vec<usize>>,
            /// Keeps the rows whose `id` passes.
            filter: Option<fn(i64) -> bool>,
            selection: Option<RowSelection>,
        }

        impl Scan {
            fn with_projection(mut self, projection: Vec<usize>) -> Self {
                self.projection = Some(projection);
                self
            }

            fn with_groups(mut self, row_groups: Vec<usize>) -> Self {
                self.row_groups = Some(row_groups);
                self
            }

            fn with_filter(mut self, keep: fn(i64) -> bool) -> Self {
                self.filter = Some(keep);
                self
            }

            fn with_selection(mut self, selection: RowSelection) -> Self {
                self.selection = Some(selection);
                self
            }

            fn read_order(&self, meta: &ArrowReaderMetadata) -> Vec<usize> {
                self.row_groups
                    .clone()
                    .unwrap_or_else(|| (0..meta.metadata().num_row_groups()).collect())
            }

            fn stream<R: AsyncFileReader + Unpin + Send + 'static>(
                &self,
                reader: R,
                meta: &ArrowReaderMetadata,
            ) -> parquet::arrow::async_reader::ParquetRecordBatchStream<R> {
                let mut builder =
                    ParquetRecordBatchStreamBuilder::new_with_metadata(reader, meta.clone());
                if let Some(projection) = &self.projection {
                    let mask =
                        ProjectionMask::roots(builder.parquet_schema(), projection.iter().copied());
                    builder = builder.with_projection(mask);
                }
                if let Some(row_groups) = &self.row_groups {
                    builder = builder.with_row_groups(row_groups.clone());
                }
                if let Some(selection) = &self.selection {
                    builder = builder.with_row_selection(selection.clone());
                }
                if let Some(keep) = self.filter {
                    let mask = ProjectionMask::roots(builder.parquet_schema(), [0]);
                    let predicate = ArrowPredicateFn::new(mask, move |batch| {
                        let ids = batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .expect("id is Int64");
                        Ok(ids
                            .values()
                            .iter()
                            .map(|&id| Some(keep(id)))
                            .collect::<BooleanArray>())
                    });
                    builder = builder.with_row_filter(RowFilter::new(vec![Box::new(predicate)]));
                }
                builder.build().unwrap()
            }

            /// Yields between batches, standing in for a consumer's decode work,
            /// so that a spawned prefetch runs on a current-thread runtime.
            async fn collect<R: AsyncFileReader + Unpin + Send + 'static>(
                &self,
                reader: R,
                meta: &ArrowReaderMetadata,
            ) -> parquet::errors::Result<Vec<RecordBatch>> {
                let mut stream = self.stream(reader, meta);
                let mut out = Vec::new();
                while let Some(batch) = stream.next().await {
                    out.push(batch?);
                    yield_a_few().await;
                }
                Ok(out)
            }
        }

        async fn yield_a_few() {
            for _ in 0..8 {
                tokio::task::yield_now().await;
            }
        }

        /// Reads `scan` with and without the prefetch and asserts the batches
        /// match, returning the prefetching read's log.
        async fn assert_same_batches(data: &Bytes, meta: &ArrowReaderMetadata, scan: &Scan) -> Log {
            let plain = scan
                .collect(MemReader::new(data.clone()), meta)
                .await
                .unwrap();
            let reader = MemReader::new(data.clone());
            let log = reader.log.clone();
            let prefetched = scan
                .collect(
                    prefetching(reader, meta, scan.read_order(meta), scan.filter.is_some()),
                    meta,
                )
                .await
                .unwrap();
            assert_eq!(prefetched, plain);
            log
        }

        #[tokio::test]
        async fn prefetched_reads_match_plain_reads() {
            let data = parquet_bytes(5);
            // Rows 13..17 and 37..40 only: groups 0, 2 and 4 select nothing.
            let selection = RowSelection::from(vec![
                RowSelector::skip(13),
                RowSelector::select(4),
                RowSelector::skip(20),
                RowSelector::select(3),
                RowSelector::skip(10),
            ]);
            let cases = [
                ("all", Scan::default()),
                ("projection", Scan::default().with_projection(vec![1])),
                (
                    "row filter on an unprojected column",
                    Scan::default()
                        .with_projection(vec![1, 2])
                        .with_filter(|id| id % 3 == 0),
                ),
                (
                    "row filter that rules out whole groups",
                    Scan::default().with_filter(|id| id >= 35),
                ),
                (
                    "non-contiguous groups",
                    Scan::default().with_groups(vec![0, 2, 4]),
                ),
                (
                    "groups out of file order",
                    Scan::default().with_groups(vec![4, 1, 3]),
                ),
                (
                    "row filter over selected groups",
                    Scan::default()
                        .with_groups(vec![1, 3])
                        .with_filter(|id| id % 2 == 1),
                ),
                (
                    "a repeated group",
                    Scan::default().with_groups(vec![1, 1, 3]),
                ),
                ("one group", Scan::default().with_groups(vec![3])),
                ("no groups", Scan::default().with_groups(vec![])),
                ("row selection", Scan::default().with_selection(selection)),
            ];
            // With the page index, a selection or filter makes the stream ask
            // for page ranges inside a chunk; without it, whole chunks.
            for page_index in [false, true] {
                let meta = metadata(&data, page_index);
                for (name, scan) in &cases {
                    let log = assert_same_batches(&data, &meta, scan).await;
                    if scan.row_groups.as_ref().is_some_and(|rg| rg.is_empty()) {
                        assert!(
                            log.lock().unwrap().is_empty(),
                            "{name}: an empty selection reads nothing"
                        );
                    }
                }
            }
        }

        /// Every chunk of `row_groups` reached the file before the stream asked
        /// for it.
        fn assert_fetched_ahead(
            log: &Log,
            meta: &ArrowReaderMetadata,
            row_groups: Range<usize>,
            columns: &[usize],
        ) {
            let events = log.lock().unwrap().clone();
            let first = |want: &Range<u64>, ask: bool| {
                events.iter().position(|e| match e {
                    Event::Ask(r) if ask => r.contains(want),
                    Event::Fetch(r) if !ask => r.contains(want),
                    _ => false,
                })
            };
            for row_group in row_groups {
                for &column in columns {
                    let want = chunk(meta.metadata(), row_group, column);
                    let fetch = first(&want, false).expect("the chunk was fetched");
                    let ask = first(&want, true).expect("the stream asked for the chunk");
                    assert!(
                        fetch < ask,
                        "row group {row_group} column {column} was not fetched ahead"
                    );
                }
            }
        }

        #[tokio::test]
        async fn next_row_group_is_fetched_before_the_stream_asks_for_it() {
            let data = parquet_bytes(5);
            let meta = metadata(&data, false);
            let log = assert_same_batches(&data, &meta, &Scan::default()).await;
            assert_fetched_ahead(&log, &meta, 1..5, &[0, 1, 2]);

            // Each chunk reached the file once, whole: the first group by the
            // stream's own request, every later one by the prefetch.
            let mut expected: Vec<Range<u64>> = (0..5)
                .flat_map(|rg| (0..3).map(move |c| (rg, c)))
                .map(|(rg, c)| chunk(meta.metadata(), rg, c))
                .collect();
            let mut actual = fetched(&log);
            expected.sort_by_key(|r| r.start);
            actual.sort_by_key(|r| r.start);
            assert_eq!(actual, expected);
        }

        /// With a row filter, the filter column (asked for first in every
        /// group) is fetched ahead, and the projection columns on demand: a
        /// group the filter rules out never has them fetched by the stream, so
        /// the prefetch must not fetch them either. No chunk is fetched twice.
        #[tokio::test]
        async fn a_filtered_read_fetches_no_more_than_a_plain_read() {
            let data = parquet_bytes(5);
            let meta = metadata(&data, false);
            let bytes = |log: &Log| fetched(log).iter().map(|r| r.end - r.start).sum::<u64>();
            let filters: [fn(i64) -> bool; 3] =
                [|id| id % 2 == 0, |id| id < 10, |id| !(10..40).contains(&id)];
            for (i, filter) in filters.into_iter().enumerate() {
                let scan = Scan::default()
                    .with_projection(vec![1, 2])
                    .with_filter(filter);
                let plain = MemReader::new(data.clone());
                let plain_log = plain.log.clone();
                scan.collect(plain, &meta).await.unwrap();
                let log = assert_same_batches(&data, &meta, &scan).await;
                assert!(
                    bytes(&log) <= bytes(&plain_log),
                    "prefetched {} bytes, plain read {}",
                    bytes(&log),
                    bytes(&plain_log)
                );
                // Only the first filter yields a batch, and with it a chance
                // for the prefetch to run, from every group.
                if i == 0 {
                    assert_fetched_ahead(&log, &meta, 1..5, &[0]);
                }

                let mut counts: HashMap<Range<u64>, usize> = HashMap::new();
                for range in fetched(&log) {
                    *counts.entry(range).or_default() += 1;
                }
                assert!(counts.values().all(|&n| n == 1), "refetched: {counts:?}");
            }
        }

        /// Buffers are held for the group the stream is on and the next one
        /// only.
        #[tokio::test]
        async fn prefetched_buffers_stay_within_one_group_ahead() {
            let data = parquet_bytes(5);
            let meta = metadata(&data, false);
            let mut reader =
                prefetching(MemReader::new(data), &meta, (0..5).collect(), false).inner;
            for rg in 0..5 {
                let ranges = (0..3).map(|c| chunk(meta.metadata(), rg, c)).collect();
                reader.get_byte_ranges(ranges).await.unwrap();
                yield_a_few().await;
                let held: Vec<usize> = reader.prefetched.iter().map(|p| p.position).collect();
                assert!(
                    held.iter().all(|&p| p == rg || p == rg + 1),
                    "on group {rg}, holding {held:?}"
                );
            }
        }

        /// With the page index, a selection asks for page ranges inside each
        /// chunk. Later groups are still fetched as whole chunks, once, and the
        /// page ranges are cut from them.
        #[tokio::test]
        async fn page_ranges_are_served_from_prefetched_chunks() {
            let data = parquet_bytes(4);
            let meta = metadata(&data, true);
            // Rows 4 and 5 of every group: the third two-row page.
            let selection = RowSelection::from(
                (0..4)
                    .flat_map(|_| {
                        [
                            RowSelector::skip(4),
                            RowSelector::select(2),
                            RowSelector::skip(4),
                        ]
                    })
                    .collect::<Vec<_>>(),
            );
            let scan = Scan::default().with_selection(selection);
            let log = assert_same_batches(&data, &meta, &scan).await;

            let later_chunks: Vec<Range<u64>> = (1..4)
                .flat_map(|rg| (0..3).map(move |c| (rg, c)))
                .map(|(rg, c)| chunk(meta.metadata(), rg, c))
                .collect();
            let asked_sub_ranges = log.lock().unwrap().iter().any(|e| match e {
                Event::Ask(ranges) => ranges.iter().any(|r| {
                    later_chunks
                        .iter()
                        .any(|c| contains(c, r) && (r.start, r.end) != (c.start, c.end))
                }),
                Event::Fetch(_) => false,
            });
            assert!(
                asked_sub_ranges,
                "the selection did not ask for page ranges"
            );

            let mut later_fetches: Vec<Range<u64>> = fetched(&log)
                .into_iter()
                .filter(|r| later_chunks.iter().any(|c| contains(c, r)))
                .collect();
            later_fetches.sort_by_key(|r| r.start);
            let mut expected = later_chunks.clone();
            expected.sort_by_key(|r| r.start);
            assert_eq!(later_fetches, expected);
        }

        /// A failed prefetch falls back to a normal fetch; only when that fails
        /// too does the read return the error.
        #[tokio::test]
        async fn a_failed_prefetch_falls_back_to_a_normal_fetch() {
            let data = parquet_bytes(3);
            let meta = metadata(&data, false);
            let plain = Scan::default()
                .collect(MemReader::new(data.clone()), &meta)
                .await
                .unwrap();
            let target = chunk(meta.metadata(), 1, 0);
            for failures in [1, 2] {
                let mut reader = MemReader::new(data.clone());
                reader.fail_at = Some(target.start);
                reader.failures_left.store(failures, Ordering::SeqCst);
                let log = reader.log.clone();
                let result = Scan::default()
                    .collect(prefetching(reader, &meta, vec![0, 1, 2], false), &meta)
                    .await;
                let tries = fetched(&log).iter().filter(|r| **r == target).count();
                assert_eq!(tries, 2, "the failed prefetch, then the normal fetch");
                match result {
                    Ok(batches) if failures == 1 => assert_eq!(batches, plain),
                    Err(e) if failures == 2 => assert!(e.to_string().contains("injected")),
                    other => panic!("{failures} failures: {other:?}"),
                }
            }
        }

        #[tokio::test]
        async fn dropping_the_stream_aborts_an_in_flight_prefetch() {
            let data = parquet_bytes(3);
            let meta = metadata(&data, false);
            let mut reader = MemReader::new(data.clone());
            reader.stall_at = Some(chunk(meta.metadata(), 1, 0).start);
            let stalled = reader.stalled.clone();

            let mut stream =
                Scan::default().stream(prefetching(reader, &meta, vec![0, 1, 2], false), &meta);
            stream.next().await.unwrap().unwrap();
            yield_a_few().await;
            assert_eq!(
                stalled.load(Ordering::SeqCst),
                1,
                "the prefetch is in flight"
            );

            drop(stream);
            yield_a_few().await;
            assert_eq!(
                stalled.load(Ordering::SeqCst),
                0,
                "the prefetch was aborted"
            );
        }

        /// Through the base-file reader: the selection decided in
        /// `apply_options` is the prefetch's read order, so a read that skips
        /// row groups fetches exactly the chunks of the groups it reads.
        #[tokio::test]
        async fn read_stream_prefetches_only_the_selected_row_groups() {
            let dir = tempfile::tempdir().unwrap();
            let data = parquet_bytes(5);
            std::fs::write(dir.path().join("multi.parquet"), &data).unwrap();
            let storage =
                Storage::new_with_base_url(Url::from_directory_path(dir.path()).unwrap()).unwrap();
            let volume = storage.read_volume();
            let reader = ParquetBaseFileReader::new(storage);

            let keep = vec![0, 2, 4];
            let selector = keep.clone();
            let opts = BaseFileReadOptions::default()
                .with_projection(["id", "value"])
                .with_row_group_selector(Arc::new(move |_| Some(selector.clone())));
            let mut stream = reader
                .read_stream("multi.parquet", opts)
                .await
                .unwrap()
                .into_stream();
            let mut batches = Vec::new();
            while let Some(batch) = stream.next().await {
                batches.push(batch.unwrap());
                yield_a_few().await;
            }

            let meta = metadata(&data, false);
            let scan = Scan::default()
                .with_projection(vec![0, 2])
                .with_groups(keep.clone());
            let plain = scan
                .collect(MemReader::new(data.clone()), &meta)
                .await
                .unwrap();
            assert_eq!(batches, plain);

            let selected_bytes: u64 = keep
                .iter()
                .flat_map(|&rg| [0, 2].map(|c| chunk(meta.metadata(), rg, c)))
                .map(|r| r.end - r.start)
                .sum();
            assert_eq!(
                volume.bytes_read.load(Ordering::Relaxed),
                selected_bytes,
                "each selected chunk once, and nothing from the skipped groups"
            );
            assert_eq!(
                volume.io_calls.load(Ordering::Relaxed),
                keep.len() as u64,
                "the first group's own request plus one prefetch per later group"
            );
        }
    }
}
