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

//! Declarative e2e harness for `HoodieFileGroupReader`.
//!
//! A test = [`FgReaderCase`]: fixture × read config × expected data.
//! All cases run through [`try_run_case`]; assertions validate the FULL
//! output dataset (never just counts). Adding coverage = adding a case.

use std::sync::Arc;

use crate::config::HudiConfigs;
use crate::config::table::HudiTableConfig;
use crate::error::Result;
use crate::file_group::reader_v2::MAX_INSTANT_TIME;
use crate::file_group::reader_v2::engine::HoodieFileGroupReader;
use crate::file_group::reader_v2::input_split::InputSplit;
use crate::file_group::reader_v2::read_stats::HoodieReadStats;
use crate::file_group::reader_v2::reader_context::ReaderContext;
use crate::file_group::reader_v2::reader_parameters::ReaderParameters;
use crate::file_group::reader_v2::schema_handler::FileGroupReaderSchemaHandler;
use crate::storage::{RowFilterBuilder, Storage};
use crate::table::builder::OptionResolver;
use crate::timeline::selector::InstantRange;
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Decimal128Type, Float32Type, Float64Type, Int32Type, Int64Type,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType,
};
use arrow_array::{Array, BooleanArray, RecordBatch};
use arrow_schema::{ArrowError, DataType, SchemaRef};
use hudi_test::QuickstartTripsTable;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowPredicateFn, RowFilter};

/// How the reader is constructed — mirrors the four call paths used today.
pub enum SchemaSpec {
    /// `new(.., None, None)` — no projection.
    None,
    /// `new(.., data_from_base_footer, Some(requested))`.
    Projection(SchemaRef),
    /// `new(.., Some(data), Some(requested))` — the FFI explicit-schema path.
    Explicit {
        data: SchemaRef,
        requested: SchemaRef,
    },
    /// Builder path: schemas live on `ReaderContext.schema_handler`
    /// (mimics `new_file_group_reader_with_context`).
    BuilderProjection(SchemaRef),
    /// Full FFI mirror: data/requested schemas given as Avro JSON, set on the
    /// schema handler as BOTH arrow and JSON — exactly what
    /// `new_file_group_reader_with_context` does. This is the only arm that
    /// arms `required_schema_json`, i.e. the avro resolution / extended-
    /// promotion branches of the log-block decoder. Use it for
    /// schema-evolution cases.
    ExplicitJson {
        data_json: &'static str,
        requested_json: &'static str,
    },
}

/// What the output must contain. Full-data assertions only.
pub enum Expected {
    /// Exact rows: sort output by `sort_key` (which must be unique), render each
    /// listed column with `arrow_cast::display::array_value_to_string`, compare
    /// cell-by-cell. A NULL cell is asserted with the [`NULL_SENTINEL`] token
    /// (`"<null>"`) so it is never conflated with the empty string `""`.
    Rows {
        sort_key: &'static str,
        columns: &'static [&'static str],
        rows: &'static [&'static [&'static str]],
    },
    /// Compare against the fixture's gold parquet (Spark `SELECT *` snapshot),
    /// sorted by "key", skipping `_hoodie_*` columns.
    GoldParquet,
    /// Typed escape hatch for shapes string rendering can't capture
    /// (e.g. NULL elements inside containers). `rows` is the expected row count,
    /// enforced by the harness BEFORE `validate` runs, so a validator can never
    /// vacuously pass on an empty or short batch.
    ///
    /// Invariant: validators must not panic — use `ok_or()?`/`Err` so failures
    /// surface as harness errors with case-name context.
    Custom {
        rows: usize,
        validate: fn(&RecordBatch) -> std::result::Result<(), String>,
    },
    /// The read must FAIL and the error's Debug text must contain this.
    ErrContains(&'static str),
}

/// Predicate of a [`RowFilterSpec`]: a comparison against one literal, or a
/// membership test against a literal set. Literals are `&str` so the case
/// table stays `'static`; they are parsed per the column's arrow type (see
/// [`compare_column_predicate`] for the supported types and literal formats).
#[derive(Clone, Copy)]
pub enum FilterPredicate {
    Eq(&'static str),
    Gt(&'static str),
    Lt(&'static str),
    /// SQL `IN (...)` semantics: row matches if the column equals ANY listed
    /// literal. Nulls never match.
    In(&'static [&'static str]),
}

/// Declarative row filter compiled into a [`RowFilterBuilder`] by the harness.
///
/// Closures can't live in a `const` case table, so a case declares its filter
/// declaratively and [`build_row_filter_builder`] compiles it. The compiled
/// builder is installed via the `with_row_filter_builder` /
/// `with_mor_pk_safe` builder methods, exercising the same Rust-reachable
/// channel Gluten/FFI uses (D-P2-1).
pub struct RowFilterSpec {
    /// Column the predicate references, by name. Located in the parquet
    /// `SchemaDescriptor` the reader hands the builder.
    pub column: &'static str,
    pub predicate: FilterPredicate,
    /// Marks the filter PK-safe. Gold contract
    /// (`SparkFileFormatInternalRowReaderContext.filterIsSafeForPrimaryKey`):
    /// only record-key filters are safe to push under merge, because PKs are
    /// immutable across upserts. Sets `builder.with_mor_pk_safe`; the
    /// `can_push_row_filter` gate (`is_cow() || mor_pk_safe`) decides whether
    /// the filter is actually installed on the base parquet + parquet log
    /// blocks.
    pub mor_pk_safe: bool,
}

/// Post-read assertion on the reader's [`HoodieReadStats`]. `fn` (not a
/// closure) keeps the case table `'static`; returns `Err(msg)` to fail the case.
pub type StatsCheck = fn(&HoodieReadStats) -> std::result::Result<(), String>;

/// Construct with struct-update syntax (`..Default::default()`); defaults are
/// safe-by-failure (a case that forgets to set its fields fails loudly rather
/// than vacuously passing).
pub struct FgReaderCase {
    pub name: &'static str,
    pub fixture: QuickstartTripsTable,
    pub partition: &'static str,
    /// "" = no base file (log-only file group).
    pub base_file: &'static str,
    pub log_files: &'static [&'static str],
    pub schema: SchemaSpec,
    /// When Some, the output schema's column names must equal this exactly
    /// (proves projection stripped merge-internal fields).
    pub expect_output_columns: Option<&'static [&'static str]>,
    /// Override the merge mode (default "COMMIT_TIME_ORDERING").
    pub merge_mode: Option<&'static str>,
    /// Override the latest-commit-time watermark (default far-future sentinel
    /// [`MAX_INSTANT_TIME`]). Gold gate 2: log blocks whose INSTANT_TIME header
    /// is `> latest_commit_time` are FUTURE blocks and excluded.
    pub latest_commit_time: Option<&'static str>,
    /// Instant range applied to the read; the harness invokes the fn and sets
    /// `reader_context.instant_range`. Stored as `fn()` so the case table stays
    /// `'static`-friendly. Gold: blocks whose INSTANT_TIME is outside the range
    /// are skipped, and base rows are filtered by `_hoodie_commit_time`.
    pub instant_range: Option<fn() -> InstantRange>,
    /// Reader parameters override (default `ReaderParameters::default()`).
    pub reader_parameters: Option<ReaderParameters>,
    /// Extra `hoodie_reader_config` entries merged onto the reader context
    /// before the read. This is the SAME map the FFI/gluten adapter populates
    /// and that `SpillConfig::from_config` reads, so a case can drive real
    /// config-string behavior end-to-end (e.g. the merge spill budget
    /// `hoodie.memory.merge.max.size` or the hard peak cap
    /// `hoodie.memory.merge.max.peak.size`) through the actual reader path
    /// rather than by constructing a `SpillConfig` directly.
    pub reader_config: &'static [(&'static str, &'static str)],
    /// Declarative parquet `RowFilter` pushdown. When `Some`, the case is
    /// FORCED through the builder/`BuilderProjection` construction path
    /// regardless of its `schema` arm (the only path that threads a
    /// `row_filter_builder` onto the base parquet read — `HoodieFileGroupReader::new`
    /// has no filter param, and the unprojected base read skips pushdown).
    /// The harness derives the requested schema from the case's
    /// `expect_output_columns` (required) so the projected base read engages.
    pub row_filter: Option<RowFilterSpec>,
    /// Optional post-read assertion on the reader's [`HoodieReadStats`]
    /// (corrupt/rollback/log-block counters). Invoked AFTER a successful read +
    /// expected-data validation; an `Err(msg)` fails the case with the message.
    /// Skipped for `Expected::ErrContains` cases (no successful read to inspect).
    pub expect_stats: Option<StatsCheck>,
    pub expected: Expected,
}

impl Default for FgReaderCase {
    fn default() -> Self {
        Self {
            name: "UNNAMED_CASE",
            fixture: QuickstartTripsTable::V9Mor8I4UCommitTime,
            partition: "",
            base_file: "",
            log_files: &[],
            schema: SchemaSpec::None,
            expect_output_columns: None,
            merge_mode: None,
            latest_commit_time: None,
            instant_range: None,
            reader_parameters: None,
            reader_config: &[],
            row_filter: None,
            expect_stats: None,
            // Zero expected rows: any real read output fails the row-count
            // check, so an under-specified case cannot vacuously pass.
            expected: Expected::Rows {
                sort_key: "id",
                columns: &[],
                rows: &[],
            },
        }
    }
}

/// Create HudiConfigs and Storage from table path using OptionResolver.
pub async fn create_configs_and_storage(
    table_path: &str,
) -> Result<(Arc<HudiConfigs>, Arc<Storage>)> {
    let empty_opts: Vec<(&str, &str)> = vec![];
    let mut resolver = OptionResolver::new_with_options(table_path, empty_opts);
    resolver.resolve_options().await?;
    let hudi_configs = Arc::new(HudiConfigs::new(resolver.hudi_options));
    let storage = Storage::new(Arc::new(resolver.storage_options), hudi_configs.clone())?;
    Ok((hudi_configs, storage))
}

/// Join a partition prefix onto a file name; "" partition means no prefix.
fn join_partition(partition: &str, file: &str) -> String {
    if partition.is_empty() {
        file.to_string()
    } else {
        format!("{partition}/{file}")
    }
}

/// Compile a [`RowFilterSpec`] into a [`RowFilterBuilder`] closure.
///
/// The closure mirrors what the FFI layer (`cpp/src/predicate.rs`) does, but
/// without the substrait wire format: it locates the predicate column in the
/// parquet `SchemaDescriptor` the reader passes in, builds a single-root
/// `ProjectionMask` for it, and installs one `ArrowPredicateFn` that compares
/// the column against the literal (parsed per the arrow column type) using
/// arrow-ord-style element-wise comparison. Returns `None` if the column is
/// absent from the parquet schema (gold-parity: drop the pushdown, never fail
/// the read), which keeps the builder honest for evolved/added columns.
///
/// Supported column types: Utf8/LargeUtf8, Int32, Int64, Float32, Float64,
/// Boolean, Date32, Timestamp, Decimal128. Literal formats are documented on
/// [`compare_column_predicate`].
fn build_row_filter_builder(spec: &RowFilterSpec) -> RowFilterBuilder {
    let column = spec.column.to_string();
    let pred = spec.predicate;

    Arc::new(move |parquet_schema, _projected| {
        // _projected = the storage-layer's already-projected arrow schema; column lookup uses
        // parquet_schema (full file schema) instead, so it's unused here.
        // Locate the predicate column as a top-level (root) parquet column.
        let root = parquet_schema.root_schema();
        let root_idx = root.get_fields().iter().position(|f| f.name() == column)?;
        let mask = ProjectionMask::roots(parquet_schema, [root_idx]);

        let column = column.clone();
        let predicate = ArrowPredicateFn::new(mask, move |batch: RecordBatch| {
            let col = batch.column_by_name(&column).ok_or_else(|| {
                ArrowError::ComputeError(format!(
                    "row_filter predicate column '{column}' not in predicate batch"
                ))
            })?;
            compare_column_predicate(col, &pred)
        });
        Some(RowFilter::new(vec![Box::new(predicate)]))
    })
}

/// Parse a literal as `T`, with the type name in the error for context.
fn parse_literal<T: std::str::FromStr>(s: &str, ty: &str) -> std::result::Result<T, ArrowError> {
    s.parse::<T>()
        .map_err(|_| ArrowError::ComputeError(format!("cannot parse '{s}' as {ty}")))
}

/// Parse a decimal literal (e.g. "300.30") into the i128 representation of a
/// `Decimal128(_, scale)` column. Fails on too many fraction digits rather
/// than rounding, so a case literal always matches the column exactly.
fn parse_decimal_literal(s: &str, scale: i8) -> std::result::Result<i128, ArrowError> {
    let err = |m: &str| ArrowError::ComputeError(format!("decimal literal '{s}': {m}"));
    let (neg, digits) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s),
    };
    let (int_part, frac_part) = match digits.split_once('.') {
        Some((i, f)) => (i, f),
        None => (digits, ""),
    };
    let scale = usize::try_from(scale).map_err(|_| err("negative scale unsupported"))?;
    if frac_part.len() > scale {
        return Err(err("more fraction digits than the column scale"));
    }
    let mut unscaled = String::with_capacity(int_part.len() + scale);
    unscaled.push_str(int_part);
    unscaled.push_str(frac_part);
    for _ in frac_part.len()..scale {
        unscaled.push('0');
    }
    let v: i128 = parse_literal(&unscaled, "i128")?;
    Ok(if neg { -v } else { v })
}

/// Evaluate `predicate` element-wise over a typed view of the column. The
/// caller parses each literal into `T` once via `parse`; `is_null`/`value`
/// adapt the concrete arrow array. Nulls never match (SQL WHERE semantics).
fn eval_predicate<T: PartialOrd + Copy>(
    len: usize,
    is_null: impl Fn(usize) -> bool,
    value: impl Fn(usize) -> T,
    predicate: &FilterPredicate,
    parse: impl Fn(&str) -> std::result::Result<T, ArrowError>,
) -> std::result::Result<BooleanArray, ArrowError> {
    let matches: Box<dyn Fn(T) -> bool> = match predicate {
        FilterPredicate::Eq(lit) => {
            let rhs = parse(lit)?;
            Box::new(move |lhs| lhs == rhs)
        }
        FilterPredicate::Gt(lit) => {
            let rhs = parse(lit)?;
            Box::new(move |lhs| lhs > rhs)
        }
        FilterPredicate::Lt(lit) => {
            let rhs = parse(lit)?;
            Box::new(move |lhs| lhs < rhs)
        }
        FilterPredicate::In(lits) => {
            let set: Vec<T> = lits
                .iter()
                .map(|lit| parse(lit))
                .collect::<std::result::Result<_, _>>()?;
            Box::new(move |lhs| set.contains(&lhs))
        }
    };
    Ok((0..len)
        .map(|i| (!is_null(i)).then(|| matches(value(i))))
        .collect())
}

/// Element-wise `column <predicate>`, parsing literals per the column's arrow
/// type. Nulls compare false (dropped — SQL WHERE semantics).
///
/// Literal formats per column type:
/// - Utf8/LargeUtf8: the string itself
/// - Int32/Int64/Float32/Float64: standard numeric literals
/// - Boolean: "true" / "false"
/// - Date32: raw days since epoch (e.g. "19754" = 2024-02-01)
/// - Timestamp(any unit/tz): raw i64 in the column's native unit
///   (e.g. "1709251203000000" for a microsecond column)
/// - Decimal128(p, s): decimal string (e.g. "300.30"), scaled to the column
fn compare_column_predicate(
    col: &Arc<dyn Array>,
    predicate: &FilterPredicate,
) -> std::result::Result<BooleanArray, ArrowError> {
    match col.data_type() {
        DataType::Utf8 => {
            let arr = col.as_string::<i32>();
            // Strings aren't Copy; evaluate via the generic helper over &str.
            eval_str_predicate(arr.len(), |i| arr.is_null(i), |i| arr.value(i), predicate)
        }
        DataType::LargeUtf8 => {
            let arr = col.as_string::<i64>();
            eval_str_predicate(arr.len(), |i| arr.is_null(i), |i| arr.value(i), predicate)
        }
        DataType::Int32 => {
            let arr = col.as_primitive::<Int32Type>();
            eval_predicate(
                arr.len(),
                |i| arr.is_null(i),
                |i| arr.value(i),
                predicate,
                |s| parse_literal::<i32>(s, "i32"),
            )
        }
        DataType::Int64 => {
            let arr = col.as_primitive::<Int64Type>();
            eval_predicate(
                arr.len(),
                |i| arr.is_null(i),
                |i| arr.value(i),
                predicate,
                |s| parse_literal::<i64>(s, "i64"),
            )
        }
        DataType::Float32 => {
            let arr = col.as_primitive::<Float32Type>();
            eval_predicate(
                arr.len(),
                |i| arr.is_null(i),
                |i| arr.value(i),
                predicate,
                |s| parse_literal::<f32>(s, "f32"),
            )
        }
        DataType::Float64 => {
            let arr = col.as_primitive::<Float64Type>();
            eval_predicate(
                arr.len(),
                |i| arr.is_null(i),
                |i| arr.value(i),
                predicate,
                |s| parse_literal::<f64>(s, "f64"),
            )
        }
        DataType::Boolean => {
            let arr = col.as_boolean();
            eval_predicate(
                arr.len(),
                |i| arr.is_null(i),
                |i| arr.value(i),
                predicate,
                |s| parse_literal::<bool>(s, "bool"),
            )
        }
        DataType::Date32 => {
            let arr = col.as_primitive::<Date32Type>();
            eval_predicate(
                arr.len(),
                |i| arr.is_null(i),
                |i| arr.value(i),
                predicate,
                |s| parse_literal::<i32>(s, "date32 days"),
            )
        }
        DataType::Timestamp(unit, _tz) => {
            // All four timestamp units store i64; compare on the raw value in
            // the column's own unit (the case literal must use that unit).
            let values: Vec<i64> = match unit {
                arrow_schema::TimeUnit::Second => {
                    let a = col.as_primitive::<TimestampSecondType>();
                    (0..a.len()).map(|i| a.value(i)).collect()
                }
                arrow_schema::TimeUnit::Millisecond => {
                    let a = col.as_primitive::<TimestampMillisecondType>();
                    (0..a.len()).map(|i| a.value(i)).collect()
                }
                arrow_schema::TimeUnit::Microsecond => {
                    let a = col.as_primitive::<TimestampMicrosecondType>();
                    (0..a.len()).map(|i| a.value(i)).collect()
                }
                arrow_schema::TimeUnit::Nanosecond => {
                    let a = col.as_primitive::<TimestampNanosecondType>();
                    (0..a.len()).map(|i| a.value(i)).collect()
                }
            };
            eval_predicate(
                col.len(),
                |i| col.is_null(i),
                |i| values[i],
                predicate,
                |s| parse_literal::<i64>(s, "timestamp i64"),
            )
        }
        DataType::Decimal128(_, scale) => {
            let arr = col.as_primitive::<Decimal128Type>();
            let scale = *scale;
            eval_predicate(
                arr.len(),
                |i| arr.is_null(i),
                |i| arr.value(i),
                predicate,
                move |s| parse_decimal_literal(s, scale),
            )
        }
        other => Err(ArrowError::ComputeError(format!(
            "row_filter unsupported column type {other} (add an arm in fg_harness)"
        ))),
    }
}

/// String flavor of [`eval_predicate`] (&str isn't `Copy`; literals need no
/// parsing).
fn eval_str_predicate<'a>(
    len: usize,
    is_null: impl Fn(usize) -> bool,
    value: impl Fn(usize) -> &'a str,
    predicate: &FilterPredicate,
) -> std::result::Result<BooleanArray, ArrowError> {
    let predicate = *predicate;
    Ok((0..len)
        .map(|i| {
            (!is_null(i)).then(|| {
                let lhs = value(i);
                match predicate {
                    FilterPredicate::Eq(rhs) => lhs == rhs,
                    FilterPredicate::Gt(rhs) => lhs > rhs,
                    FilterPredicate::Lt(rhs) => lhs < rhs,
                    FilterPredicate::In(set) => set.contains(&lhs),
                }
            })
        })
        .collect())
}

/// Build the base `ReaderContext` shared by every construction path, honoring
/// the case's optional read-config overrides (merge mode, watermark, instant
/// range). Defaults preserve the prior behavior: COMMIT_TIME_ORDERING merge and
/// the far-future [`MAX_INSTANT_TIME`] watermark sentinel.
fn base_reader_context(case: &FgReaderCase, has_log_files: bool) -> ReaderContext {
    let mut reader_context = ReaderContext::empty();
    reader_context.latest_commit_time = case
        .latest_commit_time
        .unwrap_or(MAX_INSTANT_TIME)
        .to_string();
    reader_context.merge_mode = case
        .merge_mode
        .unwrap_or("COMMIT_TIME_ORDERING")
        .to_string();
    reader_context.instant_range = case.instant_range.map(|f| f());
    reader_context.table_config.insert(
        HudiTableConfig::OrderingFields.as_ref().to_string(),
        "ts".to_string(),
    );
    reader_context.rebuild_record_context(case.partition.to_string());
    reader_context.has_log_files = has_log_files;
    for (key, value) in case.reader_config {
        reader_context
            .hoodie_reader_config
            .insert((*key).to_string(), (*value).to_string());
    }
    reader_context
}

/// The reader parameters for a case (override or default).
fn reader_parameters(case: &FgReaderCase) -> ReaderParameters {
    case.reader_parameters.clone().unwrap_or_default()
}

/// Run a case through the reader, returning the merged `RecordBatch` plus a
/// snapshot of the reader's [`HoodieReadStats`] (cloned after `read()`, so the
/// reader can be dropped while the caller still inspects stats).
///
/// Matches on [`SchemaSpec`] to replicate the four construction paths that
/// the standalone helpers in `file_group_reader_tests.rs` use today.
async fn read_case(
    case: &FgReaderCase,
    table_path: &str,
) -> Result<(RecordBatch, HoodieReadStats)> {
    let (_hudi_configs, storage) = create_configs_and_storage(table_path).await?;

    let base_path = if case.base_file.is_empty() {
        None
    } else {
        Some(join_partition(case.partition, case.base_file))
    };
    let log_paths: Vec<String> = case
        .log_files
        .iter()
        .map(|lf| join_partition(case.partition, lf))
        .collect();

    // Leave base_file_commit_time None here: the harness mirrors only the paths.
    // The instant-range filter derives the base file's instant from base_file_path
    // when this is None (reader/mod.rs), so instant-range cases still work; the
    // real FFI populates it (cpp/src/lib.rs), and the position-based loader path
    // (loader.rs) keeps its existing None behavior for the non-instant cases.
    let input_split = InputSplit::new(
        base_path.clone(),
        None,
        log_paths,
        case.partition.to_string(),
    );

    let has_log_files = !case.log_files.is_empty();

    // Filtered cases route through the builder path: it is the only
    // construction that threads a `row_filter_builder` onto the base parquet
    // read (`HoodieFileGroupReader::new` has no filter param, and the
    // unprojected base read explicitly skips pushdown — reader/mod.rs:562).
    // The requested schema (driving the projected base read that engages the
    // filter) is derived from `expect_output_columns`.
    if let Some(spec) = &case.row_filter {
        return read_case_with_filter(case, storage, input_split, base_path, has_log_files, spec)
            .await;
    }

    match &case.schema {
        SchemaSpec::None => {
            let reader_context = base_reader_context(case, has_log_files);
            let mut reader = HoodieFileGroupReader::new(
                Arc::new(reader_context),
                storage,
                input_split,
                reader_parameters(case),
                None,
                None,
            )?;
            let batch = reader.read().await?;
            Ok((batch, reader.read_stats().clone()))
        }
        SchemaSpec::Projection(requested) => {
            let reader_context = base_reader_context(case, has_log_files);
            // Derive data schema from the base parquet footer.
            let data_schema: Option<SchemaRef> = if let Some(ref bp) = base_path {
                crate::file_group::base_file::parquet::ParquetBaseFileReader::new(storage.clone())
                    .get_schema(bp)
                    .await
                    .ok()
                    .map(|s| Arc::new(s) as SchemaRef)
            } else {
                None
            };
            let mut reader = HoodieFileGroupReader::new(
                Arc::new(reader_context),
                storage,
                input_split,
                reader_parameters(case),
                data_schema,
                Some(requested.clone()),
            )?;
            let batch = reader.read().await?;
            Ok((batch, reader.read_stats().clone()))
        }
        SchemaSpec::Explicit { data, requested } => {
            let reader_context = base_reader_context(case, has_log_files);
            let mut reader = HoodieFileGroupReader::new(
                Arc::new(reader_context),
                storage,
                input_split,
                reader_parameters(case),
                Some(data.clone()),
                Some(requested.clone()),
            )?;
            let batch = reader.read().await?;
            Ok((batch, reader.read_stats().clone()))
        }
        SchemaSpec::BuilderProjection(requested) => {
            // Mimic the FFI bridge: schemas live on ReaderContext.schema_handler,
            // and the reader is built WITHOUT explicit data/requested schemas.
            let table_schema: Option<SchemaRef> = if let Some(ref bp) = base_path {
                crate::file_group::base_file::parquet::ParquetBaseFileReader::new(storage.clone())
                    .get_schema(bp)
                    .await
                    .ok()
                    .map(|s| Arc::new(s) as SchemaRef)
            } else {
                None
            };
            let schema_handler = {
                let mut handler = FileGroupReaderSchemaHandler::new();
                if let Some(ts) = table_schema {
                    handler = handler.with_table_schema(ts.clone()).with_data_schema(ts);
                }
                handler = handler.with_requested_schema(requested.clone());
                handler
            };
            let mut reader_context = base_reader_context(case, has_log_files);
            reader_context.schema_handler = schema_handler;
            let mut reader = HoodieFileGroupReader::builder()
                .with_reader_context(Arc::new(reader_context))
                .with_storage(storage)
                .with_input_split(input_split)
                .with_reader_parameters(reader_parameters(case))
                .build()?;
            let batch = reader.read().await?;
            Ok((batch, reader.read_stats().clone()))
        }
        SchemaSpec::ExplicitJson {
            data_json,
            requested_json,
        } => {
            // Full FFI mirror (new_file_group_reader_with_context): both the
            // arrow schemas AND the avro JSONs land on the schema handler, so
            // prepare_required_schema computes `required_schema_json` and the
            // log-block decoder takes the avro resolution / extended-promotion
            // branches instead of the legacy writer-only decode.
            let data_arrow: SchemaRef = Arc::new(
                crate::schema::resolver::avro_json_to_arrow_schema(data_json)?,
            );
            let requested_arrow: SchemaRef = Arc::new(
                crate::schema::resolver::avro_json_to_arrow_schema(requested_json)?,
            );
            let schema_handler = FileGroupReaderSchemaHandler::new()
                .with_table_schema(data_arrow.clone())
                .with_data_schema(data_arrow)
                .with_data_schema_json((*data_json).to_string())
                .with_requested_schema(requested_arrow)
                .with_requested_schema_json((*requested_json).to_string());
            let mut reader_context = base_reader_context(case, has_log_files);
            reader_context.schema_handler = schema_handler;
            let mut reader = HoodieFileGroupReader::builder()
                .with_reader_context(Arc::new(reader_context))
                .with_storage(storage)
                .with_input_split(input_split)
                .with_reader_parameters(reader_parameters(case))
                .build()?;
            let batch = reader.read().await?;
            Ok((batch, reader.read_stats().clone()))
        }
    }
}

/// Construct + read a filtered case via the builder, installing the compiled
/// `RowFilterBuilder` and `mor_pk_safe` flag through the Rust-reachable channel
/// (`with_row_filter_builder` / `with_mor_pk_safe`). Schemas live on the
/// `ReaderContext.schema_handler` (FFI-style), with the requested schema
/// derived from the case's `expect_output_columns`.
async fn read_case_with_filter(
    case: &FgReaderCase,
    storage: Arc<Storage>,
    input_split: InputSplit,
    base_path: Option<String>,
    has_log_files: bool,
    spec: &RowFilterSpec,
) -> Result<(RecordBatch, HoodieReadStats)> {
    use crate::error::CoreError;

    let out_cols = case.expect_output_columns.ok_or_else(|| {
        CoreError::ReadFileSliceError(
            "row_filter cases must set expect_output_columns (drives the requested schema)".into(),
        )
    })?;

    let bp = base_path.as_ref().ok_or_else(|| {
        CoreError::ReadFileSliceError("row_filter cases require a base file".into())
    })?;
    let table_schema: SchemaRef = Arc::new(
        crate::file_group::base_file::parquet::ParquetBaseFileReader::new(storage.clone())
            .get_schema(bp)
            .await?,
    );

    // Requested schema = the named output columns, taken from the table schema
    // so types match exactly.
    let requested_fields: Vec<arrow_schema::FieldRef> = out_cols
        .iter()
        .map(|name| {
            table_schema
                .column_with_name(name)
                .map(|(_, f)| Arc::new(f.clone()))
                .ok_or_else(|| {
                    CoreError::ReadFileSliceError(format!(
                        "expect_output_columns names '{name}' not in base parquet schema"
                    ))
                })
        })
        .collect::<Result<_>>()?;
    let requested: SchemaRef = Arc::new(arrow_schema::Schema::new(requested_fields));

    let schema_handler = FileGroupReaderSchemaHandler::new()
        .with_table_schema(table_schema.clone())
        .with_data_schema(table_schema)
        .with_requested_schema(requested);

    let mut reader_context = base_reader_context(case, has_log_files);
    reader_context.schema_handler = schema_handler;

    // The `can_push_row_filter` gate is `is_cow() || mor_pk_safe`, and
    // `is_cow()` reads `hoodie.table.type` from the table_config. The FFI/Spark
    // path populates this from `hoodie.properties`; the harness's
    // `ReaderContext::empty()` does not, so set it here to match gold's
    // pushdown gate. A base-only slice (no log files) is read as COPY_ON_WRITE
    // (no merge can flip the predicate outcome — the CoW gate branch); a slice
    // with log files is MERGE_ON_READ (only PK-safe filters may push).
    let table_type = if has_log_files {
        "MERGE_ON_READ"
    } else {
        "COPY_ON_WRITE"
    };
    reader_context.table_config.insert(
        HudiTableConfig::TableType.as_ref().to_string(),
        table_type.to_string(),
    );

    let mut reader = HoodieFileGroupReader::builder()
        .with_reader_context(Arc::new(reader_context))
        .with_storage(storage)
        .with_input_split(input_split)
        .with_reader_parameters(reader_parameters(case))
        .with_row_filter_builder(build_row_filter_builder(spec))
        .with_mor_pk_safe(spec.mor_pk_safe)
        .build()?;
    let batch = reader.read().await?;
    Ok((batch, reader.read_stats().clone()))
}

/// Expected-cell token that asserts a NULL. `array_value_to_string` renders
/// both NULL and the empty string `""` as `""`, so a case must spell NULL out
/// explicitly to distinguish the two; [`render_cell`] returns this token for a
/// null cell and the rendered value (never this token) for a non-null cell.
pub const NULL_SENTINEL: &str = "<null>";

/// Sort a `RecordBatch` ascending by `sort_key`, then verify the key is unique.
///
/// `sort_to_indices` is not stable, so rows that share a sort-key value can be
/// permuted arbitrarily; a non-unique key would make positional row comparison
/// unreliable. Enforcing uniqueness keeps the row assertions honest.
fn sort_batch(batch: &RecordBatch, sort_key: &str) -> std::result::Result<RecordBatch, String> {
    let idx = batch
        .schema()
        .index_of(sort_key)
        .map_err(|e| format!("sort key column '{sort_key}' not found: {e}"))?;
    let key_col = batch.column(idx).clone();
    let indices = arrow_ord::sort::sort_to_indices(&key_col, None, None)
        .map_err(|e| format!("sort_to_indices on '{sort_key}' failed: {e}"))?;
    let columns: std::result::Result<Vec<_>, String> = batch
        .columns()
        .iter()
        .map(|col| {
            arrow_select::take::take(col, &indices, None).map_err(|e| format!("take failed: {e}"))
        })
        .collect();
    let sorted = RecordBatch::try_new(batch.schema(), columns?)
        .map_err(|e| format!("rebuild sorted batch failed: {e}"))?;
    ensure_unique_sort_key(&sorted, sort_key)?;
    Ok(sorted)
}

/// Fail if the (already key-sorted) `batch` has duplicate `sort_key` values.
fn ensure_unique_sort_key(batch: &RecordBatch, sort_key: &str) -> std::result::Result<(), String> {
    for row in 1..batch.num_rows() {
        let prev = render_cell_raw(batch, sort_key, row - 1)?;
        let cur = render_cell_raw(batch, sort_key, row)?;
        let prev_null = column_is_null(batch, sort_key, row - 1)?;
        let cur_null = column_is_null(batch, sort_key, row)?;
        if prev_null == cur_null && prev == cur {
            return Err(format!(
                "duplicate sort key '{sort_key}' value ('{cur}'); row comparison \
                 requires a unique sort key"
            ));
        }
    }
    Ok(())
}

/// Whether the named column's cell at `row` is null.
fn column_is_null(batch: &RecordBatch, col: &str, row: usize) -> std::result::Result<bool, String> {
    let idx = batch
        .schema()
        .index_of(col)
        .map_err(|e| format!("column '{col}' not found: {e}"))?;
    Ok(batch.column(idx).is_null(row))
}

/// Render a single cell as a string via `arrow_cast::display`, mapping a NULL to
/// [`NULL_SENTINEL`] so it cannot be conflated with the empty string `""`.
fn render_cell(batch: &RecordBatch, col: &str, row: usize) -> std::result::Result<String, String> {
    if column_is_null(batch, col, row)? {
        return Ok(NULL_SENTINEL.to_string());
    }
    render_cell_raw(batch, col, row)
}

/// Render a cell verbatim (NULL renders to `""`); used for the uniqueness probe.
fn render_cell_raw(
    batch: &RecordBatch,
    col: &str,
    row: usize,
) -> std::result::Result<String, String> {
    let idx = batch
        .schema()
        .index_of(col)
        .map_err(|e| format!("column '{col}' not found: {e}"))?;
    arrow_cast::display::array_value_to_string(batch.column(idx), row)
        .map_err(|e| format!("render cell col='{col}' row={row} failed: {e}"))
}

/// Validate the output against an exact set of expected rows (full-data check).
fn validate_rows(
    batch: &RecordBatch,
    sort_key: &str,
    columns: &[&str],
    rows: &[&[&str]],
) -> std::result::Result<(), String> {
    let sorted = sort_batch(batch, sort_key)?;
    if sorted.num_rows() != rows.len() {
        return Err(format!(
            "row count mismatch: actual={} expected={}",
            sorted.num_rows(),
            rows.len()
        ));
    }
    for (row_idx, expected_row) in rows.iter().enumerate() {
        if expected_row.len() != columns.len() {
            return Err(format!(
                "expected row {row_idx} has {} cells but {} columns were named",
                expected_row.len(),
                columns.len()
            ));
        }
        for (col_idx, col) in columns.iter().enumerate() {
            let actual = render_cell(&sorted, col, row_idx)?;
            let expected = expected_row[col_idx];
            if actual != expected {
                return Err(format!(
                    "mismatch at row={row_idx} col='{col}': actual='{actual}' expected='{expected}'"
                ));
            }
        }
    }
    Ok(())
}

/// Compare actual reader output against the fixture's gold parquet snapshot.
///
/// Delegates to [`hudi_test::gold`], the single source of truth for gold
/// comparison shared with the cpp consumer tests (sorted by `key`, skips
/// `_hoodie_*` columns, normalizes differing timestamp representations).
fn validate_gold(batch: &RecordBatch, gold_dir: &str) -> std::result::Result<(), String> {
    let gold = hudi_test::gold::read_gold_parquet(gold_dir)?;
    hudi_test::gold::compare_against_gold(batch, &gold)
}

/// Run a case and validate it, returning `Err(message)` on any failure.
pub async fn try_run_case(case: &FgReaderCase) -> std::result::Result<(), String> {
    let table_path = case.fixture.path_to_mor_avro();

    // ErrContains: the read itself must fail with a matching message.
    if let Expected::ErrContains(pattern) = case.expected {
        return match read_case(case, &table_path).await {
            Ok(_) => Err(format!(
                "expected read to fail containing '{pattern}', but it succeeded"
            )),
            Err(e) => {
                let dbg = format!("{e:?}");
                if dbg.contains(pattern) {
                    Ok(())
                } else {
                    Err(format!(
                        "error did not contain '{pattern}'; actual error: {dbg}"
                    ))
                }
            }
        };
    }

    let (batch, stats) = read_case(case, &table_path)
        .await
        .map_err(|e| format!("read failed: {e:?}"))?;

    // Optional exact output-column-names check.
    if let Some(expected_cols) = case.expect_output_columns {
        let schema = batch.schema();
        let actual_cols: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        if actual_cols != expected_cols {
            return Err(format!(
                "output columns mismatch: actual={actual_cols:?} expected={expected_cols:?}"
            ));
        }
    }

    match &case.expected {
        Expected::Rows {
            sort_key,
            columns,
            rows,
        } => validate_rows(&batch, sort_key, columns, rows),
        Expected::GoldParquet => {
            let gold_dir = case.fixture.gold_dir(hudi_test::TableFormat::MorAvro);
            validate_gold(&batch, &gold_dir)
        }
        Expected::Custom { rows, validate } => {
            if batch.num_rows() != *rows {
                Err(format!(
                    "row count mismatch: actual={} expected={rows}",
                    batch.num_rows()
                ))
            } else {
                validate(&batch)
            }
        }
        Expected::ErrContains(_) => unreachable!("handled above"),
    }?;

    // Post-read stats assertion (corrupt/rollback/log-block counters), run only
    // after the data validation above succeeds.
    if let Some(check) = case.expect_stats {
        check(&stats).map_err(|m| format!("read_stats assertion failed: {m}"))?;
    }
    Ok(())
}

/// Run a case, panicking with the case name on failure.
pub async fn run_case(case: FgReaderCase) {
    if let Err(msg) = try_run_case(&case).await {
        panic!("[{}] {msg}", case.name);
    }
}

/// Generate a `#[tokio::test]` that runs a single harness case.
#[macro_export]
macro_rules! fg_case_test {
    ($name:ident, $case:expr) => {
        #[tokio::test]
        async fn $name() {
            $crate::file_group::reader_v2::harness::run_case($case).await;
        }
    };
    // Variant for cases pinned to a tracked gap: the case body stays intact
    // (never weakened) but the test is `#[ignore]`d with the finding as its
    // reason, so `cargo test -- --ignored` still exercises it on demand.
    ($name:ident, $case:expr, ignore = $reason:literal) => {
        #[tokio::test]
        #[ignore = $reason]
        async fn $name() {
            $crate::file_group::reader_v2::harness::run_case($case).await;
        }
    };
}
