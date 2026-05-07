# keyFilterOpt port — design spec

**Date:** 2026-05-07
**Owner:** Davis Zhang
**Source repo:** `/home/ubuntu/ws3/hudi-rs`
**Reference (Java):** `/home/ubuntu/ws3/hudi-internal`
**Call-stack reference:** `/home/ubuntu/operations/tasks/quantonMORScanSupport/scanSpecPushDown/Tasks/phase3_rust_fg_integration/migrateFilterOptCode/readerContext_callstack.md`

## 1. Goals

Bring hudi-rs's `ReaderContext` to parity with Java's `HoodieReaderContext.keyFilterOpt`. The work has three goals:

1. Port the entire `org.apache.hudi.expression` package (Predicate hierarchy) into hudi-rs.
2. Port the `org.apache.hudi.internal.schema.{Type, Types}` Type system the Predicate hierarchy depends on. Schema-evolution code (`InternalSchema`, `InternalSchemaBuilder`, `AvroInternalSchemaConverter`, schema-evolution actions/visitors, `SchemaChangeUtils`, etc.) is **out of scope**.
3. Add a `key_filter_opt: Option<Arc<dyn Predicate>>` field on hudi-rs's `ReaderContext` and wire it through the file-group reader so a key-based In-predicate actually filters output rows end-to-end on a v9 MOR + COMMIT_TIME_ORDERING table.

The principle is faithful structural mirroring: same Rust class, same fn name, same arg order, same observable side-effect as Java, except where Rust's type system forces a deviation (called out explicitly in §6).

## 2. Boundaries

In-scope:
- Spark-only assumption.
- Table version 9, MOR, COMMIT_TIME_ORDERING.
- Predicate hierarchy (Predicate, Expression, Predicates with all 12 inner classes — TrueExpression, FalseExpression, And, Or, Not, BinaryComparison, In, IsNull, IsNotNull, StringStartsWith, StringStartsWithAny, StringContains; Literal, NameReference, BinaryExpression, LeafExpression, ExpressionVisitor, BoundReference, BindVisitor, PartialBindVisitor, Comparators, ArrayData, StructLike).
- `Type` / `Types` full port from `internal.schema`.

Out-of-scope:
- FFI / `FfiReaderContext` integration.
- Schema evolution (no `InternalSchema`, `AvroInternalSchemaConverter`, no schema-change actions/visitors, no `SchemaChangeUtils`).
- Engines other than Spark (no Flink/Trino/Hive ReaderContext changes).
- Merge modes other than COMMIT_TIME_ORDERING.
- `HoodieAvroReaderContext.getFileRecordIterator` HFile-supporting branch (no HFile reader in hudi-rs).
- `ReusableKeyBasedRecordBuffer` (metadata-table buffer).
- Per-record `Predicate.eval(StructLike)` evaluation against scan-time rows. The hierarchy ports `eval` so unit tests pass and behavior matches Java, but the e2e reader path uses Arrow `filter_record_batch` for the actual filtering.

## 3. Phases

The work lands in three independently-reviewable commits.

| Phase | Deliverable | Verification |
|------|------|------|
| 1 | Port `org.apache.hudi.expression` (Predicate hierarchy) + full port of `org.apache.hudi.internal.schema.{Type, Types}`. **No reader integration yet.** | Unit tests on each Predicate / Type, plus structural pattern-match tests (`predicates::in_(...)` produces `predicates::In` with the right children). |
| 2 | Add `key_filter_opt: Option<Arc<dyn Predicate>>` on `ReaderContext`. Add `KeySpec` enum + `create_key_spec()` (Java input/output preserved; body uses Rust `match` on `PredicateKind` instead of Java's `instanceof`/cast — see §6 deviation #2). Apply the filter at two call sites: (a) `merged_log_record_reader::perform_scan` → KeySpec → narrow log scan; (b) `HoodieFileGroupReader::make_base_file_batches` → row filter on base parquet (mirroring `HoodieAvroReaderContext.getFileRecordIterator`'s key-extraction path). | Unit tests on `create_key_spec`, plus per-site tests, plus a manual cross-check pass against `readerContext_callstack.md` confirming each call site listed there has a 1:1 hudi-rs equivalent. The cross-check produces a checklist in the Phase 2 PR description that names specific Rust `file:line` for each Java line. |
| 3 | One e2e smoke test on `v9_mor_8i4u_commit_time` reading a single file group with and without `Predicates.In(_hoodie_record_key, [k])`. Compare row count + row content. | Single integration test in `crates/core/tests/file_group_reader_tests.rs`. |

Phase 1 is purely additive — it compiles standalone and adds two new top-level modules. Phase 2 wires the field in but defaults to `None` so all existing tests keep passing. Phase 3 demonstrates the end-to-end contract.

## 4. Phase 1 — file layout & module structure

Two new module trees under `crates/core/src/`:

```
crates/core/src/
├── internal_schema/                ← NEW (mirrors org.apache.hudi.internal.schema)
│   ├── mod.rs
│   ├── type_.rs                    ← `Type` trait + Type::PrimitiveType / Type::NestedType (Type.java in full)
│   └── types.rs                    ← all concrete types from Types.java (full port)
│
└── expression/                     ← NEW (mirrors org.apache.hudi.expression)
    ├── mod.rs
    ├── expression.rs               ← Expression trait + Operator enum
    ├── predicate.rs                ← Predicate trait + PredicateKind enum
    ├── leaf_expression.rs          ← LeafExpression trait
    ├── binary_expression.rs        ← BinaryExpression
    ├── literal.rs                  ← Literal + LiteralValue enum
    ├── name_reference.rs           ← NameReference
    ├── bound_reference.rs          ← BoundReference
    ├── struct_like.rs              ← StructLike trait
    ├── array_data.rs               ← ArrayData
    ├── comparators.rs              ← Comparators::for_type
    ├── expression_visitor.rs       ← ExpressionVisitor trait
    ├── bind_visitor.rs             ← BindVisitor
    ├── partial_bind_visitor.rs     ← PartialBindVisitor
    └── predicates.rs               ← Predicates factory + 12 inner classes (TrueExpression, FalseExpression, And, Or, Not, BinaryComparison, In, IsNull, IsNotNull, StringStartsWith, StringStartsWithAny, StringContains). One file mirrors Java's single-file shape.
```

`expression.rs` also defines an `ExpressionKind<'a>` enum (a borrowed view) alongside the `Expression` trait. `predicate.rs` defines `PredicateKind<'a>` alongside `Predicate`. See §4.1 for the rationale.

Naming choices:

- Java package `org.apache.hudi.expression` → Rust `expression` (snake_case).
- Java package `org.apache.hudi.internal.schema` → Rust `internal_schema`.
- Java `Predicates.In` → Rust `predicates::In` (struct in module). Factory functions `predicates::in_(...)`, `predicates::and(...)`, `predicates::eq(...)`, etc. (`in` is a Rust keyword, hence the trailing underscore.)
- Polymorphism: Java `interface Expression` becomes Rust `trait Expression: Send + Sync + Debug` (with a `kind() -> ExpressionKind<'_>` accessor — see §4.1). The 12 Predicate variants become concrete structs implementing `Predicate: Expression` (with `kind() -> PredicateKind<'_>`). Anywhere Java passes `Expression` or `Predicate`, Rust uses `Box<dyn Expression>` / `Arc<dyn Predicate>`.
- Java `Comparator<T>` returned by `Comparators` becomes a Rust `fn(&LiteralValue, &LiteralValue) -> std::cmp::Ordering` returned by `Comparators::for_type(&Type)`.
- Visitor pattern: Java `ExpressionVisitor<T>` → Rust trait with one method per `visit*` overload. `BindVisitor` and `PartialBindVisitor` become structs implementing `ExpressionVisitor<Box<dyn Expression>>`.

### 4.1 Pattern-match accessors (`PredicateKind` / `ExpressionKind`)

The Predicate hierarchy is a *closed* set of variants in Hudi (12 Predicate types, 4 Expression non-Predicate types: Literal, NameReference, BoundReference, plus Predicate-as-Expression). Rust handles closed variant sets best with pattern matching, not `Any::downcast_ref`. To keep the Java-faithful trait hierarchy *and* give downstream Rust code an idiomatic inspection point, we add two `kind()` accessors that return borrowed enum views:

```rust
pub trait Predicate: Expression + Send + Sync + Debug {
    /// Mirrors Java `Predicate.getOperator()`.
    fn operator(&self) -> Operator;
    /// Borrowed enum view for pattern matching.
    fn kind(&self) -> PredicateKind<'_>;
}

pub enum PredicateKind<'a> {
    True,
    False,
    And(&'a predicates::And),
    Or(&'a predicates::Or),
    Not(&'a predicates::Not),
    BinaryComparison(&'a predicates::BinaryComparison),
    In(&'a predicates::In),
    IsNull(&'a predicates::IsNull),
    IsNotNull(&'a predicates::IsNotNull),
    StringStartsWith(&'a predicates::StringStartsWith),
    StringStartsWithAny(&'a predicates::StringStartsWithAny),
    StringContains(&'a predicates::StringContains),
}

pub trait Expression: Send + Sync + Debug {
    fn data_type(&self) -> &Type;
    fn kind(&self) -> ExpressionKind<'_>;
    // … other Java-faithful methods
}

pub enum ExpressionKind<'a> {
    Literal(&'a Literal),
    NameReference(&'a NameReference),
    BoundReference(&'a BoundReference),
    Predicate(&'a dyn Predicate),
}
```

Each concrete struct's `kind()` is one line: `fn kind(&self) -> PredicateKind<'_> { PredicateKind::In(self) }`. Adding a new variant means adding one enum variant + one `kind()` method, and the compiler points at every match site that needs to handle it.

`Literal` uses a `LiteralValue` enum for its value field:

```rust
pub struct Literal {
    value: LiteralValue,
    data_type: Type,
}

pub enum LiteralValue {
    Bool(bool),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Date(i32),
    Time(i64),
    Timestamp(i64),
    TimestampMillis(i64),
    LocalTimestampMillis(i64),
    LocalTimestampMicros(i64),
    Decimal { unscaled: i128, precision: u8, scale: u8 },
    String(String),
    Binary(Vec<u8>),
    Fixed(Vec<u8>),
    UUID([u8; 16]),
    Null,
}
```

This is the one place where Rust shape diverges from Java (Java has `Literal<T>(T value, Type type)`). The reason is a chain: `kind()` returns `ExpressionKind::Literal(&Literal)` — to extract the value via pattern matching without a second `downcast_ref`, the value field has to be a closed enum. Behavior is identical; the type-parameter shape is the only deviation.

Crate exports: both `internal_schema` and `expression` are re-exported from `crates/core/src/lib.rs` so downstream code can import at `hudi_core::expression::Predicate` / `hudi_core::internal_schema::Type`.

No changes to `crates/core/src/expr/filter.rs` and its callers — that is the file-pruning predicate path and remains separate per the explicit decision to not converge the two abstractions.

## 5. Phase 2 — keyFilterOpt integration call sites

### 5.1 Field on `ReaderContext`

```rust
// crates/core/src/file_group/reader/reader_context.rs
pub struct ReaderContext {
    // … existing fields …
    /// Mirrors Java `HoodieReaderContext.keyFilterOpt`.
    /// Constructor-set, never mutated.
    pub key_filter_opt: Option<Arc<dyn Predicate>>,
}

impl ReaderContext {
    /// Mirrors Java `HoodieReaderContext.getKeyFilterOpt()`.
    pub fn get_key_filter_opt(&self) -> Option<Arc<dyn Predicate>> { … }
}
```

`ReaderContext::empty()` and the FFI bridge initialize `key_filter_opt = None`, matching Java's `Option.empty()` default in `BaseSparkInternalRowReaderContext`.

### 5.2 Call site A — log scan

New file `crates/core/src/file_group/reader/key_spec.rs`:

```rust
pub enum KeySpec {
    /// Mirrors Java FullKeySpec — exact-match key list (from Predicates::In).
    FullKeys(Vec<String>),
    /// Mirrors Java PrefixKeySpec — prefix list (from Predicates::StringStartsWithAny).
    PrefixKeys(Vec<String>),
}

/// Mirrors `HoodieMergedLogRecordReader.createKeySpec(Option<Predicate>)`
/// (HoodieMergedLogRecordReader.java lines 109-126), implemented in idiomatic
/// Rust via `PredicateKind` pattern matching instead of Java's `instanceof`/cast.
pub fn create_key_spec(filter: Option<&dyn Predicate>) -> Option<KeySpec> {
    match filter?.kind() {
        PredicateKind::In(p) => {
            Some(KeySpec::FullKeys(string_literals(p.right_children())?))
        }
        PredicateKind::StringStartsWithAny(p) => {
            Some(KeySpec::PrefixKeys(string_literals(p.right_children())?))
        }
        _ => None,
    }
}

/// Extract `String` values from a list of `Literal(LiteralValue::String(_))`
/// expressions. Returns `None` if any child is not a String literal.
fn string_literals(exprs: &[Box<dyn Expression>]) -> Option<Vec<String>> {
    exprs.iter().map(|e| match e.kind() {
        ExpressionKind::Literal(lit) => match lit.value() {
            LiteralValue::String(s) => Some(s.clone()),
            _ => None,
        },
        _ => None,
    }).collect()
}
```

The Rust match-on-`kind()` is the structural equivalent of Java's `if (filter.get().getOperator() == Expression.Operator.IN) { ... } else if (filter.get().getOperator() == Expression.Operator.STARTS_WITH) { ... }` plus the cast on the next line. Same input, same output, idiomatic Rust on the inside.

`merged_log_record_reader::perform_scan`:

- Drops the existing TODO ("KeySpec filtering not yet implemented in Rust").
- Calls `create_key_spec(reader_context.get_key_filter_opt())`.
- Threads the resulting `Option<KeySpec>` into `BaseHoodieLogRecordReader::scan_internal(key_spec_opt, skip_processing_blocks)`.

`BaseHoodieLogRecordReader::scan_internal` signature changes from `scan_internal(skip: bool)` to `scan_internal(key_spec_opt: Option<KeySpec>, skip: bool)`. All existing non-`perform_scan` call sites pass `None` for `key_spec_opt`. Inside `scan_internal`, the per-record processing in `process_data_block` / `process_delete_block` consults `KeySpec` and skips records whose key isn't in the FullKeys set / doesn't have a matching prefix in PrefixKeys — mirroring Java `BaseHoodieLogRecordReader.scanInternal` filtering.

### 5.3 Call site B — base file

In `HoodieFileGroupReader::make_base_file_batches`, after the parquet read returns its batches, apply a row-level filter when `reader_context.key_filter_opt.is_some()`. The filter materializes the keys/prefixes via the same `create_key_spec` (or directly extracts `Predicates::In::right_children` / `Predicates::StringStartsWithAny::right_children`) and uses Arrow `arrow::compute::filter_record_batch` against the `_hoodie_record_key` column. This mirrors Java's "fall through to row-level filter when reader doesn't `supportKeyPredicate`" — we always go through that path because Parquet has no native key predicate.

The shared key-extraction helper lives on `key_spec.rs` so both sites use the same code.

### 5.4 No `HoodieFileGroupReader::Builder` changes

No new builder method — `key_filter_opt` is set on `ReaderContext` *before* the reader is constructed, exactly mirroring Java where `keyFilterOpt` is a constructor arg of `HoodieReaderContext`, not of `HoodieFileGroupReader`.

### 5.5 Manual cross-check (Phase 2 verification)

As part of the Phase 2 PR, walk through `readerContext_callstack.md` line by line and produce a checklist in the PR description: for each `readerContext.*` line in the doc that touches `keyFilterOpt` directly or indirectly, confirm hudi-rs has the same call in the same Rust class/function with the same args. Specifically the doc's §3.1 (eager-during-construction stack) and §3.2 (lazy `getClosableIterator` stack).

Each verification line must name the specific Rust `file:line`, not just "yes, equivalent".

## 6. Deviations from Java structure

These are the only places where the Rust port intentionally differs from Java. Each is called out so future readers don't get confused:

1. **`Literal` is non-generic; value is a `LiteralValue` enum.** Java has `Literal<T>(T value, Type type)`; Rust has `Literal { value: LiteralValue, data_type: Type }`. The reason is the `kind()`-driven inspection pattern (deviation #2): when downstream code matches `ExpressionKind::Literal(lit)` and then needs to read the underlying value, a closed `LiteralValue` enum lets it pattern-match directly without a second `downcast_ref`. Behavior is identical; only the type-parameter shape differs.
2. **`kind() -> {Predicate,Expression}Kind<'_>` accessors.** Not present in Java. Added in Rust because the canonical Rust idiom for inspecting a closed set of variants is enum pattern matching, not `Any::downcast_ref` on a trait object. Each concrete struct's `kind()` is one line returning `Self`-as-enum-variant. Lets `create_key_spec` (and any future inspection code) be a clean `match` instead of a chain of `instanceof`-style downcasts. The trait hierarchy itself stays Java-faithful — `kind()` is pure inspection sugar that returns borrowed references into the existing concrete types.
3. **`Predicate` ownership.** Java passes `Predicate` references; Rust uses `Arc<dyn Predicate>` so predicates can be cloned into ReaderContext, scanners, and buffers without lifetime juggling. Concrete impls must avoid `Rc` (the trait bound `Send + Sync` enforces this).
4. **`Comparator<T>` return.** Java `Comparators::forType` returns `Comparator<T>`; Rust returns `fn(&LiteralValue, &LiteralValue) -> std::cmp::Ordering`. Same dispatch table, different language idiom.
5. **`expr/filter.rs` is not converged.** Java has only one expression abstraction. Rust keeps `Filter` (the existing flat file-pruning struct) separate from the new `Predicate` hierarchy, since they serve different purposes (file pruning vs key filtering) and converging them is out of scope.
6. **`BindVisitor` / `PartialBindVisitor` ported but not wired.** They produce `BoundReference`s from `NameReference`s given a schema; nothing in keyFilterOpt's reader-side consumption needs binding (we extract literal values directly). They are ported for completeness and hierarchy faithfulness.

## 7. Phase 3 — E2E smoke test

**Location:** `crates/core/tests/file_group_reader_tests.rs` (existing file, add one new `#[tokio::test]`).

**Fixture:** `QuickstartTripsTable::V9Mor8I4UCommitTime`. Pick partition `city=sf` which has one updated id (id=1) and one not-updated id (id=2). With both base + log content, this is the smallest interesting file group.

**Two reads on the same file group:**

```rust
#[tokio::test]
async fn fg_reader_with_key_filter_filters_rows() -> Result<()> {
    let table_path = QuickstartTripsTable::V9Mor8I4UCommitTime.url_to_mor_avro();
    let (configs, storage) = create_configs_and_storage(&table_path).await?;
    let (partition, base_file, log_files) = locate_sf_file_group(&storage).await?;

    // ── Read 1: no filter (baseline) ────────────────────────────────
    let baseline_ctx = build_reader_context(/* key_filter_opt = */ None, &configs);
    let baseline_batch = run_reader(baseline_ctx, …).await?;
    assert_eq!(baseline_batch.num_rows(), 2);  // id=1 (V2 from log) + id=2 (from base)

    // Look up the actual _hoodie_record_key string for id=1 from the baseline
    // batch so the filter literal matches whatever the keygen produced.
    let key_for_id1 = lookup_record_key(&baseline_batch, 1);

    // ── Read 2: filter with In(_hoodie_record_key, [<key for id=1>]) ───────
    //  Construction mirrors Java: Predicates.in(NameReference("_hoodie_record_key"),
    //                                            [Literal.from(key_for_id1)])
    let key_filter: Arc<dyn Predicate> = Arc::new(predicates::in_(
        Box::new(NameReference::new("_hoodie_record_key")),
        vec![Box::new(Literal::string(key_for_id1.clone()))],
    ));
    let filtered_ctx = build_reader_context(Some(key_filter), &configs);
    let filtered_batch = run_reader(filtered_ctx, …).await?;
    assert_eq!(filtered_batch.num_rows(), 1);

    // ── Cross-validate: the filtered row equals the matching baseline row.
    let expected = extract_row_with_id(&baseline_batch, 1);
    let actual = extract_row_with_id(&filtered_batch, 1);
    assert_eq!(expected, actual);

    // Negative assertion: id=2 (base-only) is NOT in filtered output.
    assert!(extract_row_with_id_opt(&filtered_batch, 2).is_none());

    Ok(())
}
```

**Helper functions** added to the same test file (no shared test util churn):

- `locate_sf_file_group(&storage)` — finds the latest base-file + log-files for partition `city=sf`. Reuses existing `Table::file_slices(...)` if available, otherwise walks `.hoodie/timeline`.
- `build_reader_context(key_filter_opt, configs)` — constructs `ReaderContext` with `merge_mode=COMMIT_TIME_ORDERING`, `latest_commit_time` from configs, partition path = `city=sf`, populated `table_config`, and the requested `key_filter_opt`.
- `run_reader(ctx, …)` — wraps the existing `read_file_group` helper but takes a pre-built `ReaderContext`.
- `lookup_record_key(batch, id) -> String` — pulls the `_hoodie_record_key` string for a given `id` from the baseline batch. Avoids hard-coding the keygen encoding (`id:1` vs `1` vs `id=1`).
- `extract_row_with_id` / `extract_row_with_id_opt` — small Arrow helpers comparing row tuples.

**One test, not multiple variants.** This is a smoke test for the integration. Comprehensive Predicate / KeySpec coverage is in Phase 1 + Phase 2 unit tests.

## 8. Risks and assumptions

1. **Record-key encoding ambiguity.** The exact `_hoodie_record_key` string format for `V9Mor8I4UCommitTime` is determined at test-write time by reading one baseline row. If the fixture is regenerated with a different key generator, the literal in the test breaks — kept tight by deriving the literal from the baseline read via `lookup_record_key`, not by hard-coding it.
2. **`BaseHoodieLogRecordReader::scan_internal` signature change** breaks any internal callers. Audit all call sites at Phase 2 implementation time and update each to pass `None` for the new `key_spec_opt` parameter.
3. **Manual cross-check discipline.** The walkthrough against `readerContext_callstack.md` is valuable but only if each verification line names a specific Rust `file:line`. Vague "yes, equivalent" entries defeat the point.
4. **Open assumption — populate_meta_fields.** The v9 MOR fixture is assumed to populate `_hoodie_record_key`. If virtual keys are in play we'd need to filter on the actual key field name; we'll verify when writing the test by inspecting the baseline batch.
5. **Trait-object hygiene.** `Arc<dyn Predicate>` requires concrete impls to be `Send + Sync` and avoid `Rc`. Standard Rust trait-object hygiene; called out so reviewers don't get surprised by `Rc<…>` showing up later.

---

## Appendix A — Phase 2 cross-check vs readerContext_callstack.md

This appendix maps each Java `readerContext.*` interaction in
`/home/ubuntu/operations/tasks/quantonMORScanSupport/scanSpecPushDown/Tasks/phase3_rust_fg_integration/migrateFilterOptCode/readerContext_callstack.md`
to its hudi-rs file:line equivalent, validating that Phase 2 of the
keyFilterOpt port preserves structural parity for the keyFilterOpt
flow.

**Date of cross-check:** 2026-05-07
**Phase 2 SHA range:** `08c5ff7` .. `021960f`

Commits in range (oldest → newest):
- `3780b8b` — add KeySpec + create_key_spec for keyFilterOpt log-scan path
- `40a5d87` — add key_filter_opt field to ReaderContext
- `540ff15` — scan_internal accepts Option<KeySpec>; buffer applies key-spec filter
- `86ff53e` — wire create_key_spec into HoodieMergedLogRecordReader::perform_scan
- `021960f` — apply keyFilterOpt as row filter on base parquet batches

### keyFilterOpt-relevant interactions (must match)

| # | Java site | Rust file:line equivalent | Notes |
|---|-----------|---------------------------|-------|
| 1 | `HoodieReaderContext.keyFilterOpt` field declaration (`HoodieReaderContext.java:81`) | `crates/core/src/file_group/reader/reader_context.rs:90` — `pub key_filter_opt: Option<Arc<dyn Predicate>>` | Direct structural equivalent. Java uses `Option<Predicate>` (Avro/OSS abstraction); Rust uses `Option<Arc<dyn Predicate>>`. Arc needed for trait-object sharing across async call sites. |
| 2 | `HoodieReaderContext.getKeyFilterOpt()` (`HoodieReaderContext.java:221-222`) | `crates/core/src/file_group/reader/reader_context.rs:175` — `pub fn get_key_filter_opt(&self) -> Option<Arc<dyn Predicate>>` | Method signature and semantics match. Returns a clone of the Arc (cheap) rather than a borrow, to avoid lifetime entanglement with callers. |
| 3 | `HoodieReaderContext` ctor sets `keyFilterOpt = Option.empty()` for Spark FileFormat path (`BaseSparkInternalRowReaderContext`) | `crates/core/src/file_group/reader/reader_context.rs:206` — `ReaderContext::empty()` initializes `key_filter_opt: None`. FFI bridge at `cpp/context.rs` excluded per spec §2. | Initialisation to `None` matches Java's `Option.empty()`. The Java ctor receiving a non-empty value (incremental scan path) maps to callers explicitly setting `key_filter_opt` on the struct before passing it into the reader. |
| 4 | `HoodieMergedLogRecordReader.createKeySpec(Option<Predicate>)` (`HoodieMergedLogRecordReader.java:109-126`) | `crates/core/src/file_group/reader/key_spec.rs:44` — `pub fn create_key_spec(filter: Option<&dyn Predicate>) -> Option<KeySpec>` and `key_spec.rs:71` — `pub fn create_key_spec_from_arc(filter: &Option<Arc<dyn Predicate>>) -> Option<KeySpec>` | Java uses `instanceof` + cast (`Predicate instanceof In`, `instanceof StringStartsWith`) to route to `FullKeys` vs `PrefixKeys`. Rust uses `pred_kind()` pattern match (`PredicateKind::In`, `PredicateKind::StringStartsWithAny`) — same routing logic, idiomatic Rust. Spec §6 deviation #2 documents this. Non-`In`/non-`StringStartsWithAny` predicates return `None` in both Java and Rust. |
| 5 | `HoodieMergedLogRecordReader.performScan` calls `createKeySpec(readerContext.getKeyFilterOpt())` then `scanInternal(keySpec, false)` (`HoodieMergedLogRecordReader.java:95-107`) | `crates/core/src/file_group/reader/merged_log_record_reader.rs:133` — `async fn perform_scan` calls `create_key_spec_from_arc(&self.base.reader_context.key_filter_opt)` at line 137-139, then `self.base.scan_internal(key_spec_opt, false).await` at line 141 | Structural parity: same two-step pattern (derive KeySpec from filter, then call scan_internal). Rust accesses `key_filter_opt` directly on the struct (no getter call needed since perform_scan is in the same module); Java goes through `getKeyFilterOpt()`. Equivalent. |
| 6 | `BaseHoodieLogRecordReader.scanInternal(Option<KeySpec>, boolean)` filters records by KeySpec inside per-record processing (`BaseHoodieLogRecordReader.java`) | `crates/core/src/file_group/reader/log_record_reader.rs:376` — `pub async fn scan_internal(&mut self, key_spec_opt: Option<KeySpec>, skip_processing_blocks: bool)`. Propagates to buffer at line 395 via `self.record_buffer.set_key_spec(self.key_spec_opt.clone())`. Buffer applies filter at `crates/core/src/file_group/reader/buffer/key_based.rs:235-236` (`process_data_block`, `if !spec.matches(&key) { continue }`) and `key_based.rs:307-308` (`process_delete_block`) | Full structural parity: `scan_internal` stores `key_spec_opt` on self (line 382), propagates to buffer (line 395), buffer applies `spec.matches(&key)` gate per record in both data-block (line 235) and delete-block (line 307) paths. Java's equivalent is threaded through `KeyBasedFileGroupRecordBuffer.processDataBlock` / `processDeleteBlock`. |
| 7 | `HoodieAvroReaderContext.getFileRecordIterator` lines 218-228 (key-extract path, fall-through row filter for non-HFile readers) | `crates/core/src/file_group/reader/mod.rs:498` — `apply_key_filter_to_batch` called from `make_base_file_batches` (defined at line 456). `apply_key_filter_to_batch` itself at `mod.rs:745` | Java's HFile branch is out of scope (no HFile reader in hudi-rs); Rust always takes the row-filter fall-through path. `apply_key_filter_to_batch` calls `create_key_spec(key_filter_opt)` then row-filters the Arrow `RecordBatch` on `_hoodie_record_key` — same semantics as Java's per-row `recordKey` extraction + `keyFilterOpt.test(recordKey)`. The `make_base_file_batches` → `apply_key_filter_to_batch` chain at line 342/456 mirrors Java's `makeBaseFileIterator` → `getFileRecordIterator`. |

**Summary:** No gaps found. All 7 keyFilterOpt-relevant Java interactions have confirmed Rust `file:line` equivalents. The two design deviations noted in spec §6 (Arc wrapping, `pred_kind()` dispatch vs `instanceof`) are intentional and preserve semantics.

### Other readerContext interactions (informational — outside Phase 2 scope)

The table below accounts for all non-keyFilterOpt `readerContext.*` call sites listed in §3.1, §3.2, and §5 of the call-stack doc. "Equivalent" means hudi-rs has a structurally corresponding field/method; "no equivalent" means the concept does not exist in hudi-rs (typically engine-specific Spark plumbing). No audit depth is warranted for Phase 2.

| Java call-stack site | Rust equivalent? | Where |
|---|---|---|
| `readerContext.setHasLogFiles(...)` / `getHasLogFiles()` | Equivalent | `ReaderContext.has_log_files: bool` (`reader_context.rs:61`) |
| `readerContext.getRecordContext().setPartitionPath(...)` | Equivalent | `ReaderContext.record_context.partition_path` (`reader_context.rs:79`; set via `rebuild_record_context`) |
| `readerContext.initRecordMerger(props)` | No equivalent | hudi-rs uses DataFusion merge pipeline, not a Java `HoodieRecordMerger` |
| `readerContext.setTablePath(tablePath)` | Equivalent | `ReaderContext.table_path: String` (`reader_context.rs:58`) |
| `readerContext.setLatestCommitTime(latestCommitTime)` | Equivalent | `ReaderContext.latest_commit_time: String` (`reader_context.rs:59`) |
| `readerContext.setShouldMergeUseRecordPosition(...)` | Equivalent | `ReaderContext.should_merge_use_record_position: bool` (`reader_context.rs:64`) |
| `readerContext.setHasBootstrapBaseFile(...)` | Equivalent | `ReaderContext.has_bootstrap_base_file: bool` (`reader_context.rs:62`) |
| `readerContext.setSchemaHandler(...)` | Equivalent | `ReaderContext.schema_handler: FileGroupReaderSchemaHandler` (`reader_context.rs:84`) |
| `readerContext.getSchemaHandler().getOutputConverter()` | No equivalent | Spark-specific `outputConverter`; hudi-rs uses Arrow `RecordBatch` throughout |
| `readerContext.getMergeMode()` | Equivalent | `ReaderContext.merge_mode: String` (`reader_context.rs:67`) |
| `readerContext.getRecordContext().seal(rec)` | No equivalent | Spark InternalRow lifecycle concept; hudi-rs works with owned `RecordBatch` rows |
| `readerContext.getIteratorMode()` / `setIteratorMode(...)` | Equivalent | `ReaderContext.iterator_mode: String` (`reader_context.rs:66`) |
| `readerContext.getInstantRange()` + `applyInstantRangeFilter` | Equivalent | `ReaderContext.instant_range: Option<InstantRange>` (`reader_context.rs:69`); filter applied in file-group reader pipeline |
| `readerContext.getFileRecordIterator(...)` (StoragePathInfo / StoragePath branches) | Equivalent | `make_base_file_batches` at `mod.rs:456`; reads base Parquet via DataFusion |
| `readerContext.getRecordContext().convertPartitionValueToEngineType(...)` | No equivalent | Spark partition value type conversion; hudi-rs handles via Arrow schema directly |
| `readerContext.mergeBootstrapReaders(...)` | No equivalent | Bootstrap merge is not implemented in hudi-rs Phase 2 scope |
| `readerContext.getStorageConfiguration()` | No equivalent | JVM Hadoop `Configuration`; hudi-rs uses `object_store` + filesystem path directly |
| `readerContext.getRecordContext().constructFinalHoodieRecord(...)` | No equivalent | Spark `HoodieRecord` wrapping; hudi-rs returns `RecordBatch` directly |
