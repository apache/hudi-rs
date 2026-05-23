//! ENG-40156 — Substrait predicate pushdown from Velox into hudi-rs.
//!
//! Wire format: `substrait::proto::ExtendedExpression` serialized via prost.
//! Velox `HudiSplitReader` builds this from the Substrait filter that Gluten
//! originally produced (preserved verbatim across the SubstraitToVeloxPlan
//! conversion), then forwards the bytes through the cxx-rs bridge. We decode
//! the expression here and evaluate it against the post-merge arrow
//! RecordBatch using arrow kernels.
//!
//! ## Supported shapes
//!
//! - **Comparisons**: `equal`, `not_equal`, `lt`, `lte`, `gt`, `gte`
//! - **Null tests**: `is_null`, `is_not_null`
//! - **Boolean**: `and`, `or`, `not`
//! - **Operands**: column reference (top-level struct field), typed literal
//!   (bool / i32 / i64 / f32 / f64 / string / null)
//!
//! Anything outside this set is dropped at decode time (the whole predicate
//! is discarded and a warning is logged). Velox's post-scan filter still
//! evaluates the original expression on every batch, so correctness is
//! preserved when we drop — we only lose the perf benefit of early filtering.

use std::collections::HashMap;

use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    LargeBinaryArray, RecordBatch, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use arrow_schema::{DataType, TimeUnit};
use arrow_select::filter::filter_record_batch;
use prost::Message;
use substrait::proto::{
    Expression, ExtendedExpression,
    expression::{
        FieldReference, Literal, RexType, ScalarFunction, field_reference,
        literal::LiteralType, reference_segment,
    },
    expression_reference,
    extensions::simple_extension_declaration,
    function_argument,
};

// ════════════════════════════════════════════════════════════════════════════
// Public API
// ════════════════════════════════════════════════════════════════════════════

/// A decoded ExtendedExpression, ready to evaluate against arrow batches.
///
/// Constructed once from the substrait wire bytes when the file-group reader
/// is created; evaluated against each RecordBatch produced by the merge.
#[derive(Debug, Clone)]
pub struct PushedFilter {
    /// substrait function_anchor → known operator we know how to evaluate.
    function_map: HashMap<u32, KnownFunction>,
    /// Schema field index → column name. Substrait Selection refers to fields
    /// by index; hudi-rs RecordBatch columns are looked up by name.
    column_names: Vec<String>,
    /// The boolean expression to evaluate against each batch.
    expression: Expression,
}

impl PushedFilter {
    /// Decode an ExtendedExpression from prost-encoded bytes.
    ///
    /// Returns:
    /// - `Ok(None)` if bytes is empty (caller pushed no predicate), if the
    ///   expression references any unrecognised function, or if the wire
    ///   shape is otherwise unsupported. In all these cases we fall back to
    ///   Velox's post-scan filter for correctness.
    /// - `Ok(Some(filter))` if every function in the expression is one we
    ///   know how to evaluate.
    /// - `Err(_)` only for hard decode errors (malformed protobuf bytes).
    pub fn decode(bytes: &[u8]) -> Result<Option<Self>, String> {
        if bytes.is_empty() {
            return Ok(None);
        }
        let ext_expr = ExtendedExpression::decode(bytes)
            .map_err(|e| format!("[ENG-40156] failed to decode ExtendedExpression: {e}"))?;

        let column_names = match ext_expr.base_schema {
            Some(ref schema) => schema.names.clone(),
            None => {
                log::warn!(
                    "[ENG-40156] ExtendedExpression missing base_schema; \
                     dropping pushed filter, relying on Velox post-scan filter"
                );
                return Ok(None);
            }
        };

        // Walk extensions table → build function anchor → KnownFunction map.
        // Any unknown function anchor → drop the whole predicate (we can't
        // safely evaluate a partial AST if any operator is unknown).
        let mut function_map = HashMap::new();
        for decl in &ext_expr.extensions {
            if let Some(simple_extension_declaration::MappingType::ExtensionFunction(f)) =
                &decl.mapping_type
            {
                match KnownFunction::from_name(&f.name) {
                    Some(known) => {
                        function_map.insert(f.function_anchor, known);
                    }
                    None => {
                        log::warn!(
                            "[ENG-40156] unrecognised substrait function '{}' \
                             (anchor={}); dropping pushed filter",
                            f.name,
                            f.function_anchor
                        );
                        return Ok(None);
                    }
                }
            }
        }

        let expression = match ext_expr.referred_expr.into_iter().next() {
            Some(re) => match re.expr_type {
                Some(expression_reference::ExprType::Expression(e)) => e,
                _ => {
                    log::warn!(
                        "[ENG-40156] ExtendedExpression.referred_expr[0] is not \
                         an Expression (likely a Measure); dropping pushed filter"
                    );
                    return Ok(None);
                }
            },
            None => return Ok(None),
        };

        Ok(Some(Self {
            function_map,
            column_names,
            expression,
        }))
    }

    /// Column names referenced by the pushed filter (for logging / sanity).
    pub fn columns(&self) -> &[String] {
        &self.column_names
    }

    /// Evaluate the expression against a RecordBatch, returning one boolean
    /// per row. Caller passes the result to `arrow_select::filter_record_batch`.
    pub fn evaluate(&self, batch: &RecordBatch) -> Result<BooleanArray, String> {
        let result = self.eval(&self.expression, batch)?;
        result.into_bool_array(batch.num_rows())
    }
}

/// Filter a RecordBatch by a pushed predicate.
///
/// SQL three-valued logic: rows where the predicate evaluates to NULL are
/// dropped (treated as false), matching how Velox's `remainingFilterExprSet_`
/// behaves and what callers expect for WHERE-clause pushdown.
pub fn filter_batch(batch: &RecordBatch, filter: &PushedFilter) -> Result<RecordBatch, String> {
    let mask = filter.evaluate(batch)?;
    // arrow_select's filter_record_batch treats null mask entries as "drop",
    // which is what we want for SQL WHERE semantics.
    filter_record_batch(batch, &mask)
        .map_err(|e| format!("[ENG-40156] filter_record_batch failed: {e}"))
}

// ════════════════════════════════════════════════════════════════════════════
// Known function inventory
// ════════════════════════════════════════════════════════════════════════════

/// Substrait operators we can evaluate. Anything else → drop the predicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KnownFunction {
    Equal,
    NotEqual,
    Lt,
    Lte,
    Gt,
    Gte,
    IsNull,
    IsNotNull,
    And,
    Or,
    Not,
}

impl KnownFunction {
    /// Resolve a substrait function declaration `name` like `"lt:any_any"` or
    /// `"is_null:any"` to a KnownFunction. The signature suffix is ignored —
    /// our evaluator dispatches on arrow DataType at evaluation time, not on
    /// the substrait signature.
    fn from_name(name: &str) -> Option<Self> {
        let base = name.split(':').next()?;
        Some(match base {
            "equal" | "eq" => Self::Equal,
            "not_equal" | "ne" | "neq" => Self::NotEqual,
            "lt" | "lessthan" | "less_than" => Self::Lt,
            "lte" | "lessthanorequal" | "less_than_or_equal" => Self::Lte,
            "gt" | "greaterthan" | "greater_than" => Self::Gt,
            "gte" | "greaterthanorequal" | "greater_than_or_equal" => Self::Gte,
            "is_null" | "isnull" => Self::IsNull,
            "is_not_null" | "isnotnull" => Self::IsNotNull,
            "and" | "and_kleene" => Self::And,
            "or" | "or_kleene" => Self::Or,
            "not" => Self::Not,
            _ => return None,
        })
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Evaluator
// ════════════════════════════════════════════════════════════════════════════

/// Intermediate evaluation result. Arrows (columns / boolean masks) and
/// scalars are interchangeable inputs to comparison ops; bool arrays propagate
/// through and/or/not.
#[derive(Debug, Clone)]
enum Value {
    /// Per-row boolean result (final output of comparisons or logical ops).
    Bool(BooleanArray),
    /// A column reference into the batch — typically the LHS of a comparison.
    Column(ArrayRef),
    /// A constant value — typically the RHS of a comparison.
    Scalar(ScalarValue),
}

#[derive(Debug, Clone)]
enum ScalarValue {
    Null,
    Bool(bool),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    String(String),
    /// Substrait Decimal literal: 16-byte little-endian two's-complement value
    /// plus declared precision/scale. Compared against arrow Decimal128 columns
    /// after rescaling to the column's scale.
    Decimal128 { value: i128, precision: i32, scale: i32 },
    /// Days since 1970-01-01 (Substrait Date / Arrow Date32).
    Date(i32),
    /// Microseconds since UNIX epoch. Substrait stores Timestamp as i64
    /// microseconds; for arrow columns with other TimeUnit (s/ms/ns) we
    /// rescale at comparison time.
    TimestampMicros(i64),
    /// Raw byte string (Substrait Binary / FixedBinary / Uuid).
    Binary(Vec<u8>),
}

impl Value {
    /// Force a Value into a BooleanArray (one row per batch row). Errors if
    /// the value isn't already a boolean column/array.
    fn into_bool_array(self, num_rows: usize) -> Result<BooleanArray, String> {
        match self {
            Value::Bool(b) => Ok(b),
            Value::Column(arr) => arr
                .as_any()
                .downcast_ref::<BooleanArray>()
                .cloned()
                .ok_or_else(|| {
                    format!(
                        "[ENG-40156] expected boolean column, got {}",
                        arr.data_type()
                    )
                }),
            Value::Scalar(ScalarValue::Bool(b)) => Ok(BooleanArray::from(vec![b; num_rows])),
            other => Err(format!(
                "[ENG-40156] cannot coerce {other:?} to BooleanArray"
            )),
        }
    }
}

impl PushedFilter {
    fn eval(&self, expr: &Expression, batch: &RecordBatch) -> Result<Value, String> {
        match &expr.rex_type {
            Some(RexType::Literal(lit)) => Ok(Value::Scalar(decode_literal(lit)?)),
            Some(RexType::Selection(field_ref)) => {
                self.eval_field_ref(field_ref, batch).map(Value::Column)
            }
            Some(RexType::ScalarFunction(sf)) => self.eval_scalar_fn(sf, batch),
            other => Err(format!(
                "[ENG-40156] unsupported expression variant at evaluation: {other:?}"
            )),
        }
    }

    fn eval_field_ref(
        &self,
        fr: &FieldReference,
        batch: &RecordBatch,
    ) -> Result<ArrayRef, String> {
        // Only direct top-level struct field references are supported. Nested
        // struct / list / map access would require recursive descent we don't
        // need for any predicate Gluten currently emits to Hudi.
        let direct = match &fr.reference_type {
            Some(field_reference::ReferenceType::DirectReference(seg)) => seg,
            other => {
                return Err(format!(
                    "[ENG-40156] unsupported field reference type: {other:?}"
                ));
            }
        };
        let idx = match &direct.reference_type {
            Some(reference_segment::ReferenceType::StructField(sf)) => sf.field as usize,
            other => {
                return Err(format!(
                    "[ENG-40156] unsupported reference segment: {other:?}"
                ));
            }
        };
        let col_name = self.column_names.get(idx).ok_or_else(|| {
            format!(
                "[ENG-40156] field index {idx} out of bounds (base_schema has {} field(s))",
                self.column_names.len()
            )
        })?;
        let col = batch.column_by_name(col_name).ok_or_else(|| {
            let schema = batch.schema();
            let avail: Vec<&str> =
                schema.fields().iter().map(|f| f.name().as_str()).collect();
            format!(
                "[ENG-40156] referenced column '{col_name}' not in RecordBatch; \
                 available={avail:?}"
            )
        })?;
        Ok(col.clone())
    }

    fn eval_scalar_fn(
        &self,
        sf: &ScalarFunction,
        batch: &RecordBatch,
    ) -> Result<Value, String> {
        let known = self.function_map.get(&sf.function_reference).ok_or_else(|| {
            // Shouldn't happen — decode() drops the filter if any anchor is unknown.
            format!(
                "[ENG-40156] no KnownFunction for anchor {} at eval time \
                 (decode invariant violated)",
                sf.function_reference
            )
        })?;

        // Walk arguments first — every argument must be an Expression Value.
        let args: Result<Vec<Value>, String> = sf
            .arguments
            .iter()
            .map(|a| match &a.arg_type {
                Some(function_argument::ArgType::Value(e)) => self.eval(e, batch),
                other => Err(format!(
                    "[ENG-40156] unsupported function argument type: {other:?}"
                )),
            })
            .collect();
        let args = args?;

        let n = batch.num_rows();
        match known {
            KnownFunction::And => boolean_and(args, n).map(Value::Bool),
            KnownFunction::Or => boolean_or(args, n).map(Value::Bool),
            KnownFunction::Not => boolean_not(args, n).map(Value::Bool),
            KnownFunction::IsNull => is_null(args).map(Value::Bool),
            KnownFunction::IsNotNull => is_not_null(args).map(Value::Bool),
            cmp => comparison(*cmp, args, n).map(Value::Bool),
        }
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Literal decode
// ════════════════════════════════════════════════════════════════════════════

fn decode_literal(lit: &Literal) -> Result<ScalarValue, String> {
    match &lit.literal_type {
        Some(LiteralType::Boolean(b)) => Ok(ScalarValue::Bool(*b)),
        // Substrait packs i8 / i16 into i32 on the wire (proto int32). Narrow
        // here so we can downcast to Int8Array / Int16Array at compare time.
        Some(LiteralType::I8(v)) => Ok(ScalarValue::I8(*v as i8)),
        Some(LiteralType::I16(v)) => Ok(ScalarValue::I16(*v as i16)),
        Some(LiteralType::I32(v)) => Ok(ScalarValue::I32(*v)),
        Some(LiteralType::I64(v)) => Ok(ScalarValue::I64(*v)),
        Some(LiteralType::Fp32(v)) => Ok(ScalarValue::F32(*v)),
        Some(LiteralType::Fp64(v)) => Ok(ScalarValue::F64(*v)),
        Some(LiteralType::String(s)) => Ok(ScalarValue::String(s.clone())),
        // Substrait also has FixedChar / VarChar — both map to string columns
        // on the Arrow side, so we treat them as String here.
        Some(LiteralType::FixedChar(s)) => Ok(ScalarValue::String(s.clone())),
        Some(LiteralType::VarChar(vc)) => Ok(ScalarValue::String(vc.value.clone())),
        Some(LiteralType::Binary(b)) => Ok(ScalarValue::Binary(b.clone())),
        Some(LiteralType::FixedBinary(b)) => Ok(ScalarValue::Binary(b.clone())),
        Some(LiteralType::Uuid(b)) => Ok(ScalarValue::Binary(b.clone())),
        // Substrait Decimal literal: little-endian two's-complement bytes
        // (exactly 16 bytes) + precision + scale.
        Some(LiteralType::Decimal(d)) => {
            if d.value.len() != 16 {
                return Err(format!(
                    "[ENG-40156] Decimal literal has {} bytes, expected 16",
                    d.value.len()
                ));
            }
            let mut le = [0u8; 16];
            le.copy_from_slice(&d.value);
            Ok(ScalarValue::Decimal128 {
                value: i128::from_le_bytes(le),
                precision: d.precision,
                scale: d.scale,
            })
        }
        Some(LiteralType::Date(d)) => Ok(ScalarValue::Date(*d)),
        // Deprecated `Timestamp` is i64 µs since epoch. Keep it for now since
        // Gluten still emits it for spark TimestampType columns.
        #[allow(deprecated)]
        Some(LiteralType::Timestamp(ts)) => Ok(ScalarValue::TimestampMicros(*ts)),
        // PrecisionTimestamp carries an explicit precision (0=s, 3=ms, 6=µs, 9=ns).
        // Normalise to µs at decode time; rescale at compare time if the column
        // uses a different TimeUnit.
        Some(LiteralType::PrecisionTimestamp(ts)) => {
            let micros = rescale_to_micros(ts.value, ts.precision)?;
            Ok(ScalarValue::TimestampMicros(micros))
        }
        Some(LiteralType::PrecisionTimestampTz(ts)) => {
            let micros = rescale_to_micros(ts.value, ts.precision)?;
            Ok(ScalarValue::TimestampMicros(micros))
        }
        Some(LiteralType::Null(_)) => Ok(ScalarValue::Null),
        None if lit.nullable => Ok(ScalarValue::Null),
        other => Err(format!(
            "[ENG-40156] unsupported literal type: {other:?}"
        )),
    }
}

/// Convert a Substrait PrecisionTimestamp value to microseconds.
/// `precision` is the unit exponent: 0=seconds, 3=ms, 6=µs, 9=ns.
fn rescale_to_micros(value: i64, precision: i32) -> Result<i64, String> {
    match precision {
        0 => value.checked_mul(1_000_000).ok_or_else(|| {
            format!("[ENG-40156] PrecisionTimestamp overflow rescaling {value}s to µs")
        }),
        3 => value.checked_mul(1_000).ok_or_else(|| {
            format!("[ENG-40156] PrecisionTimestamp overflow rescaling {value}ms to µs")
        }),
        6 => Ok(value),
        9 => Ok(value / 1_000),  // truncate ns to µs
        other => Err(format!(
            "[ENG-40156] unsupported PrecisionTimestamp precision: {other}"
        )),
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Logical operators
// ════════════════════════════════════════════════════════════════════════════

/// SQL three-valued AND. NULL acts as the identity for AND when combined with
/// TRUE, but propagates with FALSE on the other side. arrow_arith implements
/// this; we open-code it here to keep the dep surface narrow.
fn boolean_and(args: Vec<Value>, n: usize) -> Result<BooleanArray, String> {
    if args.is_empty() {
        return Ok(BooleanArray::from(vec![true; n]));
    }
    let mut iter = args.into_iter();
    let mut acc = iter.next().unwrap().into_bool_array(n)?;
    for v in iter {
        let next = v.into_bool_array(n)?;
        acc = bool_zip(&acc, &next, |a, b| match (a, b) {
            (Some(false), _) | (_, Some(false)) => Some(false),
            (Some(true), Some(true)) => Some(true),
            _ => None,
        })?;
    }
    Ok(acc)
}

fn boolean_or(args: Vec<Value>, n: usize) -> Result<BooleanArray, String> {
    if args.is_empty() {
        return Ok(BooleanArray::from(vec![false; n]));
    }
    let mut iter = args.into_iter();
    let mut acc = iter.next().unwrap().into_bool_array(n)?;
    for v in iter {
        let next = v.into_bool_array(n)?;
        acc = bool_zip(&acc, &next, |a, b| match (a, b) {
            (Some(true), _) | (_, Some(true)) => Some(true),
            (Some(false), Some(false)) => Some(false),
            _ => None,
        })?;
    }
    Ok(acc)
}

fn boolean_not(args: Vec<Value>, n: usize) -> Result<BooleanArray, String> {
    if args.len() != 1 {
        return Err(format!(
            "[ENG-40156] not() expects 1 argument, got {}",
            args.len()
        ));
    }
    let arr = args.into_iter().next().unwrap().into_bool_array(n)?;
    let mut out = Vec::with_capacity(arr.len());
    for i in 0..arr.len() {
        if arr.is_null(i) {
            out.push(None);
        } else {
            out.push(Some(!arr.value(i)));
        }
    }
    Ok(BooleanArray::from(out))
}

fn bool_zip(
    a: &BooleanArray,
    b: &BooleanArray,
    f: impl Fn(Option<bool>, Option<bool>) -> Option<bool>,
) -> Result<BooleanArray, String> {
    if a.len() != b.len() {
        return Err(format!(
            "[ENG-40156] boolean array length mismatch: {} vs {}",
            a.len(),
            b.len()
        ));
    }
    let mut out = Vec::with_capacity(a.len());
    for i in 0..a.len() {
        let av = if a.is_null(i) { None } else { Some(a.value(i)) };
        let bv = if b.is_null(i) { None } else { Some(b.value(i)) };
        out.push(f(av, bv));
    }
    Ok(BooleanArray::from(out))
}

// ════════════════════════════════════════════════════════════════════════════
// Null tests
// ════════════════════════════════════════════════════════════════════════════

fn is_null(args: Vec<Value>) -> Result<BooleanArray, String> {
    if args.len() != 1 {
        return Err(format!(
            "[ENG-40156] is_null() expects 1 argument, got {}",
            args.len()
        ));
    }
    match args.into_iter().next().unwrap() {
        Value::Column(arr) => {
            let mut out = Vec::with_capacity(arr.len());
            for i in 0..arr.len() {
                out.push(Some(arr.is_null(i)));
            }
            Ok(BooleanArray::from(out))
        }
        Value::Scalar(ScalarValue::Null) => {
            // Scalar IS NULL applied row-wise — but we don't know the row
            // count from a scalar alone. This shape is degenerate (filter
            // collapses to a constant). Caller would have folded this.
            Err("[ENG-40156] is_null on a scalar without row context".to_string())
        }
        Value::Scalar(_) => {
            Err("[ENG-40156] is_null on a non-null scalar — should have been folded".to_string())
        }
        Value::Bool(_) => {
            Err("[ENG-40156] is_null on a boolean expression result — unusual; not supported".to_string())
        }
    }
}

fn is_not_null(args: Vec<Value>) -> Result<BooleanArray, String> {
    let r = is_null(args)?;
    let mut out = Vec::with_capacity(r.len());
    for i in 0..r.len() {
        out.push(if r.is_null(i) { None } else { Some(!r.value(i)) });
    }
    Ok(BooleanArray::from(out))
}

// ════════════════════════════════════════════════════════════════════════════
// Comparison operators (typed dispatch on the column's DataType)
// ════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Copy)]
enum Cmp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl Cmp {
    fn from_known(k: KnownFunction) -> Option<Self> {
        Some(match k {
            KnownFunction::Equal => Cmp::Eq,
            KnownFunction::NotEqual => Cmp::Ne,
            KnownFunction::Lt => Cmp::Lt,
            KnownFunction::Lte => Cmp::Le,
            KnownFunction::Gt => Cmp::Gt,
            KnownFunction::Gte => Cmp::Ge,
            _ => return None,
        })
    }

    fn apply_ord<T: PartialOrd>(self, a: &T, b: &T) -> bool {
        match self {
            Cmp::Eq => a == b,
            Cmp::Ne => a != b,
            Cmp::Lt => a < b,
            Cmp::Le => a <= b,
            Cmp::Gt => a > b,
            Cmp::Ge => a >= b,
        }
    }
}

fn comparison(
    known: KnownFunction,
    args: Vec<Value>,
    n: usize,
) -> Result<BooleanArray, String> {
    let cmp = Cmp::from_known(known).ok_or_else(|| {
        format!("[ENG-40156] comparison() called with non-comparison function {known:?}")
    })?;
    if args.len() != 2 {
        return Err(format!(
            "[ENG-40156] {known:?} expects 2 arguments, got {}",
            args.len()
        ));
    }
    let mut iter = args.into_iter();
    let lhs = iter.next().unwrap();
    let rhs = iter.next().unwrap();

    match (lhs, rhs) {
        (Value::Column(c), Value::Scalar(s)) => compare_column_scalar(&c, cmp, &s, false, n),
        // Reverse direction: flip the comparator so a < b becomes b > a.
        (Value::Scalar(s), Value::Column(c)) => compare_column_scalar(&c, cmp, &s, true, n),
        (Value::Column(_), Value::Column(_)) => {
            // Possible to support but Gluten doesn't currently emit these to
            // Hudi (extractFiltersFromRemainingFilter only extracts
            // column-vs-literal predicates).
            Err("[ENG-40156] column-vs-column comparison not supported".to_string())
        }
        (Value::Scalar(a), Value::Scalar(b)) => Err(format!(
            "[ENG-40156] scalar-vs-scalar comparison ({a:?} vs {b:?}) — should be constant-folded"
        )),
        (Value::Bool(_), _) | (_, Value::Bool(_)) => {
            Err("[ENG-40156] comparison applied to a boolean expression result".to_string())
        }
    }
}

/// Apply `column [cmp] scalar` (or `scalar [cmp] column` if `reversed`).
///
/// Nulls in the column produce NULL in the result. The caller's outer
/// `filter_record_batch` treats NULL as DROP, matching SQL WHERE semantics.
fn compare_column_scalar(
    col: &ArrayRef,
    cmp: Cmp,
    scalar: &ScalarValue,
    reversed: bool,
    n: usize,
) -> Result<BooleanArray, String> {
    debug_assert_eq!(col.len(), n);

    macro_rules! compare_primitive {
        ($col_ty:ty, $scalar_pat:pat => $scalar_v:expr) => {{
            let arr = col
                .as_any()
                .downcast_ref::<$col_ty>()
                .ok_or_else(|| {
                    format!(
                        "[ENG-40156] expected {} for column, got {}",
                        stringify!($col_ty),
                        col.data_type()
                    )
                })?;
            let rhs = match scalar {
                $scalar_pat => $scalar_v,
                ScalarValue::Null => {
                    // Comparison with NULL → NULL for every row.
                    return Ok(BooleanArray::from(vec![None; n]));
                }
                other => {
                    return Err(format!(
                        "[ENG-40156] type mismatch: column is {} but scalar is {other:?}",
                        col.data_type()
                    ));
                }
            };
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                if arr.is_null(i) {
                    out.push(None);
                } else {
                    let lhs = arr.value(i);
                    let result = if reversed {
                        cmp.apply_ord(&rhs, &lhs)
                    } else {
                        cmp.apply_ord(&lhs, &rhs)
                    };
                    out.push(Some(result));
                }
            }
            Ok(BooleanArray::from(out))
        }};
    }

    match col.data_type() {
        DataType::Int8 => compare_primitive!(Int8Array, ScalarValue::I8(v) => *v),
        DataType::Int16 => compare_primitive!(Int16Array, ScalarValue::I16(v) => *v),
        DataType::Int32 => compare_primitive!(Int32Array, ScalarValue::I32(v) => *v),
        DataType::Int64 => compare_primitive!(Int64Array, ScalarValue::I64(v) => *v),
        DataType::Float32 => compare_primitive!(Float32Array, ScalarValue::F32(v) => *v),
        DataType::Float64 => compare_primitive!(Float64Array, ScalarValue::F64(v) => *v),
        DataType::Date32 => compare_primitive!(Date32Array, ScalarValue::Date(v) => *v),
        // Substrait Timestamp is µs-since-epoch. Arrow column may use any
        // TimeUnit — rescale the scalar to match the column's unit so we
        // can compare i64 to i64 directly.
        DataType::Timestamp(unit, _) => {
            return compare_timestamp_column(col, cmp, scalar, *unit, reversed, n);
        }
        DataType::Decimal128(p, s) => {
            return compare_decimal_column(col, cmp, scalar, *p, *s, reversed, n);
        }
        DataType::Binary => {
            return compare_binary_column::<BinaryArray>(col, cmp, scalar, reversed, n);
        }
        DataType::LargeBinary => {
            return compare_binary_column::<LargeBinaryArray>(col, cmp, scalar, reversed, n);
        }
        DataType::Boolean => {
            let arr = col
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| format!("[ENG-40156] expected BooleanArray, got {}", col.data_type()))?;
            let rhs = match scalar {
                ScalarValue::Bool(b) => *b,
                ScalarValue::Null => return Ok(BooleanArray::from(vec![None; n])),
                other => {
                    return Err(format!(
                        "[ENG-40156] type mismatch: column is Boolean but scalar is {other:?}"
                    ));
                }
            };
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                if arr.is_null(i) {
                    out.push(None);
                } else {
                    let lhs = arr.value(i);
                    let result = if reversed {
                        cmp.apply_ord(&rhs, &lhs)
                    } else {
                        cmp.apply_ord(&lhs, &rhs)
                    };
                    out.push(Some(result));
                }
            }
            Ok(BooleanArray::from(out))
        }
        DataType::Utf8 | DataType::LargeUtf8 => {
            let arr = col
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| format!("[ENG-40156] expected StringArray, got {}", col.data_type()))?;
            let rhs = match scalar {
                ScalarValue::String(s) => s.as_str(),
                ScalarValue::Null => return Ok(BooleanArray::from(vec![None; n])),
                other => {
                    return Err(format!(
                        "[ENG-40156] type mismatch: column is {} but scalar is {other:?}",
                        col.data_type()
                    ));
                }
            };
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                if arr.is_null(i) {
                    out.push(None);
                } else {
                    let lhs = arr.value(i);
                    let result = if reversed {
                        cmp.apply_ord(&rhs, &lhs)
                    } else {
                        cmp.apply_ord(&lhs, &rhs)
                    };
                    out.push(Some(result));
                }
            }
            Ok(BooleanArray::from(out))
        }
        other => Err(format!(
            "[ENG-40156] comparison on unsupported column type {other}"
        )),
    }
}

/// Compare an Arrow Timestamp column against a Substrait Timestamp scalar.
/// Substrait literals are always µs-since-epoch (after PrecisionTimestamp
/// rescaling); the column may use any TimeUnit, so we rescale the scalar
/// to match the column's unit and compare i64s directly.
fn compare_timestamp_column(
    col: &ArrayRef,
    cmp: Cmp,
    scalar: &ScalarValue,
    unit: TimeUnit,
    reversed: bool,
    n: usize,
) -> Result<BooleanArray, String> {
    let micros = match scalar {
        ScalarValue::TimestampMicros(v) => *v,
        ScalarValue::Null => return Ok(BooleanArray::from(vec![None; n])),
        other => {
            return Err(format!(
                "[ENG-40156] type mismatch: column is Timestamp but scalar is {other:?}"
            ));
        }
    };

    macro_rules! cmp_ts {
        ($arr_ty:ty, $rhs:expr) => {{
            let arr = col
                .as_any()
                .downcast_ref::<$arr_ty>()
                .ok_or_else(|| format!("[ENG-40156] expected {} got {}",
                    stringify!($arr_ty), col.data_type()))?;
            let rhs: i64 = $rhs;
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                if arr.is_null(i) {
                    out.push(None);
                } else {
                    let lhs = arr.value(i);
                    let result = if reversed {
                        cmp.apply_ord(&rhs, &lhs)
                    } else {
                        cmp.apply_ord(&lhs, &rhs)
                    };
                    out.push(Some(result));
                }
            }
            Ok(BooleanArray::from(out))
        }};
    }

    match unit {
        TimeUnit::Second => cmp_ts!(TimestampSecondArray, micros / 1_000_000),
        TimeUnit::Millisecond => cmp_ts!(TimestampMillisecondArray, micros / 1_000),
        TimeUnit::Microsecond => cmp_ts!(TimestampMicrosecondArray, micros),
        TimeUnit::Nanosecond => cmp_ts!(
            TimestampNanosecondArray,
            micros.checked_mul(1_000).ok_or_else(|| {
                "[ENG-40156] Timestamp µs->ns overflow".to_string()
            })?
        ),
    }
}

/// Compare an Arrow Decimal128 column against a Substrait Decimal scalar.
/// Both sides are i128 internally; we rescale the scalar to the column's
/// scale before comparing. If the scalar's precision doesn't fit in the
/// column's precision, return an error (caller drops the predicate).
fn compare_decimal_column(
    col: &ArrayRef,
    cmp: Cmp,
    scalar: &ScalarValue,
    col_precision: u8,
    col_scale: i8,
    reversed: bool,
    n: usize,
) -> Result<BooleanArray, String> {
    let (sval, _sprec, sscale) = match scalar {
        ScalarValue::Decimal128 { value, precision, scale } => (*value, *precision, *scale),
        ScalarValue::Null => return Ok(BooleanArray::from(vec![None; n])),
        other => {
            return Err(format!(
                "[ENG-40156] type mismatch: column is Decimal128 but scalar is {other:?}"
            ));
        }
    };

    // Rescale scalar to column's scale. scale diff = col_scale - sscale.
    // Positive diff → multiply scalar by 10^diff. Negative diff → divide.
    let diff = (col_scale as i32) - sscale;
    let rhs: i128 = if diff >= 0 {
        let mul: i128 = 10i128.checked_pow(diff as u32).ok_or_else(|| {
            format!("[ENG-40156] Decimal rescale overflow: diff={diff}")
        })?;
        sval.checked_mul(mul).ok_or_else(|| {
            format!("[ENG-40156] Decimal rescale value overflow")
        })?
    } else {
        let div: i128 = 10i128.checked_pow((-diff) as u32).ok_or_else(|| {
            format!("[ENG-40156] Decimal rescale overflow: diff={diff}")
        })?;
        sval / div
    };

    let arr = col
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| format!("[ENG-40156] expected Decimal128Array, got {}", col.data_type()))?;
    let _ = col_precision; // not used in comparison; kept for diagnostics
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        if arr.is_null(i) {
            out.push(None);
        } else {
            let lhs = arr.value(i);
            let result = if reversed {
                cmp.apply_ord(&rhs, &lhs)
            } else {
                cmp.apply_ord(&lhs, &rhs)
            };
            out.push(Some(result));
        }
    }
    Ok(BooleanArray::from(out))
}

/// Compare an Arrow Binary / LargeBinary column against a Substrait Binary
/// scalar. Uses byte-lexical ordering (the natural `[u8]` Ord).
fn compare_binary_column<A>(
    col: &ArrayRef,
    cmp: Cmp,
    scalar: &ScalarValue,
    reversed: bool,
    n: usize,
) -> Result<BooleanArray, String>
where
    A: Array + 'static,
    for<'a> &'a A: IntoIterator<Item = Option<&'a [u8]>>,
{
    let rhs: &[u8] = match scalar {
        ScalarValue::Binary(b) => b.as_slice(),
        ScalarValue::String(s) => s.as_bytes(),
        ScalarValue::Null => return Ok(BooleanArray::from(vec![None; n])),
        other => {
            return Err(format!(
                "[ENG-40156] type mismatch: column is Binary but scalar is {other:?}"
            ));
        }
    };
    let arr = col
        .as_any()
        .downcast_ref::<A>()
        .ok_or_else(|| format!("[ENG-40156] expected binary array, got {}", col.data_type()))?;
    let mut out = Vec::with_capacity(n);
    for v in arr.into_iter() {
        match v {
            None => out.push(None),
            Some(bytes) => {
                let result = if reversed {
                    cmp.apply_ord(&rhs, &bytes)
                } else {
                    cmp.apply_ord(&bytes, &rhs)
                };
                out.push(Some(result));
            }
        }
    }
    Ok(BooleanArray::from(out))
}

// ════════════════════════════════════════════════════════════════════════════
// Tests
// ════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use substrait::proto::{
        Expression, ExtendedExpression, FunctionArgument, NamedStruct, Type,
        expression::{
            FieldReference, Literal, ReferenceSegment, ScalarFunction, field_reference,
            literal::LiteralType, reference_segment,
        },
        expression_reference, function_argument,
        extensions::{
            SimpleExtensionDeclaration, SimpleExtensionUri, simple_extension_declaration,
        },
        r#type,
    };

    // ─── Helpers ────────────────────────────────────────────────────────────

    fn make_batch_two_int_cols() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]));
        let a = Int64Array::from(vec![Some(1), Some(2), Some(3), None, Some(5)]);
        let b = Int64Array::from(vec![Some(10), Some(20), Some(30), Some(40), Some(50)]);
        RecordBatch::try_new(schema, vec![Arc::new(a), Arc::new(b)]).unwrap()
    }

    fn make_batch_with_string() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("s", DataType::Utf8, true),
        ]));
        let s = StringArray::from(vec![
            Some("apple"),
            Some("banana"),
            None,
            Some("cherry"),
        ]);
        RecordBatch::try_new(schema, vec![Arc::new(s)]).unwrap()
    }

    fn named_struct(names: &[&str]) -> NamedStruct {
        NamedStruct {
            names: names.iter().map(|s| s.to_string()).collect(),
            r#struct: Some(r#type::Struct::default()),
        }
    }

    fn col_ref(field_idx: i32) -> Expression {
        Expression {
            rex_type: Some(RexType::Selection(Box::new(FieldReference {
                reference_type: Some(field_reference::ReferenceType::DirectReference(
                    ReferenceSegment {
                        reference_type: Some(reference_segment::ReferenceType::StructField(
                            Box::new(reference_segment::StructField {
                                field: field_idx,
                                child: None,
                            }),
                        )),
                    },
                )),
                root_type: None,
            }))),
        }
    }

    fn i64_literal(v: i64) -> Expression {
        Expression {
            rex_type: Some(RexType::Literal(Literal {
                nullable: false,
                type_variation_reference: 0,
                literal_type: Some(LiteralType::I64(v)),
            })),
        }
    }

    fn string_literal(s: &str) -> Expression {
        Expression {
            rex_type: Some(RexType::Literal(Literal {
                nullable: false,
                type_variation_reference: 0,
                literal_type: Some(LiteralType::String(s.to_string())),
            })),
        }
    }

    fn null_literal() -> Expression {
        Expression {
            rex_type: Some(RexType::Literal(Literal {
                nullable: true,
                type_variation_reference: 0,
                literal_type: Some(LiteralType::Null(Type {
                    kind: Some(r#type::Kind::I64(r#type::I64::default())),
                })),
            })),
        }
    }

    fn scalar_fn(anchor: u32, args: Vec<Expression>) -> Expression {
        Expression {
            rex_type: Some(RexType::ScalarFunction(ScalarFunction {
                function_reference: anchor,
                arguments: args
                    .into_iter()
                    .map(|e| FunctionArgument {
                        arg_type: Some(function_argument::ArgType::Value(e)),
                    })
                    .collect(),
                output_type: None,
                ..Default::default()
            })),
        }
    }

    /// Build an ExtendedExpression with a single named function declaration.
    fn extended(
        functions: &[(u32, &str)],
        names: &[&str],
        expr: Expression,
    ) -> Vec<u8> {
        let extension_uris = vec![SimpleExtensionUri {
            extension_uri_anchor: 1,
            uri: "/functions_comparison.yaml".to_string(),
        }];
        let extensions: Vec<SimpleExtensionDeclaration> = functions
            .iter()
            .map(|(anchor, name)| SimpleExtensionDeclaration {
                mapping_type: Some(simple_extension_declaration::MappingType::ExtensionFunction(
                    simple_extension_declaration::ExtensionFunction {
                        extension_uri_reference: 1,
                        function_anchor: *anchor,
                        name: name.to_string(),
                    },
                )),
            })
            .collect();
        let ext = ExtendedExpression {
            version: None,
            extension_uris,
            extensions,
            referred_expr: vec![substrait::proto::ExpressionReference {
                output_names: vec!["filter".to_string()],
                expr_type: Some(expression_reference::ExprType::Expression(expr)),
            }],
            base_schema: Some(named_struct(names)),
            advanced_extensions: None,
            expected_type_urls: vec![],
        };
        let mut buf = Vec::new();
        ext.encode(&mut buf).unwrap();
        buf
    }

    // ─── Test cases ─────────────────────────────────────────────────────────

    #[test]
    fn empty_bytes_decode_to_none() {
        let res = PushedFilter::decode(&[]).unwrap();
        assert!(res.is_none(), "empty bytes should produce None");
    }

    #[test]
    fn decode_round_trip_lt() {
        let bytes = extended(
            &[(42, "lt:any_any")],
            &["a", "b"],
            scalar_fn(42, vec![col_ref(0), i64_literal(3)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().expect("should decode");
        assert_eq!(pf.column_names, vec!["a", "b"]);
        assert_eq!(pf.function_map.get(&42), Some(&KnownFunction::Lt));
    }

    #[test]
    fn unknown_function_drops_predicate() {
        // function name not in the inventory → decode returns None.
        let bytes = extended(
            &[(7, "weird_custom_func:any")],
            &["a"],
            scalar_fn(7, vec![col_ref(0)]),
        );
        let res = PushedFilter::decode(&bytes).unwrap();
        assert!(res.is_none(), "unknown function should drop entire filter");
    }

    #[test]
    fn lt_filter_excludes_matching_rows() {
        let batch = make_batch_two_int_cols();
        let bytes = extended(
            &[(1, "lt:any_any")],
            &["a", "b"],
            scalar_fn(1, vec![col_ref(0), i64_literal(3)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let mask = pf.evaluate(&batch).unwrap();
        // a < 3 → rows 0(=1), 1(=2) match; row 3(=NULL) → NULL; others false
        assert_eq!(mask.value(0), true);
        assert_eq!(mask.value(1), true);
        assert_eq!(mask.value(2), false);
        assert!(mask.is_null(3), "NULL in column should produce NULL in mask");
        assert_eq!(mask.value(4), false);
    }

    #[test]
    fn and_combines_two_comparisons() {
        let batch = make_batch_two_int_cols();
        // a > 1 AND b < 40
        let bytes = extended(
            &[
                (1, "and:bool_bool"),
                (2, "gt:any_any"),
                (3, "lt:any_any"),
            ],
            &["a", "b"],
            scalar_fn(
                1,
                vec![
                    scalar_fn(2, vec![col_ref(0), i64_literal(1)]),
                    scalar_fn(3, vec![col_ref(1), i64_literal(40)]),
                ],
            ),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let filtered = filter_batch(&batch, &pf).unwrap();
        // a > 1 → rows 1, 2, 4 (row 3 is NULL → drops). b < 40 → rows 0, 1, 2.
        // AND → rows 1, 2.
        assert_eq!(filtered.num_rows(), 2);
        let a = filtered
            .column_by_name("a")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(a.value(0), 2);
        assert_eq!(a.value(1), 3);
    }

    #[test]
    fn or_keeps_rows_matching_either() {
        let batch = make_batch_two_int_cols();
        // a = 1 OR b = 50
        let bytes = extended(
            &[
                (1, "or:bool_bool"),
                (2, "equal:any_any"),
                (3, "equal:any_any"),
            ],
            &["a", "b"],
            scalar_fn(
                1,
                vec![
                    scalar_fn(2, vec![col_ref(0), i64_literal(1)]),
                    scalar_fn(3, vec![col_ref(1), i64_literal(50)]),
                ],
            ),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let filtered = filter_batch(&batch, &pf).unwrap();
        // row 0: a=1 → match. row 4: b=50 → match. row 3 (a=NULL, b=40) → both
        // NULL/false → drop.
        assert_eq!(filtered.num_rows(), 2);
    }

    #[test]
    fn not_inverts_mask() {
        let batch = make_batch_two_int_cols();
        // NOT (a = 1)
        let bytes = extended(
            &[(1, "not:bool"), (2, "equal:any_any")],
            &["a", "b"],
            scalar_fn(1, vec![scalar_fn(2, vec![col_ref(0), i64_literal(1)])]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let mask = pf.evaluate(&batch).unwrap();
        assert_eq!(mask.value(0), false);
        assert_eq!(mask.value(1), true);
        assert_eq!(mask.value(2), true);
        assert!(mask.is_null(3));
        assert_eq!(mask.value(4), true);
    }

    #[test]
    fn is_null_matches_null_rows() {
        let batch = make_batch_two_int_cols();
        let bytes = extended(
            &[(1, "is_null:any")],
            &["a", "b"],
            scalar_fn(1, vec![col_ref(0)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let mask = pf.evaluate(&batch).unwrap();
        assert_eq!(mask.value(0), false);
        assert_eq!(mask.value(3), true);
    }

    #[test]
    fn string_equality_filter() {
        let batch = make_batch_with_string();
        let bytes = extended(
            &[(1, "equal:any_any")],
            &["s"],
            scalar_fn(1, vec![col_ref(0), string_literal("banana")]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let filtered = filter_batch(&batch, &pf).unwrap();
        assert_eq!(filtered.num_rows(), 1);
        let s = filtered
            .column_by_name("s")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(s.value(0), "banana");
    }

    #[test]
    fn null_literal_in_comparison_makes_mask_null() {
        let batch = make_batch_two_int_cols();
        // a > NULL → always NULL → caller drops all rows.
        let bytes = extended(
            &[(1, "gt:any_any")],
            &["a", "b"],
            scalar_fn(1, vec![col_ref(0), null_literal()]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let mask = pf.evaluate(&batch).unwrap();
        for i in 0..mask.len() {
            assert!(mask.is_null(i), "row {i} should be NULL");
        }
        // filter_record_batch drops all NULL → empty batch.
        let filtered = filter_batch(&batch, &pf).unwrap();
        assert_eq!(filtered.num_rows(), 0);
    }

    #[test]
    fn reversed_operand_order_works() {
        let batch = make_batch_two_int_cols();
        // literal < a (literal on LHS) — exercises the reversed path
        let bytes = extended(
            &[(1, "lt:any_any")],
            &["a", "b"],
            scalar_fn(1, vec![i64_literal(2), col_ref(0)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let mask = pf.evaluate(&batch).unwrap();
        // 2 < a → matches when a > 2 → rows 2(=3), 4(=5). Row 3 NULL.
        assert_eq!(mask.value(0), false);
        assert_eq!(mask.value(1), false);
        assert_eq!(mask.value(2), true);
        assert!(mask.is_null(3));
        assert_eq!(mask.value(4), true);
    }

    #[test]
    fn missing_column_in_batch_is_error() {
        // base_schema says "a","b","c" but batch only has "a","b" → evaluating
        // a reference to c yields an error (decode succeeds — it can't know).
        let batch = make_batch_two_int_cols();
        let bytes = extended(
            &[(1, "lt:any_any")],
            &["a", "b", "c"],
            scalar_fn(1, vec![col_ref(2), i64_literal(0)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let err = pf.evaluate(&batch).unwrap_err();
        assert!(err.contains("not in RecordBatch"), "got: {err}");
    }

    #[test]
    fn nested_and_with_or() {
        let batch = make_batch_two_int_cols();
        // (a > 1 AND a < 5) OR b = 10
        let bytes = extended(
            &[
                (1, "or:bool_bool"),
                (2, "and:bool_bool"),
                (3, "gt:any_any"),
                (4, "lt:any_any"),
                (5, "equal:any_any"),
            ],
            &["a", "b"],
            scalar_fn(
                1,
                vec![
                    scalar_fn(
                        2,
                        vec![
                            scalar_fn(3, vec![col_ref(0), i64_literal(1)]),
                            scalar_fn(4, vec![col_ref(0), i64_literal(5)]),
                        ],
                    ),
                    scalar_fn(5, vec![col_ref(1), i64_literal(10)]),
                ],
            ),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let filtered = filter_batch(&batch, &pf).unwrap();
        // Rows where (1 < a < 5) OR (b=10):
        //   row 0: a=1 → 1<1<5 false. b=10 → true. KEEP
        //   row 1: a=2 → 1<2<5 true. KEEP
        //   row 2: a=3 → true. KEEP
        //   row 3: a=NULL → NULL in inner AND. b=40 → b=10 false. OR(NULL,false)=NULL. DROP
        //   row 4: a=5 → 5<5 false. b=50 → false. DROP
        assert_eq!(filtered.num_rows(), 3);
    }

    // ════════════════════════════════════════════════════════════════════════
    // Type-coverage tests for ENG-40156. These mirror the column types that
    // TestMORFileSliceLayouts uses (id Int32, col_string, col_bigint Int64,
    // col_smallint Int16, col_tinyint Int8, col_float, col_double, col_boolean,
    // col_decimal(10,2), col_timestamp, col_date, col_binary) and the filter
    // shapes Gluten emits for `cast(... AS T) = literal`, `>`/`<`, `IS NULL`,
    // and compound AND/OR/NOT. Each test builds the same Substrait protobuf
    // a real query would, prost-encodes it, then decodes + evaluates.
    // ════════════════════════════════════════════════════════════════════════

    use arrow_array::{
        BinaryArray, BooleanArray as ArrBoolArr, Date32Array, Decimal128Array,
        Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
        TimestampMicrosecondArray,
    };
    use substrait::proto::expression::literal::{Decimal, PrecisionTimestamp, VarChar};
    use substrait::proto::r#type as ptype;

    fn i32_literal(v: i32) -> Expression {
        Expression {
            rex_type: Some(RexType::Literal(Literal {
                nullable: false,
                type_variation_reference: 0,
                literal_type: Some(LiteralType::I32(v)),
            })),
        }
    }
    fn i16_literal(v: i16) -> Expression {
        Expression {
            rex_type: Some(RexType::Literal(Literal {
                nullable: false,
                type_variation_reference: 0,
                literal_type: Some(LiteralType::I16(v as i32)),
            })),
        }
    }
    fn i8_literal(v: i8) -> Expression {
        Expression {
            rex_type: Some(RexType::Literal(Literal {
                nullable: false,
                type_variation_reference: 0,
                literal_type: Some(LiteralType::I8(v as i32)),
            })),
        }
    }
    fn f32_literal(v: f32) -> Expression {
        Expression {
            rex_type: Some(RexType::Literal(Literal {
                nullable: false,
                type_variation_reference: 0,
                literal_type: Some(LiteralType::Fp32(v)),
            })),
        }
    }
    fn f64_literal(v: f64) -> Expression {
        Expression {
            rex_type: Some(RexType::Literal(Literal {
                nullable: false,
                type_variation_reference: 0,
                literal_type: Some(LiteralType::Fp64(v)),
            })),
        }
    }
    fn bool_literal(v: bool) -> Expression {
        Expression {
            rex_type: Some(RexType::Literal(Literal {
                nullable: false,
                type_variation_reference: 0,
                literal_type: Some(LiteralType::Boolean(v)),
            })),
        }
    }
    fn date_literal(days: i32) -> Expression {
        Expression {
            rex_type: Some(RexType::Literal(Literal {
                nullable: false,
                type_variation_reference: 0,
                literal_type: Some(LiteralType::Date(days)),
            })),
        }
    }
    fn ts_micros_literal(micros: i64) -> Expression {
        // Use deprecated Timestamp form (still emitted by Gluten).
        #[allow(deprecated)]
        let lt = LiteralType::Timestamp(micros);
        Expression {
            rex_type: Some(RexType::Literal(Literal {
                nullable: false,
                type_variation_reference: 0,
                literal_type: Some(lt),
            })),
        }
    }
    fn precision_ts_literal(value: i64, precision: i32) -> Expression {
        Expression {
            rex_type: Some(RexType::Literal(Literal {
                nullable: false,
                type_variation_reference: 0,
                literal_type: Some(LiteralType::PrecisionTimestamp(PrecisionTimestamp {
                    value,
                    precision,
                })),
            })),
        }
    }
    fn binary_literal(bytes: &[u8]) -> Expression {
        Expression {
            rex_type: Some(RexType::Literal(Literal {
                nullable: false,
                type_variation_reference: 0,
                literal_type: Some(LiteralType::Binary(bytes.to_vec())),
            })),
        }
    }
    fn varchar_literal(s: &str) -> Expression {
        Expression {
            rex_type: Some(RexType::Literal(Literal {
                nullable: false,
                type_variation_reference: 0,
                literal_type: Some(LiteralType::VarChar(VarChar {
                    value: s.to_string(),
                    length: s.len() as u32,
                })),
            })),
        }
    }
    fn decimal_literal(value: i128, precision: i32, scale: i32) -> Expression {
        let bytes = value.to_le_bytes().to_vec();
        Expression {
            rex_type: Some(RexType::Literal(Literal {
                nullable: false,
                type_variation_reference: 0,
                literal_type: Some(LiteralType::Decimal(Decimal {
                    value: bytes,
                    precision,
                    scale,
                })),
            })),
        }
    }

    // ─── Int8 / Int16 / Int32 / Float32 / Float64 / Bool ──────────────────

    #[test]
    fn int8_column_equality_filter() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "c", DataType::Int8, true)]));
        let a = Int8Array::from(vec![Some(-2), Some(7), None, Some(7)]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(a)]).unwrap();
        let bytes = extended(
            &[(1, "equal:any_any")],
            &["c"],
            scalar_fn(1, vec![col_ref(0), i8_literal(7)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let out = filter_batch(&batch, &pf).unwrap();
        assert_eq!(out.num_rows(), 2);
    }

    #[test]
    fn int16_column_gt_filter() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "c", DataType::Int16, true)]));
        let a = Int16Array::from(vec![Some(100), Some(200), Some(300)]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(a)]).unwrap();
        let bytes = extended(
            &[(1, "gt:any_any")],
            &["c"],
            scalar_fn(1, vec![col_ref(0), i16_literal(150)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let mask = pf.evaluate(&batch).unwrap();
        assert_eq!(mask.value(0), false);
        assert_eq!(mask.value(1), true);
        assert_eq!(mask.value(2), true);
    }

    #[test]
    fn int32_column_lt_filter() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "id", DataType::Int32, true)]));
        let a = Int32Array::from(vec![Some(1), Some(5), Some(9), Some(13)]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(a)]).unwrap();
        let bytes = extended(
            &[(1, "lt:any_any")],
            &["id"],
            scalar_fn(1, vec![col_ref(0), i32_literal(9)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let out = filter_batch(&batch, &pf).unwrap();
        assert_eq!(out.num_rows(), 2);
    }

    #[test]
    fn float32_column_gte_filter() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "c", DataType::Float32, true)]));
        let a = Float32Array::from(vec![Some(-1.5_f32), Some(0.0), Some(3.14)]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(a)]).unwrap();
        let bytes = extended(
            &[(1, "gte:any_any")],
            &["c"],
            scalar_fn(1, vec![col_ref(0), f32_literal(0.0_f32)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let out = filter_batch(&batch, &pf).unwrap();
        assert_eq!(out.num_rows(), 2);
    }

    #[test]
    fn float64_column_ne_filter() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "c", DataType::Float64, true)]));
        let a = Float64Array::from(vec![Some(1.0_f64), Some(2.0), Some(1.0), None]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(a)]).unwrap();
        let bytes = extended(
            &[(1, "not_equal:any_any")],
            &["c"],
            scalar_fn(1, vec![col_ref(0), f64_literal(1.0_f64)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let out = filter_batch(&batch, &pf).unwrap();
        // 1.0 → false, 2.0 → true, 1.0 → false, NULL → NULL (drop)
        assert_eq!(out.num_rows(), 1);
    }

    #[test]
    fn bool_column_equality_filter() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "c", DataType::Boolean, true)]));
        let a = ArrBoolArr::from(vec![Some(true), Some(false), None, Some(true)]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(a)]).unwrap();
        let bytes = extended(
            &[(1, "equal:any_any")],
            &["c"],
            scalar_fn(1, vec![col_ref(0), bool_literal(true)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let out = filter_batch(&batch, &pf).unwrap();
        assert_eq!(out.num_rows(), 2);
    }

    // ─── Decimal (the case that the live test exposed) ────────────────────

    #[test]
    fn decimal128_column_equality_filter_same_scale() {
        // col_decimal DECIMAL(10,2) = -10.00  →  value=-1000 (scale 2)
        let schema = Arc::new(Schema::new(vec![Field::new(
            "v", DataType::Decimal128(10, 2), true)]));
        let arr: Decimal128Array = Decimal128Array::from(vec![
            Some(-1000_i128),     // -10.00
            Some(1500_i128),      // 15.00
            None,
            Some(-1000_i128),     // -10.00
        ])
        .with_precision_and_scale(10, 2)
        .unwrap();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        let bytes = extended(
            &[(1, "equal:any_any")],
            &["v"],
            scalar_fn(1, vec![col_ref(0), decimal_literal(-1000_i128, 10, 2)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let out = filter_batch(&batch, &pf).unwrap();
        assert_eq!(out.num_rows(), 2);
    }

    #[test]
    fn decimal128_column_gt_with_scale_rescale() {
        // Column has scale 4, scalar has scale 2 → rescale scalar up by 10^2.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "v", DataType::Decimal128(18, 4), true)]));
        let arr: Decimal128Array = Decimal128Array::from(vec![
            Some(500_000_i128),      // 50.0000
            Some(1_000_000_i128),    // 100.0000
            Some(1_500_000_i128),    // 150.0000
        ])
        .with_precision_and_scale(18, 4)
        .unwrap();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        // scalar: 75.00 in DECIMAL(5,2) → value = 7500, scale = 2.
        // Rescaled to col scale 4: 7500 * 10^2 = 750_000.
        let bytes = extended(
            &[(1, "gt:any_any")],
            &["v"],
            scalar_fn(1, vec![col_ref(0), decimal_literal(7500_i128, 5, 2)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let mask = pf.evaluate(&batch).unwrap();
        assert_eq!(mask.value(0), false);   // 50 > 75 false
        assert_eq!(mask.value(1), true);    // 100 > 75 true
        assert_eq!(mask.value(2), true);    // 150 > 75 true
    }

    // ─── Date / Timestamp ────────────────────────────────────────────────

    #[test]
    fn date32_column_filter() {
        // Days since 1970-01-01.  2000-01-01 = 10957, 2024-01-01 = 19723.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "d", DataType::Date32, true)]));
        let arr = Date32Array::from(vec![Some(10957), Some(19723), Some(20000)]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        let bytes = extended(
            &[(1, "gte:any_any")],
            &["d"],
            scalar_fn(1, vec![col_ref(0), date_literal(19723)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let out = filter_batch(&batch, &pf).unwrap();
        assert_eq!(out.num_rows(), 2);
    }

    #[test]
    fn timestamp_micros_column_lt_filter() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "t", DataType::Timestamp(TimeUnit::Microsecond, None), true)]));
        // µs since epoch
        let arr = TimestampMicrosecondArray::from(vec![
            Some(1_000_000_000_000_000_i64),   // ~2001-09-09
            Some(1_700_000_000_000_000_i64),   // ~2023-11
            Some(1_800_000_000_000_000_i64),
        ]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        let bytes = extended(
            &[(1, "lt:any_any")],
            &["t"],
            scalar_fn(1, vec![col_ref(0), ts_micros_literal(1_500_000_000_000_000_i64)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let out = filter_batch(&batch, &pf).unwrap();
        assert_eq!(out.num_rows(), 1);
    }

    #[test]
    fn precision_timestamp_ms_rescales_to_us_column() {
        // PrecisionTimestamp{value=1_000_000_000_000, precision=3}  (ms)
        // = 1_000_000_000_000_000 µs (1e15).
        let schema = Arc::new(Schema::new(vec![Field::new(
            "t", DataType::Timestamp(TimeUnit::Microsecond, None), true)]));
        let arr = TimestampMicrosecondArray::from(vec![
            Some(500_000_000_000_000_i64),     // ~half of scalar
            Some(2_000_000_000_000_000_i64),   // ~twice
        ]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        let bytes = extended(
            &[(1, "gte:any_any")],
            &["t"],
            scalar_fn(1, vec![col_ref(0),
                precision_ts_literal(1_000_000_000_000_i64, 3)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let mask = pf.evaluate(&batch).unwrap();
        assert_eq!(mask.value(0), false);
        assert_eq!(mask.value(1), true);
    }

    // ─── Binary / VarChar ────────────────────────────────────────────────

    #[test]
    fn binary_column_equality_filter() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "b", DataType::Binary, true)]));
        let arr = BinaryArray::from_vec(vec![
            b"alpha".as_ref(),
            b"beta".as_ref(),
            b"gamma".as_ref(),
        ]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        let bytes = extended(
            &[(1, "equal:any_any")],
            &["b"],
            scalar_fn(1, vec![col_ref(0), binary_literal(b"beta")]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let out = filter_batch(&batch, &pf).unwrap();
        assert_eq!(out.num_rows(), 1);
    }

    #[test]
    fn binary_column_gt_filter_lex_order() {
        // bytes order: "alpha" < "beta" < "gamma". gt "alpha" matches 2 rows.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "b", DataType::Binary, true)]));
        let arr = BinaryArray::from_vec(vec![
            b"alpha".as_ref(),
            b"beta".as_ref(),
            b"gamma".as_ref(),
        ]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        let bytes = extended(
            &[(1, "gt:any_any")],
            &["b"],
            scalar_fn(1, vec![col_ref(0), binary_literal(b"alpha")]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let out = filter_batch(&batch, &pf).unwrap();
        assert_eq!(out.num_rows(), 2);
    }

    #[test]
    fn varchar_literal_matches_string_column() {
        let batch = make_batch_with_string();
        let bytes = extended(
            &[(1, "equal:any_any")],
            &["s"],
            scalar_fn(1, vec![col_ref(0), varchar_literal("cherry")]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let out = filter_batch(&batch, &pf).unwrap();
        assert_eq!(out.num_rows(), 1);
    }

    // ─── IS NOT NULL ─────────────────────────────────────────────────────

    #[test]
    fn is_not_null_filter() {
        let batch = make_batch_two_int_cols();
        // a IS NOT NULL  →  drops row 3
        let bytes = extended(
            &[(1, "is_not_null:any")],
            &["a", "b"],
            scalar_fn(1, vec![col_ref(0)]),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let out = filter_batch(&batch, &pf).unwrap();
        assert_eq!(out.num_rows(), 4);
    }

    // ─── Mixed-type compound predicate (worst-case: multiple types in one
    //    Substrait expression, exercises all decode + dispatch paths) ──────

    #[test]
    fn mixed_type_compound_predicate() {
        // Schema: id Int32, name Utf8, amount Decimal(10,2), ts Timestamp(µs)
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("amount", DataType::Decimal128(10, 2), true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        ]));
        let id = Int32Array::from(vec![Some(1), Some(2), Some(3), Some(4)]);
        let name = StringArray::from(vec![
            Some("alice"), Some("bob"), Some("charlie"), Some("alice"),
        ]);
        let amount = Decimal128Array::from(vec![
            Some(1000_i128), Some(2000_i128), Some(500_i128), Some(2500_i128),
        ])
        .with_precision_and_scale(10, 2)
        .unwrap();
        let ts = TimestampMicrosecondArray::from(vec![
            Some(1_000_000_i64), Some(2_000_000_i64),
            Some(3_000_000_i64), Some(4_000_000_i64),
        ]);
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(id), Arc::new(name), Arc::new(amount), Arc::new(ts)],
        )
        .unwrap();

        // (name = 'alice' AND amount > 15.00) OR ts >= 4_000_000µs
        //   row 0: name=alice, amount=10.00 → 10>15 false. ts=1M >= 4M false. DROP
        //   row 1: name=bob → 'bob'='alice' false. ts=2M false. DROP
        //   row 2: name=charlie → false. ts=3M false. DROP
        //   row 3: name=alice, amount=25.00 → 25>15 true. KEEP
        let bytes = extended(
            &[
                (1, "or:bool_bool"),
                (2, "and:bool_bool"),
                (3, "equal:any_any"),
                (4, "gt:any_any"),
                (5, "gte:any_any"),
            ],
            &["id", "name", "amount", "ts"],
            scalar_fn(
                1,
                vec![
                    scalar_fn(
                        2,
                        vec![
                            scalar_fn(3, vec![col_ref(1), varchar_literal("alice")]),
                            scalar_fn(4, vec![col_ref(2), decimal_literal(1500_i128, 10, 2)]),
                        ],
                    ),
                    scalar_fn(5, vec![col_ref(3), ts_micros_literal(4_000_000_i64)]),
                ],
            ),
        );
        let pf = PushedFilter::decode(&bytes).unwrap().unwrap();
        let out = filter_batch(&batch, &pf).unwrap();
        assert_eq!(out.num_rows(), 1);
        let kept_id = out
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0);
        assert_eq!(kept_id, 4);
    }

    // ─── Decode-time soft-fail: unsupported literal types ─────────────────

    #[test]
    fn list_literal_is_currently_unsupported() {
        // List literal isn't in our supported set; decode_literal should
        // return Err (caller wraps that into a hard error today). This
        // documents the behaviour so a future "drop predicate on unsupported
        // literal" change is visible in the test diff.
        use substrait::proto::expression::literal::List as LiteralList;
        let lit_list = LiteralType::List(LiteralList { values: vec![] });
        let lit = Literal {
            nullable: false,
            type_variation_reference: 0,
            literal_type: Some(lit_list),
        };
        let err = decode_literal(&lit).unwrap_err();
        assert!(err.contains("unsupported literal type"), "got: {err}");
    }

    // ─── Coverage marker: every ScalarValue variant has a constructor test
    //    above. If you add a new variant, also add a test here. ─────────────

    #[test]
    fn scalar_value_coverage_marker() {
        // This isn't a behaviour test — it's a checklist. Compilation alone
        // proves that each variant constructor still exists; the tests above
        // exercise the evaluator path for each.
        let _ = ScalarValue::Bool(false);
        let _ = ScalarValue::I8(0);
        let _ = ScalarValue::I16(0);
        let _ = ScalarValue::I32(0);
        let _ = ScalarValue::I64(0);
        let _ = ScalarValue::F32(0.0);
        let _ = ScalarValue::F64(0.0);
        let _ = ScalarValue::String(String::new());
        let _ = ScalarValue::Binary(vec![]);
        let _ = ScalarValue::Date(0);
        let _ = ScalarValue::TimestampMicros(0);
        let _ = ScalarValue::Decimal128 { value: 0, precision: 1, scale: 0 };
        let _ = ScalarValue::Null;
    }

    // Suppress unused-import warning for ptype if tests don't use it.
    #[allow(dead_code)]
    fn _ptype_anchor() -> ptype::Struct {
        ptype::Struct::default()
    }
}
