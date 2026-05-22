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
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
    RecordBatch, StringArray,
};
use arrow_schema::DataType;
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
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    String(String),
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
        Some(LiteralType::I32(v)) => Ok(ScalarValue::I32(*v)),
        Some(LiteralType::I64(v)) => Ok(ScalarValue::I64(*v)),
        Some(LiteralType::Fp32(v)) => Ok(ScalarValue::F32(*v)),
        Some(LiteralType::Fp64(v)) => Ok(ScalarValue::F64(*v)),
        Some(LiteralType::String(s)) => Ok(ScalarValue::String(s.clone())),
        Some(LiteralType::Null(_)) => Ok(ScalarValue::Null),
        None if lit.nullable => Ok(ScalarValue::Null),
        other => Err(format!(
            "[ENG-40156] unsupported literal type: {other:?}"
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
        DataType::Int32 => compare_primitive!(Int32Array, ScalarValue::I32(v) => *v),
        DataType::Int64 => compare_primitive!(Int64Array, ScalarValue::I64(v) => *v),
        DataType::Float32 => compare_primitive!(Float32Array, ScalarValue::F32(v) => *v),
        DataType::Float64 => compare_primitive!(Float64Array, ScalarValue::F64(v) => *v),
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
}
