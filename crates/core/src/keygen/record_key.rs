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
//! Record key encoding shared by the writer and the virtual-key reader, so a
//! key derived at read time equals the one a Java writer stored.

use arrow::array::{
    Array, BooleanArray, Date32Array, Decimal128Array, Decimal256Array, Int8Array, Int16Array,
    Int32Array, Int64Array, LargeStringArray, StringArray, StringViewArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::DataType;
use chrono::NaiveDate;

use crate::Result;
use crate::error::CoreError;

/// Java `KeyGenerator.NULL_RECORDKEY_PLACEHOLDER`.
pub(crate) const NULL_RECORDKEY_PLACEHOLDER: &str = "__null__";
/// Java `KeyGenerator.EMPTY_RECORDKEY_PLACEHOLDER`.
pub(crate) const EMPTY_RECORDKEY_PLACEHOLDER: &str = "__empty__";
/// Java `KeyGenerator.DEFAULT_COLUMN_VALUE_SEPARATOR`.
const COLUMN_VALUE_SEPARATOR: char = ':';
/// Java `KeyGenerator.DEFAULT_RECORD_KEY_PARTS_SEPARATOR`.
const RECORD_KEY_PARTS_SEPARATOR: char = ',';

/// Render a record key field column as the Spark and Avro key generators do:
/// `toString` of the value after `HoodieAvroUtils.convertValueForAvroLogicalTypes`.
/// Nulls stay null. A date renders as its ISO `LocalDate` and a decimal as
/// `BigDecimal.toString`. Flink's `RowDataKeyGen` renders dates as the epoch
/// day and decimals with `toPlainString`, so a Flink-written table keyed on
/// those types does not round-trip.
///
/// Every other type is [`CoreError::Unsupported`]: Java writers store
/// timestamp keys in several forms (raw epoch count, JVM-zone
/// `java.sql.Timestamp`, epoch millis) that the table does not record, and
/// float keys depend on the JDK's `Double.toString` digits.
pub(crate) fn render_key_field(column: &dyn Array, field: &str) -> Result<StringArray> {
    macro_rules! render {
        ($array:ty, $render:expr) => {
            downcast::<$array>(column, field)?
                .iter()
                .map(|value| value.map($render))
                .collect::<Vec<Option<String>>>()
        };
    }
    let rendered = match column.data_type() {
        DataType::Utf8 => return Ok(downcast::<StringArray>(column, field)?.clone()),
        DataType::LargeUtf8 => render!(LargeStringArray, str::to_string),
        DataType::Utf8View => render!(StringViewArray, str::to_string),
        DataType::Boolean => render!(BooleanArray, |v: bool| v.to_string()),
        DataType::Int8 => render!(Int8Array, |v: i8| v.to_string()),
        DataType::Int16 => render!(Int16Array, |v: i16| v.to_string()),
        DataType::Int32 => render!(Int32Array, |v: i32| v.to_string()),
        DataType::Int64 => render!(Int64Array, |v: i64| v.to_string()),
        DataType::UInt8 => render!(UInt8Array, |v: u8| v.to_string()),
        DataType::UInt16 => render!(UInt16Array, |v: u16| v.to_string()),
        DataType::UInt32 => render!(UInt32Array, |v: u32| v.to_string()),
        DataType::UInt64 => render!(UInt64Array, |v: u64| v.to_string()),
        DataType::Decimal128(_, scale) => {
            render!(Decimal128Array, |v: i128| java_big_decimal(
                &v.to_string(),
                *scale
            ))
        }
        DataType::Decimal256(_, scale) => render!(Decimal256Array, |v: arrow::datatypes::i256| {
            java_big_decimal(&v.to_string(), *scale)
        }),
        DataType::Date32 => downcast::<Date32Array>(column, field)?
            .iter()
            .map(|days| days.map(|d| java_local_date(d, field)).transpose())
            .collect::<Result<Vec<Option<String>>>>()?,
        other => {
            return Err(CoreError::Unsupported(format!(
                "record key field '{field}' has type {other}, whose Java key rendering is not \
                 reproduced; supported record key types are strings, booleans, integers, \
                 Date32, and decimals"
            )));
        }
    };
    Ok(StringArray::from(rendered))
}

/// Java `KeyGenerator.constructRecordKey`: `field:value` parts in field order,
/// joined by `,`, with null and empty values replaced by their placeholders.
/// Values containing `,` or `:` are not escaped, as in Java.
///
/// Returns Java's `HoodieKeyException` message as the error when every part is
/// null or empty, so callers can raise it in their own error variant.
pub(crate) fn construct_record_key(
    fields: &[String],
    values: &[StringArray],
    row: usize,
) -> std::result::Result<String, String> {
    let mut key = String::new();
    let mut entirely_null_or_empty = true;
    for (i, (field, column)) in fields.iter().zip(values).enumerate() {
        if i > 0 {
            key.push(RECORD_KEY_PARTS_SEPARATOR);
        }
        key.push_str(field);
        key.push(COLUMN_VALUE_SEPARATOR);
        if column.is_null(row) {
            key.push_str(NULL_RECORDKEY_PLACEHOLDER);
        } else if column.value(row).is_empty() {
            key.push_str(EMPTY_RECORDKEY_PLACEHOLDER);
        } else {
            key.push_str(column.value(row));
            entirely_null_or_empty = false;
        }
    }
    if entirely_null_or_empty {
        return Err(format!(
            "recordKey values: \"{key}\" for fields: [{}] cannot be entirely null or empty.",
            fields.join(", ")
        ));
    }
    Ok(key)
}

fn downcast<'a, A: 'static>(column: &'a dyn Array, field: &str) -> Result<&'a A> {
    column.as_any().downcast_ref::<A>().ok_or_else(|| {
        CoreError::Schema(format!(
            "record key field '{field}' does not match its declared type {}",
            column.data_type()
        ))
    })
}

/// Java `LocalDate.ofEpochDay(days).toString()`.
fn java_local_date(days: i32, field: &str) -> Result<String> {
    // 719_163 days separate 0001-01-01 (day 1 of the common era) from the epoch.
    days.checked_add(719_163)
        .and_then(NaiveDate::from_num_days_from_ce_opt)
        .map(|date| date.to_string())
        .ok_or_else(|| {
            CoreError::Unsupported(format!(
                "record key field '{field}' holds date {days} days from the epoch, outside the \
                 supported range"
            ))
        })
}

/// Java `BigDecimal.toString()` for an unscaled value and scale: plain notation
/// unless the scale is negative or the adjusted exponent is below -6.
fn java_big_decimal(unscaled: &str, scale: i8) -> String {
    let (sign, digits) = match unscaled.strip_prefix('-') {
        Some(digits) => ("-", digits),
        None => ("", unscaled),
    };
    let scale = i64::from(scale);
    let len = digits.len() as i64;
    let adjusted = len - 1 - scale;
    if scale >= 0 && adjusted >= -6 {
        if scale == 0 {
            return format!("{sign}{digits}");
        }
        if len > scale {
            let (int_part, frac_part) = digits.split_at((len - scale) as usize);
            return format!("{sign}{int_part}.{frac_part}");
        }
        let zeros = "0".repeat((scale - len) as usize);
        return format!("{sign}0.{zeros}{digits}");
    }
    let (first, rest) = digits.split_at(1);
    let point = if rest.is_empty() { "" } else { "." };
    let exponent_sign = if adjusted > 0 { "+" } else { "" };
    format!("{sign}{first}{point}{rest}E{exponent_sign}{adjusted}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        ArrayRef, BinaryArray, Float32Array, Float64Array, TimestampMicrosecondArray,
        TimestampMillisecondArray,
    };
    use arrow::datatypes::i256;
    use std::sync::Arc;

    fn rendered(column: ArrayRef) -> Vec<Option<String>> {
        render_key_field(column.as_ref(), "k")
            .unwrap()
            .iter()
            .map(|v| v.map(str::to_string))
            .collect()
    }

    fn some(values: &[&str]) -> Vec<Option<String>> {
        values.iter().map(|v| Some(v.to_string())).collect()
    }

    #[test]
    fn test_render_key_field_scalar_types() {
        assert_eq!(
            rendered(Arc::new(LargeStringArray::from(vec![Some("x"), None]))),
            vec![Some("x".to_string()), None]
        );
        assert_eq!(
            rendered(Arc::new(BooleanArray::from(vec![true, false]))),
            some(&["true", "false"])
        );
        assert_eq!(
            rendered(Arc::new(Int64Array::from(vec![i64::MIN]))),
            some(&["-9223372036854775808"])
        );
        assert_eq!(
            rendered(Arc::new(UInt64Array::from(vec![u64::MAX]))),
            some(&["18446744073709551615"])
        );
    }

    #[test]
    fn test_render_key_field_dates_match_local_date() {
        // LocalDate.ofEpochDay: 4-digit years, sign outside 0..=9999.
        assert_eq!(
            rendered(Arc::new(Date32Array::from(vec![
                0, 19_724, -719_528, -719_529, 2_932_897,
            ]))),
            some(&[
                "1970-01-01",
                "2024-01-02",
                "0000-01-01",
                "-0001-12-31",
                "+10000-01-01"
            ])
        );
    }

    #[test]
    fn test_render_key_field_decimals_match_big_decimal_to_string() {
        let decimal = |values: Vec<i128>, precision: u8, scale: i8| -> ArrayRef {
            Arc::new(
                Decimal128Array::from(values)
                    .with_precision_and_scale(precision, scale)
                    .unwrap(),
            )
        };
        assert_eq!(
            rendered(decimal(vec![12345, -5, 0, 7], 10, 2)),
            some(&["123.45", "-0.05", "0.00", "0.07"])
        );
        // Adjusted exponent below -6 switches to scientific notation.
        assert_eq!(
            rendered(decimal(vec![1, 0, 123, 1_000_000], 20, 8)),
            some(&["1E-8", "0E-8", "0.00000123", "0.01000000"])
        );
        assert_eq!(rendered(decimal(vec![42], 5, 0)), some(&["42"]));
        assert_eq!(rendered(decimal(vec![42], 5, -2)), some(&["4.2E+3"]));
        let wide: ArrayRef = Arc::new(
            Decimal256Array::from(vec![i256::from_i128(-123_456)])
                .with_precision_and_scale(40, 3)
                .unwrap(),
        );
        assert_eq!(rendered(wide), some(&["-123.456"]));
    }

    #[test]
    fn test_render_key_field_rejects_unrendered_types() {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Float32Array::from(vec![1.0f32])),
            Arc::new(Float64Array::from(vec![1.0])),
            Arc::new(TimestampMicrosecondArray::from(vec![1])),
            Arc::new(TimestampMillisecondArray::from(vec![1]).with_timezone("UTC")),
            Arc::new(BinaryArray::from_vec(vec![b"x"])),
        ];
        for column in columns {
            let err = render_key_field(column.as_ref(), "k").unwrap_err();
            assert!(matches!(err, CoreError::Unsupported(_)), "{err:?}");
            let message = err.to_string();
            assert!(
                message.contains(&format!(
                    "record key field 'k' has type {}",
                    column.data_type()
                )),
                "{message}"
            );
        }
    }

    #[test]
    fn test_construct_record_key() {
        let fields = vec!["a".to_string(), "b".to_string()];
        let values = vec![
            StringArray::from(vec![Some("1"), None, Some("")]),
            StringArray::from(vec![Some("x,y:z"), Some("2"), None]),
        ];
        assert_eq!(
            construct_record_key(&fields, &values, 0).unwrap(),
            "a:1,b:x,y:z"
        );
        assert_eq!(
            construct_record_key(&fields, &values, 1).unwrap(),
            "a:__null__,b:2"
        );
        assert_eq!(
            construct_record_key(&fields, &values, 2).unwrap_err(),
            "recordKey values: \"a:__empty__,b:__null__\" for fields: [a, b] cannot be \
             entirely null or empty."
        );
    }
}
