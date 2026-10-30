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
//! Record key encoding shared by the writer, the index and the virtual-key
//! reader, so a key derived at read time equals the one a Java writer stored.

use arrow::array::{
    Array, BooleanArray, Int8Array, Int16Array, Int32Array, Int64Array, LargeStringArray,
    StringArray, StringViewArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use crate::Result;
use crate::config::HudiConfigs;
use crate::config::table::HudiTableConfig::RecordKeyFields;
use crate::error::CoreError;
use crate::keygen::{KeyGeneratorKind, encodes_single_key_field_name, key_generator_kind};

/// Java `KeyGenerator.NULL_RECORDKEY_PLACEHOLDER`.
pub(crate) const NULL_RECORDKEY_PLACEHOLDER: &str = "__null__";
/// Java `KeyGenerator.EMPTY_RECORDKEY_PLACEHOLDER`.
pub(crate) const EMPTY_RECORDKEY_PLACEHOLDER: &str = "__empty__";
/// Java `KeyGenerator.DEFAULT_COLUMN_VALUE_SEPARATOR`.
const COLUMN_VALUE_SEPARATOR: char = ':';
/// Java `KeyGenerator.DEFAULT_RECORD_KEY_PARTS_SEPARATOR`.
const RECORD_KEY_PARTS_SEPARATOR: char = ',';

/// Render a string, boolean or integer record key field column as the Java
/// key generators do: `toString` of the value. Nulls stay null.
///
/// Every other type is [`CoreError::Unsupported`]: Java writers store it in
/// more than one form that the table does not record, and an encoding that
/// does not match the table's would duplicate rows. Spark renders a date as
/// its ISO `LocalDate` and Flink as the epoch day; decimals differ between
/// `toString` and `toPlainString`; timestamps are stored as a raw epoch count,
/// a JVM-zone `java.sql.Timestamp` or epoch millis; and float keys depend on
/// the JDK's `Double.toString` digits.
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
        other => {
            return Err(CoreError::Unsupported(format!(
                "record key field '{field}' has type {other}, whose Java key rendering is not \
                 reproduced; supported record key types are strings, booleans, and integers"
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

/// Java `KeyGenUtils.getRecordKeyFields`: the configured record key fields,
/// trimmed, with empty entries dropped. Empty means auto-generated keys.
pub(crate) fn configured_record_key_fields(hudi_configs: &HudiConfigs) -> Result<Vec<String>> {
    let fields: Vec<String> = hudi_configs
        .try_get(RecordKeyFields)?
        .map(Into::into)
        .unwrap_or_default();
    Ok(fields
        .iter()
        .map(|f| f.trim())
        .filter(|f| !f.is_empty())
        .map(str::to_string)
        .collect())
}

/// Record keys of `batch` derived from the configured record key fields, as
/// the table's key generator encodes them (Java `KeyGenerator.getRecordKey`).
/// Fails on a table with auto-generated keys, which no row value determines.
pub(crate) fn record_keys_for_batch(
    hudi_configs: &HudiConfigs,
    batch: &RecordBatch,
) -> Result<Vec<String>> {
    let record_key_fields = configured_record_key_fields(hudi_configs)?;
    if record_key_fields.is_empty() {
        return Err(CoreError::Unsupported(
            "auto-generated record keys cannot be derived from row values".to_string(),
        ));
    }
    encode_record_keys(hudi_configs, &record_key_fields, batch)
}

/// Several fields, or one field under the complex key generator, encode as
/// `field:value` parts (Java `KeyGenUtils.getRecordKey(record, fields, ..)`);
/// one field otherwise is the bare value, which may not be null or empty.
pub(crate) fn encode_record_keys(
    hudi_configs: &HudiConfigs,
    fields: &[String],
    batch: &RecordBatch,
) -> Result<Vec<String>> {
    if fields.len() > 1 && key_generator_kind(hudi_configs)? == KeyGeneratorKind::Simple {
        return Err(CoreError::Unsupported(format!(
            "the simple key generator expects a single record key field; got '{}'",
            fields.join(",")
        )));
    }
    let mut values = Vec::with_capacity(fields.len());
    for field in fields {
        let column = batch.column_by_name(field).ok_or_else(|| {
            CoreError::Schema(format!(
                "record key field '{field}' is missing from write batch"
            ))
        })?;
        values.push(render_key_field(column.as_ref(), field)?);
    }
    if let ([field], [column]) = (fields, values.as_slice())
        && !encodes_single_key_field_name(hudi_configs)?
    {
        return (0..batch.num_rows())
            .map(|row| {
                if column.is_null(row) || column.value(row).is_empty() {
                    return Err(CoreError::Write(format!(
                        "record key field '{field}' must be non-null and non-empty"
                    )));
                }
                Ok(column.value(row).to_string())
            })
            .collect();
    }
    (0..batch.num_rows())
        .map(|row| construct_record_key(fields, &values, row).map_err(CoreError::Write))
        .collect()
}

fn downcast<'a, A: 'static>(column: &'a dyn Array, field: &str) -> Result<&'a A> {
    column.as_any().downcast_ref::<A>().ok_or_else(|| {
        CoreError::Schema(format!(
            "record key field '{field}' does not match its declared type {}",
            column.data_type()
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        ArrayRef, BinaryArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
        TimestampMicrosecondArray, TimestampMillisecondArray,
    };
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
    fn test_render_key_field_rejects_unrendered_types() {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Float32Array::from(vec![1.0f32])),
            Arc::new(Float64Array::from(vec![1.0])),
            Arc::new(TimestampMicrosecondArray::from(vec![1])),
            Arc::new(TimestampMillisecondArray::from(vec![1]).with_timezone("UTC")),
            Arc::new(BinaryArray::from_vec(vec![b"x"])),
            Arc::new(Date32Array::from(vec![0])),
            Arc::new(
                Decimal128Array::from(vec![1])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ),
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
