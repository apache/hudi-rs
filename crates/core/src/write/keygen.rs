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
//! Derive Hoodie record keys and hive-style partition paths from write batches.

use arrow::array::{Array, StringArray};
use arrow::record_batch::RecordBatch;

use crate::Result;
use crate::config::HudiConfigs;
use crate::config::table::HudiTableConfig::{
    IsHiveStylePartitioning, IsPartitionPathUrlencoded, KeyGeneratorClass, KeyGeneratorType,
    PartitionFields, RecordKeyFields, TableVersion,
};
use crate::error::CoreError;
use crate::index::HoodieKey;
use crate::keygen::record_key::{construct_record_key, render_key_field};

/// Build per-row [`HoodieKey`]s from a write batch.
///
/// When `hoodie.table.recordkey.fields` is unset/empty, keys are auto-generated
/// like Java `AutoRecordGenWrapper*KeyGenerator`: `{instant}_{partitionId}_{rowId}`.
///
/// `row_id_offset` continues the auto-key row counter across size-split chunks and
/// partitions within one commit. `partition_id` distinguishes writer partitions
/// (Java `HoodieRecord.generateSequenceId`).
///
/// Partition paths use hive-style (`field=value/...`) when
/// `hoodie.datasource.write.hive_style_partitioning` is true; otherwise value-only
/// segments. Null or empty partition values map to [`DEFAULT_PARTITION_PATH`].
/// Partition columns are not stripped from the batch: callers must leave them in
/// the written Parquet.
/// Placeholder instant for pre-fence validation passes: [`hoodie_keys_for_batch`]
/// uses the instant only to FORMAT auto-generated keys, never to decide errors,
/// so deriving keys with this dummy value surfaces every deterministic input
/// error (missing/null/wrong-type key or partition columns) before an instant
/// is requested.
pub(crate) const VALIDATION_INSTANT: &str = "00000000000000000";

/// Java `PartitionPathEncodeUtils.DEFAULT_PARTITION_PATH`.
const DEFAULT_PARTITION_PATH: &str = "__HIVE_DEFAULT_PARTITION__";

/// Java `HoodieWriteConfig.COMPLEX_KEYGEN_NEW_ENCODING`.
const COMPLEX_KEYGEN_NEW_ENCODING: &str = "hoodie.write.complex.keygen.new.encoding";

/// Java `HoodieWriteConfig.ENABLE_COMPLEX_KEYGEN_VALIDATION`.
pub(crate) const COMPLEX_KEYGEN_VALIDATION: &str = "hoodie.write.complex.keygen.validation.enable";

/// The record key encodings of the key generators the writer reproduces.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum KeyGeneratorKind {
    Simple,
    NonPartitioned,
    Complex,
}

/// The table's key generator, resolved like Java
/// `KeyGeneratorType.getKeyGeneratorClassName`: the class wins over the type,
/// and with neither it is inferred from the field lists
/// (`KeyGenUtils.inferKeyGeneratorType`). Generators whose keys or partition
/// paths the writer does not reproduce are rejected, since writing with a
/// different encoding would split a key across two index entries.
fn key_generator_kind(hudi_configs: &HudiConfigs) -> Result<KeyGeneratorKind> {
    if let Some(class) = hudi_configs
        .get_raw(KeyGeneratorClass.as_ref())
        .map(str::trim)
        .filter(|c| !c.is_empty())
    {
        return match class {
            "org.apache.hudi.keygen.SimpleKeyGenerator"
            | "org.apache.hudi.keygen.SimpleAvroKeyGenerator" => Ok(KeyGeneratorKind::Simple),
            "org.apache.hudi.keygen.NonpartitionedKeyGenerator"
            | "org.apache.hudi.keygen.NonpartitionedAvroKeyGenerator" => {
                Ok(KeyGeneratorKind::NonPartitioned)
            }
            // Spark SQL's wrapper delegates to ComplexKeyGenerator when the table
            // config names the wrapper itself rather than an origin class.
            "org.apache.hudi.keygen.ComplexKeyGenerator"
            | "org.apache.hudi.keygen.ComplexAvroKeyGenerator"
            | "org.apache.spark.sql.hudi.command.SqlKeyGenerator" => Ok(KeyGeneratorKind::Complex),
            other => Err(unsupported_key_generator(KeyGeneratorClass.as_ref(), other)),
        };
    }
    if let Some(key_generator_type) = hudi_configs
        .get_raw(KeyGeneratorType.as_ref())
        .map(str::trim)
        .filter(|t| !t.is_empty())
    {
        return match key_generator_type {
            "SIMPLE" | "SIMPLE_AVRO" => Ok(KeyGeneratorKind::Simple),
            "NON_PARTITION" | "NON_PARTITION_AVRO" => Ok(KeyGeneratorKind::NonPartitioned),
            "COMPLEX" | "COMPLEX_AVRO" | "SPARK_SQL" => Ok(KeyGeneratorKind::Complex),
            other => Err(unsupported_key_generator(KeyGeneratorType.as_ref(), other)),
        };
    }
    let partition_fields: Vec<String> = hudi_configs.get_or_default(PartitionFields).into();
    if partition_fields.first().is_some_and(|f| f.contains(':')) {
        return Err(unsupported_key_generator(
            PartitionFields.as_ref(),
            &partition_fields.join(","),
        ));
    }
    Ok(match partition_fields.len() {
        0 => KeyGeneratorKind::NonPartitioned,
        1 if configured_record_key_fields(hudi_configs)?.len() > 1 => KeyGeneratorKind::Complex,
        1 => KeyGeneratorKind::Simple,
        _ => KeyGeneratorKind::Complex,
    })
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

/// Whether the table's key generator stores a single-field record key as
/// `field:value` (Java `ComplexKeyGenerator`) rather than the bare value
/// (`SimpleKeyGenerator`, `NonpartitionedKeyGenerator`).
pub(crate) fn encodes_single_key_field_name(hudi_configs: &HudiConfigs) -> Result<bool> {
    if key_generator_kind(hudi_configs)? != KeyGeneratorKind::Complex {
        return Ok(false);
    }
    // Java `KeyGenUtils.encodeSingleKeyFieldNameForComplexKeyGen`.
    let table_version: Option<isize> = hudi_configs.try_get(TableVersion)?.map(Into::into);
    let new_encoding = hudi_configs
        .get_raw(COMPLEX_KEYGEN_NEW_ENCODING)
        .is_some_and(|v| v.trim().eq_ignore_ascii_case("true"));
    Ok(table_version.is_some_and(|v| v >= 9) || !new_encoding)
}

/// Fail a write on a table whose key generator the writer cannot reproduce.
pub(crate) fn ensure_supported_key_generator(hudi_configs: &HudiConfigs) -> Result<()> {
    encodes_single_key_field_name(hudi_configs).map(|_| ())
}

/// Java `BaseHoodieWriteClient.validateAgainstTableProperties`: below table
/// version 9, a table declaring the complex key generator with one record key
/// field may hold bare keys from releases that did not encode the field name,
/// so writing `field:value` keys would duplicate its records. Writes are
/// refused until the validation is turned off.
pub(crate) fn ensure_complex_keygen_validated(hudi_configs: &HudiConfigs) -> Result<()> {
    let table_version: Option<isize> = hudi_configs.try_get(TableVersion)?.map(Into::into);
    let record_key_fields = configured_record_key_fields(hudi_configs)?;
    if table_version.is_none_or(|v| v >= 9)
        || record_key_fields.len() != 1
        || !complex_keygen_validation_enabled(|key| hudi_configs.get_raw(key))
        || !declares_complex_key_generator(hudi_configs)
    {
        return Ok(());
    }
    Err(CoreError::Unsupported(format!(
        "this table version {} table uses the complex key generator with the single record \
         key field '{}'; Hudi 0.14.1 through 1.0.2 writers may have stored its keys without the \
         field name, so writing it can duplicate records. Set {COMPLEX_KEYGEN_VALIDATION}=false \
         once the table is known to be unaffected",
        table_version.unwrap_or_default(),
        record_key_fields[0]
    )))
}

/// Whether [`COMPLEX_KEYGEN_VALIDATION`] is on (the default).
pub(crate) fn complex_keygen_validation_enabled<'a>(
    get_raw: impl Fn(&str) -> Option<&'a str>,
) -> bool {
    !get_raw(COMPLEX_KEYGEN_VALIDATION).is_some_and(|v| v.trim().eq_ignore_ascii_case("false"))
}

/// Java `KeyGeneratorType.isComplexKeyGenerator`: only a declared type or
/// class counts, never one inferred from the field lists.
fn declares_complex_key_generator(hudi_configs: &HudiConfigs) -> bool {
    if let Some(key_generator_type) = hudi_configs.get_raw(KeyGeneratorType.as_ref()) {
        return matches!(key_generator_type.trim(), "COMPLEX" | "COMPLEX_AVRO");
    }
    matches!(
        hudi_configs
            .get_raw(KeyGeneratorClass.as_ref())
            .map(str::trim),
        Some(
            "org.apache.hudi.keygen.ComplexKeyGenerator"
                | "org.apache.hudi.keygen.ComplexAvroKeyGenerator"
        )
    )
}

fn unsupported_key_generator(config: &str, value: &str) -> CoreError {
    CoreError::Unsupported(format!(
        "writes do not support {config}={value}; supported key generators are SIMPLE, \
         COMPLEX, NON_PARTITION, and SPARK_SQL"
    ))
}

/// Run key/partition derivation for its error side only — call before fencing
/// an instant so invalid input leaves no timeline trace.
pub(crate) fn validate_keygen_inputs(
    hudi_configs: &HudiConfigs,
    batches: &[RecordBatch],
) -> Result<()> {
    for batch in batches {
        hoodie_keys_for_batch(hudi_configs, batch, Some(VALIDATION_INSTANT))?;
    }
    Ok(())
}

pub fn hoodie_keys_for_batch(
    hudi_configs: &HudiConfigs,
    batch: &RecordBatch,
    instant: Option<&str>,
) -> Result<Vec<HoodieKey>> {
    hoodie_keys_for_batch_with_offset(hudi_configs, batch, instant, 0, 0)
}

/// Like [`hoodie_keys_for_batch`], with an explicit auto-key row offset and partition id.
pub fn hoodie_keys_for_batch_with_offset(
    hudi_configs: &HudiConfigs,
    batch: &RecordBatch,
    instant: Option<&str>,
    row_id_offset: usize,
    partition_id: u32,
) -> Result<Vec<HoodieKey>> {
    ensure_supported_key_generator(hudi_configs)?;
    let record_key_fields = configured_record_key_fields(hudi_configs)?;
    let partition_fields: Vec<String> = hudi_configs.get_or_default(PartitionFields).into();
    let hive_style: bool = hudi_configs.get_or_default(IsHiveStylePartitioning).into();
    let url_encode: bool = hudi_configs
        .get_or_default(IsPartitionPathUrlencoded)
        .into();

    let record_keys = if record_key_fields.is_empty() {
        let instant = instant.ok_or_else(|| {
            CoreError::Write(
                "auto record-key generation requires a commit instant timestamp".to_string(),
            )
        })?;
        // Match Java HoodieRecord.generateSequenceId(instant, partitionId, rowId).
        (0..batch.num_rows())
            .map(|row| format!("{instant}_{partition_id}_{}", row_id_offset + row))
            .collect()
    } else {
        encode_record_keys(hudi_configs, &record_key_fields, batch)?
    };
    record_keys
        .into_iter()
        .enumerate()
        .map(|(row, record_key)| {
            Ok(HoodieKey {
                record_key,
                partition_path: partition_path_for_row(
                    batch,
                    row,
                    &partition_fields,
                    hive_style,
                    url_encode,
                )?,
            })
        })
        .collect()
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
fn encode_record_keys(
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

fn partition_path_for_row(
    batch: &RecordBatch,
    row: usize,
    partition_fields: &[String],
    hive_style: bool,
    url_encode: bool,
) -> Result<String> {
    if partition_fields.is_empty() {
        return Ok(String::new());
    }
    let mut segments = Vec::with_capacity(partition_fields.len());
    for field in partition_fields {
        let column = batch.column_by_name(field).ok_or_else(|| {
            CoreError::Schema(format!(
                "partition field '{field}' is missing from write batch"
            ))
        })?;
        let value = if column.is_null(row) {
            DEFAULT_PARTITION_PATH.to_string()
        } else {
            match array_value_as_string(column.as_ref(), row)? {
                value if value.is_empty() => DEFAULT_PARTITION_PATH.to_string(),
                value if url_encode => escape_path_name(&value),
                value => value,
            }
        };
        if hive_style {
            segments.push(format!("{field}={value}"));
        } else {
            segments.push(value);
        }
    }
    Ok(segments.join("/"))
}

/// Java `PartitionPathEncodeUtils.escapePathName` (Hive `FileUtils`): percent-encode
/// ASCII control characters and `"#%'*/:=?\{[]^` plus DEL; other characters,
/// including non-ASCII, pass through.
fn escape_path_name(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for c in value.chars() {
        let needs_escape = c < ' '
            || matches!(
                c,
                '"' | '#'
                    | '%'
                    | '\''
                    | '*'
                    | '/'
                    | ':'
                    | '='
                    | '?'
                    | '\\'
                    | '\u{7F}'
                    | '{'
                    | '['
                    | ']'
                    | '^'
            );
        if needs_escape {
            escaped.push_str(&format!("%{:02X}", c as u32));
        } else {
            escaped.push(c);
        }
    }
    escaped
}

fn array_value_as_string(array: &dyn Array, row: usize) -> Result<String> {
    if let Some(strings) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(strings.value(row).to_string());
    }
    use arrow::array::{Int32Array, Int64Array};
    if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
        return Ok(values.value(row).to_string());
    }
    if let Some(values) = array.as_any().downcast_ref::<Int32Array>() {
        return Ok(values.value(row).to_string());
    }
    Err(CoreError::Unsupported(
        "partition field types currently support Utf8, Int32, and Int64".to_string(),
    ))
}

/// Join a partition path and file name into a table-relative path.
pub fn relative_data_path(partition_path: &str, file_name: &str) -> String {
    if partition_path.is_empty() {
        file_name.to_string()
    } else {
        format!("{partition_path}/{file_name}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn configs(pairs: &[(&str, &str)]) -> HudiConfigs {
        HudiConfigs::new(pairs.iter().map(|(k, v)| (*k, v.to_string())))
    }

    fn batch(fields: Vec<Field>, columns: Vec<arrow::array::ArrayRef>) -> RecordBatch {
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    #[test]
    fn test_missing_record_key_column_errors() {
        let cfg = configs(&[("hoodie.table.recordkey.fields", "id")]);
        let b = batch(
            vec![Field::new("value", DataType::Int64, false)],
            vec![Arc::new(Int64Array::from(vec![1]))],
        );
        let err = hoodie_keys_for_batch(&cfg, &b, None).unwrap_err();
        assert!(err.to_string().contains("record key field 'id' is missing"));
    }

    #[test]
    fn test_null_record_key_errors() {
        let cfg = configs(&[("hoodie.table.recordkey.fields", "id")]);
        let b = batch(
            vec![Field::new("id", DataType::Utf8, true)],
            vec![Arc::new(StringArray::from(vec![None::<&str>]))],
        );
        let err = hoodie_keys_for_batch(&cfg, &b, None).unwrap_err();
        assert!(err.to_string().to_lowercase().contains("null"), "{err}");
    }

    #[test]
    fn test_non_string_record_key_renders_like_java() {
        let cfg = configs(&[("hoodie.table.recordkey.fields", "id")]);
        let b = batch(
            vec![Field::new("id", DataType::Int64, false)],
            vec![Arc::new(Int64Array::from(vec![7, -3]))],
        );
        let keys = hoodie_keys_for_batch(&cfg, &b, None).unwrap();
        assert_eq!(keys[0].record_key, "7");
        assert_eq!(keys[1].record_key, "-3");

        let b = batch(
            vec![Field::new("id", DataType::Binary, false)],
            vec![Arc::new(arrow::array::BinaryArray::from_vec(vec![b"x"]))],
        );
        let err = hoodie_keys_for_batch(&cfg, &b, None).unwrap_err();
        assert!(matches!(err, CoreError::Unsupported(_)), "{err:?}");
    }

    #[test]
    fn test_multi_field_record_key_encodes_field_names() {
        let fields = vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
        ];
        let columns: Vec<arrow::array::ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![Some(1), None, Some(3), None])),
            Arc::new(StringArray::from(vec![
                Some("x"),
                Some("y"),
                Some(""),
                Some(""),
            ])),
        ];
        // Java trims the configured field list and drops empty entries.
        let cfg = configs(&[("hoodie.table.recordkey.fields", " a , b ,")]);
        let b = batch(fields.clone(), columns.clone());
        let keys = hoodie_keys_for_batch(&cfg, &b.slice(0, 3), None).unwrap();
        let keys: Vec<&str> = keys.iter().map(|k| k.record_key.as_str()).collect();
        assert_eq!(keys, vec!["a:1,b:x", "a:__null__,b:y", "a:3,b:__empty__"]);
        let err = hoodie_keys_for_batch(&cfg, &b, None).unwrap_err();
        assert!(
            err.to_string().contains(
                "recordKey values: \"a:__null__,b:__empty__\" for fields: [a, b] cannot be \
                 entirely null or empty"
            ),
            "{err}"
        );

        // One partition field with several key fields infers the complex generator.
        let cfg = configs(&[
            ("hoodie.table.recordkey.fields", "a,b"),
            ("hoodie.table.partition.fields", "b"),
        ]);
        assert_eq!(
            hoodie_keys_for_batch(&cfg, &b.slice(0, 1), None).unwrap()[0].record_key,
            "a:1,b:x"
        );

        // Java's simple key generator rejects several key fields.
        let cfg = configs(&[
            ("hoodie.table.recordkey.fields", "a,b"),
            ("hoodie.table.keygenerator.type", "SIMPLE"),
        ]);
        let err = hoodie_keys_for_batch(&cfg, &b, None).unwrap_err();
        assert!(err.to_string().contains("single record key field"), "{err}");
    }

    #[test]
    fn test_auto_keys_require_instant() {
        let cfg = configs(&[]);
        let b = batch(
            vec![Field::new("v", DataType::Int64, false)],
            vec![Arc::new(Int64Array::from(vec![1]))],
        );
        let err = hoodie_keys_for_batch(&cfg, &b, None).unwrap_err();
        assert!(err.to_string().contains("commit instant timestamp"));
    }

    #[test]
    fn test_partition_value_renderings_and_errors() {
        let cfg = configs(&[
            ("hoodie.table.recordkey.fields", "id"),
            ("hoodie.table.partition.fields", "p"),
            ("hoodie.datasource.write.hive_style_partitioning", "true"),
        ]);
        // Int32 partition values render as plain decimal segments.
        let b = batch(
            vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("p", DataType::Int32, false),
            ],
            vec![
                Arc::new(StringArray::from(vec!["k"])),
                Arc::new(Int32Array::from(vec![7])),
            ],
        );
        let keys = hoodie_keys_for_batch(&cfg, &b, None).unwrap();
        assert_eq!(keys[0].partition_path, "p=7");

        // Int64 partition values.
        let b = batch(
            vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("p", DataType::Int64, false),
            ],
            vec![
                Arc::new(StringArray::from(vec!["k"])),
                Arc::new(Int64Array::from(vec![9])),
            ],
        );
        assert_eq!(
            hoodie_keys_for_batch(&cfg, &b, None).unwrap()[0].partition_path,
            "p=9"
        );

        // Null and empty partition values: Java's default partition.
        let b = batch(
            vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("p", DataType::Utf8, true),
            ],
            vec![
                Arc::new(StringArray::from(vec!["k", "l"])),
                Arc::new(StringArray::from(vec![None, Some("")])),
            ],
        );
        let keys = hoodie_keys_for_batch(&cfg, &b, None).unwrap();
        assert_eq!(keys[0].partition_path, "p=__HIVE_DEFAULT_PARTITION__");
        assert_eq!(keys[1].partition_path, "p=__HIVE_DEFAULT_PARTITION__");

        // Missing partition column: error.
        let b = batch(
            vec![Field::new("id", DataType::Utf8, false)],
            vec![Arc::new(StringArray::from(vec!["k"]))],
        );
        let err = hoodie_keys_for_batch(&cfg, &b, None).unwrap_err();
        assert!(err.to_string().contains("partition field 'p' is missing"));

        // Unsupported partition value type: error.
        let b = batch(
            vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("p", DataType::Float64, false),
            ],
            vec![
                Arc::new(StringArray::from(vec!["k"])),
                Arc::new(arrow::array::Float64Array::from(vec![1.5])),
            ],
        );
        assert!(hoodie_keys_for_batch(&cfg, &b, None).is_err());
    }

    #[test]
    fn test_escape_path_name_matches_java() {
        assert_eq!(escape_path_name("2024/01/02"), "2024%2F01%2F02");
        assert_eq!(
            escape_path_name("a b:c=d%e#f?g*h'i\"j\\k{l[m]n^o"),
            "a b%3Ac%3Dd%25e%23f%3Fg%2Ah%27i%22j%5Ck%7Bl%5Bm%5Dn%5Eo"
        );
        assert_eq!(escape_path_name("\u{0}\t\u{1F}\u{7F}"), "%00%09%1F%7F");
        // Java leaves `-`, `_`, `}`, `.` and non-ASCII characters alone.
        assert_eq!(escape_path_name("a-b_c}d.e\u{e9}"), "a-b_c}d.e\u{e9}");
    }

    #[test]
    fn test_key_generator_resolution() {
        type Case<'a> = (&'a [(&'a str, &'a str)], Option<bool>);
        let cases: &[Case] = &[
            (&[("hoodie.table.keygenerator.type", "SIMPLE")], Some(false)),
            (
                &[("hoodie.table.keygenerator.type", "NON_PARTITION")],
                Some(false),
            ),
            (&[("hoodie.table.keygenerator.type", "COMPLEX")], Some(true)),
            (
                &[("hoodie.table.keygenerator.type", "SPARK_SQL")],
                Some(true),
            ),
            (&[("hoodie.table.keygenerator.type", "TIMESTAMP")], None),
            (&[("hoodie.table.keygenerator.type", "GLOBAL_DELETE")], None),
            // The class wins over the type.
            (
                &[
                    ("hoodie.table.keygenerator.type", "COMPLEX"),
                    (
                        "hoodie.table.keygenerator.class",
                        "org.apache.hudi.keygen.SimpleKeyGenerator",
                    ),
                ],
                Some(false),
            ),
            (
                &[(
                    "hoodie.table.keygenerator.class",
                    "org.apache.hudi.keygen.ComplexAvroKeyGenerator",
                )],
                Some(true),
            ),
            (
                &[(
                    "hoodie.table.keygenerator.class",
                    "org.apache.hudi.keygen.TimestampBasedAvroKeyGenerator",
                )],
                None,
            ),
            // Inferred like Java: one partition field is SIMPLE, more is COMPLEX,
            // a `field:type` partition spec is CUSTOM.
            (&[("hoodie.table.partition.fields", "p")], Some(false)),
            (&[("hoodie.table.partition.fields", "p,q")], Some(true)),
            (&[("hoodie.table.partition.fields", "p:SIMPLE")], None),
            // Table version 8 encodes unless the new encoding is opted into.
            (
                &[
                    ("hoodie.table.keygenerator.type", "COMPLEX"),
                    ("hoodie.table.version", "8"),
                ],
                Some(true),
            ),
            (
                &[
                    ("hoodie.table.keygenerator.type", "COMPLEX"),
                    ("hoodie.table.version", "8"),
                    ("hoodie.write.complex.keygen.new.encoding", "true"),
                ],
                Some(false),
            ),
            (
                &[
                    ("hoodie.table.keygenerator.type", "COMPLEX"),
                    ("hoodie.table.version", "9"),
                    ("hoodie.write.complex.keygen.new.encoding", "true"),
                ],
                Some(true),
            ),
        ];
        for (pairs, expected) in cases {
            let result = encodes_single_key_field_name(&configs(pairs));
            match expected {
                Some(encode) => assert_eq!(result.unwrap(), *encode, "{pairs:?}"),
                None => assert!(
                    matches!(result, Err(CoreError::Unsupported(_))),
                    "{pairs:?}: {result:?}"
                ),
            }
        }
    }

    #[test]
    fn test_empty_record_key_errors() {
        let cfg = configs(&[("hoodie.table.recordkey.fields", "id")]);
        let b = batch(
            vec![Field::new("id", DataType::Utf8, false)],
            vec![Arc::new(StringArray::from(vec![""]))],
        );
        let err = hoodie_keys_for_batch(&cfg, &b, None).unwrap_err();
        assert!(err.to_string().contains("non-empty"), "{err}");
    }
}
