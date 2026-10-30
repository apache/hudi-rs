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

//! Key generator implementations: record key encoding, and transforms of user
//! filters to partition filters.

pub(crate) mod record_key;
pub mod timestamp_based;

use crate::Result;
use crate::config::HudiConfigs;
use crate::config::table::HudiTableConfig;
use crate::config::table::HudiTableConfig::{
    KeyGeneratorClass, KeyGeneratorType, PartitionFields, TableVersion,
};
use crate::error::CoreError;
use crate::expr::filter::Filter;
use crate::keygen::record_key::configured_record_key_fields;

/// Java `HoodieWriteConfig.COMPLEX_KEYGEN_NEW_ENCODING`.
const COMPLEX_KEYGEN_NEW_ENCODING: &str = "hoodie.write.complex.keygen.new.encoding";

/// The key generators hudi-rs tells apart, as Java `KeyGeneratorType` names them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum KeyGeneratorKind {
    Simple,
    NonPartitioned,
    Complex,
    /// Spark SQL's `SqlKeyGenerator`, which delegates to `ComplexKeyGenerator`
    /// when the table config names the wrapper itself rather than an origin class.
    SparkSql,
    TimestampBased,
    /// Custom, global-delete and user-provided generators.
    Other,
}

impl KeyGeneratorKind {
    fn from_class(class: &str) -> Self {
        match class {
            "org.apache.hudi.keygen.SimpleKeyGenerator"
            | "org.apache.hudi.keygen.SimpleAvroKeyGenerator" => Self::Simple,
            "org.apache.hudi.keygen.NonpartitionedKeyGenerator"
            | "org.apache.hudi.keygen.NonpartitionedAvroKeyGenerator" => Self::NonPartitioned,
            "org.apache.hudi.keygen.ComplexKeyGenerator"
            | "org.apache.hudi.keygen.ComplexAvroKeyGenerator" => Self::Complex,
            "org.apache.spark.sql.hudi.command.SqlKeyGenerator" => Self::SparkSql,
            "org.apache.hudi.keygen.TimestampBasedKeyGenerator"
            | "org.apache.hudi.keygen.TimestampBasedAvroKeyGenerator" => Self::TimestampBased,
            _ => Self::Other,
        }
    }

    fn from_type(key_generator_type: &str) -> Self {
        match key_generator_type.to_ascii_uppercase().as_str() {
            "SIMPLE" | "SIMPLE_AVRO" => Self::Simple,
            "NON_PARTITION" | "NON_PARTITION_AVRO" => Self::NonPartitioned,
            "COMPLEX" | "COMPLEX_AVRO" => Self::Complex,
            "SPARK_SQL" => Self::SparkSql,
            "TIMESTAMP" | "TIMESTAMP_AVRO" => Self::TimestampBased,
            _ => Self::Other,
        }
    }
}

fn declared<'a>(hudi_configs: &'a HudiConfigs, config: &HudiTableConfig) -> Option<&'a str> {
    hudi_configs
        .get_raw(config.as_ref())
        .map(str::trim)
        .filter(|v| !v.is_empty())
}

/// The generator the table config names. The class wins over the type, as in
/// Java `KeyGeneratorType.getKeyGeneratorClassName`.
fn declared_key_generator(hudi_configs: &HudiConfigs) -> Option<KeyGeneratorKind> {
    declared(hudi_configs, &KeyGeneratorClass)
        .map(KeyGeneratorKind::from_class)
        .or_else(|| declared(hudi_configs, &KeyGeneratorType).map(KeyGeneratorKind::from_type))
}

/// The table's key generator: the declared one, and with none it is inferred
/// from the field lists (`KeyGenUtils.inferKeyGeneratorType`). Generators whose
/// keys or partition paths hudi-rs does not reproduce are rejected, since
/// writing with a different encoding would split a key across two index entries.
pub(crate) fn key_generator_kind(hudi_configs: &HudiConfigs) -> Result<KeyGeneratorKind> {
    let kind = match declared_key_generator(hudi_configs) {
        Some(kind) => kind,
        None => {
            let partition_fields: Vec<String> = hudi_configs.get_or_default(PartitionFields).into();
            if partition_fields.first().is_some_and(|f| f.contains(':')) {
                // A `field:type` partition spec is the custom key generator.
                KeyGeneratorKind::Other
            } else {
                match partition_fields.len() {
                    0 => KeyGeneratorKind::NonPartitioned,
                    1 if configured_record_key_fields(hudi_configs)?.len() > 1 => {
                        KeyGeneratorKind::Complex
                    }
                    1 => KeyGeneratorKind::Simple,
                    _ => KeyGeneratorKind::Complex,
                }
            }
        }
    };
    if matches!(
        kind,
        KeyGeneratorKind::TimestampBased | KeyGeneratorKind::Other
    ) {
        let setting = [KeyGeneratorClass, KeyGeneratorType, PartitionFields]
            .iter()
            .find_map(|config| {
                declared(hudi_configs, config).map(|value| format!("{}={value}", config.as_ref()))
            })
            .unwrap_or_default();
        return Err(CoreError::Unsupported(format!(
            "writes do not support {setting}; supported key generators are SIMPLE, COMPLEX, \
             NON_PARTITION, and SPARK_SQL"
        )));
    }
    Ok(kind)
}

/// Whether the table's key generator stores a single-field record key as
/// `field:value` (Java `ComplexKeyGenerator`) rather than the bare value
/// (`SimpleKeyGenerator`, `NonpartitionedKeyGenerator`).
pub(crate) fn encodes_single_key_field_name(hudi_configs: &HudiConfigs) -> Result<bool> {
    if !matches!(
        key_generator_kind(hudi_configs)?,
        KeyGeneratorKind::Complex | KeyGeneratorKind::SparkSql
    ) {
        return Ok(false);
    }
    // Java `KeyGenUtils.encodeSingleKeyFieldNameForComplexKeyGen`.
    let table_version: Option<isize> = hudi_configs.try_get(TableVersion)?.map(Into::into);
    let new_encoding = hudi_configs
        .get_raw(COMPLEX_KEYGEN_NEW_ENCODING)
        .is_some_and(|v| v.trim().eq_ignore_ascii_case("true"));
    Ok(table_version.is_some_and(|v| v >= 9) || !new_encoding)
}

/// Java `KeyGeneratorType.isComplexKeyGenerator`: only a declared type or
/// class counts, never one inferred from the field lists, and the type is
/// checked before the class.
pub(crate) fn declares_complex_key_generator(hudi_configs: &HudiConfigs) -> bool {
    let kind = match hudi_configs.get_raw(KeyGeneratorType.as_ref()) {
        Some(key_generator_type) => KeyGeneratorKind::from_type(key_generator_type.trim()),
        None => declared(hudi_configs, &KeyGeneratorClass)
            .map_or(KeyGeneratorKind::Other, KeyGeneratorKind::from_class),
    };
    kind == KeyGeneratorKind::Complex
}

/// Returns true if the table uses a timestamp-based key generator.
pub fn is_timestamp_based_keygen(hudi_configs: &HudiConfigs) -> Result<bool> {
    Ok(declared_key_generator(hudi_configs) == Some(KeyGeneratorKind::TimestampBased))
}

/// Trait for key generators that can transform user filters on data columns
/// to filters on partition path columns.
pub trait KeyGeneratorFilterTransformer {
    /// Returns the source field name that this key generator operates on.
    fn source_field(&self) -> &str;

    /// Transforms a filter on the source field to one or more filters on partition fields.
    fn transform_filter(&self, filter: &Filter) -> Result<Vec<Filter>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn configs(pairs: &[(&str, &str)]) -> HudiConfigs {
        HudiConfigs::new(pairs.iter().map(|(k, v)| (*k, v.to_string())))
    }

    #[test]
    fn test_declared_key_generator_kinds() {
        let timestamp = configs(&[(
            "hoodie.table.keygenerator.class",
            "org.apache.hudi.keygen.TimestampBasedAvroKeyGenerator",
        )]);
        assert!(is_timestamp_based_keygen(&timestamp).unwrap());
        assert!(
            is_timestamp_based_keygen(&configs(&[("hoodie.table.keygenerator.type", "timestamp")]))
                .unwrap()
        );
        assert!(!is_timestamp_based_keygen(&configs(&[])).unwrap());
        // The class wins over the type.
        assert!(
            !is_timestamp_based_keygen(&configs(&[
                ("hoodie.table.keygenerator.type", "TIMESTAMP"),
                (
                    "hoodie.table.keygenerator.class",
                    "org.apache.hudi.keygen.SimpleKeyGenerator",
                ),
            ]))
            .unwrap()
        );

        // Java checks the type before the class, and Spark SQL's wrapper is not complex.
        let complex_type = configs(&[
            ("hoodie.table.keygenerator.type", "COMPLEX"),
            (
                "hoodie.table.keygenerator.class",
                "org.apache.hudi.keygen.SimpleKeyGenerator",
            ),
        ]);
        assert!(declares_complex_key_generator(&complex_type));
        assert!(!declares_complex_key_generator(&configs(&[(
            "hoodie.table.keygenerator.type",
            "SPARK_SQL"
        )])));
        assert!(!declares_complex_key_generator(&configs(&[(
            "hoodie.table.partition.fields",
            "p,q"
        )])));
    }
}
