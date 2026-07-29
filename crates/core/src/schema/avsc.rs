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
//! Load Apache Hudi `.avsc` schemas vendored from `hudi-common` (Java).
//!
//! Schemas are copied under `crates/core/schemas/` and must stay in sync with
//! the Java project — do not hand-edit field layouts here.

use std::sync::OnceLock;

use apache_avro::Schema;

use crate::error::{CoreError, Result};

/// Strip `//` line comments that appear in some Java `.avsc` sources (e.g. HoodieMetadata.avsc).
/// Does not strip `//` inside JSON string literals (Hudi avscs don't put those there).
pub fn strip_avro_line_comments(src: &str) -> String {
    let mut out = String::with_capacity(src.len());
    for line in src.lines() {
        let trimmed = match line.find("//") {
            Some(idx) => {
                // Keep URLs like http:// in license headers by only stripping when
                // `//` is outside a quoted string — license block is before `{`.
                if line[..idx].contains('"') {
                    line
                } else {
                    &line[..idx]
                }
            }
            None => line,
        };
        out.push_str(trimmed);
        out.push('\n');
    }
    out
}

fn parse_vendored(avsc: &str, name: &str) -> Result<Schema> {
    let cleaned = strip_avro_line_comments(avsc);
    // Drop the Apache license block before the opening `{`.
    let json = cleaned
        .find('{')
        .map(|i| &cleaned[i..])
        .ok_or_else(|| CoreError::Schema(format!("vendored {name}.avsc has no JSON object")))?;
    Schema::parse_str(json)
        .map_err(|e| CoreError::Schema(format!("failed to parse vendored {name}.avsc: {e}")))
}

/// Serialize `value` to Avro OCF using a Java writer schema.
///
/// Goes JSON → untyped Avro [`Value`] → [`Value::resolve`] so union indexes and
/// field order match the schema (unlike `to_value`/`append_ser`, which assume
/// null-first Option unions and Serialize field order).
pub fn encode_with_schema<T: serde::Serialize>(value: &T, schema: &Schema) -> Result<Vec<u8>> {
    use apache_avro::types::Value as AvroValue;
    use apache_avro::Writer as AvroWriter;

    let json = serde_json::to_value(value)
        .map_err(|e| CoreError::CommitMetadata(format!("Failed to JSON-encode for Avro: {e}")))?;
    let resolved = AvroValue::from(json).resolve(schema).map_err(|e| {
        CoreError::CommitMetadata(format!("Failed to resolve Avro value against schema: {e}"))
    })?;
    let mut writer = AvroWriter::new(schema, Vec::new());
    writer.append(resolved).map_err(|e| {
        CoreError::CommitMetadata(format!("Failed to append Avro record: {e}"))
    })?;
    writer.flush().map_err(|e| {
        CoreError::CommitMetadata(format!("Failed to flush Avro writer: {e}"))
    })?;
    writer.into_inner().map_err(|e| {
        CoreError::CommitMetadata(format!("Failed to finish Avro writer: {e}"))
    })
}

macro_rules! vendored_schema {
    ($static_name:ident, $file:literal, $label:literal) => {
        pub fn $static_name() -> Result<&'static Schema> {
            static SCHEMA: OnceLock<Result<Schema, String>> = OnceLock::new();
            let result = SCHEMA.get_or_init(|| {
                parse_vendored(
                    include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/schemas/", $file)),
                    $label,
                )
                .map_err(|e| e.to_string())
            });
            match result {
                Ok(schema) => Ok(schema),
                Err(e) => Err(CoreError::Schema(e.clone())),
            }
        }
    };
}

vendored_schema!(
    hoodie_metadata_schema,
    "HoodieMetadata.avsc",
    "HoodieMetadata"
);
vendored_schema!(
    hoodie_commit_metadata_schema,
    "HoodieCommitMetadata.avsc",
    "HoodieCommitMetadata"
);

/// Java `HoodieReplaceCommitMetadata.avsc` references nested `HoodieWriteStat`
/// from `HoodieCommitMetadata.avsc`. apache-avro `parse_list` does not lift nested
/// named types, so we inline WriteStat into the replace schema at load time.
pub fn hoodie_replace_commit_metadata_schema() -> Result<&'static Schema> {
    static SCHEMA: OnceLock<Result<Schema, String>> = OnceLock::new();
    let result = SCHEMA.get_or_init(|| {
        let commit = strip_avro_line_comments(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/schemas/HoodieCommitMetadata.avsc"
        )));
        let replace = strip_avro_line_comments(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/schemas/HoodieReplaceCommitMetadata.avsc"
        )));
        let commit_json = commit
            .find('{')
            .map(|i| &commit[i..])
            .ok_or_else(|| "HoodieCommitMetadata.avsc has no JSON object".to_string())?;
        let replace_json = replace
            .find('{')
            .map(|i| &replace[i..])
            .ok_or_else(|| "HoodieReplaceCommitMetadata.avsc has no JSON object".to_string())?;

        let commit_val: serde_json::Value = serde_json::from_str(commit_json)
            .map_err(|e| format!("invalid HoodieCommitMetadata.avsc JSON: {e}"))?;
        let write_stat = commit_val
            .pointer("/fields/0/type/1/values/items")
            .cloned()
            .ok_or_else(|| "HoodieWriteStat missing from HoodieCommitMetadata.avsc".to_string())?;

        let mut replace_val: serde_json::Value = serde_json::from_str(replace_json)
            .map_err(|e| format!("invalid HoodieReplaceCommitMetadata.avsc JSON: {e}"))?;
        let items = replace_val
            .pointer_mut("/fields/0/type/1/values/items")
            .ok_or_else(|| {
                "partitionToWriteStats.items missing from HoodieReplaceCommitMetadata.avsc"
                    .to_string()
            })?;
        *items = write_stat;

        let inlined = serde_json::to_string(&replace_val)
            .map_err(|e| format!("failed to serialize inlined replace schema: {e}"))?;
        Schema::parse_str(&inlined)
            .map_err(|e| format!("failed to parse inlined HoodieReplaceCommitMetadata: {e}"))
    });
    match result {
        Ok(schema) => Ok(schema),
        Err(e) => Err(CoreError::Schema(e.clone())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_vendored_java_schemas() {
        assert!(matches!(
            hoodie_metadata_schema().unwrap(),
            Schema::Record(_)
        ));
        assert!(matches!(
            hoodie_commit_metadata_schema().unwrap(),
            Schema::Record(_)
        ));
        assert!(matches!(
            hoodie_replace_commit_metadata_schema().unwrap(),
            Schema::Record(_)
        ));
    }

    #[test]
    fn strip_comments_preserves_json() {
        let src = "{\n  // note\n  \"type\": \"record\"\n}\n";
        let cleaned = strip_avro_line_comments(src);
        assert!(!cleaned.contains("// note"));
        assert!(cleaned.contains("\"type\""));
    }
}
