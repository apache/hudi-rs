// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Reading which commits a rollback instant rolled back.

use crate::Result;
use crate::error::CoreError;
use apache_avro::Reader as AvroReader;
use apache_avro::from_value;
use serde::Deserialize;
use std::io::Cursor;

/// The fields of `HoodieRollbackMetadata` this crate reads.
///
/// Deliberately a subset. Hudi's schema also carries timings, per-partition
/// detail and a version, none of which the valid-instant set consults; Avro
/// deserialisation ignores fields the target struct does not name, so a schema
/// that grows does not break this.
///
/// `commitsRollback` is the field that matters: Java's `getRollbackedCommits`
/// returns exactly it for a rollback instant
/// (`HoodieTableMetadataUtil.java:2164`).
#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct RollbackMetadata {
    /// The instant this rollback ran at.
    pub start_rollback_time: String,
    /// The commits this rollback rolled back. Their log blocks were written and
    /// then re-applied, which is why the metadata table counts them as valid.
    pub commits_rollback: Vec<String>,
}

impl RollbackMetadata {
    /// Decode the bytes of a completed `.rollback` instant.
    ///
    /// The file is an Avro **object container** -- schema in the header, one
    /// datum -- because Hudi writes it with `DataFileWriter`
    /// (`TimelineMetadataUtils.serializeAvroMetadata`). That is the same shape
    /// [`crate::metadata::commit::HoodieCommitMetadata::from_avro_bytes`] reads,
    /// so this follows it rather than inventing a second convention.
    pub fn from_avro_bytes(bytes: &[u8]) -> Result<Self> {
        let reader = AvroReader::new(Cursor::new(bytes)).map_err(|e| {
            CoreError::CommitMetadata(format!("Failed to create Avro reader for rollback: {e}"))
        })?;
        let mut records = reader;
        let value = records
            .next()
            .ok_or_else(|| {
                CoreError::CommitMetadata("Rollback metadata contains no records".to_string())
            })?
            .map_err(|e| {
                CoreError::CommitMetadata(format!("Failed to read rollback record: {e}"))
            })?;
        from_value::<Self>(&value).map_err(|e| {
            CoreError::CommitMetadata(format!("Failed to deserialize rollback metadata: {e}"))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::types::Value;
    use apache_avro::{Schema, Writer};

    /// Hudi's own schema, copied verbatim into the test data.
    ///
    /// Read from the file rather than typed into this test on purpose: a
    /// hand-written schema would let the fixture drift into agreeing with the
    /// reader instead of with Hudi. If Hudi's schema changes, this fixture
    /// changes with it.
    /// Hudi's rollback schema with its one named dependency **inlined**.
    ///
    /// Hudi writes the container header with `SpecificDatumWriter`, whose schema
    /// has `HoodieInstantInfo` defined at its first use rather than referenced
    /// across files. A header carrying an unresolved reference is not something
    /// a real `.rollback` file contains, and a reader cannot resolve one from the
    /// file alone -- so a fixture built from the raw cross-file text would fail
    /// for a reason no real file exhibits.
    ///
    /// The substitution is textual and deliberate: both halves are Hudi's own
    /// schema files, unedited apart from splicing one into the other where the
    /// reference sits.
    fn inlined_schema_text() -> String {
        let dir =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../test/data/avro_schemas");
        let read = |name: &str| {
            std::fs::read_to_string(dir.join(name))
                .unwrap_or_else(|e| panic!("{name} must be present: {e}"))
        };
        let info = read("HoodieInstantInfo.avsc");
        let rollback = read("HoodieRollbackMetadata.avsc");
        let reference = "\"HoodieInstantInfo\"";
        assert!(
            rollback.contains(reference),
            "the rollback schema must reference HoodieInstantInfo, or this splice is stale"
        );
        rollback.replace(reference, info.trim())
    }

    #[allow(dead_code)]
    fn hudi_schemas() -> Vec<Schema> {
        let dir =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../test/data/avro_schemas");
        let read = |name: &str| {
            std::fs::read_to_string(dir.join(name))
                .unwrap_or_else(|e| panic!("{name} must be present: {e}"))
        };
        // `HoodieRollbackMetadata` names `HoodieInstantInfo`, which lives in its
        // own file -- Hudi's build resolves the reference across files, so a
        // standalone parse of the one schema fails on an unknown type. Both are
        // parsed together, dependency first.
        let info = read("HoodieInstantInfo.avsc");
        let rollback = read("HoodieRollbackMetadata.avsc");
        Schema::parse_list([info.as_str(), rollback.as_str()])
            .expect("Hudi's schemas must parse together")
    }

    /// An Avro object-container file, the shape `DataFileWriter` produces and so
    /// the shape a real `.rollback` instant has on disk.
    fn container_bytes(start: &str, rolled_back: &[&str]) -> Vec<u8> {
        let schema = Schema::parse_str(&inlined_schema_text())
            .expect("Hudi's schema, with its dependency inlined, must parse");
        let mut writer = Writer::new(&schema, Vec::new());
        let record = Value::Record(vec![
            ("startRollbackTime".into(), Value::String(start.into())),
            ("timeTakenInMillis".into(), Value::Long(42)),
            ("totalFilesDeleted".into(), Value::Int(3)),
            (
                "commitsRollback".into(),
                Value::Array(
                    rolled_back
                        .iter()
                        .map(|c| Value::String((*c).into()))
                        .collect(),
                ),
            ),
            ("partitionMetadata".into(), Value::Map(Default::default())),
            ("version".into(), Value::Union(0, Box::new(Value::Int(1)))),
            ("instantsRollback".into(), Value::Array(vec![])),
        ]);
        writer.append(record).expect("append");
        writer.into_inner().expect("container bytes")
    }

    /// The commits a rollback rolled back are read back exactly.
    ///
    /// Two of them, in order, so the test distinguishes "read the array" from
    /// "read the first element" -- a single-element fixture would pass either
    /// way.
    #[test]
    fn a_rollback_yields_the_commits_it_rolled_back() -> Result<()> {
        let bytes = container_bytes(
            "20250103000000000",
            &["20250101000000000", "20250102000000000"],
        );
        let parsed = RollbackMetadata::from_avro_bytes(&bytes)?;
        assert_eq!(parsed.start_rollback_time, "20250103000000000");
        assert_eq!(
            parsed.commits_rollback,
            vec!["20250101000000000", "20250102000000000"],
            "every rolled-back commit must come back, in order"
        );
        Ok(())
    }

    /// A rollback that rolled nothing back yields an empty list, not an error.
    /// Hudi writes such instants, and treating one as a failure would make a
    /// valid timeline unreadable.
    #[test]
    fn a_rollback_of_nothing_is_not_an_error() -> Result<()> {
        let parsed = RollbackMetadata::from_avro_bytes(&container_bytes("20250103000000000", &[]))?;
        assert!(parsed.commits_rollback.is_empty());
        Ok(())
    }

    /// Bytes that are not an Avro container fail loudly rather than yielding an
    /// empty set. An empty set here would silently drop every commit the
    /// rollback covered.
    #[test]
    fn garbage_is_an_error_not_an_empty_result() {
        let err = RollbackMetadata::from_avro_bytes(b"not avro at all").unwrap_err();
        assert!(
            err.to_string().contains("rollback"),
            "the error must name what failed to read, got: {err}"
        );
    }
}
