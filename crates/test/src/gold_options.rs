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
//! The read-option cases a fixture ships, and where each case's gold lives.
//!
//! A fixture's `gold_options/` directory holds one subdirectory per case — what
//! Hudi returns for that case — plus `manifest.json` naming the cases and the
//! read options that produced them.
//!
//! The manifest has a single author: the Spark generator
//! (`data/quickstart_trips_table/mor/avro/gold_options.scala`), which derives
//! each case from the fixture's own schema and `hoodie.properties` and then
//! records what it selected. This module only reads it back. Nothing about a
//! case is expressed twice, so the generator and the sweep cannot come to
//! disagree about what a case means — which is the failure the incremental
//! windows already have, spelled out once in `.gold.scala` and again in the
//! Rust test with nothing tying them together.
//!
//! Which cases a fixture ships is therefore a property *of the fixture*, not a
//! constant here: a table that declares no ordering field has no
//! `drop_ordering` case, and one with no container columns has no
//! `nested_only`. Callers must not assume a fixed set.

use std::path::Path;

use serde::Deserialize;

/// One read-option case: the projection that produced this case's gold.
///
/// `projection` is passed verbatim to `ReadOptions::with_projection`, so the
/// output's column order is expected to match it element-for-element — the
/// `reordered` case exists precisely to assert that.
#[derive(Debug, Clone, Deserialize)]
pub struct OptionCase {
    pub name: String,
    pub projection: Vec<String>,
}

/// Every case one fixture ships.
#[derive(Debug, Clone, Deserialize)]
pub struct OptionManifest {
    /// The fixture this manifest belongs to, as the generator saw it. Checked
    /// against the sibling table directory so a manifest copied into the wrong
    /// fixture is caught rather than silently comparing the wrong gold.
    pub fixture: String,
    pub cases: Vec<OptionCase>,
}

/// Whether `options_dir` holds a manifest at all.
///
/// A fixture predating the option sweep has no `gold_options/`; the sweep counts
/// those rather than failing, the same way it counts fixtures with no
/// `gold_data/`.
pub fn has_option_manifest(options_dir: &str) -> bool {
    manifest_path(options_dir).is_file()
}

fn manifest_path(options_dir: &str) -> std::path::PathBuf {
    Path::new(options_dir).join("manifest.json")
}

/// Where one case's gold parquet lives.
pub fn case_gold_dir(options_dir: &str, case: &str) -> String {
    Path::new(options_dir)
        .join(case)
        .to_str()
        .unwrap_or_default()
        .to_string()
}

/// Read and validate a fixture's option manifest.
///
/// Validation is deliberately strict: a manifest is generated data, so anything
/// malformed means the generator or the fixture is wrong, and the sweep must say
/// so rather than quietly comparing fewer cases. An empty case list, an empty
/// projection, a duplicate case name, or a case whose gold directory is missing
/// are all rejected here — each would otherwise read as "this fixture is
/// covered" while asserting nothing.
///
/// # Errors
/// Returns `Err(message)` if the manifest is unreadable, is not valid JSON, names
/// a different fixture than the directory it sits in, or fails any check above.
pub fn read_option_manifest(options_dir: &str) -> Result<OptionManifest, String> {
    let path = manifest_path(options_dir);
    let text = std::fs::read_to_string(&path)
        .map_err(|e| format!("read manifest '{}': {e}", path.display()))?;
    let manifest: OptionManifest = serde_json::from_str(&text)
        .map_err(|e| format!("parse manifest '{}': {e}", path.display()))?;

    // `gold_options/` sits beside the table directory, which is named after the
    // fixture. A manifest naming a different fixture was copied from elsewhere
    // and would compare this table's output against another table's gold.
    let sibling = Path::new(options_dir)
        .parent()
        .map(|root| root.join(&manifest.fixture));
    if !sibling.is_some_and(|s| s.is_dir()) {
        return Err(format!(
            "manifest '{}' claims fixture '{}', but no table directory by that \
             name sits beside it",
            path.display(),
            manifest.fixture
        ));
    }

    if manifest.cases.is_empty() {
        return Err(format!(
            "manifest '{}' lists no cases; a fixture that ships gold_options must \
             exercise at least one",
            path.display()
        ));
    }

    let mut seen = std::collections::HashSet::new();
    for case in &manifest.cases {
        if case.projection.is_empty() {
            return Err(format!(
                "manifest '{}' case '{}' has an empty projection",
                path.display(),
                case.name
            ));
        }
        if !seen.insert(case.name.as_str()) {
            return Err(format!(
                "manifest '{}' names case '{}' twice",
                path.display(),
                case.name
            ));
        }
        let dir = case_gold_dir(options_dir, &case.name);
        if !Path::new(&dir).is_dir() {
            return Err(format!(
                "manifest '{}' names case '{}' but '{dir}' does not exist",
                path.display(),
                case.name
            ));
        }
    }
    Ok(manifest)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Lay out an extraction root the way a fixture zip unpacks — a table
    /// directory named `fixture` beside a `gold_options/` holding
    /// `manifest.json` and a directory per name in `case_dirs`.
    ///
    /// Case directories are created separately from the cases the manifest
    /// names, so a test can express a manifest that references a directory which
    /// is not there. Returns the `gold_options/` path.
    fn write_options_dir(
        root: &Path,
        fixture: &str,
        manifest_json: &str,
        case_dirs: &[&str],
    ) -> String {
        std::fs::create_dir_all(root.join(fixture)).unwrap();
        let dir = root.join("gold_options");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("manifest.json"), manifest_json).unwrap();
        for case in case_dirs {
            std::fs::create_dir_all(dir.join(case)).unwrap();
        }
        dir.to_str().unwrap().to_string()
    }

    const VALID: &str = r#"{
        "fixture": "t",
        "cases": [
            { "name": "key_only", "projection": ["key"] },
            { "name": "drop_key", "projection": ["ts", "value"] }
        ]
    }"#;

    #[test]
    fn test_read_option_manifest_valid_round_trips() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = write_options_dir(tmp.path(), "t", VALID, &["key_only", "drop_key"]);

        let manifest = read_option_manifest(&dir).expect("a well-formed manifest must load");
        assert_eq!(manifest.fixture, "t");
        assert_eq!(manifest.cases.len(), 2);
        assert_eq!(manifest.cases[1].name, "drop_key");
        // Projection order is the contract with `with_projection`, so it must
        // survive the round trip exactly rather than as a set.
        assert_eq!(manifest.cases[1].projection, vec!["ts", "value"]);
    }

    /// A case naming gold that isn't there must fail loudly. Skipping it would
    /// silently reduce coverage while still reporting the fixture as swept.
    #[test]
    fn test_read_option_manifest_missing_case_dir_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = write_options_dir(tmp.path(), "t", VALID, &["key_only"]);

        let err = read_option_manifest(&dir).expect_err("a missing case directory must error");
        assert!(err.contains("drop_key"), "error must name the case: {err}");
    }

    /// A manifest lifted from another fixture must not be read against this
    /// table's output — the gold it points at describes a different table.
    #[test]
    fn test_read_option_manifest_wrong_fixture_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = write_options_dir(tmp.path(), "other", VALID, &["key_only", "drop_key"]);

        let err = read_option_manifest(&dir).expect_err("a foreign manifest must error");
        assert!(
            err.contains("claims fixture 't'"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_read_option_manifest_empty_cases_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = write_options_dir(tmp.path(), "t", r#"{"fixture": "t", "cases": []}"#, &[]);

        let err = read_option_manifest(&dir).expect_err("an empty case list must error");
        assert!(err.contains("no cases"), "unexpected error: {err}");
    }

    #[test]
    fn test_read_option_manifest_empty_projection_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let json = r#"{"fixture": "t", "cases": [{"name": "c", "projection": []}]}"#;
        let dir = write_options_dir(tmp.path(), "t", json, &["c"]);

        let err = read_option_manifest(&dir).expect_err("an empty projection must error");
        assert!(err.contains("empty projection"), "unexpected error: {err}");
    }

    /// Two cases sharing a name would silently compare one case's output against
    /// the other's gold.
    #[test]
    fn test_read_option_manifest_duplicate_case_name_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let json = r#"{"fixture": "t", "cases": [
            {"name": "c", "projection": ["a"]},
            {"name": "c", "projection": ["b"]}
        ]}"#;
        let dir = write_options_dir(tmp.path(), "t", json, &["c"]);

        let err = read_option_manifest(&dir).expect_err("a duplicate case name must error");
        assert!(err.contains("twice"), "unexpected error: {err}");
    }

    #[test]
    fn test_has_option_manifest_reports_absence() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(!has_option_manifest(tmp.path().to_str().unwrap()));

        let dir = write_options_dir(tmp.path(), "t", VALID, &["key_only", "drop_key"]);
        assert!(has_option_manifest(&dir));
    }
}
