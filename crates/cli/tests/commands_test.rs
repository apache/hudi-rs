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

//! Integration tests for hudi-cli commands using real Hudi test tables.

use assert_cmd::Command;
use hudi_test::SampleTable;
use predicates::prelude::*;

fn hudi_cli() -> Command {
    Command::cargo_bin("hudi-cli").unwrap()
}

// ===== table info =====

#[test]
fn test_table_info_cow_nonpartitioned() {
    let path = SampleTable::V6Nonpartitioned.path_to_cow();
    hudi_cli()
        .args(["table", "info", &path])
        .assert()
        .success()
        .stdout(predicate::str::contains("Table Info"))
        .stdout(predicate::str::contains("Table Name"))
        .stdout(predicate::str::contains("Table Type"))
        .stdout(predicate::str::contains("COPY_ON_WRITE"));
}

#[test]
fn test_table_info_cow_partitioned() {
    let path = SampleTable::V6SimplekeygenNonhivestyle.path_to_cow();
    hudi_cli()
        .args(["table", "info", &path])
        .assert()
        .success()
        .stdout(predicate::str::contains("Table Info"))
        .stdout(predicate::str::contains("COPY_ON_WRITE"));
}

#[test]
fn test_table_info_mor() {
    let path = SampleTable::V6Nonpartitioned.path_to_mor_parquet();
    hudi_cli()
        .args(["table", "info", &path])
        .assert()
        .success()
        .stdout(predicate::str::contains("MERGE_ON_READ"));
}

#[test]
fn test_table_info_json_output() {
    let path = SampleTable::V6Nonpartitioned.path_to_cow();
    let output = hudi_cli()
        .args(["--output", "json", "table", "info", &path])
        .output()
        .unwrap();
    assert!(output.status.success());
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(json.get("table_name").is_some());
    assert!(json.get("table_type").is_some());
}

#[test]
fn test_table_info_empty_table() {
    let path = SampleTable::V6Empty.path_to_cow();
    hudi_cli()
        .args(["table", "info", &path])
        .assert()
        .success()
        .stdout(predicate::str::contains("Table Info"));
}

#[test]
fn test_table_info_invalid_path() {
    hudi_cli()
        .args(["table", "info", "/nonexistent/path"])
        .assert()
        .failure();
}

// ===== table config =====

#[test]
fn test_table_config_all() {
    let path = SampleTable::V6Nonpartitioned.path_to_cow();
    hudi_cli()
        .args(["table", "config", &path])
        .assert()
        .success()
        .stdout(predicate::str::contains("Configuration"))
        .stdout(predicate::str::contains("hoodie."));
}

#[test]
fn test_table_config_with_filter() {
    let path = SampleTable::V6Nonpartitioned.path_to_cow();
    hudi_cli()
        .args(["table", "config", &path, "--filter", "hoodie.table"])
        .assert()
        .success()
        .stdout(predicate::str::contains("hoodie.table"));
}

#[test]
fn test_table_config_json_output() {
    let path = SampleTable::V6Nonpartitioned.path_to_cow();
    let output = hudi_cli()
        .args(["--output", "json", "table", "config", &path])
        .output()
        .unwrap();
    assert!(output.status.success());
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(json.get("data").is_some());
}

// ===== timeline show =====

#[test]
fn test_timeline_show() {
    let path = SampleTable::V6Nonpartitioned.path_to_cow();
    hudi_cli()
        .args(["timeline", "show", &path])
        .assert()
        .success()
        .stdout(predicate::str::contains("Timeline"))
        .stdout(predicate::str::contains("Timestamp"));
}

#[test]
fn test_timeline_show_with_limit() {
    let path = SampleTable::V6Nonpartitioned.path_to_cow();
    hudi_cli()
        .args(["timeline", "show", &path, "--limit", "5"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Timeline"));
}

#[test]
fn test_timeline_show_with_action_filter() {
    let path = SampleTable::V6Nonpartitioned.path_to_cow();
    hudi_cli()
        .args(["timeline", "show", &path, "--action", "commit"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Timeline"));
}

#[test]
fn test_timeline_show_json_output() {
    let path = SampleTable::V6Nonpartitioned.path_to_cow();
    let output = hudi_cli()
        .args(["--output", "json", "timeline", "show", &path])
        .output()
        .unwrap();
    assert!(output.status.success());
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(json.get("total_completed_instants").is_some());
}

#[test]
fn test_timeline_show_empty_table() {
    let path = SampleTable::V6Empty.path_to_cow();
    hudi_cli()
        .args(["timeline", "show", &path])
        .assert()
        .success()
        .stdout(predicate::str::contains("Total completed instants"));
}

// ===== timeline pending =====

#[test]
fn test_timeline_pending() {
    let path = SampleTable::V6Nonpartitioned.path_to_cow();
    hudi_cli()
        .args(["timeline", "pending", &path])
        .assert()
        .success()
        .stdout(predicate::str::contains("Pending Operations"));
}

// ===== commits show =====

#[test]
fn test_commits_show() {
    let path = SampleTable::V6Nonpartitioned.path_to_cow();
    hudi_cli()
        .args(["commits", "show", &path])
        .assert()
        .success()
        .stdout(predicate::str::contains("Commits"))
        .stdout(predicate::str::contains("Total commits"));
}

#[test]
fn test_commits_show_with_limit() {
    let path = SampleTable::V6Nonpartitioned.path_to_cow();
    hudi_cli()
        .args(["commits", "show", &path, "--limit", "2"])
        .assert()
        .success()
        .stdout(predicate::str::contains("latest 2"));
}

#[test]
fn test_commits_show_json_output() {
    let path = SampleTable::V6Nonpartitioned.path_to_cow();
    let output = hudi_cli()
        .args(["--output", "json", "commits", "show", &path])
        .output()
        .unwrap();
    assert!(output.status.success());
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(json.get("total_commits").is_some());
    assert!(json.get("data").is_some());
}

#[test]
fn test_commits_show_empty_table() {
    let path = SampleTable::V6Empty.path_to_cow();
    hudi_cli()
        .args(["commits", "show", &path])
        .assert()
        .success()
        .stdout(predicate::str::contains("Total commits"));
}

// ===== files list =====

#[test]
fn test_files_list() {
    let path = SampleTable::V6Nonpartitioned.path_to_cow();
    hudi_cli()
        .args(["files", "list", &path])
        .assert()
        .success()
        .stdout(predicate::str::contains("File Slices"))
        .stdout(predicate::str::contains("Total file slices"));
}

#[test]
fn test_files_list_partitioned() {
    let path = SampleTable::V6SimplekeygenNonhivestyle.path_to_cow();
    hudi_cli()
        .args(["files", "list", &path])
        .assert()
        .success()
        .stdout(predicate::str::contains("File Slices"));
}

#[test]
fn test_files_list_with_limit() {
    let path = SampleTable::V6Nonpartitioned.path_to_cow();
    hudi_cli()
        .args(["files", "list", &path, "--limit", "2"])
        .assert()
        .success()
        .stdout(predicate::str::contains("File Slices"));
}

#[test]
fn test_files_list_json_output() {
    let path = SampleTable::V6Nonpartitioned.path_to_cow();
    let output = hudi_cli()
        .args(["--output", "json", "files", "list", &path])
        .output()
        .unwrap();
    assert!(output.status.success());
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(json.get("total_file_slices").is_some());
}

#[test]
fn test_files_list_jsonl_output() {
    let path = SampleTable::V6Nonpartitioned.path_to_cow();
    let output = hudi_cli()
        .args(["--output", "jsonl", "files", "list", &path])
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    // Each line should be valid JSON
    for line in stdout.lines() {
        let _: serde_json::Value = serde_json::from_str(line).unwrap();
    }
}

// ===== files stats =====

#[test]
fn test_files_stats() {
    let path = SampleTable::V6Nonpartitioned.path_to_cow();
    hudi_cli()
        .args(["files", "stats", &path])
        .assert()
        .success()
        .stdout(predicate::str::contains("File Statistics"))
        .stdout(predicate::str::contains("Total file groups"))
        .stdout(predicate::str::contains("Avg file size"));
}

#[test]
fn test_files_stats_empty_table() {
    let path = SampleTable::V6Empty.path_to_cow();
    hudi_cli()
        .args(["files", "stats", &path])
        .assert()
        .success()
        .stdout(predicate::str::contains("No files found"));
}

#[test]
fn test_files_stats_json_output() {
    let path = SampleTable::V6Nonpartitioned.path_to_cow();
    let output = hudi_cli()
        .args(["--output", "json", "files", "stats", &path])
        .output()
        .unwrap();
    assert!(output.status.success());
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(json.get("file_statistics").is_some());
    assert!(json.get("size_distribution").is_some());
}

// ===== schema show =====

#[test]
fn test_schema_show() {
    let path = SampleTable::V6Nonpartitioned.path_to_cow();
    hudi_cli()
        .args(["schema", "show", &path])
        .assert()
        .success()
        .stdout(predicate::str::contains("Schema"))
        .stdout(predicate::str::contains("Field Name"))
        .stdout(predicate::str::contains("Data Type"));
}

#[test]
fn test_schema_show_json_output() {
    let path = SampleTable::V6Nonpartitioned.path_to_cow();
    let output = hudi_cli()
        .args(["--output", "json", "schema", "show", &path])
        .output()
        .unwrap();
    assert!(output.status.success());
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(json.get("total_fields").is_some());
    assert!(json.get("data").is_some());
    let fields = json["data"].as_array().unwrap();
    assert!(!fields.is_empty());
}

// ===== diagnose =====

#[test]
fn test_diagnose_cow() {
    let path = SampleTable::V6Nonpartitioned.path_to_cow();
    hudi_cli()
        .args(["diagnose", &path])
        .assert()
        .success()
        .stdout(predicate::str::contains("Diagnostic Summary"))
        .stdout(predicate::str::contains("Findings"))
        .stdout(predicate::str::contains("Health"));
}

#[test]
fn test_diagnose_mor() {
    let path = SampleTable::V6Nonpartitioned.path_to_mor_parquet();
    hudi_cli()
        .args(["diagnose", &path])
        .assert()
        .success()
        .stdout(predicate::str::contains("Diagnostic Summary"))
        .stdout(predicate::str::contains("MERGE_ON_READ"));
}

#[test]
fn test_diagnose_empty_table() {
    let path = SampleTable::V6Empty.path_to_cow();
    hudi_cli()
        .args(["diagnose", &path])
        .assert()
        .success()
        .stdout(predicate::str::contains("Diagnostic Summary"))
        .stdout(predicate::str::contains("WARNING"));
}

#[test]
fn test_diagnose_json_output() {
    let path = SampleTable::V6Nonpartitioned.path_to_cow();
    let output = hudi_cli()
        .args(["--output", "json", "diagnose", &path])
        .output()
        .unwrap();
    assert!(output.status.success());
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(json.get("diagnostic_summary").is_some());
    assert!(json.get("findings").is_some());
}

#[test]
fn test_diagnose_partitioned() {
    let path = SampleTable::V6SimplekeygenNonhivestyle.path_to_cow();
    hudi_cli()
        .args(["diagnose", &path])
        .assert()
        .success()
        .stdout(predicate::str::contains("Diagnostic Summary"))
        .stdout(predicate::str::contains("Partitions"));
}

// ===== v8 table support =====

#[test]
fn test_table_info_v8() {
    let path = SampleTable::V8Nonpartitioned.path_to_cow();
    hudi_cli()
        .args(["table", "info", &path])
        .assert()
        .success()
        .stdout(predicate::str::contains("Table Info"));
}

#[test]
fn test_diagnose_v8() {
    let path = SampleTable::V8Nonpartitioned.path_to_cow();
    hudi_cli()
        .args(["diagnose", &path])
        .assert()
        .success()
        .stdout(predicate::str::contains("Diagnostic Summary"));
}

// ===== global CLI flags =====

#[test]
fn test_version_flag() {
    hudi_cli()
        .arg("--version")
        .assert()
        .success()
        .stdout(predicate::str::contains("hudi-cli"));
}

#[test]
fn test_help_flag() {
    hudi_cli()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("inspecting and diagnosing"))
        .stdout(predicate::str::contains("EXAMPLES"));
}

#[test]
fn test_subcommand_help() {
    hudi_cli()
        .args(["table", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("info"))
        .stdout(predicate::str::contains("config"));
}
