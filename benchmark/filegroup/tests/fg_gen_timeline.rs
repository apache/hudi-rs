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

//! The generated table must carry a timeline that a `Table` read accepts.
//!
//! A log block is gated on its own instant having completed. `fg-gen` writes its
//! log blocks at an instant one day after the base commit, so without a matching
//! delta commit the blocks are readable through a standalone `FileGroupReader`
//! -- which has no timeline and admits everything -- and silently dropped by
//! `Table::read`. A generator that produces a table the two paths disagree about
//! makes every merge-on-read measurement taken on it meaningless.

use std::path::{Path, PathBuf};
use std::process::Command;

const BASE_COMMIT: &str = "20250101000000000.commit";
const LOG_DELTA_COMMIT: &str = "20250102000000000.deltacommit";

/// A directory unique to this test run, removed on drop.
struct Scratch(PathBuf);

impl Scratch {
    fn new(tag: &str) -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("fg-gen-test-{tag}-{}-{nanos}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        Scratch(path)
    }

    /// `fg-gen` refuses to write into a directory that exists, so hand it a
    /// child rather than the scratch root.
    fn table(&self) -> PathBuf {
        self.0.join("table")
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn generate(out: &Path, log_files: usize) {
    let status = Command::new(env!("CARGO_BIN_EXE_fg-gen"))
        .args([
            "--out",
            out.to_str().unwrap(),
            "--files",
            "1",
            "--total-bytes",
            "200000",
            "--row-group-rows",
            "1000",
            "--log-files",
            &log_files.to_string(),
            "--log-records",
            "50",
        ])
        .status()
        .expect("fg-gen runs");
    assert!(status.success(), "fg-gen exited with {status}");
}

#[test]
fn a_merge_on_read_table_commits_the_instant_its_log_blocks_carry() {
    let scratch = Scratch::new("mor");
    let table = scratch.table();
    generate(&table, 2);

    let hoodie = table.join(".hoodie");
    assert!(
        hoodie.join(BASE_COMMIT).is_file(),
        "the base files' commit must exist"
    );
    assert!(
        hoodie.join(LOG_DELTA_COMMIT).is_file(),
        "the log blocks' instant must be committed, or a Table read drops every log record"
    );
}

#[test]
fn a_copy_on_write_table_has_no_delta_commit() {
    let scratch = Scratch::new("cow");
    let table = scratch.table();
    generate(&table, 0);

    let hoodie = table.join(".hoodie");
    assert!(hoodie.join(BASE_COMMIT).is_file());
    assert!(
        !hoodie.join(LOG_DELTA_COMMIT).exists(),
        "no log files were written, so nothing should claim a delta commit"
    );
}
