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

//! Minimal host-contention pre-flight (minimal version).
//!
//! Reads `/proc/loadavg` and `/proc/meminfo` to snapshot host pressure before
//! each measured iteration. No waiting/rerun loop yet — this only records the
//! snapshot and flags a run `contended` when `load1 / nproc > LOAD_THRESHOLD`.

/// Fraction of cores above which a run is flagged `contended`.
pub const LOAD_THRESHOLD: f64 = 0.5;

/// A point-in-time host pressure snapshot.
#[derive(Debug, Clone, Copy)]
pub struct HostSnapshot {
    /// 1-minute load average (`/proc/loadavg` field 1).
    pub load1: f64,
    /// Available memory in KB (`MemAvailable` from `/proc/meminfo`).
    pub mem_available_kb: u64,
}

impl HostSnapshot {
    pub fn capture() -> Self {
        HostSnapshot {
            load1: read_loadavg1().unwrap_or(0.0),
            mem_available_kb: read_mem_available_kb().unwrap_or(0),
        }
    }

    /// Whether this snapshot indicates the host is contended, given `nproc`.
    pub fn is_contended(&self, nproc: usize) -> bool {
        nproc > 0 && (self.load1 / nproc as f64) > LOAD_THRESHOLD
    }
}

fn read_loadavg1() -> Option<f64> {
    let s = std::fs::read_to_string("/proc/loadavg").ok()?;
    s.split_whitespace().next()?.parse::<f64>().ok()
}

fn read_mem_available_kb() -> Option<u64> {
    let s = std::fs::read_to_string("/proc/meminfo").ok()?;
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("MemAvailable:") {
            // Format: "MemAvailable:   12345678 kB"
            return rest.split_whitespace().next()?.parse::<u64>().ok();
        }
    }
    None
}

/// Number of online logical CPUs.
pub fn nproc() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}
