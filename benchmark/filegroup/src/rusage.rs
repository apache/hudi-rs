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

//! Thin `getrusage(RUSAGE_SELF)` wrapper for CPU + peak-RSS accounting.

/// A snapshot of `getrusage(RUSAGE_SELF)`.
#[derive(Debug, Clone, Copy)]
pub struct Rusage {
    /// Total user-mode CPU time in milliseconds (cumulative, process-wide).
    pub user_ms: u64,
    /// Total system-mode CPU time in milliseconds (cumulative, process-wide).
    pub sys_ms: u64,
    /// Peak resident set size in kilobytes (`ru_maxrss`, monotonic).
    pub max_rss_kb: u64,
}

/// `ru_maxrss` in kilobytes, whichever unit the platform reports it in.
///
/// Linux documents the field as kilobytes; the BSDs, macOS included, report
/// **bytes**. Reading it as kilobytes on macOS overstates peak RSS by 1024x,
/// which silently turns a 21 MB process into a 21 GB one in the report. The
/// units are a platform property, not a runtime one, so this is a `cfg`, not a
/// probe.
fn max_rss_kb(ru_maxrss: i64) -> u64 {
    let raw = ru_maxrss.max(0) as u64;
    #[cfg(target_os = "macos")]
    {
        raw / 1024
    }
    #[cfg(not(target_os = "macos"))]
    {
        raw
    }
}

impl Rusage {
    /// Capture the current cumulative `RUSAGE_SELF` counters.
    pub fn capture() -> Self {
        // SAFETY: `getrusage` writes a fully-initialized `rusage` into the
        // zeroed struct; we only read POD fields afterwards.
        let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
        let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
        if rc != 0 {
            return Rusage {
                user_ms: 0,
                sys_ms: 0,
                max_rss_kb: 0,
            };
        }
        Rusage {
            user_ms: timeval_ms(usage.ru_utime),
            sys_ms: timeval_ms(usage.ru_stime),
            max_rss_kb: max_rss_kb(usage.ru_maxrss),
        }
    }

    /// Per-iteration deltas: CPU time is the difference vs the `before`
    /// snapshot; `max_rss_kb` is the absolute peak observed at `self`
    /// (`ru_maxrss` is monotonic, so the after-snapshot value is the
    /// high-water mark reached by the end of the iteration).
    pub fn delta(&self, before: &Rusage) -> RusageDelta {
        RusageDelta {
            user_ms: self.user_ms.saturating_sub(before.user_ms),
            sys_ms: self.sys_ms.saturating_sub(before.sys_ms),
            max_rss_kb: self.max_rss_kb,
        }
    }
}

/// Per-iteration CPU deltas + absolute peak RSS.
#[derive(Debug, Clone, Copy)]
pub struct RusageDelta {
    pub user_ms: u64,
    pub sys_ms: u64,
    pub max_rss_kb: u64,
}

fn timeval_ms(tv: libc::timeval) -> u64 {
    (tv.tv_sec as u64) * 1000 + (tv.tv_usec as u64) / 1000
}
