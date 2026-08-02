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

//! Ported from the merge-on-read reader. Nothing consumes it yet, so its
//! items are unreachable from the crate's call graph until the reader wires in.
#![allow(dead_code, unused_macros, unused_imports)]

//! Stage-timing profiling primitives for the file-group reader (perf harness).
//!
//! A single canonical write path for the per-stage wall-time counters, so the
//! `let s = Instant::now(); …; self.x [+]= s.elapsed()…` shape isn't copy-pasted
//! at every timing site. Inlining (rather than an RAII/`Drop` guard) is
//! deliberate: a guard holding `&mut self.field` for the scope would double-borrow
//! `self` against bodies that also need `&mut self` (e.g.
//! `process_queued_blocks_for_instant`); a macro releases the body's borrow before
//! the field write.

/// Record wall-ms of `$body` once into the u64 place `$dst`.
///
/// Where the body is a single fallible expression, keep `?` OUTSIDE the macro so
/// the elapsed time is still attributed on the error path.
macro_rules! profile_once {
    ($dst:expr, $body:expr) => {{
        let __start = std::time::Instant::now();
        #[allow(clippy::let_unit_value)]
        let __out = $body;
        $dst = __start.elapsed().as_millis() as u64;
        __out
    }};
}

pub(crate) use profile_once;
