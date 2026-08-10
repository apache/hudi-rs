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

//! Completion time query view for looking up completion timestamps from request timestamps.
//!
//! This module provides the [`CompletionTimeView`] trait for mapping request timestamps
//! to completion timestamps. This is essential for timeline layout v2 where file names contain
//! request timestamps but file ordering and association must be based on completion timestamps.
//!
//! # Timeline Layout Differences
//!
//! - **Timeline layout v1**: Completion timestamps are not tracked. The completion time map is empty
//!   and `get_completion_time()` always returns `None`.
//!
//! - **Timeline layout v2**: Completion timestamps are tracked in completed instants. The completion timestamp
//!   is stored in the timeline instant filename as `{request}_{completion}.{action}`.
//!   The completion time map is populated from timeline instants.
//!
//! # Implementation
//!
//! [`TimelineView`] is the main implementation of this trait. It checks
//! [`HudiTableConfig::TimelineLayoutVersion`] to determine whether to build the completion time map.
//!
//! [`TimelineView`]: crate::timeline::view::TimelineView
//! [`HudiTableConfig::TimelineLayoutVersion`]: crate::config::table::HudiTableConfig::TimelineLayoutVersion

/// A view for querying completion timestamps from request timestamps.
///
/// This trait abstracts the completion time lookup logic to support both
/// V1 and V2 timeline layouts.
pub trait CompletionTimeView {
    /// Get the completion timestamp for a given request timestamp.
    ///
    /// Returns `Some(completion_timestamp)` if the request timestamp corresponds
    /// to a completed commit, `None` if the commit is pending/unknown or if
    /// completion time is not tracked.
    fn get_completion_time(&self, request_timestamp: &str) -> Option<&str>;

    /// Whether the commit that wrote `request_timestamp` has committed, and so
    /// whether the files it wrote may be read.
    ///
    /// Mirrors Java's `containsInstant(ts) || isBeforeTimelineStarts(ts)`
    /// (`BaseHoodieTimeline.java:494`) — a commit counts as committed if it is a
    /// completed instant in the active timeline, **or** if it sorts before the
    /// active timeline starts, which means it was archived and archived instants
    /// are completed by definition.
    ///
    /// This deliberately does not key off whether a completion timestamp is
    /// available. Timeline layout v1 records none at all, so a
    /// completion-timestamp test can only ever be applied to layout v2 — which
    /// left v1 tables reading files from commits that never completed. And on
    /// layout v2 the completion map is built from the *active* timeline, so the
    /// same test discarded files from commits that had merely been archived.
    fn is_committed(&self, request_timestamp: &str) -> bool;
}
