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
//! Behaviors this reader does not reproduce, and what a read does about them.
//!
//! Two kinds. Most are **refused**: a table that needs one gets an error naming
//! it, so nothing silently returns the wrong rows. A few are **quietly not
//! done** — the read succeeds and differs from what another reader would return.
//! Those are the dangerous ones, and the reason this module exists: each is
//! reported when a read touches it, so the difference is visible rather than
//! discovered later.
//!
//! Keep this list current. A gap that is closed should lose its entry, and a new
//! one should gain a test asserting how it is handled.

use crate::file_group::reader_v2::reader_context::ReaderContext;
use crate::file_group::reader_v2::reader_parameters::ReaderParameters;

/// A reader whose behavior this one is being compared against.
///
/// All three are named even while only one is in use, so that an entry added
/// later picks its comparison target rather than inventing one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum ComparedWith {
    /// Hudi's own reader.
    Jvm,
    /// File group reader version 1, the reader this one is replacing.
    VersionOne,
    /// Both.
    Both,
}

/// Something another reader does that this one does not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Gap {
    /// A slice this reader cannot merge by record position even though the read
    /// asked for it — the base file's instant could not be read from its name,
    /// or its format is not parquet. Hudi's reader is handed the instant by the
    /// file slice and never has to decline. Merging by key instead returns the
    /// same rows unless the file group holds duplicate record keys.
    PositionBasedMergeDeclined,
    /// A read that *started* merging by record position gave it up partway
    /// through, because a log block's positions were not usable — its
    /// base-file-instant header named a different base file, or its bitmap was
    /// empty. Everything from that block on is merged by record key.
    ///
    /// Reported separately from [`Self::PositionBasedMergeDeclined`] because the
    /// cause is in the data rather than in the read's setup, and because it
    /// happens after the read has been accepted: the setup gate saw nothing wrong.
    PositionBasedMergeAbandonedMidScan,
}

impl Gap {
    /// Which reader this differs from.
    pub(crate) fn compared_with(&self) -> ComparedWith {
        match self {
            // Version 1 has no position-based merge at all, so it is not the
            // reader this is measured against.
            Gap::PositionBasedMergeDeclined => ComparedWith::Jvm,
            // Hudi's reader falls back the same way, so the rows agree with it.
            // Reported anyway: the read no longer does what it was asked to, and
            // on a file group with duplicate keys that changes the answer.
            Gap::PositionBasedMergeAbandonedMidScan => ComparedWith::Jvm,
        }
    }

    /// What a reader would see, said plainly enough to act on.
    pub(crate) fn describe(&self) -> &'static str {
        match self {
            Gap::PositionBasedMergeDeclined => {
                "this read asked to merge by record position but the base file's instant or \
                 format could not support it; merging by record key instead, which differs only \
                 where a file group holds duplicate keys"
            }
            Gap::PositionBasedMergeAbandonedMidScan => {
                "this read began merging by record position and stopped partway through, because \
                 a log block's positions were not usable; the rest of the scan merged by record \
                 key, which differs only where a file group holds duplicate keys"
            }
        }
    }
}

/// Report every gap this read runs into.
///
/// Called once per read, *after* the log scan — early enough to precede the rows,
/// late enough to see what the scan actually did. Only gaps that are *silently*
/// not done belong here; anything refused already says so through its error.
///
/// `chose_position_merge` is the reader's setup decision for this slice, and
/// `still_merging_by_position` is whether the scan was still doing it at the end.
/// The difference between the two is what separates declining up front from
/// giving up partway through.
pub(crate) fn report_for_read(
    context: &ReaderContext,
    parameters: &ReaderParameters,
    chose_position_merge: bool,
    still_merging_by_position: bool,
) {
    for gap in applicable(
        context,
        parameters,
        chose_position_merge,
        still_merging_by_position,
    ) {
        log::warn!(
            "{} (differs from: {:?})",
            gap.describe(),
            gap.compared_with()
        );
    }
}

/// The gaps that apply to a read, given how it was set up and what it did.
fn applicable(
    context: &ReaderContext,
    parameters: &ReaderParameters,
    chose_position_merge: bool,
    still_merging_by_position: bool,
) -> Vec<Gap> {
    let mut gaps = Vec::new();
    // The context flag is what the engine acts on; the parameter is what the
    // caller asked for. Either one means the read expected position merging.
    let asked = parameters.use_record_position || context.should_merge_use_record_position;
    // With no log files there is nothing to merge, so the two strategies cannot
    // disagree — reporting there would fire on every base-only read.
    if !asked || !context.has_log_files {
        return gaps;
    }
    if !chose_position_merge {
        gaps.push(Gap::PositionBasedMergeDeclined);
    } else if !still_merging_by_position {
        gaps.push(Gap::PositionBasedMergeAbandonedMidScan);
    }
    gaps
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mor_context() -> ReaderContext {
        let mut context = ReaderContext::empty();
        context.has_log_files = true;
        context
    }

    fn asked_for_positions() -> ReaderParameters {
        ReaderParameters {
            use_record_position: true,
            ..Default::default()
        }
    }

    /// A read that does not ask for anything missing reports nothing. Without
    /// this the warning could fire on every read and be tuned out.
    #[test]
    fn test_an_ordinary_read_has_no_gaps() {
        let gaps = applicable(&mor_context(), &ReaderParameters::default(), false, false);

        assert!(gaps.is_empty());
    }

    /// A read that asked for position merge and got it is not a gap — this is
    /// what closes the entry, and what would start failing if the wiring broke.
    #[test]
    fn test_merging_by_position_is_not_reported() {
        let gaps = applicable(&mor_context(), &asked_for_positions(), true, true);

        assert!(gaps.is_empty());
    }

    /// Asking and being declined is reported, because the read still succeeds
    /// and returns something Hudi's reader would not on a file group with
    /// duplicate keys.
    #[test]
    fn test_declining_to_merge_by_position_is_reported() {
        let gaps = applicable(&mor_context(), &asked_for_positions(), false, false);

        assert_eq!(gaps, vec![Gap::PositionBasedMergeDeclined]);
        assert_eq!(gaps[0].compared_with(), ComparedWith::Jvm);
        assert!(gaps[0].describe().contains("record position"));
    }

    /// The same when it is the context carrying the request rather than the
    /// caller's parameters — the engine reads the context, so both have to count.
    #[test]
    fn test_a_context_asking_for_position_merge_is_reported() {
        let mut context = mor_context();
        context.should_merge_use_record_position = true;

        let gaps = applicable(&context, &ReaderParameters::default(), false, false);

        assert_eq!(gaps, vec![Gap::PositionBasedMergeDeclined]);
    }

    /// Choosing position merge and then abandoning it partway through is its own
    /// report: the setup gate saw nothing wrong, so the earlier
    /// `PositionBasedMergeDeclined` entry never fires — which is how a read that
    /// silently stopped doing what it was asked went unreported.
    #[test]
    fn test_abandoning_position_merge_mid_scan_is_reported() {
        let gaps = applicable(&mor_context(), &asked_for_positions(), true, false);

        assert_eq!(gaps, vec![Gap::PositionBasedMergeAbandonedMidScan]);
        assert!(
            gaps[0].describe().contains("partway"),
            "the description must distinguish this from declining up front, got: {}",
            gaps[0].describe()
        );
    }

    /// A slice with no log files has nothing to merge, so neither strategy can
    /// win — reporting there would fire on every base-only read of a table
    /// configured for position merge.
    #[test]
    fn test_a_slice_with_no_log_files_is_not_reported() {
        let gaps = applicable(
            &ReaderContext::empty(),
            &asked_for_positions(),
            false,
            false,
        );

        assert!(gaps.is_empty());
    }

    /// Reporting is what makes a silent fallback observable, so the reporting
    /// entry point is exercised rather than only the decision behind it.
    #[test]
    fn test_report_for_read_walks_every_applicable_gap() {
        // A gap to report, and a read with none — both paths through the loop.
        report_for_read(&mor_context(), &asked_for_positions(), true, false);
        report_for_read(&mor_context(), &ReaderParameters::default(), false, false);
    }
}
