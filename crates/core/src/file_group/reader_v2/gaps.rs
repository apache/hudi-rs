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
    /// The reader this one is replacing.
    Legacy,
    /// Both.
    Both,
}

/// Something another reader does that this one does not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Gap {
    /// Merging by a record's position in the base file rather than by key. The
    /// row-number column the buffer needs is never attached, so a read that
    /// asks for it is merged by key instead — the same rows, reached a
    /// different way, except where duplicate keys exist.
    PositionBasedMerge,
}

impl Gap {
    /// Which reader this differs from.
    pub(crate) fn compared_with(&self) -> ComparedWith {
        match self {
            // The legacy reader has no position-based merge either.
            Gap::PositionBasedMerge => ComparedWith::Jvm,
        }
    }

    /// What a reader would see, said plainly enough to act on.
    pub(crate) fn describe(&self) -> &'static str {
        match self {
            Gap::PositionBasedMerge => {
                "this read asked to merge by record position; merging by record key instead, \
                 which differs only where a file group holds duplicate keys"
            }
        }
    }
}

/// Report every gap this read runs into.
///
/// Called once per read. Only gaps that are *silently* not done belong here —
/// anything refused already says so through its error.
pub(crate) fn report_for_read(context: &ReaderContext, parameters: &ReaderParameters) {
    for gap in applicable(context, parameters) {
        log::warn!(
            "{} (differs from: {:?})",
            gap.describe(),
            gap.compared_with()
        );
    }
}

/// The gaps that apply to a read, given how it was set up.
fn applicable(context: &ReaderContext, parameters: &ReaderParameters) -> Vec<Gap> {
    let mut gaps = Vec::new();
    // The context flag is what the engine would act on; the parameter is what the
    // caller asked for. Either one means the read expected position merging.
    if parameters.use_record_position || context.should_merge_use_record_position {
        gaps.push(Gap::PositionBasedMerge);
    }
    gaps
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A read that does not ask for anything missing reports nothing. Without
    /// this the warning could fire on every read and be tuned out.
    #[test]
    fn test_an_ordinary_read_has_no_gaps() {
        let context = ReaderContext::empty();
        let parameters = ReaderParameters::default();

        assert!(applicable(&context, &parameters).is_empty());
    }

    /// Asking to merge by position is reported, because the read still succeeds
    /// and returns something a reader that has it would not.
    #[test]
    fn test_asking_to_merge_by_position_is_reported() {
        let context = ReaderContext::empty();
        let parameters = ReaderParameters {
            use_record_position: true,
            ..Default::default()
        };

        let gaps = applicable(&context, &parameters);

        assert_eq!(gaps, vec![Gap::PositionBasedMerge]);
        assert_eq!(gaps[0].compared_with(), ComparedWith::Jvm);
        assert!(gaps[0].describe().contains("record position"));
    }

    /// The same when it is the context carrying the request rather than the
    /// caller's parameters — the engine reads the context, so both have to count.
    #[test]
    fn test_a_context_asking_for_position_merge_is_reported() {
        let mut context = ReaderContext::empty();
        context.should_merge_use_record_position = true;

        let gaps = applicable(&context, &ReaderParameters::default());

        assert_eq!(gaps, vec![Gap::PositionBasedMerge]);
    }
}
