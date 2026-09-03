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

//! How many file slices a scan may read at once, given a memory budget.
//!
//! # Why a count rather than a smaller per-merge budget
//!
//! Peak memory is bounded per merge and never per scan, so the real peak is the
//! product of the per-merge budget, the slices open inside one engine partition,
//! and the partitions running at once — a number no configuration expresses.
//!
//! Shrinking the per-merge budget does not fix that, and measurement says it
//! makes it worse: tightening `hoodie.memory.merge.max.size` from 128 MiB to
//! 32 MiB on a 1 GiB merge-on-read table *raised* peak RSS from 182 MiB to
//! 229 MiB and cost six times the wall clock, because the disk tier the budget
//! pushes work into consumes more memory than it frees.
//!
//! Slices in flight is the lever that responds. Over the same table, peak RSS is
//! linear in it — 181, 299, 408, 532, 781 and 1052 MiB at 1, 2, 3, 4, 6 and 8
//! slices — so a byte budget divides into an admission count.
//!
//! # Estimating a slice
//!
//! Cost does not track slice size. Once the base file is streamed rather than
//! materialised, a 108 MiB slice costs 33 MiB and a 133 MiB slice costs 117 MiB;
//! what separates them is log bytes, because the merge map is built from log
//! records. Fitting the measured shapes gives
//!
//! ```text
//! cost ≈ STREAMING_WORKING_SET + LOG_EXPANSION × log_bytes
//! ```
//!
//! The coefficients are measured on generated tables and will move with record
//! width, compression and how many log records collide on one key. They are
//! deliberately not treated as truth: over-estimating admits fewer slices, which
//! costs throughput, and the requirement this serves is that a scan may get
//! slower but may not fail.

/// Per-slice cost that does not depend on the data: one parquet row group plus
/// the decode buffers a streaming base read holds. Measured at 31-33 MiB across
/// base files of 54 MiB and 107 MiB, which is why it is a constant here rather
/// than a function of base size.
pub const STREAMING_WORKING_SET_BYTES: u64 = 33 * 1024 * 1024;

/// How much resident memory one byte of log file turns into once decoded into
/// the merge map. Measured at ≈3.2 on generated tables; rounded up, because
/// over-estimating costs throughput and under-estimating costs the bound.
pub const LOG_EXPANSION: u64 = 4;

/// What one slice is expected to cost while it is being read.
///
/// `log_bytes` is the total size of the slice's log files, known from the
/// listing before the read starts — which is what makes reserving up front
/// possible at all.
pub fn estimated_slice_bytes(log_bytes: u64) -> u64 {
    STREAMING_WORKING_SET_BYTES.saturating_add(log_bytes.saturating_mul(LOG_EXPANSION))
}

/// How many slices one engine partition may read at once.
///
/// `budget_bytes` is the whole scan's allowance; `partitions` is how many engine
/// partitions share it, so the product across partitions stays inside the budget
/// rather than multiplying it. `configured_max` is the caller's own concurrency
/// ceiling, which this only ever lowers.
///
/// Returns at least 1. A budget too small for even one slice admits one anyway:
/// refusing to read is not a degraded read, and the requirement is that the scan
/// completes.
pub fn slices_in_flight(
    budget_bytes: Option<u64>,
    partitions: usize,
    slice_log_bytes: &[Option<u64>],
    configured_max: usize,
) -> usize {
    let configured_max = configured_max.max(1);
    let Some(budget) = budget_bytes else {
        return configured_max;
    };

    // A slice whose log sizes the listing did not record cannot be estimated,
    // and a missing size reads as zero — which would admit the most slices
    // exactly when the least is known. A caller that asked for a bound gets the
    // conservative answer instead of a confident wrong one.
    if slice_log_bytes.iter().any(Option::is_none) {
        return 1;
    }

    let share = budget / (partitions.max(1) as u64);

    // The most expensive slice, not the average: admission has to hold for the
    // worst case it might admit, or the bound is only true on average.
    let per_slice = slice_log_bytes
        .iter()
        .filter_map(|b| *b)
        .map(estimated_slice_bytes)
        .max()
        .unwrap_or_else(|| estimated_slice_bytes(0));

    let admitted = share / per_slice.max(1);
    (admitted as usize).clamp(1, configured_max)
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1024 * 1024;

    /// No budget means the caller's own ceiling stands: this narrows a scan, it
    /// never widens one.
    #[test]
    fn without_a_budget_the_configured_ceiling_is_returned() {
        assert_eq!(slices_in_flight(None, 4, &[Some(100 * MIB)], 4), 4);
        assert_eq!(slices_in_flight(None, 1, &[], 7), 7);
    }

    /// The budget is divided across engine partitions rather than granted to
    /// each, because the peak this exists to bound is the product.
    #[test]
    fn the_budget_is_shared_across_partitions_not_granted_to_each() {
        // 1 GiB budget, slices costing 33 + 4*0 = 33 MiB each.
        let one_partition = slices_in_flight(Some(1024 * MIB), 1, &[Some(0)], 64);
        let four_partitions = slices_in_flight(Some(1024 * MIB), 4, &[Some(0)], 64);
        assert_eq!(one_partition, 31, "1024 / 33");
        assert_eq!(four_partitions, 7, "256 / 33");
        assert!(
            four_partitions * 4 <= one_partition + 4,
            "splitting the budget must not multiply it: {four_partitions} x 4 vs {one_partition}"
        );
    }

    /// Cost is driven by log bytes, so a log-heavy slice admits fewer.
    #[test]
    fn a_log_heavy_slice_admits_fewer_than_a_log_light_one() {
        let budget = Some(512 * MIB);
        let light = slices_in_flight(budget, 1, &[Some(MIB)], 64);
        let heavy = slices_in_flight(budget, 1, &[Some(64 * MIB)], 64);
        assert!(
            heavy < light,
            "64 MiB of log must admit fewer than 1 MiB: {heavy} vs {light}"
        );
        assert_eq!(heavy, 1, "512 / (33 + 256) = 1");
    }

    /// The worst slice decides, not the average — otherwise the bound holds only
    /// for a scan whose slices happen to be evenly sized.
    #[test]
    fn the_most_expensive_slice_sets_the_admission_count() {
        let budget = Some(512 * MIB);
        let even = slices_in_flight(budget, 1, &[Some(MIB); 4], 64);
        let skewed = slices_in_flight(
            budget,
            1,
            &[Some(MIB), Some(MIB), Some(MIB), Some(64 * MIB)],
            64,
        );
        assert!(
            skewed < even,
            "one expensive slice must lower the count: {skewed} vs {even}"
        );
    }

    /// A budget too small for one slice still admits one. Refusing to read is
    /// not a degraded read.
    #[test]
    fn a_budget_smaller_than_one_slice_still_admits_one() {
        assert_eq!(slices_in_flight(Some(MIB), 1, &[Some(512 * MIB)], 8), 1);
        assert_eq!(slices_in_flight(Some(0), 1, &[Some(0)], 8), 1);
    }

    /// An unknown log size must not read as a cheap slice. The listing does not
    /// always record file sizes, and a missing size sums to zero — which would
    /// admit the most slices exactly when the least is known.
    #[test]
    fn an_unestimatable_slice_admits_one() {
        let budget = Some(4096 * MIB);
        assert_eq!(
            slices_in_flight(budget, 1, &[Some(MIB), None, Some(MIB)], 8),
            1,
            "one slice with no recorded size makes the whole estimate unsafe"
        );
        assert!(
            slices_in_flight(budget, 1, &[Some(MIB), Some(MIB), Some(MIB)], 8) > 1,
            "the same slices with sizes known must admit more, or the test above \\
             passes for the wrong reason"
        );
        assert_eq!(
            slices_in_flight(None, 1, &[None], 8),
            8,
            "with no budget there is nothing to be conservative about"
        );
    }

    /// The estimate never claims a slice is free, whatever the log size.
    #[test]
    fn the_estimate_has_a_floor_and_grows_with_log_bytes() {
        assert_eq!(estimated_slice_bytes(0), STREAMING_WORKING_SET_BYTES);
        assert!(estimated_slice_bytes(MIB) > estimated_slice_bytes(0));
        // The measured shape: a 26 MiB log slice cost 117 MiB.
        let predicted = estimated_slice_bytes(26 * MIB) / MIB;
        assert!(
            (100..=160).contains(&predicted),
            "estimate for a 26 MiB log slice should be near the measured 117 MiB, got {predicted}"
        );
    }
}
