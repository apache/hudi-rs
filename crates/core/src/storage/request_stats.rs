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
//! Latency, size and concurrency of the object-store fetches made through one
//! [`Storage`](super::Storage).

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

const LATENCY_BUCKETS: usize = 24;
const SIZE_BUCKETS: usize = 32;

/// In-flight count in the high bits, start of the current busy period (nanos
/// since `epoch`) in the low bits, so both change in one CAS.
const IN_FLIGHT_SHIFT: u32 = 48;
const IN_FLIGHT_ONE: u64 = 1 << IN_FLIGHT_SHIFT;
const OFFSET_MASK: u64 = IN_FLIGHT_ONE - 1;

/// Fetch requests made through one [`Storage`](super::Storage): how many, how
/// large, how long each waited, and how many overlapped.
///
/// A request is one awaited fetch that hands back bytes: a ranged or
/// whole-object GET, or one `get_ranges` call. The default `get_ranges` (used by
/// the cloud stores) coalesces nearby ranges and issues up to ten GETs
/// concurrently underneath; that call still counts once here, with its total
/// bytes and its wall time. Latency is from first poll to the last byte in
/// hand, so it includes the transfer, not just time to first byte. It is
/// measured on the task awaiting the fetch, so it also includes any time that
/// task spends on other work before polling it again, as when one task drives
/// several slice reads at once.
///
/// Counted at these boundaries: base-file column-chunk reads (where
/// [`ReadVolume::bytes_read`](super::ReadVolume::bytes_read) is counted), log
/// and HFile reads through `StorageReader` and `LogBlockFetcher`, and whole-file
/// reads through `Storage::get_file_data` (timeline, properties, small HFiles).
/// Not counted: HEADs, listings, parquet footer fetches, and Lance base files.
///
/// Every request that starts and ends adds its elapsed time to
/// `latency_nanos`, `max_latency_nanos` and `latency_buckets`, whether it
/// completed, failed, or was cancelled. Only completed ones add to `count`,
/// `bytes`, `max_bytes` and `size_buckets`; the rest add to `failed`.
///
/// Deriving the usual numbers:
/// - mean latency: `latency_nanos / (count + failed)`
/// - mean requests in flight while any is: `latency_nanos / busy_nanos`
/// - mean requests in flight over a caller's wall time `w`: `latency_nanos / w`
/// - fraction of `w` with a request outstanding: `busy_nanos / w`
///
/// `busy_nanos` only accrues when the in-flight count returns to zero, so read
/// it after the reads have finished.
///
/// The in-flight count saturates at [`Self::IN_FLIGHT_CAP`]: a request
/// started at the cap is timed and counted but not tracked as in flight, so
/// `peak_in_flight` reads the cap and the ratio above undercounts slightly.
///
/// `Relaxed` throughout, for the same reason as the rest of
/// [`ReadVolume`](super::ReadVolume).
#[derive(Debug)]
pub struct RequestStats {
    /// Completed requests.
    pub count: AtomicU64,
    /// Requests that returned an error or were dropped before completing.
    pub failed: AtomicU64,
    /// Bytes returned by completed requests.
    pub bytes: AtomicU64,
    /// Sum of all requests' latencies, in nanoseconds.
    pub latency_nanos: AtomicU64,
    /// Slowest request, in nanoseconds.
    pub max_latency_nanos: AtomicU64,
    /// Largest completed request, in bytes.
    pub max_bytes: AtomicU64,
    /// All requests by latency in microseconds, log2 buckets: bucket 0 is
    /// under 1 us, bucket `i` covers `[2^(i-1), 2^i)` us, and the last bucket
    /// is open-ended. See [`Self::bucket_lower_bound`].
    pub latency_buckets: [AtomicU64; LATENCY_BUCKETS],
    /// Completed requests by size in bytes, same log2 layout as
    /// `latency_buckets`.
    pub size_buckets: [AtomicU64; SIZE_BUCKETS],
    /// Most requests in flight at the same instant, up to
    /// [`Self::IN_FLIGHT_CAP`].
    pub peak_in_flight: AtomicU64,
    /// Wall time, in nanoseconds, during which at least one request was in
    /// flight.
    pub busy_nanos: AtomicU64,
    state: AtomicU64,
    epoch: Instant,
}

impl Default for RequestStats {
    fn default() -> Self {
        Self {
            count: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            latency_nanos: AtomicU64::new(0),
            max_latency_nanos: AtomicU64::new(0),
            max_bytes: AtomicU64::new(0),
            latency_buckets: [const { AtomicU64::new(0) }; LATENCY_BUCKETS],
            size_buckets: [const { AtomicU64::new(0) }; SIZE_BUCKETS],
            peak_in_flight: AtomicU64::new(0),
            busy_nanos: AtomicU64::new(0),
            state: AtomicU64::new(0),
            epoch: Instant::now(),
        }
    }
}

fn log2_bucket(v: u64, buckets: usize) -> usize {
    ((u64::BITS - v.leading_zeros()) as usize).min(buckets - 1)
}

fn saturating_nanos(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

impl RequestStats {
    /// Most requests tracked as in flight at once; the width of the packed
    /// count.
    pub const IN_FLIGHT_CAP: u64 = (1 << (u64::BITS - IN_FLIGHT_SHIFT)) - 1;

    /// Smallest value that lands in bucket `i` of `latency_buckets` or
    /// `size_buckets`: 0 for bucket 0, `2^(i-1)` otherwise.
    pub fn bucket_lower_bound(i: usize) -> u64 {
        match i {
            0 => 0,
            _ => 1u64 << (i - 1).min(63),
        }
    }

    #[cfg(test)]
    pub(crate) fn in_flight(&self) -> u64 {
        self.state.load(Ordering::Relaxed) >> IN_FLIGHT_SHIFT
    }

    /// Time `fut` as one request, sized by `size` on success.
    pub(crate) async fn time<T, E>(
        &self,
        fut: impl Future<Output = Result<T, E>>,
        size: impl FnOnce(&T) -> u64,
    ) -> Result<T, E> {
        let request = self.start();
        let out = fut.await;
        if let Ok(value) = &out {
            request.finish(size(value));
        }
        out
    }

    /// Start timing one request. Finish it with [`InFlight::finish`]; dropping
    /// it unfinished records a failed request.
    pub(crate) fn start(&self) -> InFlight<'_> {
        self.start_at(Instant::now())
    }

    fn start_at(&self, now: Instant) -> InFlight<'_> {
        let offset = self.offset(now);
        let mut cur = self.state.load(Ordering::Relaxed);
        let tracked = loop {
            let in_flight = cur >> IN_FLIGHT_SHIFT;
            if in_flight >= Self::IN_FLIGHT_CAP {
                break false;
            }
            let next = if in_flight == 0 {
                IN_FLIGHT_ONE | offset
            } else {
                cur + IN_FLIGHT_ONE
            };
            match self
                .state
                .compare_exchange_weak(cur, next, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => break true,
                Err(actual) => cur = actual,
            }
        };
        if tracked {
            self.peak_in_flight
                .fetch_max((cur >> IN_FLIGHT_SHIFT) + 1, Ordering::Relaxed);
        }
        InFlight {
            stats: self,
            started: now,
            tracked,
            ended: false,
        }
    }

    fn leave(&self, now: Instant) {
        let offset = self.offset(now);
        let mut cur = self.state.load(Ordering::Relaxed);
        loop {
            let in_flight = cur >> IN_FLIGHT_SHIFT;
            let next = if in_flight <= 1 {
                0
            } else {
                cur - IN_FLIGHT_ONE
            };
            match self
                .state
                .compare_exchange_weak(cur, next, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(actual) => cur = actual,
            }
        }
        if cur >> IN_FLIGHT_SHIFT == 1 {
            // Another thread can open the busy period with an `Instant` read
            // later than ours, so a negative span (a huge masked value) is
            // clamped to zero rather than added.
            let span = offset.wrapping_sub(cur & OFFSET_MASK) & OFFSET_MASK;
            if span <= OFFSET_MASK / 2 {
                self.busy_nanos.fetch_add(span, Ordering::Relaxed);
            }
        }
    }

    fn record_latency(&self, latency: Duration) {
        let nanos = saturating_nanos(latency);
        let micros = u64::try_from(latency.as_micros()).unwrap_or(u64::MAX);
        self.latency_nanos.fetch_add(nanos, Ordering::Relaxed);
        self.max_latency_nanos.fetch_max(nanos, Ordering::Relaxed);
        self.latency_buckets[log2_bucket(micros, LATENCY_BUCKETS)].fetch_add(1, Ordering::Relaxed);
    }

    fn record_completed(&self, bytes: u64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
        self.max_bytes.fetch_max(bytes, Ordering::Relaxed);
        self.size_buckets[log2_bucket(bytes, SIZE_BUCKETS)].fetch_add(1, Ordering::Relaxed);
    }

    /// Nanos since `epoch`, truncated to the packed width. Busy spans are taken
    /// as wrapping differences, so truncation is harmless while one busy period
    /// lasts under about 39 hours.
    fn offset(&self, now: Instant) -> u64 {
        saturating_nanos(now.saturating_duration_since(self.epoch)) & OFFSET_MASK
    }
}

/// One request in flight. Dropping it without [`Self::finish`] (an error, or
/// the future cancelled) records it as failed.
pub(crate) struct InFlight<'a> {
    stats: &'a RequestStats,
    started: Instant,
    /// False when started at the in-flight cap, so the matching decrement is
    /// skipped too.
    tracked: bool,
    ended: bool,
}

impl InFlight<'_> {
    pub(crate) fn finish(self, bytes: u64) {
        self.finish_at(Instant::now(), bytes);
    }

    fn finish_at(mut self, now: Instant, bytes: u64) {
        self.stats.record_completed(bytes);
        self.end(now);
    }

    fn end(&mut self, now: Instant) {
        self.ended = true;
        self.stats
            .record_latency(now.saturating_duration_since(self.started));
        if self.tracked {
            self.stats.leave(now);
        }
    }
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        if !self.ended {
            self.stats.failed.fetch_add(1, Ordering::Relaxed);
            self.end(Instant::now());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::Ordering::Relaxed;

    fn at(stats: &RequestStats, millis: u64) -> Instant {
        stats.epoch + Duration::from_millis(millis)
    }

    fn total(buckets: &[AtomicU64]) -> u64 {
        buckets.iter().map(|b| b.load(Relaxed)).sum()
    }

    impl InFlight<'_> {
        fn fail_at(mut self, now: Instant) {
            self.stats.failed.fetch_add(1, Relaxed);
            self.end(now);
        }
    }

    /// Two overlapping requests, an idle gap, then a third. Busy time is the
    /// union of the intervals, latency the sum, so their ratio is the mean
    /// in-flight count while busy.
    #[test]
    fn test_overlapping_requests_peak_busy_and_latency() {
        let stats = RequestStats::default();
        let a = stats.start_at(at(&stats, 0));
        let b = stats.start_at(at(&stats, 5));
        assert_eq!(stats.in_flight(), 2);
        a.finish_at(at(&stats, 10), 100);
        b.finish_at(at(&stats, 20), 3000);
        assert_eq!(stats.in_flight(), 0);
        let c = stats.start_at(at(&stats, 30));
        c.finish_at(at(&stats, 35), 1);

        assert_eq!(stats.count.load(Relaxed), 3);
        assert_eq!(stats.bytes.load(Relaxed), 3101);
        assert_eq!(stats.peak_in_flight.load(Relaxed), 2);
        assert_eq!(stats.latency_nanos.load(Relaxed), 30_000_000);
        assert_eq!(stats.max_latency_nanos.load(Relaxed), 15_000_000);
        assert_eq!(stats.busy_nanos.load(Relaxed), 25_000_000);
        assert_eq!(stats.max_bytes.load(Relaxed), 3000);
    }

    /// A failed request overlapping a completed one: its time is in
    /// `latency_nanos`, so `latency_nanos / busy_nanos` stays the true mean
    /// in-flight count, but it is not a completed request and has no size.
    #[test]
    fn test_failed_request_counts_toward_latency_not_completion() {
        let stats = RequestStats::default();
        let ok = stats.start_at(at(&stats, 0));
        let bad = stats.start_at(at(&stats, 0));
        bad.fail_at(at(&stats, 10));
        ok.finish_at(at(&stats, 10), 64);

        assert_eq!(stats.count.load(Relaxed), 1);
        assert_eq!(stats.failed.load(Relaxed), 1);
        assert_eq!(stats.bytes.load(Relaxed), 64);
        assert_eq!(total(&stats.size_buckets), 1);
        assert_eq!(total(&stats.latency_buckets), 2);
        assert_eq!(stats.latency_nanos.load(Relaxed), 20_000_000);
        assert_eq!(stats.busy_nanos.load(Relaxed), 10_000_000);
        assert_eq!(stats.in_flight(), 0);
    }

    /// 5 ms lands in [4096, 8192) us and 10 ms and 15 ms in [8192, 16384) us;
    /// sizes 1, 100 and 3000 land in buckets 1, 7 and 12.
    #[test]
    fn test_latency_and_size_buckets() {
        let stats = RequestStats::default();
        for (start, end, bytes) in [(0, 10, 100), (0, 15, 3000), (20, 25, 1)] {
            stats
                .start_at(at(&stats, start))
                .finish_at(at(&stats, end), bytes);
        }
        let latency: Vec<u64> = stats
            .latency_buckets
            .iter()
            .map(|b| b.load(Relaxed))
            .collect();
        assert_eq!(latency[13], 1);
        assert_eq!(latency[14], 2);
        assert_eq!(total(&stats.latency_buckets), 3);
        assert!(
            RequestStats::bucket_lower_bound(13) <= 5_000
                && 5_000 < RequestStats::bucket_lower_bound(14)
        );

        let size: Vec<u64> = stats.size_buckets.iter().map(|b| b.load(Relaxed)).collect();
        assert_eq!((size[1], size[7], size[12]), (1, 1, 1));
        assert_eq!(total(&stats.size_buckets), 3);
    }

    /// Zero, and values past the last bound, stay inside the arrays.
    #[test]
    fn test_bucket_edges() {
        assert_eq!(log2_bucket(0, SIZE_BUCKETS), 0);
        assert_eq!(log2_bucket(1, SIZE_BUCKETS), 1);
        assert_eq!(log2_bucket(2, SIZE_BUCKETS), 2);
        assert_eq!(log2_bucket(3, SIZE_BUCKETS), 2);
        assert_eq!(log2_bucket(u64::MAX, SIZE_BUCKETS), SIZE_BUCKETS - 1);
        assert_eq!(RequestStats::bucket_lower_bound(0), 0);
        assert_eq!(RequestStats::bucket_lower_bound(1), 1);
        assert_eq!(RequestStats::bucket_lower_bound(11), 1024);
    }

    /// A request dropped mid-flight leaves the in-flight count, closes the
    /// busy period and counts as failed with its elapsed time.
    #[test]
    fn test_dropped_request_is_failed() {
        let stats = RequestStats::default();
        drop(stats.start_at(at(&stats, 0)));
        assert_eq!(stats.in_flight(), 0);
        assert_eq!(stats.count.load(Relaxed), 0);
        assert_eq!(stats.failed.load(Relaxed), 1);
        assert_eq!(stats.peak_in_flight.load(Relaxed), 1);
        assert_eq!(total(&stats.latency_buckets), 1);
        assert_eq!(total(&stats.size_buckets), 0);
    }

    /// A busy period opened with a later `Instant` than the one that closes it
    /// adds nothing rather than a wrapped span.
    #[test]
    fn test_out_of_order_instants_do_not_wrap_busy_time() {
        let stats = RequestStats::default();
        let r = stats.start_at(at(&stats, 10));
        r.finish_at(at(&stats, 5), 1);
        assert_eq!(stats.busy_nanos.load(Relaxed), 0);
        assert_eq!(stats.in_flight(), 0);
    }

    /// Past the cap, requests are timed and counted but not tracked, and the
    /// untracked ones skip the decrement, so the packed count never wraps into
    /// the busy-period offset and returns to zero once all have ended.
    #[test]
    fn test_in_flight_saturates_at_cap() {
        let stats = RequestStats::default();
        let cap = RequestStats::IN_FLIGHT_CAP;
        assert_eq!(cap, 65_535);
        let held: Vec<_> = (0..cap + 2)
            .map(|_| stats.start_at(at(&stats, 0)))
            .collect();
        assert_eq!(stats.in_flight(), cap);
        assert_eq!(stats.peak_in_flight.load(Relaxed), cap);
        for r in held {
            r.finish_at(at(&stats, 1), 1);
        }
        assert_eq!(stats.in_flight(), 0);
        assert_eq!(stats.count.load(Relaxed), cap + 2);
        assert_eq!(stats.latency_nanos.load(Relaxed), (cap + 2) * 1_000_000);
        assert_eq!(stats.busy_nanos.load(Relaxed), 1_000_000);
    }

    /// `n` futures that can only complete once all `n` have started must all
    /// be in flight together, so the peak is exactly `n`.
    #[tokio::test]
    async fn test_time_counts_concurrent_futures() {
        let n = 4;
        let stats = RequestStats::default();
        let barrier = Arc::new(tokio::sync::Barrier::new(n));
        let fetches = (0..n).map(|i| {
            let barrier = barrier.clone();
            stats.time(
                async move {
                    barrier.wait().await;
                    Ok::<_, ()>(vec![0u8; 10 * (i + 1)])
                },
                |v| v.len() as u64,
            )
        });
        let out = futures::future::join_all(fetches).await;
        assert!(out.iter().all(Result::is_ok));
        assert_eq!(stats.peak_in_flight.load(Relaxed), n as u64);
        assert_eq!(stats.in_flight(), 0);
        assert_eq!(stats.count.load(Relaxed), n as u64);
        assert_eq!(stats.bytes.load(Relaxed), 100);
        assert_eq!(stats.max_bytes.load(Relaxed), 40);
        assert!(stats.busy_nanos.load(Relaxed) <= stats.latency_nanos.load(Relaxed));
    }

    /// An `Err` from the timed future is passed through and counted as failed.
    #[tokio::test]
    async fn test_time_passes_errors_through_as_failed() {
        let stats = RequestStats::default();
        let out = stats
            .time(async { Err::<Vec<u8>, _>("boom") }, |v| v.len() as u64)
            .await;
        assert_eq!(out, Err("boom"));
        assert_eq!(stats.count.load(Relaxed), 0);
        assert_eq!(stats.failed.load(Relaxed), 1);
        assert_eq!(stats.in_flight(), 0);
    }

    /// A timed future polled once, left pending, then dropped (a cancelled
    /// read) is in flight while pending and failed once dropped.
    #[tokio::test]
    async fn test_time_future_cancelled_while_pending() {
        let stats = RequestStats::default();
        let mut fut = Box::pin(
            stats.time(futures::future::pending::<Result<Vec<u8>, ()>>(), |v| {
                v.len() as u64
            }),
        );
        assert!(futures::poll!(&mut fut).is_pending());
        assert_eq!(stats.in_flight(), 1);
        drop(fut);
        assert_eq!(stats.in_flight(), 0);
        assert_eq!(stats.failed.load(Relaxed), 1);
        assert_eq!(stats.count.load(Relaxed), 0);
        assert_eq!(total(&stats.latency_buckets), 1);
    }

    /// Many threads entering and leaving at once: every request is counted,
    /// the in-flight count returns to zero, and the peak never exceeds the
    /// number of threads.
    #[test]
    fn test_contended_start_finish_balances() {
        let threads = 8;
        let per_thread = 1000;
        let stats = RequestStats::default();
        std::thread::scope(|s| {
            for _ in 0..threads {
                s.spawn(|| {
                    for _ in 0..per_thread {
                        stats.start().finish(8);
                    }
                });
            }
        });
        assert_eq!(stats.in_flight(), 0);
        assert_eq!(stats.count.load(Relaxed), threads * per_thread);
        assert_eq!(stats.bytes.load(Relaxed), 8 * threads * per_thread);
        let peak = stats.peak_in_flight.load(Relaxed);
        assert!((1..=threads).contains(&peak), "peak {peak}");
    }
}
