// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Running an async operation over a list with a ceiling on how many are in
//! flight at once.

use crate::Result;
use crate::error::CoreError;
use std::future::Future;

/// Run `fut` as a task on the current multi-thread runtime, so it can make
/// progress on another worker thread than the one awaiting it. Without one (no
/// runtime, or a current-thread runtime with no other thread to use) `fut` is
/// awaited in place.
///
/// Dropping the returned future aborts the task, so a caller that gives up on a
/// read (an error elsewhere, a dropped stream) does not leave it running. A
/// panic in a spawned task comes back as an error rather than unwinding the
/// caller.
pub(crate) async fn spawn_aborting<R, Fut>(fut: Fut) -> Result<R>
where
    R: Send + 'static,
    Fut: Future<Output = Result<R>> + Send + 'static,
{
    struct AbortOnDrop<R>(tokio::task::JoinHandle<R>);
    impl<R> Drop for AbortOnDrop<R> {
        fn drop(&mut self) {
            self.0.abort();
        }
    }
    let handle = match tokio::runtime::Handle::try_current() {
        Ok(h) if h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => h,
        _ => return fut.await,
    };
    let mut task = AbortOnDrop(handle.spawn(fut));
    (&mut task.0).await.unwrap_or_else(|e| {
        Err(CoreError::ReadFileSliceError(format!(
            "read task failed: {e}"
        )))
    })
}

/// Apply `op` to every item, at most `concurrency` at a time, keeping input
/// order in the output.
///
/// Order is preserved because a caller concatenating the results should not see
/// its row order shift with scheduling. Any single failure aborts the whole run:
/// a partially-read table is not a useful answer.
///
/// One `try_join_all` per chunk rather than a sliding window. A chunk waits for
/// its slowest member, so this is slightly less busy than a true window — but
/// the goal is the ceiling, not maximal overlap, and the sliding-window form
/// (`buffered` over these futures) cannot be proven `Send`: the futures borrow
/// their item, and the higher-ranked lifetimes inside `FuturesOrdered` then
/// defeat auto-trait inference for any caller that spawns the read.
///
/// `concurrency` is clamped to at least 1, so a misconfigured 0 reads
/// sequentially rather than returning nothing — silently reading no slices would
/// look like an empty table.
pub(crate) async fn bounded_in_order<'a, T, R, F, Fut>(
    items: &'a [T],
    concurrency: usize,
    op: F,
) -> Result<Vec<R>>
where
    // The input lifetime is tied to the slice rather than left higher-ranked:
    // a `Fn(&T) -> Fut` with a single `Fut` cannot name the borrow its future
    // holds, and the resulting `'1 must outlive '2` is unsatisfiable.
    F: Fn(&'a T) -> Fut,
    Fut: Future<Output = Result<R>>,
{
    let concurrency = concurrency.max(1);
    let mut out = Vec::with_capacity(items.len());
    for chunk in items.chunks(concurrency) {
        let running = chunk.iter().map(&op);
        out.extend(futures::future::try_join_all(running).await?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    /// Dropping the wrapper stops the task instead of leaving it running.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn spawn_aborting_aborts_the_task_on_drop() {
        let finished = Arc::new(AtomicBool::new(false));
        let flag = finished.clone();
        let read = spawn_aborting(async move {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            flag.store(true, Ordering::SeqCst);
            Ok(())
        });
        // Start the task, then give up on it.
        let _ = tokio::time::timeout(std::time::Duration::from_millis(10), read).await;
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        assert!(
            !finished.load(Ordering::SeqCst),
            "the dropped task kept running"
        );
    }

    /// Outside any runtime the future is awaited in place instead of panicking
    /// on a missing reactor.
    #[test]
    fn spawn_aborting_runs_in_place_without_a_runtime() {
        let got = futures::executor::block_on(spawn_aborting(async { Ok(7) })).unwrap();
        assert_eq!(got, 7);
    }

    /// A panic in the task comes back as an error.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn spawn_aborting_reports_a_panic_as_an_error() {
        let result: Result<()> = spawn_aborting(async { panic!("boom") }).await;
        let err = result.expect_err("a panicking task must fail the read");
        assert!(err.to_string().contains("read task failed"), "{err}");
    }

    /// Output order follows input order, not completion order.
    #[tokio::test]
    async fn results_keep_input_order() -> Result<()> {
        let items: Vec<u32> = (0..10).collect();
        let got = bounded_in_order(&items, 4, |n| {
            let n = *n;
            async move { Ok(n * 2) }
        })
        .await?;
        assert_eq!(got, (0..10).map(|n| n * 2).collect::<Vec<_>>());
        Ok(())
    }

    /// The ceiling is real: with a concurrency of 2 over 6 items, no more than
    /// two are ever in flight. Asserting the observed peak rather than the
    /// configured value, which would pass even if the chunking were removed.
    #[tokio::test]
    async fn no_more_than_concurrency_run_at_once() -> Result<()> {
        let live = AtomicUsize::new(0);
        let peak = AtomicUsize::new(0);
        let items: Vec<u32> = (0..6).collect();
        bounded_in_order(&items, 2, |_| {
            let (live, peak) = (&live, &peak);
            async move {
                let now = live.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                tokio::task::yield_now().await;
                live.fetch_sub(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .await?;
        assert_eq!(
            peak.load(Ordering::SeqCst),
            2,
            "at most `concurrency` operations may be in flight"
        );
        Ok(())
    }

    /// A concurrency of 0 must read everything sequentially, never nothing.
    #[tokio::test]
    async fn zero_concurrency_reads_sequentially() -> Result<()> {
        let items: Vec<u32> = (0..5).collect();
        let got = bounded_in_order(&items, 0, |n| {
            let n = *n;
            async move { Ok(n) }
        })
        .await?;
        assert_eq!(got, items, "0 must clamp to 1, not drop every item");
        Ok(())
    }
}
