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
//! How write tasks use the tokio runtime.
//!
//! This file holds its own test binary because it installs a global allocator
//! that counts allocations made on a marked thread.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use hudi_core::table::Table;
use tempfile::tempdir;

thread_local! {
    static COUNT_ALLOCS: Cell<bool> = const { Cell::new(false) };
}
static MARKED_THREAD_ALLOCS: AtomicUsize = AtomicUsize::new(0);

struct CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNT_ALLOCS.try_with(Cell::get).unwrap_or(false) {
            MARKED_THREAD_ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn batch(ids: &[String], value: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from_iter_values(ids)),
            Arc::new(Int64Array::from(vec![value; ids.len()])),
        ],
    )
    .unwrap()
}

/// A copy-on-write upsert runs its merge off the async workers.
///
/// On a one-worker runtime the worker thread is marked and its allocations are
/// counted during the upsert. The merge builds an owned key per stored and
/// incoming row, so a merge on the worker allocates several times per row,
/// while the worker's own share (the async read and the put) allocates per
/// batch. Allocation counts do not depend on scheduling, so the check holds on
/// a loaded machine.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_cow_upsert_merge_runs_off_the_async_worker() {
    const GROUPS: usize = 2;
    const ROWS: usize = 20_000;
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path().to_str().unwrap())
        .with_table_name("trips")
        .with_record_key_fields(["id"])
        .with_metadata_table_enabled(false)
        .with_option("hoodie.parquet.small.file.limit", "0")
        .create()
        .await
        .unwrap();
    let ids: Vec<String> = (0..GROUPS * ROWS).map(|i| format!("key{i:08}")).collect();
    for group in ids.chunks(ROWS) {
        table.append([batch(group, 1)]).await.unwrap();
    }

    // Spawned tasks run only on the runtime's single worker.
    tokio::spawn(async { COUNT_ALLOCS.with(|count| count.set(true)) })
        .await
        .unwrap();
    MARKED_THREAD_ALLOCS.store(0, Ordering::SeqCst);
    let result = table.upsert([batch(&ids, 2)]).await.unwrap();
    let worker_allocs = MARKED_THREAD_ALLOCS.load(Ordering::SeqCst);

    assert_eq!(result.num_updates, GROUPS * ROWS);
    assert!(
        worker_allocs < ROWS,
        "the async worker made {worker_allocs} allocations during the upsert; the merge \
         of {ROWS}-row file groups ran on it"
    );
}
