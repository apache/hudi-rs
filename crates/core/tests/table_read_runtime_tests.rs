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
//! Where a table read decodes its file slices.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use hudi_core::table::{ReadOptions, Table};
use tempfile::tempdir;

/// Counts the bytes allocated by threads that set [`COUNT_ALLOCS`].
struct CountingAllocator;

thread_local! {
    static COUNT_ALLOCS: Cell<bool> = const { Cell::new(false) };
}

static MARKED_THREAD_BYTES: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // `try_with` because the flag may already be gone while a thread exits.
        if COUNT_ALLOCS.try_with(Cell::get).unwrap_or(false) {
            MARKED_THREAD_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

const GROUPS: usize = 4;
const ROWS: usize = 50_000;

fn batch(group: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let ids: Vec<String> = (0..ROWS).map(|i| format!("key{group}-{i:08}")).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(ids)),
            Arc::new(Int64Array::from_iter_values(
                (0..ROWS as i64).map(|i| i * 7),
            )),
        ],
    )
    .unwrap()
}

/// A caller that blocks on a read, as the Python binding does, must not decode
/// the file slices on its own thread: that serializes every slice onto one
/// core. Measured by the bytes the calling thread allocates during the read:
/// decoding there costs several times the batches read, while planning alone
/// stays well below them.
#[test]
fn test_read_decodes_file_slices_off_the_calling_thread() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let dir = tempdir().unwrap();
    let table = rt.block_on(async {
        let mut table = Table::create(dir.path().to_str().unwrap())
            .with_table_name("trips")
            .with_record_key_fields(["id"])
            .with_metadata_table_enabled(false)
            // One file group per append, so the read has several slices.
            .with_option("hoodie.parquet.small.file.limit", "0")
            .create()
            .await
            .unwrap();
        for group in 0..GROUPS {
            table.append([batch(group)]).await.unwrap();
        }
        Table::new(dir.path().to_str().unwrap()).await.unwrap()
    });

    COUNT_ALLOCS.with(|c| c.set(true));
    let batches = rt.block_on(table.read(&ReadOptions::new())).unwrap();
    COUNT_ALLOCS.with(|c| c.set(false));

    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(rows, GROUPS * ROWS);
    let decoded: usize = batches.iter().map(RecordBatch::get_array_memory_size).sum();
    let caller = MARKED_THREAD_BYTES.load(Ordering::Relaxed);
    assert!(
        caller < decoded,
        "the calling thread allocated {caller} bytes reading {decoded} bytes of batches; \
         the file slices were decoded on it"
    );
}
