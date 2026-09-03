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

//! An object store that counts the requests made through it.
//!
//! Test-only. It exists so a test can assert which read strategy a reader took
//! by the requests it issued, rather than by the rows it returned: two
//! strategies over the same file return the same rows, so a row assertion
//! cannot tell them apart.

use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};

/// Request counts, shared with the store that increments them.
#[derive(Debug, Default)]
pub(crate) struct RequestCounts {
    /// Byte-range and whole-object reads.
    gets: AtomicUsize,
    /// Metadata-only lookups.
    heads: AtomicUsize,
}

impl RequestCounts {
    pub(crate) fn gets(&self) -> usize {
        self.gets.load(Ordering::Relaxed)
    }

    pub(crate) fn heads(&self) -> usize {
        self.heads.load(Ordering::Relaxed)
    }
}

/// Wraps a store and counts what passes through it.
#[derive(Debug)]
pub(crate) struct CountingObjectStore {
    inner: Arc<dyn ObjectStore>,
    counts: Arc<RequestCounts>,
}

impl CountingObjectStore {
    pub(crate) fn new(inner: Arc<dyn ObjectStore>) -> (Arc<Self>, Arc<RequestCounts>) {
        let counts = Arc::new(RequestCounts::default());
        (
            Arc::new(Self {
                inner,
                counts: counts.clone(),
            }),
            counts,
        )
    }
}

impl Display for CountingObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CountingObjectStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for CountingObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        // A metadata-only lookup reaches the store as a `get_opts` carrying
        // `head`, not as its own trait method, so the two request kinds are
        // told apart here.
        if options.head {
            self.counts.heads.fetch_add(1, Ordering::Relaxed);
        } else {
            self.counts.gets.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
