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

//! Object stores that fail chosen puts or deletes.
//!
//! Test-only. It models the ambiguous outcomes of a cloud PUT: the request
//! times out after the object landed, or a retried `If-None-Match` request
//! reports the object its own first attempt created.

use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};

/// What the faulted put does before it reports failure.
#[derive(Debug, Clone, Copy)]
pub(crate) enum PutFault {
    /// The object is stored, then the call fails as a timeout would.
    LandedThenError,
    /// The object is stored, then the call fails with `AlreadyExists`.
    LandedThenAlreadyExists,
    /// Nothing is stored and the call fails.
    NotLanded,
}

/// Wraps a store and fails the first create-mode put whose path matches, or
/// the first matching put of any mode when built with [`Self::new_for_any_put`].
pub(crate) struct FaultyPutStore {
    inner: Arc<dyn ObjectStore>,
    matches: Box<dyn Fn(&Path) -> bool + Send + Sync>,
    fault: PutFault,
    create_only: bool,
    armed: AtomicBool,
}

impl FaultyPutStore {
    pub(crate) fn new(
        inner: Arc<dyn ObjectStore>,
        fault: PutFault,
        matches: impl Fn(&Path) -> bool + Send + Sync + 'static,
    ) -> Arc<Self> {
        Self::build(inner, fault, true, matches)
    }

    /// Faults overwrite puts too, for writers that do not use create mode.
    pub(crate) fn new_for_any_put(
        inner: Arc<dyn ObjectStore>,
        fault: PutFault,
        matches: impl Fn(&Path) -> bool + Send + Sync + 'static,
    ) -> Arc<Self> {
        Self::build(inner, fault, false, matches)
    }

    fn build(
        inner: Arc<dyn ObjectStore>,
        fault: PutFault,
        create_only: bool,
        matches: impl Fn(&Path) -> bool + Send + Sync + 'static,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner,
            matches: Box::new(matches),
            fault,
            create_only,
            armed: AtomicBool::new(true),
        })
    }

    /// Whether the fault is still waiting for a matching put.
    pub(crate) fn is_armed(&self) -> bool {
        self.armed.load(Ordering::SeqCst)
    }
}

impl std::fmt::Debug for FaultyPutStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FaultyPutStore({:?})", self.fault)
    }
}

impl Display for FaultyPutStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FaultyPutStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for FaultyPutStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        if (self.create_only && !matches!(opts.mode, PutMode::Create))
            || !(self.matches)(location)
            || !self.armed.swap(false, Ordering::SeqCst)
        {
            return self.inner.put_opts(location, payload, opts).await;
        }
        match self.fault {
            PutFault::LandedThenError => {
                self.inner.put_opts(location, payload, opts).await?;
                Err(object_store::Error::Generic {
                    store: "FaultyPutStore",
                    source: "request timed out after the object was stored".into(),
                })
            }
            PutFault::LandedThenAlreadyExists => {
                self.inner
                    .put_opts(location, payload.clone(), opts.clone())
                    .await?;
                self.inner.put_opts(location, payload, opts).await
            }
            PutFault::NotLanded => Err(object_store::Error::Generic {
                store: "FaultyPutStore",
                source: "request failed before the object was stored".into(),
            }),
        }
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
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

/// Fails every delete of a path the predicate matches.
#[derive(Debug)]
pub(crate) struct DeleteFails(pub(crate) Arc<dyn ObjectStore>, pub(crate) fn(&str) -> bool);

impl std::fmt::Display for DeleteFails {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DeleteFails({})", self.0)
    }
}

#[async_trait]
impl ObjectStore for DeleteFails {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.0.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.0.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.0.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let inner = self.0.clone();
        let fails = self.1;
        locations
            .then(move |location| {
                let inner = inner.clone();
                async move {
                    let location = location?;
                    if fails(location.as_ref()) {
                        return Err(object_store::Error::Generic {
                            store: "DeleteFails",
                            source: "delete failed".into(),
                        });
                    }
                    inner
                        .delete_stream(futures::stream::once(async { Ok(location) }).boxed())
                        .next()
                        .await
                        .unwrap()
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.0.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.0.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.0.copy_opts(from, to, options).await
    }
}

/// Fails every read of a path the predicate matches.
#[derive(Debug)]
pub(crate) struct GetFails(pub(crate) Arc<dyn ObjectStore>, pub(crate) fn(&str) -> bool);

impl std::fmt::Display for GetFails {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GetFails({})", self.0)
    }
}

#[async_trait]
impl ObjectStore for GetFails {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.0.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.0.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        if (self.1)(location.as_ref()) {
            return Err(object_store::Error::Generic {
                store: "GetFails",
                source: "get failed".into(),
            });
        }
        self.0.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.0.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.0.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.0.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.0.copy_opts(from, to, options).await
    }
}
