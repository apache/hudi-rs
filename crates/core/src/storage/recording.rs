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

//! An object store that records when each GET and LIST starts and ends.
//!
//! Test-only. [`record_storage_requests`] makes every [`Storage`](super::Storage)
//! created on the calling thread go through it, which reaches code that builds
//! its own storage, such as opening a table. Each recorded request is delayed,
//! so requests that run one after another cannot overlap by accident.
//! [`record_storage_requests_around_first_list`] also fixes the order of
//! concurrent requests, to replay a writer acting between them.

use std::cell::RefCell;
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RequestKind {
    Get,
    List,
}

#[derive(Debug, Clone)]
pub(crate) struct Request {
    pub(crate) kind: RequestKind,
    pub(crate) path: String,
    pub(crate) start: Instant,
    pub(crate) end: Instant,
}

/// The requests made through every recording store of one recorder.
#[derive(Debug, Default)]
pub(crate) struct RequestLog {
    requests: Mutex<Vec<Request>>,
}

impl RequestLog {
    pub(crate) fn requests(&self) -> Vec<Request> {
        self.requests.lock().unwrap().clone()
    }

    /// The requests of `kind` whose path ends with `suffix`.
    pub(crate) fn matching(&self, kind: RequestKind, suffix: &str) -> Vec<Request> {
        self.requests()
            .into_iter()
            .filter(|r| r.kind == kind && r.path.ends_with(suffix))
            .collect()
    }

    fn push(&self, request: Request) {
        self.requests.lock().unwrap().push(request);
    }
}

#[derive(Debug, Clone)]
struct Recorder {
    log: Arc<RequestLog>,
    delay: Duration,
    fail_next_list: Arc<AtomicBool>,
    first_list_hook: Option<Arc<FirstListHook>>,
}

/// Holds every GET until the first LIST is served and the hook has run.
struct FirstListHook {
    hook: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    gets_released: tokio::sync::Semaphore,
}

impl std::fmt::Debug for FirstListHook {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FirstListHook").finish_non_exhaustive()
    }
}

thread_local! {
    static RECORDER: RefCell<Option<Recorder>> = const { RefCell::new(None) };
}

/// Stops recording on drop.
pub(crate) struct RecordingGuard {
    pub(crate) log: Arc<RequestLog>,
}

impl Drop for RecordingGuard {
    fn drop(&mut self) {
        RECORDER.with(|recorder| recorder.borrow_mut().take());
    }
}

/// Record the GETs and LISTs of every storage created on this thread until the
/// guard drops, delaying each by `delay`. With `fail_first_list`, the first
/// LIST fails after its delay.
///
/// Recording is per thread, so the test must run on a current-thread runtime,
/// which `#[tokio::test]` is by default.
pub(crate) fn record_storage_requests(delay: Duration, fail_first_list: bool) -> RecordingGuard {
    install(delay, fail_first_list, None)
}

/// [`record_storage_requests`], also holding every GET until the first LIST
/// has been served and `hook` has run after it.
pub(crate) fn record_storage_requests_around_first_list(
    delay: Duration,
    hook: impl FnOnce() + Send + 'static,
) -> RecordingGuard {
    let hook = FirstListHook {
        hook: Mutex::new(Some(Box::new(hook))),
        gets_released: tokio::sync::Semaphore::new(0),
    };
    install(delay, false, Some(Arc::new(hook)))
}

fn install(
    delay: Duration,
    fail_first_list: bool,
    first_list_hook: Option<Arc<FirstListHook>>,
) -> RecordingGuard {
    let log = Arc::new(RequestLog::default());
    RECORDER.with(|recorder| {
        *recorder.borrow_mut() = Some(Recorder {
            log: log.clone(),
            delay,
            fail_next_list: Arc::new(AtomicBool::new(fail_first_list)),
            first_list_hook,
        })
    });
    RecordingGuard { log }
}

/// Wrap `store` in a recording store when this thread is recording.
pub(super) fn wrap_if_recording(store: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
    match RECORDER.with(|recorder| recorder.borrow().clone()) {
        Some(recorder) => Arc::new(RecordingObjectStore {
            inner: store,
            recorder,
        }),
        None => store,
    }
}

#[derive(Debug)]
struct RecordingObjectStore {
    inner: Arc<dyn ObjectStore>,
    recorder: Recorder,
}

impl RecordingObjectStore {
    fn record(&self, kind: RequestKind, path: &str, start: Instant) {
        self.recorder.log.push(Request {
            kind,
            path: path.to_string(),
            start,
            end: Instant::now(),
        });
    }
}

impl Display for RecordingObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RecordingObjectStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for RecordingObjectStore {
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
        if let Some(first_list_hook) = &self.recorder.first_list_hook {
            // A permit is returned on drop, so once released every GET passes.
            let _permit = first_list_hook.gets_released.acquire().await;
        }
        let start = Instant::now();
        tokio::time::sleep(self.recorder.delay).await;
        let result = self.inner.get_opts(location, options).await;
        self.record(RequestKind::Get, location.as_ref(), start);
        result
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
        let start = Instant::now();
        tokio::time::sleep(self.recorder.delay).await;
        let result = if self.recorder.fail_next_list.swap(false, Ordering::SeqCst) {
            Err(object_store::Error::Generic {
                store: "RecordingObjectStore",
                source: "injected LIST failure".into(),
            })
        } else {
            self.inner.list_with_delimiter(prefix).await
        };
        if let Some(first_list_hook) = &self.recorder.first_list_hook {
            let hook = first_list_hook.hook.lock().unwrap().take();
            if let Some(hook) = hook {
                hook();
                first_list_hook.gets_released.add_permits(1);
            }
        }
        self.record(
            RequestKind::List,
            prefix.map(|p| p.as_ref()).unwrap_or_default(),
            start,
        );
        result
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
