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
//! Hands the workspace's object store to Lance, which is compiled against a
//! different `object_store` major.
//!
//! Lance takes the store as a trait object of its own `object_store`, and to
//! rustc the two crates' `ObjectStore` traits are unrelated, so the store is
//! wrapped in an adapter that implements Lance's trait by converting each
//! request and response. Both majors carry the same read types field for
//! field. hudi-core only reads through Lance, so the write operations report
//! `NotImplemented`.

use std::fmt::{Display, Formatter};
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream, StreamExt};
use object_store::path::Path as HostPath;
use object_store::{
    Attribute as HostAttribute, Attributes as HostAttributes, Error as HostError,
    GetOptions as HostGetOptions, GetRange as HostGetRange, GetResult as HostGetResult,
    GetResultPayload as HostGetResultPayload, ObjectMeta as HostObjectMeta, ObjectStore,
};
use object_store_lance::path::Path;
use object_store_lance::{
    Attribute, AttributeValue, Attributes, CopyOptions, Error, GetOptions, GetRange, GetResult,
    GetResultPayload, ListResult, MultipartUpload, ObjectMeta, PutMultipartOptions, PutOptions,
    PutPayload, PutResult, Result,
};

const STORE: &str = "LanceObjectStoreAdapter";

/// Implements Lance's `ObjectStore` over the workspace's store.
#[derive(Debug)]
pub(super) struct LanceObjectStoreAdapter {
    inner: Arc<dyn ObjectStore>,
}

impl LanceObjectStoreAdapter {
    pub(super) fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self { inner }
    }
}

impl Display for LanceObjectStoreAdapter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}

#[async_trait]
impl object_store_lance::ObjectStore for LanceObjectStoreAdapter {
    async fn put_opts(
        &self,
        _location: &Path,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> Result<PutResult> {
        Err(not_implemented("put_opts"))
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        Err(not_implemented("put_multipart_opts"))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let result = self
            .inner
            .get_opts(&to_host_path(location)?, to_host_get_options(options))
            .await
            .map_err(to_lance_error)?;
        to_lance_get_result(result)
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        self.inner
            .get_ranges(&to_host_path(location)?, ranges)
            .await
            .map_err(to_lance_error)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        locations
            .map(|_| Err(not_implemented("delete_stream")))
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        match prefix.map(to_host_path).transpose() {
            Ok(prefix) => to_lance_meta_stream(self.inner.list(prefix.as_ref())),
            Err(e) => stream::once(async { Err(e) }).boxed(),
        }
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let paths = prefix
            .map(to_host_path)
            .transpose()
            .and_then(|prefix| Ok((prefix, to_host_path(offset)?)));
        match paths {
            Ok((prefix, offset)) => {
                to_lance_meta_stream(self.inner.list_with_offset(prefix.as_ref(), &offset))
            }
            Err(e) => stream::once(async { Err(e) }).boxed(),
        }
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let prefix = prefix.map(to_host_path).transpose()?;
        let result = self
            .inner
            .list_with_delimiter(prefix.as_ref())
            .await
            .map_err(to_lance_error)?;
        Ok(ListResult {
            common_prefixes: result
                .common_prefixes
                .iter()
                .map(to_lance_path)
                .collect::<Result<_>>()?,
            objects: result
                .objects
                .into_iter()
                .map(to_lance_meta)
                .collect::<Result<_>>()?,
            extensions: Default::default(),
        })
    }

    async fn copy_opts(&self, _from: &Path, _to: &Path, _options: CopyOptions) -> Result<()> {
        Err(not_implemented("copy_opts"))
    }
}

fn not_implemented(operation: &str) -> Error {
    Error::NotImplemented {
        operation: operation.to_string(),
        implementer: STORE.to_string(),
    }
}

fn to_host_path(path: &Path) -> Result<HostPath> {
    HostPath::parse(path.as_ref()).map_err(|e| Error::Generic {
        store: STORE,
        source: Box::new(e),
    })
}

fn to_lance_path(path: &HostPath) -> Result<Path> {
    Ok(Path::parse(path.as_ref())?)
}

fn to_host_get_options(options: GetOptions) -> HostGetOptions {
    HostGetOptions {
        if_match: options.if_match,
        if_none_match: options.if_none_match,
        if_modified_since: options.if_modified_since,
        if_unmodified_since: options.if_unmodified_since,
        range: options.range.map(|range| match range {
            GetRange::Bounded(r) => HostGetRange::Bounded(r),
            GetRange::Offset(o) => HostGetRange::Offset(o),
            GetRange::Suffix(s) => HostGetRange::Suffix(s),
        }),
        version: options.version,
        head: options.head,
        extensions: options.extensions,
    }
}

fn to_lance_get_result(result: HostGetResult) -> Result<GetResult> {
    let payload = match result.payload {
        HostGetResultPayload::File(file, path) => GetResultPayload::File(file, path),
        HostGetResultPayload::Stream(s) => {
            GetResultPayload::Stream(s.map(|r| r.map_err(to_lance_error)).boxed())
        }
    };
    Ok(GetResult {
        payload,
        meta: to_lance_meta(result.meta)?,
        range: result.range,
        attributes: to_lance_attributes(&result.attributes),
        extensions: Default::default(),
    })
}

fn to_lance_meta(meta: HostObjectMeta) -> Result<ObjectMeta> {
    Ok(ObjectMeta {
        location: to_lance_path(&meta.location)?,
        last_modified: meta.last_modified,
        size: meta.size,
        e_tag: meta.e_tag,
        version: meta.version,
    })
}

fn to_lance_meta_stream(
    metas: BoxStream<'static, object_store::Result<HostObjectMeta>>,
) -> BoxStream<'static, Result<ObjectMeta>> {
    metas
        .map(|r| r.map_err(to_lance_error).and_then(to_lance_meta))
        .boxed()
}

fn to_lance_attributes(attributes: &HostAttributes) -> Attributes {
    attributes
        .iter()
        .filter_map(|(key, value)| {
            let key = match key {
                HostAttribute::ContentDisposition => Attribute::ContentDisposition,
                HostAttribute::ContentEncoding => Attribute::ContentEncoding,
                HostAttribute::ContentLanguage => Attribute::ContentLanguage,
                HostAttribute::ContentType => Attribute::ContentType,
                HostAttribute::CacheControl => Attribute::CacheControl,
                HostAttribute::StorageClass => Attribute::StorageClass,
                HostAttribute::Metadata(name) => Attribute::Metadata(name.clone()),
                _ => return None,
            };
            Some((key, AttributeValue::from(value.to_string())))
        })
        .collect()
}

/// Keeps the variants Lance tells apart (a missing object above all) and
/// carries every other error as `Generic` with the original as its source.
fn to_lance_error(error: HostError) -> Error {
    match error {
        HostError::NotFound { path, source } => Error::NotFound { path, source },
        HostError::AlreadyExists { path, source } => Error::AlreadyExists { path, source },
        HostError::Precondition { path, source } => Error::Precondition { path, source },
        HostError::NotModified { path, source } => Error::NotModified { path, source },
        HostError::NotSupported { source } => Error::NotSupported { source },
        HostError::PermissionDenied { path, source } => Error::PermissionDenied { path, source },
        HostError::Unauthenticated { path, source } => Error::Unauthenticated { path, source },
        HostError::NotImplemented {
            operation,
            implementer,
        } => Error::NotImplemented {
            operation,
            implementer,
        },
        HostError::Generic { store, source } => Error::Generic { store, source },
        other => Error::Generic {
            store: STORE,
            source: Box::new(other),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use object_store::{ObjectStoreExt as HostObjectStoreExt, PutPayload as HostPutPayload};
    use object_store_lance::{ObjectStore as LanceObjectStore, ObjectStoreExt};

    async fn adapter_over(files: &[(&str, &'static [u8])]) -> LanceObjectStoreAdapter {
        let inner = InMemory::new();
        for (path, data) in files {
            inner
                .put(&HostPath::from(*path), HostPutPayload::from_static(data))
                .await
                .unwrap();
        }
        LanceObjectStoreAdapter::new(Arc::new(inner))
    }

    #[tokio::test]
    async fn reads_whole_objects_ranges_and_metadata() {
        let store = adapter_over(&[("dir/a.lance", b"0123456789")]).await;
        let path = Path::from("dir/a.lance");

        let whole = store.get(&path).await.unwrap();
        assert_eq!(whole.meta.size, 10);
        assert_eq!(whole.range, 0..10);
        assert_eq!(
            whole.bytes().await.unwrap(),
            Bytes::from_static(b"0123456789")
        );

        assert_eq!(
            store.get_range(&path, 2..5).await.unwrap(),
            Bytes::from_static(b"234")
        );
        assert_eq!(
            store.get_ranges(&path, &[0..1, 8..10]).await.unwrap(),
            vec![Bytes::from_static(b"0"), Bytes::from_static(b"89")]
        );

        let suffix = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Suffix(3)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(suffix.range, 7..10);

        let offset = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Offset(6)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(offset.range, 6..10);
        assert_eq!(offset.bytes().await.unwrap(), Bytes::from_static(b"6789"));

        let head = store.head(&path).await.unwrap();
        assert_eq!(head.location, path);
        assert_eq!(head.size, 10);
    }

    #[tokio::test]
    async fn reads_local_files_through_the_file_payload() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.lance"), b"0123456789").unwrap();
        let inner = object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let store = LanceObjectStoreAdapter::new(Arc::new(inner));
        let path = Path::from("a.lance");

        let result = store.get(&path).await.unwrap();
        assert!(matches!(result.payload, GetResultPayload::File(..)));
        assert_eq!(
            result.bytes().await.unwrap(),
            Bytes::from_static(b"0123456789")
        );
        assert_eq!(
            store.get_range(&path, 4..6).await.unwrap(),
            Bytes::from_static(b"45")
        );
    }

    #[tokio::test]
    async fn lists_with_and_without_delimiter() {
        let store = adapter_over(&[
            ("dir/a.lance", b"a"),
            ("dir/b.lance", b"b"),
            ("dir/sub/c.lance", b"c"),
        ])
        .await;
        let prefix = Path::from("dir");

        let mut all: Vec<_> = store
            .list(Some(&prefix))
            .map(|r| r.unwrap().location.to_string())
            .collect()
            .await;
        all.sort();
        assert_eq!(all, ["dir/a.lance", "dir/b.lance", "dir/sub/c.lance"]);

        let after: Vec<_> = store
            .list_with_offset(Some(&prefix), &Path::from("dir/a.lance"))
            .map(|r| r.unwrap().location.to_string())
            .collect()
            .await;
        assert!(!after.contains(&"dir/a.lance".to_string()));
        assert_eq!(after.len(), 2);

        let listed = store.list_with_delimiter(Some(&prefix)).await.unwrap();
        assert_eq!(listed.common_prefixes, vec![Path::from("dir/sub")]);
        let mut objects: Vec<_> = listed
            .objects
            .iter()
            .map(|m| m.location.to_string())
            .collect();
        objects.sort();
        assert_eq!(objects, ["dir/a.lance", "dir/b.lance"]);
    }

    #[tokio::test]
    async fn keeps_not_found_distinct() {
        let store = adapter_over(&[]).await;
        let err = store.head(&Path::from("missing.lance")).await.unwrap_err();
        assert!(matches!(err, Error::NotFound { .. }), "{err:?}");
    }

    #[tokio::test]
    async fn rejects_writes() {
        let store = adapter_over(&[]).await;
        let path = Path::from("a.lance");

        let errors = [
            store
                .put(&path, PutPayload::from_static(b"x"))
                .await
                .err()
                .unwrap(),
            store.put_multipart(&path).await.err().unwrap(),
            store
                .copy(&path, &Path::from("b.lance"))
                .await
                .err()
                .unwrap(),
            store.delete(&path).await.err().unwrap(),
        ];
        for err in errors {
            assert!(matches!(err, Error::NotImplemented { .. }), "{err:?}");
        }
    }
}
