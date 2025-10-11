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

//! Utility functions for storage.
use object_store::path::Path as ObjectPath;
use url::Url;

use crate::storage::error::StorageError::{InvalidPath, UrlParseError};
use crate::storage::Result;

/// Parses a URI string into a URL.
pub fn parse_uri(uri: &str) -> Result<Url> {
    let mut url = match Url::parse(uri) {
        Ok(url) => url,
        Err(e) => Url::from_directory_path(uri).map_err(|_| UrlParseError(e))?,
    };

    if url.path().ends_with('/') {
        let err = InvalidPath(format!("Url {:?} cannot be a base", url));
        url.path_segments_mut().map_err(|_| err)?.pop();
    }

    Ok(url)
}

/// Returns the scheme and authority of a URL in the form of `scheme://authority`.
pub fn get_scheme_authority(url: &Url) -> String {
    format!("{}://{}", url.scheme(), url.authority())
}

/// Normalizes and flattens path segments by splitting on '/' and filtering empty parts.
///
/// # Arguments
/// * `segments` - Path segments to normalize
///
/// # Returns
/// A vector of non-empty, trimmed path parts
fn normalize_path_segments(segments: &[&str]) -> Vec<String> {
    segments
        .iter()
        .flat_map(|s| {
            s.split('/')
                .filter(|part| !part.is_empty())
                .map(|part| part.trim().to_string())
        })
        .collect()
}

/// Joins path segments for local filesystem using `std::path::PathBuf`.
///
/// # Arguments
/// * `segments` - Path segments to join
///
/// # Returns
/// A normalized path string with platform-specific separators
pub fn join_path_for_local_fs(segments: &[&str]) -> String {
    let parts = normalize_path_segments(segments);
    if parts.is_empty() {
        return String::new();
    }

    let mut path = std::path::PathBuf::new();
    for part in parts {
        path.push(part);
    }
    path.to_string_lossy().to_string()
}

/// Joins path segments for cloud storage using `object_store::path::Path`.
///
/// # Arguments
/// * `segments` - Path segments to join
///
/// # Returns
/// A normalized path string with forward slashes as separators
pub fn join_path_for_cloud(segments: &[&str]) -> String {
    let parts = normalize_path_segments(segments);
    if parts.is_empty() {
        return String::new();
    }
    ObjectPath::from_iter(parts).as_ref().to_string()
}

/// Joins path segments into a storage path.
///
/// This function uses `object_store::path::Path` which handles path normalization
/// consistently across local filesystem and cloud storage (S3, GCS, Azure, etc.).
///
/// # Arguments
/// * `segments` - Path segments to join
///
/// # Returns
/// A normalized path string with forward slashes as separators
pub fn join_storage_path(segments: &[&str]) -> String {
    join_path_for_cloud(segments)
}

/// Joins a base URL with a list of segments.
pub fn join_url_segments(base_url: &Url, segments: &[&str]) -> Result<Url> {
    let mut url = base_url.clone();

    if url.path().ends_with('/') {
        url.path_segments_mut().unwrap().pop();
    }

    for &seg in segments {
        let segs: Vec<_> = seg.split('/').filter(|&s| !s.is_empty()).collect();
        let err = InvalidPath(format!("Url {:?} cannot be a base", url));
        url.path_segments_mut().map_err(|_| err)?.extend(segs);
    }

    Ok(url)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn parse_valid_uri_in_various_forms() {
        let urls = vec![
            parse_uri("/foo/").unwrap(),
            parse_uri("file:/foo/").unwrap(),
            parse_uri("file:///foo/").unwrap(),
            parse_uri("hdfs://foo/").unwrap(),
            parse_uri("s3://foo").unwrap(),
            parse_uri("s3://foo/").unwrap(),
            parse_uri("s3a://foo/bar/").unwrap(),
            parse_uri("gs://foo/").unwrap(),
            parse_uri("wasb://foo/bar").unwrap(),
            parse_uri("wasbs://foo/").unwrap(),
        ];
        let schemes = vec![
            "file", "file", "file", "hdfs", "s3", "s3", "s3a", "gs", "wasb", "wasbs",
        ];
        let paths = vec![
            "/foo", "/foo", "/foo", "/", "", "/", "/bar", "/", "/bar", "/",
        ];
        assert_eq!(urls.iter().map(|u| u.scheme()).collect::<Vec<_>>(), schemes);
        assert_eq!(urls.iter().map(|u| u.path()).collect::<Vec<_>>(), paths);
    }

    #[test]
    fn join_base_url_with_segments() {
        let base_url = Url::from_str("file:///base").unwrap();

        assert_eq!(
            join_url_segments(&base_url, &["foo"]).unwrap(),
            Url::from_str("file:///base/foo").unwrap()
        );

        assert_eq!(
            join_url_segments(&base_url, &["/foo"]).unwrap(),
            Url::from_str("file:///base/foo").unwrap()
        );

        assert_eq!(
            join_url_segments(&base_url, &["/foo", "bar/", "/baz/"]).unwrap(),
            Url::from_str("file:///base/foo/bar/baz").unwrap()
        );

        assert_eq!(
            join_url_segments(&base_url, &["foo/", "", "bar/baz"]).unwrap(),
            Url::from_str("file:///base/foo/bar/baz").unwrap()
        );

        assert_eq!(
            join_url_segments(&base_url, &["foo1/bar1", "foo2/bar2"]).unwrap(),
            Url::from_str("file:///base/foo1/bar1/foo2/bar2").unwrap()
        );
    }

    #[test]
    fn join_failed_due_to_invalid_base() {
        let base_url = Url::from_str("foo:text/plain,bar").unwrap();
        let result = join_url_segments(&base_url, &["foo"]);
        assert!(result.is_err());
    }

    #[test]
    fn test_join_storage_path() {
        assert_eq!(
            join_storage_path(&[".hoodie", "file.commit"]),
            ".hoodie/file.commit"
        );
        assert_eq!(join_storage_path(&["path", "to", "file"]), "path/to/file");
        assert_eq!(
            join_storage_path(&["/path/", "/to/", "/file/"]),
            "path/to/file"
        );
        assert_eq!(join_storage_path(&[""]), "");
        assert_eq!(
            join_storage_path(&["", "path", "", "file", ""]),
            "path/file"
        );
        assert_eq!(
            join_storage_path(&["part1", "part2", "subpart"]),
            "part1/part2/subpart"
        );
    }
}
