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

use std::path::PathBuf;
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

/// Joins a base URL with a list of segments.
///
/// # Arguments
/// * `base_url` - Base URL to join segments with
/// * `segments` - Path segments to append
///
/// # Returns
/// The joined URL, or an error if the URL cannot be a base
pub fn join_url_segments(base_url: &Url, segments: &[&str]) -> Result<Url> {
    let mut url = base_url.clone();

    // Verify URL can be used as a base and get mutable path segments
    let url_str = url.to_string();
    let mut path_segments = url
        .path_segments_mut()
        .map_err(|_| InvalidPath(format!("URL '{}' cannot be a base", url_str)))?;

    // Remove trailing empty segment if path ends with '/'
    path_segments.pop_if_empty();

    // Add new segments, normalizing backslashes to forward slashes
    for segment in segments {
        // Normalize backslashes to forward slashes for URLs
        let normalized = segment.replace('\\', "/");
        for part in normalized.split('/') {
            if !part.is_empty() {
                path_segments.push(part);
            }
        }
    }

    drop(path_segments); // Release mutable borrow

    Ok(url)
}

/// Joins path segments into a single path string.
///
/// # Arguments
/// * `segments` - Path segments to join
///
/// # Returns
/// The joined path as a string, or an error if the path contains invalid UTF-8
pub fn join_path_segments(segments: &[&str]) -> Result<String> {
    let path = PathBuf::from_iter(Vec::from(segments));
    path.to_str()
        .map(|s| s.to_string())
        .ok_or_else(|| InvalidPath(format!("Path contains invalid UTF-8: {:?}", path)))
}

pub fn join_base_path_with_relative_path(base: &str, relative: &str) -> Result<String> {
    join_base_path_with_segments(base, &[relative])
}

/// Joins a base path with segments, handling URLs, Windows paths, and Unix paths.
///
/// # Arguments
/// * `base` - Base path (URL like "s3://bucket", Windows like "C:\foo", or Unix like "/tmp")
/// * `segments` - Path segments to append
///
/// # Returns
/// The joined path as a string, or an error if the input is invalid
pub fn join_base_path_with_segments(base: &str, segments: &[&str]) -> Result<String> {
    // 1) Windows absolute path check FIRST (before URL parsing)
    //    This prevents "C:/foo" from being parsed as a URL with scheme "C"
    if is_windows_drive_path(base) {
        let mut path = PathBuf::from(base);
        for segment in segments {
            // Split by both separators for flexibility
            for part in segment.split(&['/', '\\']) {
                if !part.is_empty() {
                    path.push(part);
                }
            }
        }
        return Ok(path.to_string_lossy().into_owned());
    }

    // 2) Try URL mode: parse base as absolute URL
    if let Ok(mut url) = Url::parse(base) {
        // Verify URL can be used as a base (has path segments)
        let mut path_segments = url
            .path_segments_mut()
            .map_err(|_| InvalidPath(format!("URL '{}' cannot be a base", base)))?;

        path_segments.pop_if_empty();

        for segment in segments {
            // Normalize backslashes to forward slashes for URLs
            let normalized = segment.replace('\\', "/");
            for part in normalized.split('/') {
                if !part.is_empty() {
                    path_segments.push(part);
                }
            }
        }
        drop(path_segments); // Release mutable borrow

        return Ok(url.to_string());
    }

    // 3) Fallback: Unix absolute/relative filesystem paths
    //    Use PathBuf for proper platform handling
    let mut path = PathBuf::from(base);
    for segment in segments {
        // Split by both separators for flexibility
        for part in segment.split(&['/', '\\']) {
            if !part.is_empty() {
                path.push(part);
            }
        }
    }
    Ok(path.to_string_lossy().into_owned())
}

/// Detects if a string starts like a Windows drive path: `C:\` or `C:/`
fn is_windows_drive_path(path: &str) -> bool {
    let mut chars = path.chars();
    match (chars.next(), chars.next(), chars.next()) {
        (Some(drive), Some(':'), third) if drive.is_ascii_alphabetic() => {
            third.map(|c| c == '\\' || c == '/').unwrap_or(true)
        }
        _ => false,
    }
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
    fn test_join_url_segments_backslash_normalization() {
        let base_url = Url::from_str("s3://bucket/data").unwrap();

        // Backslashes should be normalized to forward slashes
        assert_eq!(
            join_url_segments(&base_url, &[r"dir\subdir", "file.txt"]).unwrap(),
            Url::from_str("s3://bucket/data/dir/subdir/file.txt").unwrap()
        );

        // Mixed slashes
        assert_eq!(
            join_url_segments(&base_url, &[r"dir\sub/dir", r"more\files"]).unwrap(),
            Url::from_str("s3://bucket/data/dir/sub/dir/more/files").unwrap()
        );
    }

    #[test]
    fn test_join_base_path_with_segments_url() {
        // S3 URL
        assert_eq!(
            join_base_path_with_segments("s3://bucket/prefix", &["foo", "bar"]).unwrap(),
            "s3://bucket/prefix/foo/bar"
        );

        // File URL
        assert_eq!(
            join_base_path_with_segments("file:///tmp", &["foo", "bar.txt"]).unwrap(),
            "file:///tmp/foo/bar.txt"
        );

        // URL with backslashes in segments (should normalize to forward slashes)
        assert_eq!(
            join_base_path_with_segments("s3://bucket/data", &["dir\\subdir", "file.txt"]).unwrap(),
            "s3://bucket/data/dir/subdir/file.txt"
        );

        // URL with empty segments
        assert_eq!(
            join_base_path_with_segments("s3://bucket", &["", "foo", "", "bar"]).unwrap(),
            "s3://bucket/foo/bar"
        );
    }

    #[test]
    fn test_join_base_path_with_segments_windows() {
        // Windows path with backslash
        let result =
            join_base_path_with_segments(r"C:\Users\test", &["Documents", "file.txt"]).unwrap();
        assert!(result.contains("Users"));
        assert!(result.contains("test"));
        assert!(result.contains("Documents"));
        assert!(result.contains("file.txt"));

        // Windows path with forward slash
        let result =
            join_base_path_with_segments("C:/Users/test", &["Documents", "file.txt"]).unwrap();
        assert!(result.contains("Users"));
        assert!(result.contains("test"));
        assert!(result.contains("Documents"));
        assert!(result.contains("file.txt"));

        // Windows path with mixed separators in segments
        let result =
            join_base_path_with_segments(r"D:\data", &["dir/subdir", r"more\files"]).unwrap();
        assert!(result.contains("data"));
        assert!(result.contains("dir"));
        assert!(result.contains("subdir"));
        assert!(result.contains("more"));
        assert!(result.contains("files"));

        // Just drive letter
        let result = join_base_path_with_segments("C:", &["temp", "file.txt"]).unwrap();
        assert!(result.contains("temp"));
        assert!(result.contains("file.txt"));
    }

    #[test]
    fn test_join_base_path_with_segments_unix() {
        // Unix absolute path
        assert_eq!(
            join_base_path_with_segments("/tmp/data", &["foo", "bar.txt"]).unwrap(),
            "/tmp/data/foo/bar.txt"
        );

        // Unix path with backslashes in segments (treated as path separators)
        let result =
            join_base_path_with_segments("/home/user", &[r"dir\subdir", "file.txt"]).unwrap();
        assert!(result.contains("home"));
        assert!(result.contains("user"));
        assert!(result.contains("dir"));
        assert!(result.contains("subdir"));
        assert!(result.contains("file.txt"));

        // Unix path with empty segments
        assert_eq!(
            join_base_path_with_segments("/tmp", &["", "foo", "", "bar"]).unwrap(),
            "/tmp/foo/bar"
        );
    }

    #[test]
    fn test_join_base_path_with_segments_relative() {
        // Relative path
        let result = join_base_path_with_segments("./data", &["foo", "bar.txt"]).unwrap();
        assert!(result.contains("data"));
        assert!(result.contains("foo"));
        assert!(result.contains("bar.txt"));

        // Relative path without prefix
        let result = join_base_path_with_segments("data/dir", &["foo", "bar.txt"]).unwrap();
        assert!(result.contains("data"));
        assert!(result.contains("dir"));
        assert!(result.contains("foo"));
        assert!(result.contains("bar.txt"));
    }

    #[test]
    fn test_join_base_path_with_segments_error_cases() {
        // Cannot-be-a-base URL
        let result = join_base_path_with_segments("mailto:test@example.com", &["foo"]);
        assert!(result.is_err());

        // Another cannot-be-a-base URL
        let result = join_base_path_with_segments("data:text/plain,hello", &["bar"]);
        assert!(result.is_err());
    }

    #[test]
    fn test_is_windows_drive_path() {
        // Valid Windows paths
        assert!(is_windows_drive_path("C:\\"));
        assert!(is_windows_drive_path("C:/"));
        assert!(is_windows_drive_path("D:\\path"));
        assert!(is_windows_drive_path("E:/path"));
        assert!(is_windows_drive_path("C:"));
        assert!(is_windows_drive_path("z:"));
        assert!(is_windows_drive_path("Z:\\"));

        // Invalid Windows paths
        assert!(!is_windows_drive_path("C"));
        assert!(!is_windows_drive_path("/C:/"));
        assert!(!is_windows_drive_path("CC:/"));
        assert!(!is_windows_drive_path("1:/"));
        assert!(!is_windows_drive_path("/tmp"));
        assert!(!is_windows_drive_path("s3://bucket"));
    }

    #[test]
    fn test_join_base_path_with_relative_path() {
        // Test the wrapper function
        assert_eq!(
            join_base_path_with_relative_path("s3://bucket/data", "file.txt").unwrap(),
            "s3://bucket/data/file.txt"
        );

        assert_eq!(
            join_base_path_with_relative_path("/tmp", "file.txt").unwrap(),
            "/tmp/file.txt"
        );
    }
}
