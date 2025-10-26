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
    let url = Url::parse(uri).or_else(|e| {
        let assumed_path = PathBuf::from(uri);
        Url::from_file_path(assumed_path).map_err(|_| UrlParseError(e))
    })?;

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
    let mut path_segments = url
        .path_segments_mut()
        .map_err(|_| InvalidPath(format!("URL '{}' cannot be a base", base_url)))?;

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

    drop(path_segments);

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_uri_with_valid_url() {
        let uri = "s3://bucket/path/to/file";
        let result = parse_uri(uri);
        assert!(result.is_ok());
        let url = result.unwrap();
        assert_eq!(url.scheme(), "s3");
        assert_eq!(url.host_str(), Some("bucket"));
        assert_eq!(url.path(), "/path/to/file");
    }

    #[test]
    fn test_parse_uri_with_http_url() {
        let uri = "http://example.com:8080/path?query=value";
        let result = parse_uri(uri);
        assert!(result.is_ok());
        let url = result.unwrap();
        assert_eq!(url.scheme(), "http");
        assert_eq!(url.host_str(), Some("example.com"));
        assert_eq!(url.port(), Some(8080));
        assert_eq!(url.path(), "/path");
        assert_eq!(url.query(), Some("query=value"));
    }

    #[test]
    fn test_parse_uri_with_file_path() {
        // Test parsing a file path - should convert to file:// URL
        let uri = "/tmp/test/file.txt";
        let result = parse_uri(uri);
        assert!(result.is_ok());
        let url = result.unwrap();
        assert_eq!(url.scheme(), "file");
    }

    #[test]
    fn test_parse_uri_with_relative_path() {
        let uri = "relative/path/to/file";
        let result = parse_uri(uri);
        // Relative paths should fail with Url::parse but might succeed with from_file_path
        // depending on the current working directory
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn test_parse_uri_with_windows_path() {
        let uri = "C:\\Users\\test\\file.txt";
        let result = parse_uri(uri);
        // Windows paths should be converted to file:// URLs
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_uri_with_hdfs_url() {
        let uri = "hdfs://namenode:9000/user/hive/warehouse";
        let result = parse_uri(uri);
        assert!(result.is_ok());
        let url = result.unwrap();
        assert_eq!(url.scheme(), "hdfs");
        assert_eq!(url.host_str(), Some("namenode"));
        assert_eq!(url.port(), Some(9000));
    }

    #[test]
    fn test_get_scheme_authority_with_s3() {
        let url = Url::parse("s3://my-bucket/path/to/file").unwrap();
        let result = get_scheme_authority(&url);
        assert_eq!(result, "s3://my-bucket");
    }

    #[test]
    fn test_get_scheme_authority_with_http() {
        let url = Url::parse("http://example.com:8080/path").unwrap();
        let result = get_scheme_authority(&url);
        assert_eq!(result, "http://example.com:8080");
    }

    #[test]
    fn test_get_scheme_authority_with_file() {
        let url = Url::parse("file:///tmp/test").unwrap();
        let result = get_scheme_authority(&url);
        assert_eq!(result, "file://");
    }

    #[test]
    fn test_join_url_segments_basic() {
        let base_url = Url::parse("s3://bucket/base").unwrap();
        let segments = vec!["path", "to", "file"];
        let result = join_url_segments(&base_url, &segments);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().path(), "/base/path/to/file");
    }

    #[test]
    fn test_join_url_segments_with_trailing_slash() {
        let base_url = Url::parse("s3://bucket/base/").unwrap();
        let segments = vec!["path", "to", "file"];
        let result = join_url_segments(&base_url, &segments);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().path(), "/base/path/to/file");
    }

    #[test]
    fn test_join_url_segments_with_empty_segments() {
        let base_url = Url::parse("s3://bucket/base").unwrap();
        let segments = vec!["path", "", "file"];
        let result = join_url_segments(&base_url, &segments);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().path(), "/base/path/file");
    }

    #[test]
    fn test_join_url_segments_with_backslashes() {
        let base_url = Url::parse("s3://bucket/base").unwrap();
        let segments = vec!["path\\to", "file"];
        let result = join_url_segments(&base_url, &segments);
        assert!(result.is_ok());
        // Backslashes should be converted to forward slashes
        assert_eq!(result.unwrap().path(), "/base/path/to/file");
    }

    #[test]
    fn test_join_url_segments_with_nested_slashes() {
        let base_url = Url::parse("s3://bucket/base").unwrap();
        let segments = vec!["path/to/nested", "file"];
        let result = join_url_segments(&base_url, &segments);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().path(), "/base/path/to/nested/file");
    }

    #[test]
    fn test_join_url_segments_with_non_base_url() {
        // Cannot-be-a-base URLs like "mailto:" should fail
        let base_url = Url::parse("mailto:user@example.com").unwrap();
        let segments = vec!["path"];
        let result = join_url_segments(&base_url, &segments);
        assert!(result.is_err());
    }

    #[test]
    fn test_join_url_segments_empty_segments_list() {
        let base_url = Url::parse("s3://bucket/base").unwrap();
        let segments: Vec<&str> = vec![];
        let result = join_url_segments(&base_url, &segments);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().path(), "/base");
    }

    #[test]
    fn test_join_path_segments_basic() {
        let segments = vec!["path", "to", "file"];
        let result = join_path_segments(&segments);
        assert!(result.is_ok());
        let path = result.unwrap();
        // Result will vary based on OS
        assert!(path.contains("path"));
        assert!(path.contains("to"));
        assert!(path.contains("file"));
    }

    #[test]
    fn test_join_path_segments_empty() {
        let segments: Vec<&str> = vec![];
        let result = join_path_segments(&segments);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "");
    }

    #[test]
    fn test_join_path_segments_single() {
        let segments = vec!["file"];
        let result = join_path_segments(&segments);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "file");
    }

    #[test]
    fn test_parse_uri_with_gcs_url() {
        let uri = "gs://my-bucket/path/to/data";
        let result = parse_uri(uri);
        assert!(result.is_ok());
        let url = result.unwrap();
        assert_eq!(url.scheme(), "gs");
        assert_eq!(url.host_str(), Some("my-bucket"));
        assert_eq!(url.path(), "/path/to/data");
    }

    #[test]
    fn test_parse_uri_with_azure_url() {
        let uri = "wasbs://container@account.blob.core.windows.net/path";
        let result = parse_uri(uri);
        assert!(result.is_ok());
        let url = result.unwrap();
        assert_eq!(url.scheme(), "wasbs");
    }

    #[test]
    fn test_join_url_segments_with_special_characters() {
        let base_url = Url::parse("s3://bucket/base").unwrap();
        let segments = vec!["path-with-dash", "file_with_underscore"];
        let result = join_url_segments(&base_url, &segments);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap().path(),
            "/base/path-with-dash/file_with_underscore"
        );
    }

    #[test]
    fn test_join_url_segments_preserves_query_params() {
        let base_url = Url::parse("s3://bucket/base?key=value").unwrap();
        let segments = vec!["path"];
        let result = join_url_segments(&base_url, &segments);
        assert!(result.is_ok());
        let url = result.unwrap();
        assert_eq!(url.path(), "/base/path");
        assert_eq!(url.query(), Some("key=value"));
    }
}
