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
//! HFile record types for metadata table operations.
//!
//! This module provides simple, owned record types for HFile key-value pairs.
//! These are designed for use in metadata table operations where:
//! - Records need to be passed around and stored
//! - Key-based lookups and merging are primary operations
//! - Values are Avro-serialized payloads decoded on demand
//!
//! Unlike the `KeyValue` type which references into file bytes,
//! `HFileRecord` owns its data and can be freely moved.

use std::cmp::Ordering;

/// An owned HFile record with key and value.
///
/// This is a simple struct designed for metadata table operations. The key is
/// the UTF-8 record key (content only, without HFile key structure), and
/// the value is the raw bytes (typically Avro-serialized payload).
///
/// # Example
/// ```ignore
/// let record = HFileRecord::new("my-key".into(), value_bytes);
/// println!("Key: {}", record.key_as_str());
/// // Decode value on demand
/// let payload = decode_avro(&record.value);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HFileRecord {
    /// Record key (UTF-8 string content only, no length prefix)
    pub key: Vec<u8>,
    /// Record value (raw bytes, typically Avro-serialized)
    pub value: Vec<u8>,
}

impl HFileRecord {
    /// Create a new HFile record.
    pub fn new(key: Vec<u8>, value: Vec<u8>) -> Self {
        Self { key, value }
    }

    /// Create a record from string key and value bytes.
    pub fn from_str_key(key: &str, value: Vec<u8>) -> Self {
        Self {
            key: key.as_bytes().to_vec(),
            value,
        }
    }

    /// Returns the key as a UTF-8 string.
    ///
    /// Returns `None` if the key is not valid UTF-8.
    pub fn key_as_str(&self) -> Option<&str> {
        std::str::from_utf8(&self.key).ok()
    }

    /// Returns the key as bytes.
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Returns the value as bytes.
    pub fn value(&self) -> &[u8] {
        &self.value
    }

    /// Returns whether this record represents a deletion.
    ///
    /// In metadata table, a deleted record has an empty value.
    pub fn is_deleted(&self) -> bool {
        self.value.is_empty()
    }
}

impl PartialOrd for HFileRecord {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HFileRecord {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

impl std::fmt::Display for HFileRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.key_as_str() {
            Some(key) => write!(
                f,
                "HFileRecord{{key={}, value_len={}}}",
                key,
                self.value.len()
            ),
            None => write!(
                f,
                "HFileRecord{{key=<binary {} bytes>, value_len={}}}",
                self.key.len(),
                self.value.len()
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hfile_record_creation() {
        let record = HFileRecord::new(b"test-key".to_vec(), b"test-value".to_vec());
        assert_eq!(record.key_as_str(), Some("test-key"));
        assert_eq!(record.value(), b"test-value");
        assert!(!record.is_deleted());
    }

    #[test]
    fn test_hfile_record_from_str() {
        let record = HFileRecord::from_str_key("my-key", b"my-value".to_vec());
        assert_eq!(record.key_as_str(), Some("my-key"));
        assert_eq!(record.value(), b"my-value");
    }

    #[test]
    fn test_hfile_record_deleted() {
        let record = HFileRecord::new(b"deleted-key".to_vec(), vec![]);
        assert!(record.is_deleted());
    }

    #[test]
    fn test_hfile_record_ordering() {
        let r1 = HFileRecord::from_str_key("aaa", vec![1]);
        let r2 = HFileRecord::from_str_key("bbb", vec![2]);
        let r3 = HFileRecord::from_str_key("aaa", vec![3]);

        assert!(r1 < r2);
        assert_eq!(r1.cmp(&r3), Ordering::Equal); // Same key, ordering only by key
    }

    #[test]
    fn test_hfile_record_display() {
        let record = HFileRecord::from_str_key("test", b"value".to_vec());
        let display = format!("{}", record);
        assert!(display.contains("test"));
        assert!(display.contains("value_len=5"));
    }
}
