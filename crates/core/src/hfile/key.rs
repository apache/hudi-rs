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
//! Key and KeyValue types for HFile.

use std::cmp::Ordering;

/// Size constants
const SIZEOF_INT32: usize = 4;
const SIZEOF_INT16: usize = 2;

/// Key offset after key length (int32) and value length (int32)
pub const KEY_VALUE_HEADER_SIZE: usize = SIZEOF_INT32 * 2;

/// A key in HFile format.
///
/// In HFile, keys have the following structure:
/// - 2 bytes: key content length (short)
/// - N bytes: key content
/// - Additional bytes: other information (not used by Hudi)
///
/// For comparison and hashing, only the key content is used.
#[derive(Debug, Clone)]
pub struct Key {
    /// Raw key bytes including the length prefix
    bytes: Vec<u8>,
    /// Offset to the start of the key within bytes
    offset: usize,
    /// Total length of the key part (including length prefix and other info)
    length: usize,
}

impl Key {
    /// Create a new Key from bytes at the given offset with the specified length.
    pub fn new(bytes: &[u8], offset: usize, length: usize) -> Self {
        Self {
            bytes: bytes.to_vec(),
            offset,
            length,
        }
    }

    /// Create a Key from raw bytes (the entire key).
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        let length = bytes.len();
        Self {
            bytes,
            offset: 0,
            length,
        }
    }

    /// Returns the offset to the key content (after length prefix).
    pub fn content_offset(&self) -> usize {
        self.offset + SIZEOF_INT16
    }

    /// Returns the length of the key content.
    pub fn content_length(&self) -> usize {
        if self.bytes.len() < self.offset + SIZEOF_INT16 {
            return 0;
        }
        let len_bytes = &self.bytes[self.offset..self.offset + SIZEOF_INT16];
        i16::from_be_bytes([len_bytes[0], len_bytes[1]]) as usize
    }

    /// Returns the key content as a byte slice.
    pub fn content(&self) -> &[u8] {
        let start = self.content_offset();
        let len = self.content_length();
        if start + len > self.bytes.len() {
            return &[];
        }
        &self.bytes[start..start + len]
    }

    /// Returns the key content as a UTF-8 string.
    pub fn content_as_str(&self) -> Result<&str, std::str::Utf8Error> {
        std::str::from_utf8(self.content())
    }

    /// Returns the total length of the key part.
    pub fn length(&self) -> usize {
        self.length
    }

    /// Returns the raw bytes.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

impl PartialEq for Key {
    fn eq(&self, other: &Self) -> bool {
        self.content() == other.content()
    }
}

impl Eq for Key {}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> Ordering {
        self.content().cmp(other.content())
    }
}

impl std::hash::Hash for Key {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.content().hash(state);
    }
}

impl std::fmt::Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.content_as_str() {
            Ok(s) => write!(f, "Key{{{}}}", s),
            Err(_) => write!(f, "Key{{<binary>}}"),
        }
    }
}

/// A UTF-8 string key without length prefix.
///
/// Used for lookup keys and meta block keys where the key is just the content
/// without the HFile key structure.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Utf8Key {
    content: String,
}

impl Utf8Key {
    /// Create a new UTF-8 key from a string.
    pub fn new(s: impl Into<String>) -> Self {
        Self { content: s.into() }
    }

    /// Returns the key content as bytes.
    pub fn as_bytes(&self) -> &[u8] {
        self.content.as_bytes()
    }

    /// Returns the key content as a string slice.
    pub fn as_str(&self) -> &str {
        &self.content
    }
}

impl PartialOrd for Utf8Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Utf8Key {
    fn cmp(&self, other: &Self) -> Ordering {
        self.content.as_bytes().cmp(other.content.as_bytes())
    }
}

impl From<&str> for Utf8Key {
    fn from(s: &str) -> Self {
        Utf8Key::new(s)
    }
}

impl From<String> for Utf8Key {
    fn from(s: String) -> Self {
        Utf8Key::new(s)
    }
}

impl std::fmt::Display for Utf8Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Utf8Key{{{}}}", self.content)
    }
}

/// A key-value pair from HFile data block.
///
/// The HFile key-value format is:
/// - 4 bytes: key length (int32)
/// - 4 bytes: value length (int32)
/// - N bytes: key (structured as Key)
/// - M bytes: value
/// - 1 byte: MVCC timestamp version (always 0 for Hudi)
#[derive(Debug, Clone)]
pub struct KeyValue {
    /// The backing byte array containing the entire key-value record
    bytes: Vec<u8>,
    /// Offset to the start of this record in bytes
    offset: usize,
    /// The parsed key
    key: Key,
    /// Length of key part
    key_length: usize,
    /// Length of value part
    value_length: usize,
}

impl KeyValue {
    /// Parse a KeyValue from bytes at the given offset.
    pub fn parse(bytes: &[u8], offset: usize) -> Self {
        let key_length = i32::from_be_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]) as usize;

        let value_length = i32::from_be_bytes([
            bytes[offset + 4],
            bytes[offset + 5],
            bytes[offset + 6],
            bytes[offset + 7],
        ]) as usize;

        let key_offset = offset + KEY_VALUE_HEADER_SIZE;
        let key = Key::new(bytes, key_offset, key_length);

        Self {
            bytes: bytes.to_vec(),
            offset,
            key,
            key_length,
            value_length,
        }
    }

    /// Returns the key.
    pub fn key(&self) -> &Key {
        &self.key
    }

    /// Returns the value as a byte slice.
    pub fn value(&self) -> &[u8] {
        let value_offset = self.offset + KEY_VALUE_HEADER_SIZE + self.key_length;
        &self.bytes[value_offset..value_offset + self.value_length]
    }

    /// Returns the total size of this key-value record including MVCC timestamp.
    pub fn record_size(&self) -> usize {
        // header (8) + key + value + mvcc timestamp (1)
        KEY_VALUE_HEADER_SIZE + self.key_length + self.value_length + 1
    }

    /// Returns the key length.
    pub fn key_length(&self) -> usize {
        self.key_length
    }

    /// Returns the value length.
    pub fn value_length(&self) -> usize {
        self.value_length
    }
}

impl std::fmt::Display for KeyValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "KeyValue{{key={}}}", self.key)
    }
}

/// Compare a Key with a Utf8Key (for lookups).
///
/// This compares the key content bytes lexicographically.
pub fn compare_keys(key: &Key, lookup: &Utf8Key) -> Ordering {
    key.content().cmp(lookup.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_utf8_key_comparison() {
        let k1 = Utf8Key::new("abc");
        let k2 = Utf8Key::new("abd");
        let k3 = Utf8Key::new("abc");

        assert!(k1 < k2);
        assert_eq!(k1, k3);
    }
}
