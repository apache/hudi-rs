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
//! Record-key bloom filter and key range for base file footers, serialized as
//! Java Hudi's `BloomFilter#serializeToString` so Java's bloom index can read them.

use base64::Engine;
use base64::engine::general_purpose::STANDARD;

use crate::Result;
use crate::error::CoreError;

const BLOOM_FILTER_FOOTER_KEY: &str = "org.apache.hudi.bloomfilter";
const BLOOM_FILTER_TYPE_CODE_FOOTER_KEY: &str = "hoodie_bloom_filter_type_code";
const MIN_RECORD_KEY_FOOTER_KEY: &str = "hoodie_min_record_key";
const MAX_RECORD_KEY_FOOTER_KEY: &str = "hoodie_max_record_key";

/// Java's `InternalFilter` serialization version.
const FILTER_VERSION: i32 = -1;
/// Java's `Hash.MURMUR_HASH`.
const MURMUR_HASH_TYPE: u8 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BloomFilterType {
    Simple,
    DynamicV0,
}

impl BloomFilterType {
    /// Parses `hoodie.bloom.index.filter.type`, case-insensitively as Java does.
    pub(crate) fn parse(value: &str) -> Option<Self> {
        if value.eq_ignore_ascii_case("SIMPLE") {
            Some(Self::Simple)
        } else if value.eq_ignore_ascii_case("DYNAMIC_V0") {
            Some(Self::DynamicV0)
        } else {
            None
        }
    }
}

/// Bloom filter sizing; the defaults are Java's `HoodieStorageConfig` defaults.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct BloomFilterConfig {
    pub filter_type: BloomFilterType,
    pub num_entries: i32,
    pub fpp: f64,
    pub max_entries: i32,
}

impl Default for BloomFilterConfig {
    fn default() -> Self {
        Self {
            filter_type: BloomFilterType::DynamicV0,
            num_entries: 60_000,
            fpp: 0.000_000_001,
            max_entries: 100_000,
        }
    }
}

/// Java's `SimpleBloomFilter` (one bit vector) or `HoodieDynamicBoundedBloomFilter`
/// (a row of bit vectors that grows by `num_entries` keys up to `max_entries`,
/// then takes further keys round-robin across its rows).
#[derive(Clone, Debug)]
pub(crate) struct BloomFilter {
    filter_type: BloomFilterType,
    vector_size: i32,
    num_hashes: i32,
    /// Bit `i` of a vector is bit `i % 8` of byte `i / 8`, Java's on-disk layout.
    rows: Vec<Vec<u8>>,
    num_entries: i32,
    max_entries: i32,
    current_row_keys: i32,
    reached_max: bool,
    next_round_robin_row: usize,
}

impl BloomFilter {
    pub(crate) fn new(config: &BloomFilterConfig) -> Self {
        let ln2 = std::f64::consts::LN_2;
        let num_entries = config.num_entries;
        // Java's `BloomFilterUtils`, including its evaluation order.
        let vector_size = (num_entries as f64 * (-config.fpp.ln() / (ln2 * ln2))).ceil() as i32;
        let num_hashes = (ln2 * vector_size as f64 / num_entries as f64).ceil() as i32;
        Self {
            filter_type: config.filter_type,
            vector_size,
            num_hashes,
            rows: vec![Self::empty_row(vector_size)],
            num_entries,
            max_entries: config.max_entries,
            current_row_keys: 0,
            reached_max: false,
            next_round_robin_row: 0,
        }
    }

    fn empty_row(vector_size: i32) -> Vec<u8> {
        vec![0; (vector_size as usize).div_ceil(8)]
    }

    /// Adds a non-empty key; Java rejects an empty one.
    pub(crate) fn add(&mut self, key: &[u8]) {
        let row = match self.filter_type {
            BloomFilterType::Simple => 0,
            BloomFilterType::DynamicV0 => self.active_row(),
        };
        for position in self.positions(key) {
            self.rows[row][position / 8] |= 1 << (position % 8);
        }
        self.current_row_keys = self.current_row_keys.wrapping_add(1);
    }

    /// Java's `InternalDynamicBloomFilter#getActiveStandardBF` plus the row it adds.
    fn active_row(&mut self) -> usize {
        if self.reached_max {
            let row = self.next_round_robin_row % self.rows.len();
            self.next_round_robin_row += 1;
            return row;
        }
        if self.current_row_keys < self.num_entries {
            return self.rows.len() - 1;
        }
        if (self.rows.len() as i64) * (self.num_entries as i64) < self.max_entries as i64 {
            self.rows.push(Self::empty_row(self.vector_size));
            self.current_row_keys = 0;
            self.rows.len() - 1
        } else {
            self.reached_max = true;
            0
        }
    }

    /// Java's `HashFunction#hash`: each murmur hash seeds the next.
    fn positions(&self, key: &[u8]) -> Vec<usize> {
        let mut seed = 0;
        (0..self.num_hashes)
            .map(|_| {
                seed = murmur_hash(key, seed);
                (seed % self.vector_size).unsigned_abs() as usize
            })
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn might_contain(&self, key: &[u8]) -> bool {
        let positions = self.positions(key);
        self.rows.iter().any(|row| {
            positions
                .iter()
                .all(|position| row[position / 8] & (1 << (position % 8)) != 0)
        })
    }

    /// Java's type code footer value, written only for the dynamic filter.
    pub(crate) fn type_code(&self) -> Option<&'static str> {
        match self.filter_type {
            BloomFilterType::Simple => None,
            BloomFilterType::DynamicV0 => Some("DYNAMIC_V0"),
        }
    }

    /// Base64 of the filter's Java `DataOutput` form (big-endian).
    pub(crate) fn serialize_to_string(&self) -> String {
        let mut out = Vec::new();
        self.write_header(&mut out);
        match self.filter_type {
            BloomFilterType::Simple => out.extend_from_slice(&self.rows[0]),
            BloomFilterType::DynamicV0 => {
                out.extend_from_slice(&self.num_entries.to_be_bytes());
                out.extend_from_slice(&self.current_row_keys.to_be_bytes());
                out.extend_from_slice(&(self.rows.len() as i32).to_be_bytes());
                for row in &self.rows {
                    self.write_header(&mut out);
                    out.extend_from_slice(row);
                }
            }
        }
        STANDARD.encode(out)
    }

    fn write_header(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&FILTER_VERSION.to_be_bytes());
        out.extend_from_slice(&self.num_hashes.to_be_bytes());
        out.push(MURMUR_HASH_TYPE);
        out.extend_from_slice(&self.vector_size.to_be_bytes());
    }
}

/// Java Hudi's `MurmurHash#hash` (MurmurHash2), whose tail bytes are sign-extended.
fn murmur_hash(data: &[u8], seed: i32) -> i32 {
    const M: i32 = 0x5bd1_e995;
    let mut h = seed ^ data.len() as i32;
    let mut chunks = data.chunks_exact(4);
    for chunk in &mut chunks {
        let mut k = i32::from_le_bytes(chunk.try_into().unwrap());
        k = k.wrapping_mul(M);
        k ^= ((k as u32) >> 24) as i32;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
    }
    let tail = chunks.remainder();
    if !tail.is_empty() {
        // The first tail byte goes highest, unlike reference MurmurHash2.
        for (index, byte) in tail.iter().enumerate() {
            h ^= (*byte as i8 as i32) << (8 * (tail.len() - 1 - index));
        }
        h = h.wrapping_mul(M);
    }
    h ^= ((h as u32) >> 13) as i32;
    h = h.wrapping_mul(M);
    h ^= ((h as u32) >> 15) as i32;
    h
}

/// Java `String.compareTo` order (UTF-16 code units), which Java's bloom index
/// uses to test a key against a file's range; UTF-8 byte order differs above
/// the BMP and would let the index skip a file holding the key.
fn java_cmp(a: &str, b: &str) -> std::cmp::Ordering {
    a.encode_utf16().cmp(b.encode_utf16())
}

/// The footer entries Java's `HoodieBloomFilterWriteSupport` writes for the
/// record keys of one base file.
pub(crate) struct RecordKeyFooter {
    filter: BloomFilter,
    min_key: Option<String>,
    max_key: Option<String>,
}

impl RecordKeyFooter {
    pub(crate) fn new(config: &BloomFilterConfig) -> Self {
        Self {
            filter: BloomFilter::new(config),
            min_key: None,
            max_key: None,
        }
    }

    pub(crate) fn add(&mut self, key: &str) -> Result<()> {
        if key.is_empty() {
            return Err(CoreError::Write(
                "a record key added to the bloom filter must not be empty".to_string(),
            ));
        }
        self.filter.add(key.as_bytes());
        if self
            .min_key
            .as_deref()
            .is_none_or(|min| java_cmp(key, min).is_lt())
        {
            self.min_key = Some(key.to_string());
        }
        if self
            .max_key
            .as_deref()
            .is_none_or(|max| java_cmp(key, max).is_gt())
        {
            self.max_key = Some(key.to_string());
        }
        Ok(())
    }

    pub(crate) fn finish(self) -> Vec<(&'static str, String)> {
        let mut entries = vec![(BLOOM_FILTER_FOOTER_KEY, self.filter.serialize_to_string())];
        if let Some(type_code) = self.filter.type_code() {
            entries.push((BLOOM_FILTER_TYPE_CODE_FOOTER_KEY, type_code.to_string()));
        }
        if let (Some(min), Some(max)) = (self.min_key, self.max_key) {
            entries.push((MIN_RECORD_KEY_FOOTER_KEY, min));
            entries.push((MAX_RECORD_KEY_FOOTER_KEY, max));
        }
        entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use md5::{Digest, Md5};

    /// The keys the Java goldens below were generated from.
    fn keys(n: usize) -> Vec<String> {
        let mut keys: Vec<String> = [
            "a",
            "ab",
            "abc",
            "abcd",
            "abcde",
            "é",
            "日本",
            "id:ÿ/\u{80}",
            "2026/10/28",
            "key-😀",
        ]
        .map(String::from)
        .to_vec();
        keys.extend((0..n.saturating_sub(keys.len())).map(|i| format!("key{i}")));
        keys.truncate(n);
        keys
    }

    fn filter_with(config: &BloomFilterConfig, keys: &[String]) -> BloomFilter {
        let mut filter = BloomFilter::new(config);
        keys.iter().for_each(|key| filter.add(key.as_bytes()));
        filter
    }

    fn small(filter_type: BloomFilterType) -> BloomFilterConfig {
        BloomFilterConfig {
            filter_type,
            num_entries: 10,
            fpp: 0.01,
            max_entries: 30,
        }
    }

    /// Java's `MurmurHash.getInstance().hash(bytes, seed)` for every tail length
    /// and for bytes with the high bit set.
    #[test]
    fn test_murmur_hash_matches_java() {
        let cases: [(&str, [i32; 3]); 8] = [
            ("a", [-1838653602, 713507990, -579949966]),
            ("ab", [1525573013, -1797532438, -1822173170]),
            ("abc", [1775488903, -1465591341, -2108309701]),
            ("abcd", [646393889, 1555318719, 758563494]),
            ("abcde", [1594468574, -392878951, 232266555]),
            ("é", [-384040333, -1125067414, 1887701134]),
            ("日本", [659168028, -727673871, -762233106]),
            ("id:ÿ/\u{80}", [1086828780, 1747206571, 276313369]),
        ];
        for (key, expected) in cases {
            let actual = [0, -1, 12345].map(|seed| murmur_hash(key.as_bytes(), seed));
            assert_eq!(actual, expected, "{key}");
        }
    }

    /// 45 keys into rows of 10 capped at 30 entries: the filter adds two rows,
    /// then spreads the last 15 keys round-robin.
    #[test]
    fn test_dynamic_filter_serializes_as_java() {
        let filter = filter_with(&small(BloomFilterType::DynamicV0), &keys(45));
        assert_eq!(
            filter.serialize_to_string(),
            "/////wAAAAcBAAAAYAAAAAoAAAAZAAAAA/////8AAAAHAQAAAGC8fX7BbvPs0+eG9hz/////AAAABwEAAABg\
             +l33feftPuy+f/4L/////wAAAAcBAAAAYF/6d/+vL93L4fPL2Q=="
        );
        assert_eq!(filter.type_code(), Some("DYNAMIC_V0"));
    }

    #[test]
    fn test_simple_filter_serializes_as_java() {
        let filter = filter_with(&small(BloomFilterType::Simple), &keys(12));
        assert_eq!(
            filter.serialize_to_string(),
            "/////wAAAAcBAAAAYPh9R1Fm82zY55e2GA=="
        );
        assert_eq!(filter.type_code(), None);
    }

    /// Java's default sizing is too large to inline; pin its digest instead.
    #[test]
    fn test_default_filter_serializes_as_java() {
        let filter = filter_with(&BloomFilterConfig::default(), &keys(13));
        let serialized = filter.serialize_to_string();
        assert_eq!(serialized.len(), 431_380);
        assert_eq!(
            format!("{:x}", Md5::digest(serialized.as_bytes())),
            "1a113a32176633447cec98a2476e26ac"
        );
        assert!(
            keys(13)
                .iter()
                .all(|key| filter.might_contain(key.as_bytes()))
        );
        assert!(!(0..100).any(|i| filter.might_contain(format!("absent{i}").as_bytes())));
    }

    #[test]
    fn test_dynamic_filter_contains_keys_in_every_row() {
        let keys = keys(45);
        let filter = filter_with(&small(BloomFilterType::DynamicV0), &keys);
        assert_eq!(filter.rows.len(), 3);
        assert!(keys.iter().all(|key| filter.might_contain(key.as_bytes())));
    }

    #[test]
    fn test_footer_holds_filter_type_code_and_key_range() {
        let mut footer = RecordKeyFooter::new(&BloomFilterConfig::default());
        for key in ["k2", "k10", "é", "k1"] {
            footer.add(key).unwrap();
        }
        let entries = footer.finish();
        let keys: Vec<&str> = entries.iter().map(|(key, _)| *key).collect();
        assert_eq!(
            keys,
            [
                BLOOM_FILTER_FOOTER_KEY,
                BLOOM_FILTER_TYPE_CODE_FOOTER_KEY,
                MIN_RECORD_KEY_FOOTER_KEY,
                MAX_RECORD_KEY_FOOTER_KEY
            ]
        );
        assert_eq!(entries[1].1, "DYNAMIC_V0");
        assert_eq!((entries[2].1.as_str(), entries[3].1.as_str()), ("k1", "é"));
    }

    #[test]
    fn test_footer_key_range_uses_java_string_order() {
        let mut footer = RecordKeyFooter::new(&BloomFilterConfig::default());
        // U+FF21 sorts after U+1F600 in UTF-16 and before it in UTF-8.
        for key in ["\u{FF21}", "\u{1F600}"] {
            footer.add(key).unwrap();
        }
        let entries = footer.finish();
        assert_eq!(
            (entries[2].1.as_str(), entries[3].1.as_str()),
            ("\u{1F600}", "\u{FF21}")
        );
    }

    #[test]
    fn test_footer_without_keys_has_no_key_range() {
        let entries = RecordKeyFooter::new(&small(BloomFilterType::Simple)).finish();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, BLOOM_FILTER_FOOTER_KEY);
    }

    #[test]
    fn test_footer_rejects_an_empty_key() {
        let mut footer = RecordKeyFooter::new(&BloomFilterConfig::default());
        assert!(matches!(footer.add(""), Err(CoreError::Write(_))));
    }

    #[test]
    fn test_filter_type_parses_case_insensitively() {
        assert_eq!(
            BloomFilterType::parse("dynamic_v0"),
            Some(BloomFilterType::DynamicV0)
        );
        assert_eq!(
            BloomFilterType::parse("SIMPLE"),
            Some(BloomFilterType::Simple)
        );
        assert_eq!(BloomFilterType::parse("DYNAMIC"), None);
    }
}
