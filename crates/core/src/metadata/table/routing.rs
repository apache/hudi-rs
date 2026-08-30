// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Which metadata file group holds a record key.
//!
//! A metadata partition is sharded across file groups and the writer chose the
//! shard by hashing the record key, so a reader has to compute the same shard
//! from the same hash. Getting the hash wrong does not fail loudly: the read
//! goes to a file group that exists, finds nothing, and returns an empty result.

/// Java's `String.hashCode`, which is what Hudi hashes metadata keys with.
///
/// Two details are load-bearing and neither is visible from the formula alone.
///
/// It iterates **UTF-16 code units**, not Unicode scalars, because Java's
/// `charAt` does. A character outside the basic multilingual plane therefore
/// hashes as its surrogate pair: `"\u{1F600}"` hashes to 1772899 over its
/// surrogates and to 128512 over the scalar. Iterating Rust `char`s would route
/// such a key to a different file group than the writer used.
///
/// And it wraps as a signed 32-bit integer. Computing in `i64` or `u64` and
/// truncating later gives different values once the hash exceeds the range.
fn java_string_hash(key: &str) -> i32 {
    key.encode_utf16()
        .fold(0i32, |h, unit| h.wrapping_mul(31).wrapping_add(unit as i32))
}

/// The file group index a key belongs to, out of `num_file_groups`.
///
/// Mirrors `HoodieTableMetadataUtil.mapRecordKeyToFileGroupIndex`, including its
/// doubled `abs`. The outer one is not redundant: `i32::MIN.abs()` is `i32::MIN`
/// in Java's semantics, so the inner `abs` alone leaves a negative value and the
/// modulo of it is negative too. `"polygenelubricants"` hashes to exactly
/// `i32::MIN`, so that path has a real key rather than a synthetic hash.
///
/// Returns 0 when there is one file group, without hashing, as Java does.
pub(crate) fn file_group_index(key: &str, num_file_groups: usize) -> usize {
    if num_file_groups <= 1 {
        return 0;
    }
    let hash = java_string_hash(key);
    // `wrapping_abs` reproduces Java's `Math.abs`, which returns `i32::MIN` for
    // `i32::MIN` rather than panicking or widening.
    let folded = hash.wrapping_abs() % (num_file_groups as i32);
    folded.wrapping_abs() as usize
}

/// The slices a key lookup has to open, in file-group order.
///
/// An empty `keys` means a full scan, which opens every slice. Otherwise each
/// key routes to exactly one shard, so only the distinct shards among the keys
/// are opened -- the difference between one read and ten on a record index.
///
/// A **prefix** lookup must not use this. Sharding is by the full key, so a
/// prefix says nothing about which shard a match lives in; Java says the same at
/// `getRecordsByKeyPrefixes:239`. Such a caller passes no keys and scans.
///
/// Slices are ordered by file id before indexing, because `file_group_index`
/// returns a position among the shards and the listing order is the storage's,
/// not the shard numbering's. Hudi embeds the shard number in the file id
/// (`record-index-0003-0`), so a lexicographic sort recovers it.
pub(crate) fn slices_for_keys<'a>(
    slices: &'a [crate::file_group::file_slice::FileSlice],
    keys: &[&str],
) -> Vec<&'a crate::file_group::file_slice::FileSlice> {
    if keys.is_empty() || slices.len() <= 1 {
        return slices.iter().collect();
    }
    let mut ordered: Vec<&crate::file_group::file_slice::FileSlice> = slices.iter().collect();
    ordered.sort_by(|a, b| a.file_id().cmp(b.file_id()));

    let mut wanted: Vec<usize> = keys
        .iter()
        .map(|k| file_group_index(k, ordered.len()))
        .collect();
    wanted.sort_unstable();
    wanted.dedup();
    wanted.into_iter().map(|i| ordered[i]).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Java's published `hashCode` values, so the port is checked against the
    /// specification rather than against itself.
    #[test]
    fn the_hash_matches_java_string_hashcode() {
        assert_eq!(java_string_hash(""), 0);
        assert_eq!(java_string_hash("a"), 97);
        assert_eq!(java_string_hash("hello"), 99_162_322);
        // Known to hash to exactly i32::MIN; the doubled abs exists for it.
        assert_eq!(java_string_hash("polygenelubricants"), i32::MIN);
    }

    /// A character outside the basic multilingual plane hashes over its UTF-16
    /// surrogate pair, not over its scalar value.
    ///
    /// This is the trap that makes a wrong port silent: 31*0xD83D + 0xDE00 is
    /// 1772899, while the scalar 0x1F600 is 128512, and both are plausible
    /// numbers that route to a file group that exists.
    #[test]
    fn a_non_bmp_character_hashes_over_its_surrogates() {
        assert_eq!(java_string_hash("\u{1F600}"), 1_772_899);
        assert_ne!(java_string_hash("\u{1F600}"), 0x1F600);
        assert_eq!(java_string_hash("\u{1F600}x"), 54_959_989);
    }

    /// Keys route to the same file group Java would send them to. Values from an
    /// independent implementation of Java's algorithm, not from this code.
    #[test]
    fn keys_route_where_java_routes_them() {
        for (key, n, expected) in [
            ("a", 10, 7),
            ("a", 3, 1),
            ("hello", 10, 2),
            ("__all_partitions__", 10, 1),
            ("__all_partitions__", 3, 2),
            // A negative hash: -1106540010.
            ("city=chennai", 10, 0),
            ("\u{1F600}", 10, 9),
            // i32::MIN. `abs(MIN)` is `MIN`, so the modulo is negative and the
            // outer abs is what produces the answer: abs(MIN % 10) = abs(-8) = 8.
            // A Python-style modulo, which returns a non-negative remainder,
            // gives 2 here instead — see the note below the test.
            ("polygenelubricants", 10, 8),
            ("polygenelubricants", 3, 2),
        ] {
            assert_eq!(
                file_group_index(key, n),
                expected,
                "key {key:?} over {n} file groups"
            );
        }
    }

    // The two `polygenelubricants` rows above were wrong on first writing, and the
    // reason is worth keeping: they came from an independent implementation of
    // Java's algorithm written in Python, and Python's `%` returns a
    // non-negative remainder where Java's and Rust's take the dividend's sign.
    // For every other key the inner `abs` makes the dividend positive and the two
    // languages agree, so the one input the oracle existed to check was the one
    // input it got wrong. Rust's `%` matches Java's, and `wrapping_abs` matches
    // `Math.abs`, so the port needs no adjustment — only the expectations did.

    /// One file group short-circuits, and the index is always in range.
    #[test]
    fn the_index_is_always_in_range() {
        assert_eq!(file_group_index("anything", 1), 0);
        assert_eq!(file_group_index("anything", 0), 0);
        for n in 1..=64usize {
            for key in ["", "a", "polygenelubricants", "city=sao_paulo", "\u{1F600}"] {
                assert!(
                    file_group_index(key, n) < n,
                    "key {key:?} over {n} file groups landed out of range"
                );
            }
        }
    }

    /// Builds `n` slices whose file ids sort into shard order, so a test can ask
    /// which ones a key set selects.
    fn shards(n: usize) -> Vec<crate::file_group::file_slice::FileSlice> {
        (0..n)
            .map(|i| {
                crate::file_group::file_slice::FileSlice::new_log_only(
                    format!("record-index-{i:04}-0"),
                    "20250101000000000".to_string(),
                    "record_index".to_string(),
                )
            })
            .collect()
    }

    /// A full-key lookup opens one slice per distinct shard among the keys --
    /// measured as slices selected, not inferred from rows returned.
    #[test]
    fn a_key_lookup_opens_only_the_shards_its_keys_route_to() {
        let slices = shards(10);
        let key = "some-record-key";
        let expected = file_group_index(key, 10);

        let picked = slices_for_keys(&slices, &[key]);
        assert_eq!(picked.len(), 1, "one key routes to exactly one shard");
        assert_eq!(
            picked[0].file_id(),
            slices[expected].file_id(),
            "and it must be the shard the hash names, not merely some shard"
        );
    }

    /// Keys landing on different shards open each of them, once.
    #[test]
    fn distinct_shards_are_opened_once_each() {
        let slices = shards(10);
        // Chosen by asking the routing function, so the test does not encode a
        // hash it would then be checking against itself.
        let mut a = None;
        let mut b = None;
        for i in 0..500 {
            let k = format!("k{i}");
            match file_group_index(&k, 10) {
                idx if a.is_none() => a = Some((k, idx)),
                idx if b.is_none() && Some(idx) != a.as_ref().map(|(_, i)| *i) => {
                    b = Some((k, idx))
                }
                _ => {}
            }
            if a.is_some() && b.is_some() {
                break;
            }
        }
        let (ka, _) = a.expect("a key");
        let (kb, _) = b.expect("a key on a different shard");

        let picked = slices_for_keys(&slices, &[ka.as_str(), kb.as_str()]);
        assert_eq!(picked.len(), 2, "two shards, two slices opened");

        // The same key twice is still one slice: shards are deduplicated.
        let repeated = slices_for_keys(&slices, &[ka.as_str(), ka.as_str()]);
        assert_eq!(
            repeated.len(),
            1,
            "a repeated key must not open its shard twice"
        );
    }

    /// No keys means a full scan: every slice, because a prefix or listing read
    /// cannot know which shard holds a match.
    #[test]
    fn an_empty_key_set_opens_every_slice() {
        let slices = shards(10);
        assert_eq!(
            slices_for_keys(&slices, &[]).len(),
            10,
            "a scan must open every shard"
        );
    }

    /// Selection follows shard order, not listing order. Storage may list file
    /// groups in any order; indexing an unsorted list sends a key to the wrong
    /// shard, which returns no rows rather than an error.
    #[test]
    fn selection_does_not_depend_on_listing_order() {
        let mut forward = shards(10);
        let key = "some-record-key";
        let from_forward = slices_for_keys(&forward, &[key])[0].file_id().to_string();

        forward.reverse();
        let from_reversed = slices_for_keys(&forward, &[key])[0].file_id().to_string();

        assert_eq!(
            from_forward, from_reversed,
            "the shard a key selects must not change with listing order"
        );
    }
}
