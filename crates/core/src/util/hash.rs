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
//! Hash utilities for Hudi metadata table key generation.
//!
//! The metadata table uses composite keys for column stats lookups:
//! - ColumnIndexId: Base64(XXHash64(column_name, seed=0xdabadaba)) = 12 chars
//! - PartitionIndexId: Base64(XXHash64(partition_path, seed=0xdabadaba)) = 12 chars
//! - FileIndexId: Base64(MD5(file_name)) = 24 chars
//!
//! Key format: COLUMN_HASH + PARTITION_HASH + FILE_HASH = 48 chars total

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use md5::{Digest, Md5};

use crate::metadata::NON_PARTITIONED_NAME;

/// The seed used for XXHash64 in Hudi metadata table key generation.
const XXHASH_SEED: u64 = 0xdaba_daba;

/// Total length of a column stats key (12 + 12 + 24 = 48 chars with standard Base64 padding).
pub const COLUMN_STATS_KEY_LENGTH: usize = 48;

/// Column index ID - 12 character Base64-encoded XXHash64 of column name.
///
/// Used as the first component of column stats keys in the metadata table.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnIndexId(String);

impl ColumnIndexId {
    /// Create a new ColumnIndexId from a column name.
    ///
    /// Computes XXHash64 of the column name with seed 0xdabadaba and encodes as Base64.
    pub fn new(column_name: &str) -> Self {
        let hash = xxhash_rust::xxh64::xxh64(column_name.as_bytes(), XXHASH_SEED);
        let encoded = STANDARD.encode(hash.to_be_bytes());
        Self(encoded)
    }

    /// Get the Base64-encoded hash as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Partition index ID - 12 character Base64-encoded XXHash64 of partition path.
///
/// Used as the second component of column stats keys in the metadata table.
/// For non-partitioned tables, the partition identifier is "." (not "").
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PartitionIndexId(String);

impl PartitionIndexId {
    /// Create a new PartitionIndexId from a partition path.
    ///
    /// Computes XXHash64 of the partition path with seed 0xdabadaba and encodes as Base64.
    pub fn new(partition_path: &str) -> Self {
        let normalized = if partition_path.is_empty() {
            NON_PARTITIONED_NAME
        } else {
            partition_path
        };
        let hash = xxhash_rust::xxh64::xxh64(normalized.as_bytes(), XXHASH_SEED);
        let encoded = STANDARD.encode(hash.to_be_bytes());
        Self(encoded)
    }

    /// Get the Base64-encoded hash as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// File index ID - 24 character Base64-encoded MD5 of file name.
///
/// Used as the third component of column stats keys in the metadata table.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FileIndexId(String);

impl FileIndexId {
    /// Create a new FileIndexId from a file name.
    ///
    /// Computes MD5 of the file name and encodes as Base64.
    pub fn new(file_name: &str) -> Self {
        let mut hasher = Md5::new();
        hasher.update(file_name.as_bytes());
        let hash = hasher.finalize();
        let encoded = STANDARD.encode(hash);
        Self(encoded)
    }

    /// Get the Base64-encoded hash as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Generate a column stats key for a specific column, partition, and file.
///
/// The key format is: COLUMN_HASH + PARTITION_HASH + FILE_HASH (48 chars total)
///
/// # Arguments
/// * `column_name` - The column name
/// * `partition_path` - The partition path (empty string is normalized to "." for non-partitioned tables)
/// * `file_name` - The file name (just the filename, not the full path)
pub fn get_column_stats_key(column_name: &str, partition_path: &str, file_name: &str) -> String {
    let col_id = ColumnIndexId::new(column_name);
    let part_id = PartitionIndexId::new(partition_path);
    let file_id = FileIndexId::new(file_name);
    format!(
        "{}{}{}",
        col_id.as_str(),
        part_id.as_str(),
        file_id.as_str()
    )
}

/// Generate a prefix for looking up all column stats for a specific column.
///
/// Returns: COLUMN_HASH (12 chars)
///
/// This can be used with HFile prefix lookups to find all stats for a column.
pub fn get_column_stats_prefix_for_column(column_name: &str) -> String {
    ColumnIndexId::new(column_name).0
}

/// Generate a prefix for looking up all column stats for a column in a partition.
///
/// Returns: COLUMN_HASH + PARTITION_HASH (24 chars)
///
/// This can be used with HFile prefix lookups to find all stats for a column
/// within a specific partition.
pub fn get_column_stats_prefix_for_column_and_partition(
    column_name: &str,
    partition_path: &str,
) -> String {
    let col_id = ColumnIndexId::new(column_name);
    let part_id = PartitionIndexId::new(partition_path);
    format!("{}{}", col_id.as_str(), part_id.as_str())
}

/// Generate a partition stats key for a column and partition.
///
/// The key format is: COLUMN_HASH + PARTITION_HASH (24 chars)
///
/// Partition stats use the same key structure as the column+partition prefix
/// since they aggregate stats at the partition level rather than file level.
pub fn get_partition_stats_key(column_name: &str, partition_path: &str) -> String {
    get_column_stats_prefix_for_column_and_partition(column_name, partition_path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_component_lengths() {
        // ColumnIndexId: 64-bit XXHash -> 8 bytes -> 12 chars Base64 (with padding)
        assert_eq!(ColumnIndexId::new("test_column").as_str().len(), 12);
        assert_eq!(ColumnIndexId::new("id").as_str().len(), 12);
        assert_eq!(ColumnIndexId::new("").as_str().len(), 12);

        // PartitionIndexId: 64-bit XXHash -> 8 bytes -> 12 chars Base64
        assert_eq!(PartitionIndexId::new("city=chennai").as_str().len(), 12);
        assert_eq!(PartitionIndexId::new("").as_str().len(), 12);
        assert_eq!(PartitionIndexId::new(".").as_str().len(), 12);

        // FileIndexId: 128-bit MD5 -> 16 bytes -> 24 chars Base64
        assert_eq!(
            FileIndexId::new("abc-0_0-123-456_20231214.parquet")
                .as_str()
                .len(),
            24
        );
    }

    #[test]
    fn test_column_stats_key_total_length() {
        // Total: 12 + 12 + 24 = 48 chars
        assert_eq!(
            get_column_stats_key("id", "city=chennai", "data.parquet").len(),
            COLUMN_STATS_KEY_LENGTH
        );
        assert_eq!(
            get_column_stats_key("column_name", "", "file.parquet").len(),
            COLUMN_STATS_KEY_LENGTH
        );
    }

    #[test]
    fn test_partition_path_normalization() {
        // Empty partition path should be normalized to "." (NON_PARTITIONED_NAME)
        let empty_partition = PartitionIndexId::new("");
        let dot_partition = PartitionIndexId::new(".");
        assert_eq!(
            empty_partition.as_str(),
            dot_partition.as_str(),
            "Empty partition path should be normalized to '.'"
        );

        // Non-empty partition paths should not be modified
        let normal_partition = PartitionIndexId::new("city=chennai");
        assert_ne!(normal_partition.as_str(), dot_partition.as_str());
    }

    #[test]
    fn test_column_stats_key_with_non_partitioned_table() {
        // For non-partitioned tables, using "" or "." should produce the same key
        let key_empty = get_column_stats_key("id", "", "file.parquet");
        let key_dot = get_column_stats_key("id", ".", "file.parquet");
        assert_eq!(
            key_empty, key_dot,
            "Non-partitioned table keys should match"
        );
    }

    #[test]
    fn test_different_inputs_produce_different_hashes() {
        // Different column names
        let col1 = ColumnIndexId::new("column_a");
        let col2 = ColumnIndexId::new("column_b");
        assert_ne!(col1.as_str(), col2.as_str());

        // Different partition paths
        let part1 = PartitionIndexId::new("city=chennai");
        let part2 = PartitionIndexId::new("city=san_francisco");
        assert_ne!(part1.as_str(), part2.as_str());

        // Different file names
        let file1 = FileIndexId::new("file1.parquet");
        let file2 = FileIndexId::new("file2.parquet");
        assert_ne!(file1.as_str(), file2.as_str());
    }

    #[test]
    fn test_same_inputs_produce_same_hashes() {
        // Hash functions should be deterministic
        assert_eq!(
            ColumnIndexId::new("test").as_str(),
            ColumnIndexId::new("test").as_str()
        );
        assert_eq!(
            PartitionIndexId::new("part").as_str(),
            PartitionIndexId::new("part").as_str()
        );
        assert_eq!(
            FileIndexId::new("file.parquet").as_str(),
            FileIndexId::new("file.parquet").as_str()
        );
    }

    #[test]
    fn test_column_prefix_relationship() {
        let column_prefix = get_column_stats_prefix_for_column("id");
        let column_partition_prefix =
            get_column_stats_prefix_for_column_and_partition("id", "city=chennai");
        let full_key = get_column_stats_key("id", "city=chennai", "file.parquet");

        // Column prefix should be start of column+partition prefix
        assert!(column_partition_prefix.starts_with(&column_prefix));

        // Column+partition prefix should be start of full key
        assert!(full_key.starts_with(&column_partition_prefix));

        // Prefix lengths
        assert_eq!(column_prefix.len(), 12);
        assert_eq!(column_partition_prefix.len(), 24);
    }

    #[test]
    fn test_partition_stats_key_format() {
        // Partition stats key = column hash + partition hash (24 chars)
        let partition_stats_key = get_partition_stats_key("id", "city=chennai");
        assert_eq!(partition_stats_key.len(), 24);

        // Should equal column+partition prefix
        let prefix = get_column_stats_prefix_for_column_and_partition("id", "city=chennai");
        assert_eq!(partition_stats_key, prefix);
    }

    #[test]
    fn test_key_component_ordering() {
        // Verify the key structure: COLUMN + PARTITION + FILE
        let column = ColumnIndexId::new("id");
        let partition = PartitionIndexId::new("city=chennai");
        let file = FileIndexId::new("data.parquet");

        let full_key = get_column_stats_key("id", "city=chennai", "data.parquet");

        // Full key should be concatenation of components in correct order
        let expected = format!("{}{}{}", column.as_str(), partition.as_str(), file.as_str());
        assert_eq!(full_key, expected);
    }
}
