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
//!
//! This matches the Java implementation in:
//! - `org.apache.hudi.metadata.HoodieMetadataPayload`
//! - `org.apache.hudi.common.util.hash.ColumnIndexID`
//! - `org.apache.hudi.common.util.hash.HashID`

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use md5::{Digest, Md5};

/// The seed used for XXHash64 in Hudi Java implementation.
/// From org.apache.hudi.common.util.hash.HashID: private static final int SEED = 0xdabadaba
const XXHASH_SEED: u64 = 0xdaba_daba;

/// Total length of a column stats key (12 + 12 + 24 = 48 chars with standard Base64 padding).
pub const COLUMN_STATS_KEY_LENGTH: usize = 48;

/// Column index ID - 12 character Base64-encoded XXHash64 of column name.
///
/// Used as the first component of column stats keys in the metadata table.
/// Uses XXHash64 with seed 0xdabadaba to match Java implementation.
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
/// For non-partitioned tables, use an empty string as the partition path.
/// Uses XXHash64 with seed 0xdabadaba to match Java implementation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PartitionIndexId(String);

impl PartitionIndexId {
    /// Create a new PartitionIndexId from a partition path.
    ///
    /// Computes XXHash64 of the partition path with seed 0xdabadaba and encodes as Base64.
    /// For non-partitioned tables, pass an empty string "".
    pub fn new(partition_path: &str) -> Self {
        let hash = xxhash_rust::xxh64::xxh64(partition_path.as_bytes(), XXHASH_SEED);
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
/// Uses MD5 to match Java implementation (128-bit hash).
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
/// The key format is: COLUMN_HASH + PARTITION_HASH + FILE_HASH (44 chars total)
///
/// This matches the Java implementation in `HoodieMetadataPayload.getColumnStatsIndexKey()`.
///
/// # Arguments
/// * `column_name` - The column name
/// * `partition_path` - The partition path (empty string for non-partitioned tables)
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
/// Returns: COLUMN_HASH (11 chars)
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
    fn test_column_stats_key_format() {
        // Verify key component lengths: column(12) + partition(12) + file(24) = 48 chars
        assert_eq!(ColumnIndexId::new("test_column").as_str().len(), 12);
        assert_eq!(PartitionIndexId::new("city=chennai").as_str().len(), 12);
        assert_eq!(PartitionIndexId::new("").as_str().len(), 12); // empty partition
        assert_eq!(
            FileIndexId::new("abc-0_0-123-456_20231214.parquet")
                .as_str()
                .len(),
            24
        );
        assert_eq!(
            get_column_stats_key("id", "city=chennai", "data.parquet").len(),
            48
        );
    }

    #[test]
    fn test_column_stats_prefix_for_column_and_partition() {
        let prefix = get_column_stats_prefix_for_column_and_partition("id", "part1");
        let full_key = get_column_stats_key("id", "part1", "file1.parquet");
        assert!(full_key.starts_with(&prefix));
        // partition_stats key equals column+partition prefix
        assert_eq!(get_partition_stats_key("id", "part1"), prefix);
    }
}
