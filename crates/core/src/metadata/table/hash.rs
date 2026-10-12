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
//! Index-key hashing matching Java `org.apache.hudi.common.util.hash.HashID`.

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use md5::{Digest, Md5};
use xxhash_rust::xxh64::xxh64;

/// Java `HashID.HASH_SEED`.
const HASH_SEED: u64 = 0xdabadaba;

/// Empty data-table partition path maps to MDT files-partition key `"."`.
pub fn partition_identifier(partition_path: &str) -> &str {
    if partition_path.is_empty() {
        "."
    } else {
        partition_path
    }
}

/// XXHash64 (seed `0xdabadaba`) as big-endian bytes — Java `HashID` BITS_64.
pub fn xxhash64_bytes(message: &str) -> [u8; 8] {
    xxh64(message.as_bytes(), HASH_SEED).to_be_bytes()
}

/// MD5 digest — Java `HashID` BITS_128.
pub fn md5_bytes(message: &str) -> [u8; 16] {
    let digest = Md5::digest(message.as_bytes());
    let mut out = [0u8; 16];
    out.copy_from_slice(&digest);
    out
}

fn b64(bytes: &[u8]) -> String {
    BASE64.encode(bytes)
}

/// Java `ColumnIndexID.asBase64EncodedString`.
pub fn column_index_id_b64(column_name: &str) -> String {
    b64(&xxhash64_bytes(column_name))
}

/// Java `PartitionIndexID.asBase64EncodedString`.
pub fn partition_index_id_b64(partition_path: &str) -> String {
    b64(&xxhash64_bytes(partition_identifier(partition_path)))
}

/// Java `FileIndexID.asBase64EncodedString` (MD5 of file basename).
pub fn file_index_id_b64(file_name: &str) -> String {
    b64(&md5_bytes(file_name))
}

/// Java `HoodieMetadataPayload.getColumnStatsIndexKey`.
pub fn column_stats_index_key(partition_path: &str, file_name: &str, column_name: &str) -> String {
    format!(
        "{}{}{}",
        column_index_id_b64(column_name),
        partition_index_id_b64(partition_path),
        file_index_id_b64(file_name)
    )
}

/// Java `HoodieTableMetadataUtil.getPartitionStatsIndexKey(partition, column)`.
pub fn partition_stats_index_key(partition_path: &str, column_name: &str) -> String {
    format!(
        "{}{}",
        column_index_id_b64(column_name),
        partition_index_id_b64(partition_path)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex_upper(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02X}")).collect()
    }

    #[test]
    fn xxhash64_matches_java_golden_hex() {
        // From Java TestHashID.testHashValues BITS_64.
        assert_eq!(hex_upper(&xxhash64_bytes("Hudi")), "F7727B9A28379071");
        assert_eq!(hex_upper(&xxhash64_bytes("Data lake")), "52BC72D592EBCAE5");
        assert_eq!(hex_upper(&xxhash64_bytes("Col1")), "22FB1DD2F4784D31");
    }

    #[test]
    fn md5_matches_java_golden_hex() {
        assert_eq!(
            hex_upper(&md5_bytes("Hudi")),
            "09DAB749F255311C1C9EF6DD7B790170"
        );
        assert_eq!(
            hex_upper(&md5_bytes("A")),
            "7FC56270E7A70FA81A5935B72EACBE29"
        );
    }

    #[test]
    fn column_stats_key_concat_order() {
        let key = column_stats_index_key("city=sf", "a.parquet", "id");
        assert!(key.starts_with(&column_index_id_b64("id")));
        assert!(key.contains(&partition_index_id_b64("city=sf")));
        assert!(key.ends_with(&file_index_id_b64("a.parquet")));
    }

    #[test]
    fn empty_partition_uses_dot_identifier() {
        assert_eq!(partition_identifier(""), ".");
        assert_eq!(partition_index_id_b64(""), partition_index_id_b64("."));
    }
}
