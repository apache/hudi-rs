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

/// Java `HashID.HASH_SEED`. It is the `int` `0xdabadaba`, which Java
/// sign-extends when passing it to `XXHash64.hash(..., long seed)`.
const HASH_SEED: u64 = 0xdabadaba_u32 as i32 as i64 as u64;

/// Empty data-table partition path maps to MDT files-partition key `"."`.
pub fn partition_identifier(partition_path: &str) -> &str {
    if partition_path.is_empty() {
        "."
    } else {
        partition_path
    }
}

/// XXHash64 (seed [`HASH_SEED`]) as big-endian bytes, Java `HashID` BITS_64.
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
    fn xxhash64_matches_java_runtime() {
        // HashID.hash(_, BITS_64) as computed by the Hudi 1.1.1 Spark bundle.
        assert_eq!(hex_upper(&xxhash64_bytes("Hudi")), "B9A47174FD473C0E");
        assert_eq!(hex_upper(&xxhash64_bytes("cnt")), "B22F51D19144F1D5");
        assert_eq!(hex_upper(&xxhash64_bytes("part=p1")), "B6CB202E0F44EE9D");
    }

    #[test]
    fn stats_keys_match_spark_written_mdt() {
        // Keys read back from a column_stats / partition_stats MDT written by
        // Spark with the Hudi 1.1.1 bundle.
        assert_eq!(column_index_id_b64("cnt"), "si9R0ZFE8dU=");
        assert_eq!(column_index_id_b64("_hoodie_record_key"), "uSJGdDiVr/k=");
        assert_eq!(partition_index_id_b64("part=p1"), "tssgLg9E7p0=");
        assert_eq!(partition_index_id_b64("part=p2"), "tFBD22Dgg24=");
        assert_eq!(
            partition_stats_index_key("part=p1", "cnt"),
            "si9R0ZFE8dU=tssgLg9E7p0="
        );
        assert_eq!(
            column_stats_index_key(
                "part=p1",
                "9c313941-89ac-46f5-9346-b14cd8e6e19d-0_0-21-22_20261027125934360.parquet",
                "cnt"
            ),
            "si9R0ZFE8dU=tssgLg9E7p0=fMkQyotsskcUseEa7+Im8A=="
        );
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
