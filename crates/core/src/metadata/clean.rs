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

//! Reading a completed clean instant.

use crate::Result;
use crate::error::CoreError;
use apache_avro::Reader as AvroReader;
use apache_avro::from_value;
use serde::Deserialize;
use std::io::Cursor;

/// The fields of `HoodieCleanMetadata` this crate reads.
///
/// `earliestCommitToRetain` is a top-level field in both clean metadata
/// versions; the versions differ only in how `partitionMetadata` names files,
/// which time travel does not consult.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CleanMetadata {
    /// Empty under `KEEP_LATEST_FILE_VERSIONS`, which retains by version count
    /// instead; read it through [`Self::earliest_commit_to_retain`].
    earliest_commit_to_retain: String,
}

impl CleanMetadata {
    /// Decode the bytes of a completed `.clean` instant, an Avro object
    /// container on both timeline layouts.
    pub fn from_avro_bytes(bytes: &[u8]) -> Result<Self> {
        let mut records = AvroReader::new(Cursor::new(bytes)).map_err(|e| {
            CoreError::CommitMetadata(format!("Failed to create Avro reader for clean: {e}"))
        })?;
        let value = records
            .next()
            .ok_or_else(|| {
                CoreError::CommitMetadata("Clean metadata contains no records".to_string())
            })?
            .map_err(|e| CoreError::CommitMetadata(format!("Failed to read clean record: {e}")))?;
        from_value::<Self>(&value).map_err(|e| {
            CoreError::CommitMetadata(format!("Failed to deserialize clean metadata: {e}"))
        })
    }

    /// The earliest commit whose file slices the clean kept, if it recorded one.
    pub fn earliest_commit_to_retain(&self) -> Option<&str> {
        Some(self.earliest_commit_to_retain.as_str()).filter(|s| !s.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::types::Record;
    use apache_avro::{Schema, Writer};

    /// Encode one clean record under a schema holding only the fields every
    /// clean metadata version shares.
    fn encode(earliest_commit_to_retain: &str) -> Vec<u8> {
        let schema = Schema::parse_str(
            r#"{"type": "record", "name": "HoodieCleanMetadata", "fields": [
                {"name": "startCleanTime", "type": "string"},
                {"name": "timeTakenInMillis", "type": "long"},
                {"name": "totalFilesDeleted", "type": "int"},
                {"name": "earliestCommitToRetain", "type": "string"},
                {"name": "partitionMetadata", "type": {"type": "map", "values": "string"}},
                {"name": "version", "type": ["int", "null"], "default": 1}
            ]}"#,
        )
        .unwrap();
        let mut record = Record::new(&schema).unwrap();
        record.put("startCleanTime", "20240101000000003");
        record.put("timeTakenInMillis", 1i64);
        record.put("totalFilesDeleted", 1i32);
        record.put("earliestCommitToRetain", earliest_commit_to_retain);
        record.put(
            "partitionMetadata",
            apache_avro::types::Value::Map(Default::default()),
        );
        record.put(
            "version",
            apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Int(1))),
        );
        let mut writer = Writer::new(&schema, Vec::new());
        writer.append(record).unwrap();
        writer.into_inner().unwrap()
    }

    #[test]
    fn test_from_avro_bytes_reads_earliest_commit_to_retain() {
        let metadata = CleanMetadata::from_avro_bytes(&encode("20240101000000002")).unwrap();
        assert_eq!(
            metadata.earliest_commit_to_retain(),
            Some("20240101000000002")
        );
    }

    #[test]
    fn test_from_avro_bytes_empty_earliest_commit_to_retain_is_none() {
        let metadata = CleanMetadata::from_avro_bytes(&encode("")).unwrap();
        assert_eq!(metadata.earliest_commit_to_retain(), None);
    }

    #[test]
    fn test_from_avro_bytes_rejects_non_avro_bytes() {
        let result = CleanMetadata::from_avro_bytes(b"{}");
        assert!(
            matches!(result, Err(CoreError::CommitMetadata(_))),
            "{result:?}"
        );
    }
}
