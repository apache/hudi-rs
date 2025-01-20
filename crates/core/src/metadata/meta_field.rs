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
use crate::error::CoreError;
use crate::Result;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use lazy_static::lazy_static;
use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetaField {
    CommitTime = 0,
    CommitSeqno = 1,
    RecordKey = 2,
    PartitionPath = 3,
    FileName = 4,
    Operation = 5,
}

impl AsRef<str> for MetaField {
    fn as_ref(&self) -> &str {
        match self {
            MetaField::CommitTime => "_hoodie_commit_time",
            MetaField::CommitSeqno => "_hoodie_commit_seqno",
            MetaField::RecordKey => "_hoodie_record_key",
            MetaField::PartitionPath => "_hoodie_partition_path",
            MetaField::FileName => "_hoodie_file_name",
            MetaField::Operation => "_hoodie_operation",
        }
    }
}

impl Display for MetaField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

impl FromStr for MetaField {
    type Err = CoreError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "_hoodie_commit_time" => Ok(MetaField::CommitTime),
            "_hoodie_commit_seqno" => Ok(MetaField::CommitSeqno),
            "_hoodie_record_key" => Ok(MetaField::RecordKey),
            "_hoodie_partition_path" => Ok(MetaField::PartitionPath),
            "_hoodie_file_name" => Ok(MetaField::FileName),
            "_hoodie_operation" => Ok(MetaField::Operation),
            _ => Err(CoreError::InvalidValue(s.to_string())),
        }
    }
}

lazy_static! {
    static ref SCHEMA: Arc<Schema> = Arc::new(Schema::new(vec![
        Field::new(MetaField::CommitTime.as_ref(), DataType::Utf8, false),
        Field::new(MetaField::CommitSeqno.as_ref(), DataType::Utf8, false),
        Field::new(MetaField::RecordKey.as_ref(), DataType::Utf8, false),
        Field::new(MetaField::PartitionPath.as_ref(), DataType::Utf8, false),
        Field::new(MetaField::FileName.as_ref(), DataType::Utf8, false),
    ]));
    static ref SCHEMA_WITH_OPERATION: Arc<Schema> = Arc::new(Schema::new(vec![
        Field::new(MetaField::CommitTime.as_ref(), DataType::Utf8, false),
        Field::new(MetaField::CommitSeqno.as_ref(), DataType::Utf8, false),
        Field::new(MetaField::RecordKey.as_ref(), DataType::Utf8, false),
        Field::new(MetaField::PartitionPath.as_ref(), DataType::Utf8, false),
        Field::new(MetaField::FileName.as_ref(), DataType::Utf8, false),
        Field::new(MetaField::Operation.as_ref(), DataType::Utf8, false),
    ]));
}

impl MetaField {
    #[inline]
    pub fn field_index(&self) -> usize {
        self.clone() as usize
    }

    pub fn schema() -> SchemaRef {
        SCHEMA.clone()
    }

    pub fn schema_with_operation() -> SchemaRef {
        SCHEMA_WITH_OPERATION.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_index() {
        assert_eq!(MetaField::CommitTime.field_index(), 0);
        assert_eq!(MetaField::CommitSeqno.field_index(), 1);
        assert_eq!(MetaField::RecordKey.field_index(), 2);
        assert_eq!(MetaField::PartitionPath.field_index(), 3);
        assert_eq!(MetaField::FileName.field_index(), 4);
        assert_eq!(MetaField::Operation.field_index(), 5);
    }

    #[test]
    fn test_as_ref() {
        assert_eq!(MetaField::CommitTime.as_ref(), "_hoodie_commit_time");
        assert_eq!(MetaField::CommitSeqno.as_ref(), "_hoodie_commit_seqno");
        assert_eq!(MetaField::RecordKey.as_ref(), "_hoodie_record_key");
        assert_eq!(MetaField::PartitionPath.as_ref(), "_hoodie_partition_path");
        assert_eq!(MetaField::FileName.as_ref(), "_hoodie_file_name");
        assert_eq!(MetaField::Operation.as_ref(), "_hoodie_operation");
    }

    #[test]
    fn test_display() {
        assert_eq!(MetaField::CommitTime.to_string(), "_hoodie_commit_time");
        assert_eq!(
            format!("{}", MetaField::CommitSeqno),
            "_hoodie_commit_seqno"
        );
        assert_eq!(MetaField::RecordKey.to_string(), "_hoodie_record_key");
    }

    #[test]
    fn test_from_str_valid() -> Result<(), CoreError> {
        assert_eq!(
            MetaField::from_str("_hoodie_commit_time")?,
            MetaField::CommitTime
        );
        assert_eq!(
            MetaField::from_str("_hoodie_commit_seqno")?,
            MetaField::CommitSeqno
        );
        assert_eq!(
            MetaField::from_str("_hoodie_record_key")?,
            MetaField::RecordKey
        );
        assert_eq!(
            MetaField::from_str("_hoodie_partition_path")?,
            MetaField::PartitionPath
        );
        assert_eq!(
            MetaField::from_str("_hoodie_file_name")?,
            MetaField::FileName
        );
        assert_eq!(
            MetaField::from_str("_hoodie_operation")?,
            MetaField::Operation
        );
        Ok(())
    }

    #[test]
    fn test_from_str_invalid() {
        assert!(matches!(
            MetaField::from_str(""),
            Err(CoreError::InvalidValue(_))
        ));
        assert!(matches!(
            MetaField::from_str("_hoodie_invalid"),
            Err(CoreError::InvalidValue(_))
        ));
        assert!(matches!(
            MetaField::from_str("invalid"),
            Err(CoreError::InvalidValue(_))
        ));
    }

    #[test]
    fn test_roundtrip() -> Result<(), CoreError> {
        // Test conversion from enum -> string -> enum
        let fields = [
            MetaField::CommitTime,
            MetaField::CommitSeqno,
            MetaField::RecordKey,
            MetaField::PartitionPath,
            MetaField::FileName,
            MetaField::Operation,
        ];

        for field in fields {
            let s = field.to_string();
            let parsed = MetaField::from_str(&s)?;
            assert_eq!(field, parsed);
        }
        Ok(())
    }
}
