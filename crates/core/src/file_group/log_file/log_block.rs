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
use crate::file_group::log_file::log_format::LogFormatVersion;
use crate::file_group::record_batches::RecordBatches;
use crate::Result;
use std::collections::HashMap;
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub enum BlockType {
    Command = 0,
    Delete = 1,
    Corrupted = 2,
    AvroData = 3,
    HfileData = 4,
    ParquetData = 5,
    CdcData = 6,
}

impl AsRef<str> for BlockType {
    fn as_ref(&self) -> &str {
        match self {
            BlockType::Command => ":command",
            BlockType::Delete => ":delete",
            BlockType::Corrupted => ":corrupted",
            BlockType::AvroData => "avro",
            BlockType::HfileData => "hfile",
            BlockType::ParquetData => "parquet",
            BlockType::CdcData => "cdc",
        }
    }
}

impl FromStr for BlockType {
    type Err = CoreError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            ":command" => Ok(BlockType::Command),
            ":delete" => Ok(BlockType::Delete),
            ":corrupted" => Ok(BlockType::Corrupted),
            "avro_data" => Ok(BlockType::AvroData),
            "hfile" => Ok(BlockType::HfileData),
            "parquet" => Ok(BlockType::ParquetData),
            "cdc" => Ok(BlockType::CdcData),
            _ => Err(CoreError::LogFormatError(format!(
                "Invalid block type: {s}"
            ))),
        }
    }
}

impl TryFrom<[u8; 4]> for BlockType {
    type Error = CoreError;

    fn try_from(value_bytes: [u8; 4]) -> Result<Self, Self::Error> {
        let value = u32::from_be_bytes(value_bytes);
        match value {
            0 => Ok(BlockType::Command),
            1 => Ok(BlockType::Delete),
            2 => Ok(BlockType::Corrupted),
            3 => Ok(BlockType::AvroData),
            4 => Ok(BlockType::HfileData),
            5 => Ok(BlockType::ParquetData),
            6 => Ok(BlockType::CdcData),
            _ => Err(CoreError::LogFormatError(format!(
                "Invalid block type: {value}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlockMetadataType {
    Header,
    Footer,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum BlockMetadataKey {
    InstantTime = 0,
    TargetInstantTime = 1,
    Schema = 2,
    CommandBlockType = 3,
    CompactedBlockTimes = 4,
    RecordPositions = 5,
    BlockIdentifier = 6,
}

impl TryFrom<[u8; 4]> for BlockMetadataKey {
    type Error = CoreError;

    fn try_from(value_bytes: [u8; 4]) -> Result<Self, Self::Error> {
        let value = u32::from_be_bytes(value_bytes);
        match value {
            0 => Ok(Self::InstantTime),
            1 => Ok(Self::TargetInstantTime),
            2 => Ok(Self::Schema),
            3 => Ok(Self::CommandBlockType),
            4 => Ok(Self::CompactedBlockTimes),
            5 => Ok(Self::RecordPositions),
            6 => Ok(Self::BlockIdentifier),
            _ => Err(CoreError::LogFormatError(format!(
                "Invalid header key: {value}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[repr(u32)]
pub enum CommandBlock {
    Rollback = 0,
}

impl FromStr for CommandBlock {
    type Err = CoreError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.parse::<u32>() {
            Ok(0) => Ok(CommandBlock::Rollback),
            Ok(val) => Err(CoreError::LogFormatError(format!(
                "Invalid command block type value: {val}"
            ))),
            Err(e) => Err(CoreError::LogFormatError(format!(
                "Failed to parse command block type: {e}"
            ))),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct LogBlock {
    pub format_version: LogFormatVersion,
    pub block_type: BlockType,
    pub header: HashMap<BlockMetadataKey, String>,
    pub record_batches: RecordBatches,
    pub footer: HashMap<BlockMetadataKey, String>,
    pub skipped: bool,
}

impl LogBlock {
    pub fn instant_time(&self) -> Result<&str> {
        let v = self
            .header
            .get(&BlockMetadataKey::InstantTime)
            .ok_or_else(|| CoreError::LogBlockError("Instant time not found".to_string()))?;
        Ok(v)
    }

    pub fn target_instant_time(&self) -> Result<&str> {
        if self.block_type != BlockType::Command {
            return Err(CoreError::LogBlockError(
                "Target instant time is only available for command blocks".to_string(),
            ));
        }
        let v = self
            .header
            .get(&BlockMetadataKey::TargetInstantTime)
            .ok_or_else(|| CoreError::LogBlockError("Target instant time not found".to_string()))?;
        Ok(v)
    }

    pub fn schema(&self) -> Result<&str> {
        let v = self
            .header
            .get(&BlockMetadataKey::Schema)
            .ok_or_else(|| CoreError::LogBlockError("Schema not found".to_string()))?;
        Ok(v)
    }

    pub fn command_block_type(&self) -> Result<CommandBlock> {
        if self.block_type != BlockType::Command {
            return Err(CoreError::LogBlockError(
                "Command block type is only available for command blocks".to_string(),
            ));
        }
        let v = self
            .header
            .get(&BlockMetadataKey::CommandBlockType)
            .ok_or_else(|| {
                CoreError::LogBlockError(
                    "Command block type not found for command block".to_string(),
                )
            })?;
        v.parse::<CommandBlock>()
    }

    pub fn is_data_block(&self) -> bool {
        matches!(
            self.block_type,
            BlockType::AvroData
                | BlockType::HfileData
                | BlockType::ParquetData
                | BlockType::CdcData
        )
    }

    pub fn is_delete_block(&self) -> bool {
        self.block_type == BlockType::Delete
    }

    pub fn is_rollback_block(&self) -> bool {
        matches!(self.command_block_type(), Ok(CommandBlock::Rollback))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_type_as_ref() {
        assert_eq!(BlockType::Command.as_ref(), ":command");
        assert_eq!(BlockType::Delete.as_ref(), ":delete");
        assert_eq!(BlockType::Corrupted.as_ref(), ":corrupted");
        assert_eq!(BlockType::AvroData.as_ref(), "avro");
        assert_eq!(BlockType::HfileData.as_ref(), "hfile");
        assert_eq!(BlockType::ParquetData.as_ref(), "parquet");
        assert_eq!(BlockType::CdcData.as_ref(), "cdc");
    }

    #[test]
    fn test_block_type_from_str() {
        assert_eq!(BlockType::from_str(":command").unwrap(), BlockType::Command);
        assert_eq!(BlockType::from_str(":delete").unwrap(), BlockType::Delete);
        assert_eq!(
            BlockType::from_str(":corrupted").unwrap(),
            BlockType::Corrupted
        );
        assert_eq!(
            BlockType::from_str("avro_data").unwrap(),
            BlockType::AvroData
        );
        assert_eq!(BlockType::from_str("hfile").unwrap(), BlockType::HfileData);
        assert_eq!(
            BlockType::from_str("parquet").unwrap(),
            BlockType::ParquetData
        );
        assert_eq!(BlockType::from_str("cdc").unwrap(), BlockType::CdcData);

        // Test invalid block type
        assert!(BlockType::from_str("invalid").is_err());
    }

    #[test]
    fn test_block_type_try_from_bytes() {
        assert_eq!(
            BlockType::try_from([0, 0, 0, 0]).unwrap(),
            BlockType::Command
        );
        assert_eq!(
            BlockType::try_from([0, 0, 0, 1]).unwrap(),
            BlockType::Delete
        );
        assert_eq!(
            BlockType::try_from([0, 0, 0, 2]).unwrap(),
            BlockType::Corrupted
        );
        assert_eq!(
            BlockType::try_from([0, 0, 0, 3]).unwrap(),
            BlockType::AvroData
        );
        assert_eq!(
            BlockType::try_from([0, 0, 0, 4]).unwrap(),
            BlockType::HfileData
        );
        assert_eq!(
            BlockType::try_from([0, 0, 0, 5]).unwrap(),
            BlockType::ParquetData
        );
        assert_eq!(
            BlockType::try_from([0, 0, 0, 6]).unwrap(),
            BlockType::CdcData
        );

        // Test invalid block type
        assert!(BlockType::try_from([0, 0, 0, 7]).is_err());
    }

    #[test]
    fn test_block_metadata_key_try_from_bytes() {
        assert_eq!(
            BlockMetadataKey::try_from([0, 0, 0, 0]).unwrap(),
            BlockMetadataKey::InstantTime
        );
        assert_eq!(
            BlockMetadataKey::try_from([0, 0, 0, 1]).unwrap(),
            BlockMetadataKey::TargetInstantTime
        );
        assert_eq!(
            BlockMetadataKey::try_from([0, 0, 0, 2]).unwrap(),
            BlockMetadataKey::Schema
        );
        assert_eq!(
            BlockMetadataKey::try_from([0, 0, 0, 3]).unwrap(),
            BlockMetadataKey::CommandBlockType
        );
        assert_eq!(
            BlockMetadataKey::try_from([0, 0, 0, 4]).unwrap(),
            BlockMetadataKey::CompactedBlockTimes
        );
        assert_eq!(
            BlockMetadataKey::try_from([0, 0, 0, 5]).unwrap(),
            BlockMetadataKey::RecordPositions
        );
        assert_eq!(
            BlockMetadataKey::try_from([0, 0, 0, 6]).unwrap(),
            BlockMetadataKey::BlockIdentifier
        );

        // Test invalid metadata key
        assert!(BlockMetadataKey::try_from([0, 0, 0, 7]).is_err());
    }

    #[test]
    fn test_valid_rollback_block() {
        assert_eq!(CommandBlock::from_str("0").unwrap(), CommandBlock::Rollback);
    }

    #[test]
    fn test_invalid_rollback_block() {
        assert!(matches!(
            CommandBlock::from_str("1"),
            Err(CoreError::LogFormatError(msg)) if msg.contains("Invalid command block type value: 1")
        ));
        assert!(matches!(
            CommandBlock::from_str("invalid"),
            Err(CoreError::LogFormatError(msg)) if msg.contains("Failed to parse command block type")
        ));
        assert!(matches!(
            CommandBlock::from_str(""),
            Err(CoreError::LogFormatError(msg)) if msg.contains("Failed to parse command block type")
        ));
    }
}
