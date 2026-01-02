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
use crate::hfile::HFileRecord;
use crate::Result;
use std::collections::HashMap;
use std::str::FromStr;

/// Internal block content version.
///
/// This version is stored as the first 4 bytes of each block's content (after the header).
/// It controls the internal serialization format of the block data.
///
/// This is different from [`LogFormatVersion`] which is the file-level format version
/// read from the file header after MAGIC.
///
/// Modern Hudi tables (v6+) use V3 for block content.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum LogBlockVersion {
    V0 = 0,
    V1 = 1,
    V2 = 2,
    /// Current version used by modern Hudi tables (v6+).
    V3 = 3,
}

impl TryFrom<u32> for LogBlockVersion {
    type Error = CoreError;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::V0),
            1 => Ok(Self::V1),
            2 => Ok(Self::V2),
            3 => Ok(Self::V3),
            _ => Err(CoreError::LogBlockError(format!(
                "Invalid log block version: {value}"
            ))),
        }
    }
}

impl TryFrom<[u8; 4]> for LogBlockVersion {
    type Error = CoreError;

    fn try_from(value_bytes: [u8; 4]) -> Result<Self, Self::Error> {
        let value = u32::from_be_bytes(value_bytes);
        Self::try_from(value)
    }
}

/// Log block types.
#[derive(Debug, Clone, PartialEq, Eq)]
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

/// Log block header metadata keys.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum BlockMetadataKey {
    InstantTime = 0,
    TargetInstantTime = 1,
    Schema = 2,
    CommandBlockType = 3,
    /// Requires table version >= 5.
    CompactedBlockTimes = 4,
    /// Requires table version >= 6.
    RecordPositions = 5,
    /// Requires table version >= 6.
    BlockIdentifier = 6,
    /// Requires table version >= 8.
    IsPartial = 7,
    /// Requires table version >= 8.
    BaseFileInstantTimeOfRecordPositions = 8,
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
            7 => Ok(Self::IsPartial),
            8 => Ok(Self::BaseFileInstantTimeOfRecordPositions),
            _ => Err(CoreError::LogFormatError(format!(
                "Invalid metadata key: {value}"
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

/// Content types that a log block can hold.
///
/// Different block types store different data formats:
/// - Avro/Parquet/Delete data blocks → Arrow RecordBatches
/// - HFile data blocks → HFileRecords (raw key-value pairs)
/// - Command/Corrupted blocks → Empty (metadata only)
#[derive(Debug, Clone, Default)]
pub enum LogBlockContent {
    /// Arrow RecordBatches from Avro/Parquet/Delete decoded data blocks.
    /// Delete blocks contain record keys to be deleted.
    Records(RecordBatches),
    /// HFile records (raw key-value pairs) from HFile data blocks.
    /// Used for metadata table log files.
    HFileRecords(Vec<HFileRecord>),
    /// Empty content for command/corrupted blocks
    #[default]
    Empty,
}

impl LogBlockContent {
    /// Returns true if this content contains Arrow RecordBatches.
    #[must_use]
    pub fn is_records(&self) -> bool {
        matches!(self, LogBlockContent::Records(_))
    }

    /// Returns true if this content contains HFile records.
    #[must_use]
    pub fn is_hfile_records(&self) -> bool {
        matches!(self, LogBlockContent::HFileRecords(_))
    }

    /// Returns true if this content is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        matches!(self, LogBlockContent::Empty)
    }

    /// Returns the RecordBatches if this is a Records variant.
    pub fn as_records(&self) -> Option<&RecordBatches> {
        match self {
            LogBlockContent::Records(batches) => Some(batches),
            _ => None,
        }
    }

    /// Returns the HFile records if this is an HFileRecords variant.
    pub fn as_hfile_records(&self) -> Option<&Vec<HFileRecord>> {
        match self {
            LogBlockContent::HFileRecords(records) => Some(records),
            _ => None,
        }
    }

    /// Consumes self and returns the RecordBatches if this is a Records variant.
    pub fn into_records(self) -> Option<RecordBatches> {
        match self {
            LogBlockContent::Records(batches) => Some(batches),
            _ => None,
        }
    }

    /// Consumes self and returns the HFile records if this is an HFileRecords variant.
    pub fn into_hfile_records(self) -> Option<Vec<HFileRecord>> {
        match self {
            LogBlockContent::HFileRecords(records) => Some(records),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LogBlock {
    pub format_version: LogFormatVersion,
    pub block_type: BlockType,
    pub header: HashMap<BlockMetadataKey, String>,
    pub content: LogBlockContent,
    pub footer: HashMap<BlockMetadataKey, String>,
    pub skipped: bool,
}

impl LogBlock {
    /// Create a new log block with the given content.
    pub fn new(
        format_version: LogFormatVersion,
        block_type: BlockType,
        header: HashMap<BlockMetadataKey, String>,
        content: LogBlockContent,
        footer: HashMap<BlockMetadataKey, String>,
    ) -> Self {
        Self {
            format_version,
            block_type,
            header,
            content,
            footer,
            skipped: false,
        }
    }

    /// Create a skipped log block (used when block is out of instant range).
    ///
    /// Skipped blocks have empty content and footer.
    pub fn new_skipped(
        format_version: LogFormatVersion,
        block_type: BlockType,
        header: HashMap<BlockMetadataKey, String>,
    ) -> Self {
        Self {
            format_version,
            block_type,
            header,
            content: LogBlockContent::Empty,
            footer: HashMap::new(),
            skipped: true,
        }
    }

    /// Returns the record batches if the content contains Arrow records.
    ///
    /// This is a convenience method for backwards compatibility.
    /// For new code, prefer using `content` directly.
    pub fn record_batches(&self) -> Option<&RecordBatches> {
        self.content.as_records()
    }

    /// Returns the HFile records if the content contains HFile data.
    pub fn hfile_records(&self) -> Option<&Vec<HFileRecord>> {
        self.content.as_hfile_records()
    }

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

    #[must_use]
    pub fn is_data_block(&self) -> bool {
        matches!(
            self.block_type,
            BlockType::AvroData
                | BlockType::HfileData
                | BlockType::ParquetData
                | BlockType::CdcData
        )
    }

    #[must_use]
    pub fn is_delete_block(&self) -> bool {
        self.block_type == BlockType::Delete
    }

    #[must_use]
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
        assert_eq!(
            BlockMetadataKey::try_from([0, 0, 0, 7]).unwrap(),
            BlockMetadataKey::IsPartial
        );
        assert_eq!(
            BlockMetadataKey::try_from([0, 0, 0, 8]).unwrap(),
            BlockMetadataKey::BaseFileInstantTimeOfRecordPositions
        );

        // Test invalid metadata key
        assert!(BlockMetadataKey::try_from([0, 0, 0, 9]).is_err());
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

    #[test]
    fn test_log_block_version_try_from_bytes() {
        assert_eq!(
            LogBlockVersion::try_from([0, 0, 0, 0]).unwrap(),
            LogBlockVersion::V0
        );
        assert_eq!(
            LogBlockVersion::try_from([0, 0, 0, 1]).unwrap(),
            LogBlockVersion::V1
        );
        assert_eq!(
            LogBlockVersion::try_from([0, 0, 0, 2]).unwrap(),
            LogBlockVersion::V2
        );
        assert_eq!(
            LogBlockVersion::try_from([0, 0, 0, 3]).unwrap(),
            LogBlockVersion::V3
        );

        // Test invalid version
        assert!(LogBlockVersion::try_from([0, 0, 0, 4]).is_err());
    }

    #[test]
    fn test_log_block_version_try_from_u32() {
        assert_eq!(
            LogBlockVersion::try_from(0u32).unwrap(),
            LogBlockVersion::V0
        );
        assert_eq!(
            LogBlockVersion::try_from(1u32).unwrap(),
            LogBlockVersion::V1
        );
        assert_eq!(
            LogBlockVersion::try_from(2u32).unwrap(),
            LogBlockVersion::V2
        );
        assert_eq!(
            LogBlockVersion::try_from(3u32).unwrap(),
            LogBlockVersion::V3
        );

        // Test invalid version
        let err = LogBlockVersion::try_from(4u32).unwrap_err();
        assert!(matches!(err, CoreError::LogBlockError(_)));
        assert!(err.to_string().contains("Invalid log block version: 4"));
    }

    #[test]
    fn test_log_block_content_is_records() {
        let empty = LogBlockContent::Empty;
        assert!(!empty.is_records());
        assert!(!empty.is_hfile_records());
        assert!(empty.is_empty());

        let records = LogBlockContent::Records(RecordBatches::default());
        assert!(records.is_records());
        assert!(!records.is_hfile_records());
        assert!(!records.is_empty());

        let hfile = LogBlockContent::HFileRecords(vec![]);
        assert!(!hfile.is_records());
        assert!(hfile.is_hfile_records());
        assert!(!hfile.is_empty());
    }

    #[test]
    fn test_log_block_content_as_methods() {
        let empty = LogBlockContent::Empty;
        assert!(empty.as_records().is_none());
        assert!(empty.as_hfile_records().is_none());

        let records = LogBlockContent::Records(RecordBatches::default());
        assert!(records.as_records().is_some());
        assert!(records.as_hfile_records().is_none());

        let hfile = LogBlockContent::HFileRecords(vec![]);
        assert!(hfile.as_records().is_none());
        assert!(hfile.as_hfile_records().is_some());
    }

    #[test]
    fn test_log_block_content_into_methods() {
        let empty = LogBlockContent::Empty;
        assert!(empty.into_records().is_none());

        let empty = LogBlockContent::Empty;
        assert!(empty.into_hfile_records().is_none());

        let records = LogBlockContent::Records(RecordBatches::default());
        assert!(records.into_records().is_some());

        let hfile = LogBlockContent::HFileRecords(vec![]);
        assert!(hfile.into_hfile_records().is_some());

        // Test that into_records on HFileRecords returns None
        let hfile = LogBlockContent::HFileRecords(vec![]);
        assert!(hfile.into_records().is_none());

        // Test that into_hfile_records on Records returns None
        let records = LogBlockContent::Records(RecordBatches::default());
        assert!(records.into_hfile_records().is_none());
    }

    #[test]
    fn test_log_block_new() {
        let header = HashMap::from([(BlockMetadataKey::InstantTime, "12345".to_string())]);
        let footer = HashMap::new();
        let content = LogBlockContent::Empty;

        let block = LogBlock::new(
            LogFormatVersion::V1,
            BlockType::Command,
            header.clone(),
            content,
            footer.clone(),
        );

        assert_eq!(block.format_version, LogFormatVersion::V1);
        assert_eq!(block.block_type, BlockType::Command);
        assert!(!block.skipped);
        assert!(block.content.is_empty());
    }

    #[test]
    fn test_log_block_new_skipped() {
        let header = HashMap::from([(BlockMetadataKey::InstantTime, "12345".to_string())]);

        let block = LogBlock::new_skipped(LogFormatVersion::V1, BlockType::AvroData, header);

        assert_eq!(block.format_version, LogFormatVersion::V1);
        assert_eq!(block.block_type, BlockType::AvroData);
        assert!(block.skipped);
        assert!(block.content.is_empty());
        assert!(block.footer.is_empty());
    }

    #[test]
    fn test_log_block_record_batches_and_hfile_records() {
        // Test record_batches on Records content
        let records_content = LogBlockContent::Records(RecordBatches::default());
        let block = LogBlock::new(
            LogFormatVersion::V1,
            BlockType::AvroData,
            HashMap::new(),
            records_content,
            HashMap::new(),
        );
        assert!(block.record_batches().is_some());
        assert!(block.hfile_records().is_none());

        // Test hfile_records on HFileRecords content
        let hfile_content = LogBlockContent::HFileRecords(vec![]);
        let block = LogBlock::new(
            LogFormatVersion::V1,
            BlockType::HfileData,
            HashMap::new(),
            hfile_content,
            HashMap::new(),
        );
        assert!(block.record_batches().is_none());
        assert!(block.hfile_records().is_some());
    }

    #[test]
    fn test_log_block_instant_time() {
        // Test success case
        let header = HashMap::from([(BlockMetadataKey::InstantTime, "20231214120000".to_string())]);
        let block = LogBlock::new(
            LogFormatVersion::V1,
            BlockType::AvroData,
            header,
            LogBlockContent::Empty,
            HashMap::new(),
        );
        assert_eq!(block.instant_time().unwrap(), "20231214120000");

        // Test missing instant time
        let block = LogBlock::new(
            LogFormatVersion::V1,
            BlockType::AvroData,
            HashMap::new(),
            LogBlockContent::Empty,
            HashMap::new(),
        );
        assert!(block.instant_time().is_err());
    }

    #[test]
    fn test_log_block_target_instant_time() {
        // Test success case for command block
        let header = HashMap::from([
            (BlockMetadataKey::InstantTime, "20231214120000".to_string()),
            (
                BlockMetadataKey::TargetInstantTime,
                "20231214110000".to_string(),
            ),
            (BlockMetadataKey::CommandBlockType, "0".to_string()),
        ]);
        let block = LogBlock::new(
            LogFormatVersion::V1,
            BlockType::Command,
            header,
            LogBlockContent::Empty,
            HashMap::new(),
        );
        assert_eq!(block.target_instant_time().unwrap(), "20231214110000");

        // Test error for non-command block
        let block = LogBlock::new(
            LogFormatVersion::V1,
            BlockType::AvroData,
            HashMap::new(),
            LogBlockContent::Empty,
            HashMap::new(),
        );
        assert!(block.target_instant_time().is_err());

        // Test missing target instant time
        let header = HashMap::from([(BlockMetadataKey::InstantTime, "20231214120000".to_string())]);
        let block = LogBlock::new(
            LogFormatVersion::V1,
            BlockType::Command,
            header,
            LogBlockContent::Empty,
            HashMap::new(),
        );
        assert!(block.target_instant_time().is_err());
    }

    #[test]
    fn test_log_block_schema() {
        // Test success case
        let header = HashMap::from([(
            BlockMetadataKey::Schema,
            "{\"type\":\"record\"}".to_string(),
        )]);
        let block = LogBlock::new(
            LogFormatVersion::V1,
            BlockType::AvroData,
            header,
            LogBlockContent::Empty,
            HashMap::new(),
        );
        assert_eq!(block.schema().unwrap(), "{\"type\":\"record\"}");

        // Test missing schema
        let block = LogBlock::new(
            LogFormatVersion::V1,
            BlockType::AvroData,
            HashMap::new(),
            LogBlockContent::Empty,
            HashMap::new(),
        );
        assert!(block.schema().is_err());
    }

    #[test]
    fn test_log_block_command_block_type() {
        // Test success case - rollback command
        let header = HashMap::from([
            (BlockMetadataKey::InstantTime, "20231214120000".to_string()),
            (BlockMetadataKey::CommandBlockType, "0".to_string()),
        ]);
        let block = LogBlock::new(
            LogFormatVersion::V1,
            BlockType::Command,
            header,
            LogBlockContent::Empty,
            HashMap::new(),
        );
        assert_eq!(block.command_block_type().unwrap(), CommandBlock::Rollback);

        // Test error for non-command block
        let block = LogBlock::new(
            LogFormatVersion::V1,
            BlockType::AvroData,
            HashMap::new(),
            LogBlockContent::Empty,
            HashMap::new(),
        );
        assert!(block.command_block_type().is_err());

        // Test missing command block type
        let header = HashMap::from([(BlockMetadataKey::InstantTime, "20231214120000".to_string())]);
        let block = LogBlock::new(
            LogFormatVersion::V1,
            BlockType::Command,
            header,
            LogBlockContent::Empty,
            HashMap::new(),
        );
        assert!(block.command_block_type().is_err());
    }

    #[test]
    fn test_log_block_is_data_block() {
        // Test data block types
        for block_type in [
            BlockType::AvroData,
            BlockType::HfileData,
            BlockType::ParquetData,
            BlockType::CdcData,
        ] {
            let block = LogBlock::new(
                LogFormatVersion::V1,
                block_type,
                HashMap::new(),
                LogBlockContent::Empty,
                HashMap::new(),
            );
            assert!(block.is_data_block());
        }

        // Test non-data block types
        for block_type in [BlockType::Command, BlockType::Delete, BlockType::Corrupted] {
            let block = LogBlock::new(
                LogFormatVersion::V1,
                block_type,
                HashMap::new(),
                LogBlockContent::Empty,
                HashMap::new(),
            );
            assert!(!block.is_data_block());
        }
    }

    #[test]
    fn test_log_block_is_delete_block() {
        let delete_block = LogBlock::new(
            LogFormatVersion::V1,
            BlockType::Delete,
            HashMap::new(),
            LogBlockContent::Empty,
            HashMap::new(),
        );
        assert!(delete_block.is_delete_block());

        let avro_block = LogBlock::new(
            LogFormatVersion::V1,
            BlockType::AvroData,
            HashMap::new(),
            LogBlockContent::Empty,
            HashMap::new(),
        );
        assert!(!avro_block.is_delete_block());
    }

    #[test]
    fn test_log_block_is_rollback_block() {
        // Test rollback block
        let header = HashMap::from([
            (BlockMetadataKey::InstantTime, "20231214120000".to_string()),
            (BlockMetadataKey::CommandBlockType, "0".to_string()),
        ]);
        let block = LogBlock::new(
            LogFormatVersion::V1,
            BlockType::Command,
            header,
            LogBlockContent::Empty,
            HashMap::new(),
        );
        assert!(block.is_rollback_block());

        // Test non-rollback block (non-command)
        let block = LogBlock::new(
            LogFormatVersion::V1,
            BlockType::AvroData,
            HashMap::new(),
            LogBlockContent::Empty,
            HashMap::new(),
        );
        assert!(!block.is_rollback_block());

        // Test command block without rollback type
        let header = HashMap::from([(BlockMetadataKey::InstantTime, "20231214120000".to_string())]);
        let block = LogBlock::new(
            LogFormatVersion::V1,
            BlockType::Command,
            header,
            LogBlockContent::Empty,
            HashMap::new(),
        );
        assert!(!block.is_rollback_block());
    }
}
