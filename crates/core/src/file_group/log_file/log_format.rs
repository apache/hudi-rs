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
use crate::Result;
use crate::error::CoreError;

pub const MAGIC: &[u8] = b"#HUDI#";

/// Log format version (file-level).
///
/// This is the version of the log file format itself (read from the file header after MAGIC).
/// Modern Hudi tables (v6+) use V1 as the log format version.
///
/// For the internal block content version (first 4 bytes of each block's content),
/// see [`LogBlockVersion`](crate::file_group::log_file::log_block::LogBlockVersion).
///
/// Feature flags by version:
///
/// | Feature            | V0  | V1  |
/// |--------------------|-----|-----|
/// | has_block_type     | ✗   | ✓   |
/// | has_header         | ✗   | ✓   |
/// | has_content_length | ✗   | ✓   |
/// | has_footer         | ✗   | ✓   |
/// | has_log_block_len  | ✗   | ✓   |
#[derive(Debug, Clone, PartialEq, Eq)]
#[repr(u32)]
pub enum LogFormatVersion {
    /// Legacy format with minimal structure (no block type, header, footer, etc.).
    V0 = 0,
    /// Current version for modern Hudi tables (v6+).
    /// Has block type, header, content length, footer, and total log block length.
    V1 = 1,
}

impl TryFrom<[u8; 4]> for LogFormatVersion {
    type Error = CoreError;

    fn try_from(value_bytes: [u8; 4]) -> Result<Self, Self::Error> {
        let value = u32::from_be_bytes(value_bytes);
        Self::try_from(value)
    }
}

impl TryFrom<u32> for LogFormatVersion {
    type Error = CoreError;

    fn try_from(value: u32) -> std::result::Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::V0),
            1 => Ok(Self::V1),
            _ => Err(CoreError::LogFormatError(format!(
                "Invalid log format version: {value}"
            ))),
        }
    }
}

impl LogFormatVersion {
    #[inline]
    pub fn has_block_type(&self) -> bool {
        !matches!(self, LogFormatVersion::V0)
    }

    #[inline]
    pub fn has_header(&self) -> bool {
        !matches!(self, LogFormatVersion::V0)
    }

    #[inline]
    pub fn has_content_length(&self) -> bool {
        !matches!(self, LogFormatVersion::V0)
    }

    #[inline]
    pub fn has_footer(&self) -> bool {
        matches!(self, LogFormatVersion::V1)
    }

    #[inline]
    pub fn has_total_log_block_length(&self) -> bool {
        matches!(self, LogFormatVersion::V1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_format_version_from_bytes() {
        // Test valid versions
        assert_eq!(
            LogFormatVersion::try_from([0, 0, 0, 0]).unwrap(),
            LogFormatVersion::V0
        );
        assert_eq!(
            LogFormatVersion::try_from([0, 0, 0, 1]).unwrap(),
            LogFormatVersion::V1
        );

        // Test invalid version
        let err = LogFormatVersion::try_from([0, 0, 0, 2]).unwrap_err();
        assert!(matches!(err, CoreError::LogFormatError(_)));
        assert!(err.to_string().contains("Invalid log format version: 2"));
    }

    #[test]
    fn test_log_format_version_from_u32() {
        // Test valid versions
        assert_eq!(
            LogFormatVersion::try_from(0u32).unwrap(),
            LogFormatVersion::V0
        );
        assert_eq!(
            LogFormatVersion::try_from(1u32).unwrap(),
            LogFormatVersion::V1
        );

        // Test invalid version
        let err = LogFormatVersion::try_from(2u32).unwrap_err();
        assert!(matches!(err, CoreError::LogFormatError(_)));
        assert!(err.to_string().contains("Invalid log format version: 2"));
    }

    #[test]
    fn test_version_feature_flags_v0() {
        let version = LogFormatVersion::V0;
        assert!(!version.has_block_type());
        assert!(!version.has_header());
        assert!(!version.has_content_length());
        assert!(!version.has_footer());
        assert!(!version.has_total_log_block_length());
    }

    #[test]
    fn test_version_feature_flags_v1() {
        let version = LogFormatVersion::V1;
        assert!(version.has_block_type());
        assert!(version.has_header());
        assert!(version.has_content_length());
        assert!(version.has_footer());
        assert!(version.has_total_log_block_length());
    }
}
