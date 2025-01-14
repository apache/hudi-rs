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

pub const MAGIC: &[u8] = b"#HUDI#";

#[derive(Debug, Clone, PartialEq, Eq)]
#[repr(u32)]
pub enum LogFormatVersion {
    V0 = 0,
    V1 = 1,
    V2 = 2,
    V3 = 3,
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
            2 => Ok(Self::V2),
            3 => Ok(Self::V3),
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
        assert_eq!(
            LogFormatVersion::try_from([0, 0, 0, 2]).unwrap(),
            LogFormatVersion::V2
        );
        assert_eq!(
            LogFormatVersion::try_from([0, 0, 0, 3]).unwrap(),
            LogFormatVersion::V3
        );

        // Test invalid version
        let err = LogFormatVersion::try_from([0, 0, 0, 4]).unwrap_err();
        assert!(matches!(err, CoreError::LogFormatError(_)));
        assert!(err.to_string().contains("Invalid log format version: 4"));
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
        assert_eq!(
            LogFormatVersion::try_from(2u32).unwrap(),
            LogFormatVersion::V2
        );
        assert_eq!(
            LogFormatVersion::try_from(3u32).unwrap(),
            LogFormatVersion::V3
        );

        // Test invalid version
        let err = LogFormatVersion::try_from(4u32).unwrap_err();
        assert!(matches!(err, CoreError::LogFormatError(_)));
        assert!(err.to_string().contains("Invalid log format version: 4"));
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

    #[test]
    fn test_version_feature_flags_v2() {
        let version = LogFormatVersion::V2;
        assert!(version.has_block_type());
        assert!(version.has_header());
        assert!(version.has_content_length());
        assert!(!version.has_footer());
        assert!(!version.has_total_log_block_length());
    }

    #[test]
    fn test_version_feature_flags_v3() {
        let version = LogFormatVersion::V3;
        assert!(version.has_block_type());
        assert!(version.has_header());
        assert!(version.has_content_length());
        assert!(!version.has_footer());
        assert!(!version.has_total_log_block_length());
    }
}
