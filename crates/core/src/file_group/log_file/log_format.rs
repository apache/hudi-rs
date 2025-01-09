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
