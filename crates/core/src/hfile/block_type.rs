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
//! HFile block type definitions.

use crate::hfile::error::{HFileError, Result};

/// Length of block magic bytes
pub const MAGIC_LENGTH: usize = 8;

/// HFile block types with their magic byte sequences.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HFileBlockType {
    /// Data block containing key-value pairs
    Data,
    /// Leaf-level index block (multi-level index)
    LeafIndex,
    /// Meta block (e.g., bloom filter data)
    Meta,
    /// Intermediate-level index block (multi-level index)
    IntermediateIndex,
    /// Root-level index block
    RootIndex,
    /// File info block containing metadata key-value pairs
    FileInfo,
    /// HFile trailer
    Trailer,
}

impl HFileBlockType {
    /// Returns the magic bytes for this block type.
    pub fn magic(&self) -> &'static [u8; MAGIC_LENGTH] {
        match self {
            HFileBlockType::Data => b"DATABLK*",
            HFileBlockType::LeafIndex => b"IDXLEAF2",
            HFileBlockType::Meta => b"METABLKc",
            HFileBlockType::IntermediateIndex => b"IDXINTE2",
            HFileBlockType::RootIndex => b"IDXROOT2",
            HFileBlockType::FileInfo => b"FILEINF2",
            HFileBlockType::Trailer => b"TRABLK\"$",
        }
    }

    /// Parse block type from magic bytes.
    pub fn from_magic(magic: &[u8]) -> Result<Self> {
        if magic.len() < MAGIC_LENGTH {
            return Err(HFileError::InvalidFormat(format!(
                "Magic bytes too short: {} bytes",
                magic.len()
            )));
        }

        let magic_arr: &[u8; MAGIC_LENGTH] = magic[..MAGIC_LENGTH].try_into().unwrap();

        match magic_arr {
            b"DATABLK*" | b"DATABLKE" => Ok(HFileBlockType::Data),
            b"IDXLEAF2" => Ok(HFileBlockType::LeafIndex),
            b"METABLKc" => Ok(HFileBlockType::Meta),
            b"IDXINTE2" => Ok(HFileBlockType::IntermediateIndex),
            b"IDXROOT2" => Ok(HFileBlockType::RootIndex),
            b"FILEINF2" => Ok(HFileBlockType::FileInfo),
            b"TRABLK\"$" => Ok(HFileBlockType::Trailer),
            _ => Err(HFileError::InvalidBlockMagic {
                expected: "valid block magic".to_string(),
                actual: String::from_utf8_lossy(magic_arr).to_string(),
            }),
        }
    }

    /// Check if the given bytes start with this block type's magic.
    pub fn check_magic(&self, bytes: &[u8]) -> Result<()> {
        if bytes.len() < MAGIC_LENGTH {
            return Err(HFileError::InvalidFormat(format!(
                "Buffer too short for magic check: {} bytes",
                bytes.len()
            )));
        }

        let expected = self.magic();
        let actual = &bytes[..MAGIC_LENGTH];

        if actual != expected {
            return Err(HFileError::InvalidBlockMagic {
                expected: String::from_utf8_lossy(expected).to_string(),
                actual: String::from_utf8_lossy(actual).to_string(),
            });
        }

        Ok(())
    }
}

impl std::fmt::Display for HFileBlockType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HFileBlockType::Data => write!(f, "DATA"),
            HFileBlockType::LeafIndex => write!(f, "LEAF_INDEX"),
            HFileBlockType::Meta => write!(f, "META"),
            HFileBlockType::IntermediateIndex => write!(f, "INTERMEDIATE_INDEX"),
            HFileBlockType::RootIndex => write!(f, "ROOT_INDEX"),
            HFileBlockType::FileInfo => write!(f, "FILE_INFO"),
            HFileBlockType::Trailer => write!(f, "TRAILER"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_magic_bytes() {
        assert_eq!(HFileBlockType::Data.magic(), b"DATABLK*");
        assert_eq!(HFileBlockType::LeafIndex.magic(), b"IDXLEAF2");
        assert_eq!(HFileBlockType::Meta.magic(), b"METABLKc");
        assert_eq!(HFileBlockType::IntermediateIndex.magic(), b"IDXINTE2");
        assert_eq!(HFileBlockType::RootIndex.magic(), b"IDXROOT2");
        assert_eq!(HFileBlockType::FileInfo.magic(), b"FILEINF2");
        assert_eq!(HFileBlockType::Trailer.magic(), b"TRABLK\"$");
    }

    #[test]
    fn test_from_magic_data_block() {
        let magic = b"DATABLK*";
        assert_eq!(
            HFileBlockType::from_magic(magic).unwrap(),
            HFileBlockType::Data
        );
    }

    #[test]
    fn test_from_magic_data_block_encoded() {
        // Encoded data block magic
        let magic = b"DATABLKE";
        assert_eq!(
            HFileBlockType::from_magic(magic).unwrap(),
            HFileBlockType::Data
        );
    }

    #[test]
    fn test_from_magic_all_types() {
        assert_eq!(
            HFileBlockType::from_magic(b"IDXLEAF2").unwrap(),
            HFileBlockType::LeafIndex
        );
        assert_eq!(
            HFileBlockType::from_magic(b"METABLKc").unwrap(),
            HFileBlockType::Meta
        );
        assert_eq!(
            HFileBlockType::from_magic(b"IDXINTE2").unwrap(),
            HFileBlockType::IntermediateIndex
        );
        assert_eq!(
            HFileBlockType::from_magic(b"IDXROOT2").unwrap(),
            HFileBlockType::RootIndex
        );
        assert_eq!(
            HFileBlockType::from_magic(b"FILEINF2").unwrap(),
            HFileBlockType::FileInfo
        );
        assert_eq!(
            HFileBlockType::from_magic(b"TRABLK\"$").unwrap(),
            HFileBlockType::Trailer
        );
    }

    #[test]
    fn test_from_magic_too_short() {
        let err = HFileBlockType::from_magic(b"DATA").unwrap_err();
        assert!(matches!(err, HFileError::InvalidFormat(_)));
    }

    #[test]
    fn test_from_magic_invalid() {
        let err = HFileBlockType::from_magic(b"INVALID!").unwrap_err();
        assert!(matches!(err, HFileError::InvalidBlockMagic { .. }));
    }

    #[test]
    fn test_check_magic_success() {
        let bytes = b"DATABLK*extra_data_here";
        assert!(HFileBlockType::Data.check_magic(bytes).is_ok());
    }

    #[test]
    fn test_check_magic_too_short() {
        let err = HFileBlockType::Data.check_magic(b"DATA").unwrap_err();
        assert!(matches!(err, HFileError::InvalidFormat(_)));
    }

    #[test]
    fn test_check_magic_mismatch() {
        let err = HFileBlockType::Data.check_magic(b"IDXROOT2").unwrap_err();
        assert!(matches!(err, HFileError::InvalidBlockMagic { .. }));
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", HFileBlockType::Data), "DATA");
        assert_eq!(format!("{}", HFileBlockType::LeafIndex), "LEAF_INDEX");
        assert_eq!(format!("{}", HFileBlockType::Meta), "META");
        assert_eq!(
            format!("{}", HFileBlockType::IntermediateIndex),
            "INTERMEDIATE_INDEX"
        );
        assert_eq!(format!("{}", HFileBlockType::RootIndex), "ROOT_INDEX");
        assert_eq!(format!("{}", HFileBlockType::FileInfo), "FILE_INFO");
        assert_eq!(format!("{}", HFileBlockType::Trailer), "TRAILER");
    }
}
