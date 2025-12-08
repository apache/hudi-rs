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
use crate::config::internal::HudiInternalConfig::SkipConfigValidation;
use crate::config::read::HudiReadConfig;
use crate::config::table::HudiTableConfig;
use crate::config::table::HudiTableConfig::{
    DropsPartitionFields, TableVersion, TimelineLayoutVersion,
};
use crate::config::HudiConfigs;
use crate::error::CoreError;
use crate::merge::record_merger::RecordMerger;
use strum::IntoEnumIterator;

pub fn validate_configs(hudi_configs: &HudiConfigs) -> crate::error::Result<()> {
    if hudi_configs.get_or_default(SkipConfigValidation).into() {
        return Ok(());
    }

    for conf in HudiTableConfig::iter() {
        hudi_configs.validate(conf)?
    }

    for conf in HudiReadConfig::iter() {
        hudi_configs.validate(conf)?
    }

    // additional validation
    let table_version: isize = hudi_configs.get(TableVersion)?.into();
    if table_version != 6 && table_version != 8 {
        return Err(CoreError::Unsupported(format!(
            "Only support table version 6 and 8. Found: {}",
            table_version
        )));
    }

    let timeline_layout_version: isize = hudi_configs.get(TimelineLayoutVersion)?.into();
    // Table version 6 uses layout version 1
    // Table version 8 uses layout version 2
    let expected_layout_version = if table_version >= 8 { 2 } else { 1 };
    if timeline_layout_version != expected_layout_version {
        return Err(CoreError::Unsupported(format!(
            "Table version {} expects timeline layout version {}. Found: {}",
            table_version, expected_layout_version, timeline_layout_version
        )));
    }

    let drops_partition_cols = hudi_configs.get_or_default(DropsPartitionFields).into();
    if drops_partition_cols {
        return Err(CoreError::Unsupported(format!(
            "Only support when `{}` is disabled",
            DropsPartitionFields.as_ref()
        )));
    }

    RecordMerger::validate_configs(hudi_configs)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::table::HudiTableConfig::{
        TableName, TableType, TableVersion, TimelineLayoutVersion,
    };
    use crate::config::HudiConfigs;
    use std::collections::HashMap;

    #[test]
    fn test_table_version_5_unsupported() {
        let mut options = HashMap::new();
        options.insert(TableName.as_ref().to_string(), "test_table".to_string());
        options.insert(TableType.as_ref().to_string(), "COPY_ON_WRITE".to_string());
        options.insert(TableVersion.as_ref().to_string(), "5".to_string());
        options.insert(TimelineLayoutVersion.as_ref().to_string(), "1".to_string());

        let configs = HudiConfigs::new(options);
        let result = validate_configs(&configs);

        assert!(result.is_err());
        if let Err(CoreError::Unsupported(msg)) = result {
            assert!(
                msg.contains("Only support table version 6 and 8"),
                "Unexpected message: {}",
                msg
            );
        } else {
            panic!("Expected CoreError::Unsupported for table version 5");
        }
    }

    #[test]
    fn test_table_version_6_with_layout_1_supported() {
        let mut options = HashMap::new();
        options.insert(TableName.as_ref().to_string(), "test_table".to_string());
        options.insert(TableType.as_ref().to_string(), "COPY_ON_WRITE".to_string());
        options.insert(TableVersion.as_ref().to_string(), "6".to_string());
        options.insert(TimelineLayoutVersion.as_ref().to_string(), "1".to_string());

        let configs = HudiConfigs::new(options);
        let result = validate_configs(&configs);
        assert!(
            result.is_ok(),
            "Table version 6 with layout 1 should be supported"
        );
    }

    #[test]
    fn test_table_version_8_with_layout_2_supported() {
        let mut options = HashMap::new();
        options.insert(TableName.as_ref().to_string(), "test_table".to_string());
        options.insert(TableType.as_ref().to_string(), "COPY_ON_WRITE".to_string());
        options.insert(TableVersion.as_ref().to_string(), "8".to_string());
        options.insert(TimelineLayoutVersion.as_ref().to_string(), "2".to_string());

        let configs = HudiConfigs::new(options);
        let result = validate_configs(&configs);
        assert!(
            result.is_ok(),
            "Table version 8 with layout 2 should be supported"
        );
    }

    #[test]
    fn test_table_version_8_with_layout_1_unsupported() {
        let mut options = HashMap::new();
        options.insert(TableName.as_ref().to_string(), "test_table".to_string());
        options.insert(TableType.as_ref().to_string(), "COPY_ON_WRITE".to_string());
        options.insert(TableVersion.as_ref().to_string(), "8".to_string());
        options.insert(TimelineLayoutVersion.as_ref().to_string(), "1".to_string());

        let configs = HudiConfigs::new(options);
        let result = validate_configs(&configs);

        assert!(result.is_err());
        if let Err(CoreError::Unsupported(msg)) = result {
            assert!(
                msg.contains("expects timeline layout version 2"),
                "Unexpected message: {}",
                msg
            );
        } else {
            panic!("Expected CoreError::Unsupported for v8 with layout 1");
        }
    }

    #[test]
    fn test_table_version_6_with_layout_2_unsupported() {
        let mut options = HashMap::new();
        options.insert(TableName.as_ref().to_string(), "test_table".to_string());
        options.insert(TableType.as_ref().to_string(), "COPY_ON_WRITE".to_string());
        options.insert(TableVersion.as_ref().to_string(), "6".to_string());
        options.insert(TimelineLayoutVersion.as_ref().to_string(), "2".to_string());

        let configs = HudiConfigs::new(options);
        let result = validate_configs(&configs);

        assert!(result.is_err());
        if let Err(CoreError::Unsupported(msg)) = result {
            assert!(
                msg.contains("expects timeline layout version 1"),
                "Unexpected message: {}",
                msg
            );
        } else {
            panic!("Expected CoreError::Unsupported for v6 with layout 2");
        }
    }
}
