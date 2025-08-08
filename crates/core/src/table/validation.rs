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
    if hudi_configs
        .get_or_default(SkipConfigValidation)
        .to::<bool>()
    {
        return Ok(());
    }

    for conf in HudiTableConfig::iter() {
        hudi_configs.validate(conf)?
    }

    for conf in HudiReadConfig::iter() {
        hudi_configs.validate(conf)?
    }

    // additional validation
    let table_version = hudi_configs.get(TableVersion)?.to::<isize>();
    if table_version != 6 {
        return Err(CoreError::Unsupported(
            "Only support table version 6.".to_string(),
        ));
    }

    let timeline_layout_version = hudi_configs.get(TimelineLayoutVersion)?.to::<isize>();
    if timeline_layout_version != 1 {
        return Err(CoreError::Unsupported(
            "Only support timeline layout version 1.".to_string(),
        ));
    }

    let drops_partition_cols = hudi_configs
        .get_or_default(DropsPartitionFields)
        .to::<bool>();
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
    use crate::config::table::HudiTableConfig::{TableName, TableType, TableVersion};
    use crate::config::HudiConfigs;
    use std::collections::HashMap;

    #[test]
    fn test_table_version_5_unsupported() {
        let mut options = HashMap::new();
        options.insert(TableName.as_ref().to_string(), "test_table".to_string());
        options.insert(TableType.as_ref().to_string(), "COPY_ON_WRITE".to_string());
        options.insert(TableVersion.as_ref().to_string(), "5".to_string());
        
        let configs = HudiConfigs::new(options);
        let result = validate_configs(&configs);
        
        assert!(result.is_err());
        if let Err(CoreError::Unsupported(msg)) = result {
            assert_eq!(msg, "Only support table version 6.");
        } else {
            panic!("Expected CoreError::Unsupported for table version 5");
        }
    }
}
