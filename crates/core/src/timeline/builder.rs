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

use crate::config::internal::HudiInternalConfig::TimelineArchivedReadEnabled;
use crate::config::table::HudiTableConfig::{TableVersion, TimelineLayoutVersion};
use crate::config::HudiConfigs;
use crate::error::CoreError;
use crate::storage::Storage;
use crate::timeline::loader::TimelineLoader;
use crate::timeline::Timeline;
use crate::Result;
use std::sync::Arc;

pub struct TimelineBuilder {
    hudi_configs: Arc<HudiConfigs>,
    storage: Arc<Storage>,
}

impl TimelineBuilder {
    pub fn new(hudi_configs: Arc<HudiConfigs>, storage: Arc<Storage>) -> Self {
        Self {
            hudi_configs,
            storage,
        }
    }

    pub async fn build(self) -> Result<Timeline> {
        let (active_loader, archived_loader) = self.resolve_loader_config()?;
        let timeline = Timeline::new(
            self.hudi_configs,
            self.storage,
            active_loader,
            archived_loader,
        );
        Ok(timeline)
    }

    fn resolve_loader_config(&self) -> Result<(TimelineLoader, Option<TimelineLoader>)> {
        // Check if archived timeline reading is enabled
        let archived_enabled: bool = self
            .hudi_configs
            .get_or_default(TimelineArchivedReadEnabled)
            .into();

        let table_version: isize = match self.hudi_configs.get(TableVersion) {
            Ok(v) => v.into(),
            Err(_) => {
                let archived_loader = if archived_enabled {
                    Some(TimelineLoader::new_layout_one_archived(
                        self.hudi_configs.clone(),
                        self.storage.clone(),
                    ))
                } else {
                    None
                };
                return Ok((
                    TimelineLoader::new_layout_one_active(
                        self.hudi_configs.clone(),
                        self.storage.clone(),
                    ),
                    archived_loader,
                ));
            }
        };

        let layout_version: isize = self
            .hudi_configs
            .try_get(TimelineLayoutVersion)
            .map(|v| v.into())
            .unwrap_or_else(|| if table_version == 8 { 2isize } else { 1isize });

        match layout_version {
            1 => {
                if !(6..=8).contains(&table_version) {
                    return Err(CoreError::Unsupported(format!(
                        "Unsupported table version {} with timeline layout version {}",
                        table_version, layout_version
                    )));
                }
                let archived_loader = if archived_enabled {
                    Some(TimelineLoader::new_layout_one_archived(
                        self.hudi_configs.clone(),
                        self.storage.clone(),
                    ))
                } else {
                    None
                };
                Ok((
                    TimelineLoader::new_layout_one_active(
                        self.hudi_configs.clone(),
                        self.storage.clone(),
                    ),
                    archived_loader,
                ))
            }
            2 => {
                if table_version < 8 {
                    return Err(CoreError::Unsupported(format!(
                        "Unsupported table version {} with timeline layout version {}",
                        table_version, layout_version
                    )));
                }
                let archived_loader = if archived_enabled {
                    Some(TimelineLoader::new_layout_two_archived(
                        self.hudi_configs.clone(),
                        self.storage.clone(),
                    ))
                } else {
                    None
                };
                Ok((
                    TimelineLoader::new_layout_two_active(
                        self.hudi_configs.clone(),
                        self.storage.clone(),
                    ),
                    archived_loader,
                ))
            }
            _ => Err(CoreError::Unsupported(format!(
                "Unsupported timeline layout version: {}",
                layout_version
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::table::HudiTableConfig;
    use std::collections::HashMap;

    fn create_test_configs(options: HashMap<String, String>) -> Arc<HudiConfigs> {
        let mut opts = options;
        opts.entry(HudiTableConfig::BasePath.as_ref().to_string())
            .or_insert("/tmp/test".to_string());
        Arc::new(HudiConfigs::new(opts))
    }

    fn create_test_storage(configs: Arc<HudiConfigs>) -> Arc<Storage> {
        Storage::new(Arc::new(HashMap::new()), configs).unwrap()
    }

    #[tokio::test]
    async fn test_build_without_table_version() {
        let configs = create_test_configs(HashMap::new());
        let storage = create_test_storage(configs.clone());
        let builder = TimelineBuilder::new(configs, storage);

        let timeline = builder.build().await.unwrap();
        // Should default to layout one active
        assert!(!timeline.active_loader.is_layout_two_active());
        assert!(timeline.archived_loader.is_none());
    }

    #[tokio::test]
    async fn test_build_with_table_version_6() {
        let mut options = HashMap::new();
        options.insert(
            HudiTableConfig::TableVersion.as_ref().to_string(),
            "6".to_string(),
        );
        let configs = create_test_configs(options);
        let storage = create_test_storage(configs.clone());
        let builder = TimelineBuilder::new(configs, storage);

        let timeline = builder.build().await.unwrap();
        // v6 should use layout one
        assert!(!timeline.active_loader.is_layout_two_active());
        assert!(timeline.archived_loader.is_none());
    }

    #[tokio::test]
    async fn test_build_with_table_version_8_defaults_to_layout_2() {
        let mut options = HashMap::new();
        options.insert(
            HudiTableConfig::TableVersion.as_ref().to_string(),
            "8".to_string(),
        );
        let configs = create_test_configs(options);
        let storage = create_test_storage(configs.clone());
        let builder = TimelineBuilder::new(configs, storage);

        let timeline = builder.build().await.unwrap();
        // v8 without explicit layout version defaults to layout 2
        assert!(timeline.active_loader.is_layout_two_active());
        assert!(timeline.archived_loader.is_none());
    }

    #[tokio::test]
    async fn test_build_with_explicit_layout_version_1() {
        let mut options = HashMap::new();
        options.insert(
            HudiTableConfig::TableVersion.as_ref().to_string(),
            "8".to_string(),
        );
        options.insert(
            HudiTableConfig::TimelineLayoutVersion.as_ref().to_string(),
            "1".to_string(),
        );
        let configs = create_test_configs(options);
        let storage = create_test_storage(configs.clone());
        let builder = TimelineBuilder::new(configs, storage);

        let timeline = builder.build().await.unwrap();
        // Explicit layout version 1
        assert!(!timeline.active_loader.is_layout_two_active());
    }

    #[tokio::test]
    async fn test_build_with_archived_enabled() {
        let mut options = HashMap::new();
        options.insert(
            HudiTableConfig::TableVersion.as_ref().to_string(),
            "8".to_string(),
        );
        options.insert(
            TimelineArchivedReadEnabled.as_ref().to_string(),
            "true".to_string(),
        );
        let configs = create_test_configs(options);
        let storage = create_test_storage(configs.clone());
        let builder = TimelineBuilder::new(configs, storage);

        let timeline = builder.build().await.unwrap();
        assert!(timeline.active_loader.is_layout_two_active());
        assert!(timeline.archived_loader.is_some());
        assert!(timeline
            .archived_loader
            .as_ref()
            .unwrap()
            .is_layout_two_archived());
    }

    #[tokio::test]
    async fn test_build_layout_1_with_archived() {
        let mut options = HashMap::new();
        options.insert(
            HudiTableConfig::TableVersion.as_ref().to_string(),
            "7".to_string(),
        );
        options.insert(
            TimelineArchivedReadEnabled.as_ref().to_string(),
            "true".to_string(),
        );
        let configs = create_test_configs(options);
        let storage = create_test_storage(configs.clone());
        let builder = TimelineBuilder::new(configs, storage);

        let timeline = builder.build().await.unwrap();
        assert!(!timeline.active_loader.is_layout_two_active());
        assert!(timeline.archived_loader.is_some());
        assert!(!timeline
            .archived_loader
            .as_ref()
            .unwrap()
            .is_layout_two_archived());
    }

    #[tokio::test]
    async fn test_unsupported_table_version_with_layout_1() {
        let mut options = HashMap::new();
        options.insert(
            HudiTableConfig::TableVersion.as_ref().to_string(),
            "5".to_string(),
        );
        options.insert(
            HudiTableConfig::TimelineLayoutVersion.as_ref().to_string(),
            "1".to_string(),
        );
        let configs = create_test_configs(options);
        let storage = create_test_storage(configs.clone());
        let builder = TimelineBuilder::new(configs, storage);

        let result = builder.build().await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unsupported table version 5"));
    }

    #[tokio::test]
    async fn test_unsupported_table_version_with_layout_2() {
        let mut options = HashMap::new();
        options.insert(
            HudiTableConfig::TableVersion.as_ref().to_string(),
            "7".to_string(),
        );
        options.insert(
            HudiTableConfig::TimelineLayoutVersion.as_ref().to_string(),
            "2".to_string(),
        );
        let configs = create_test_configs(options);
        let storage = create_test_storage(configs.clone());
        let builder = TimelineBuilder::new(configs, storage);

        let result = builder.build().await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unsupported table version 7"));
    }

    #[tokio::test]
    async fn test_unsupported_layout_version() {
        let mut options = HashMap::new();
        options.insert(
            HudiTableConfig::TableVersion.as_ref().to_string(),
            "8".to_string(),
        );
        options.insert(
            HudiTableConfig::TimelineLayoutVersion.as_ref().to_string(),
            "3".to_string(),
        );
        let configs = create_test_configs(options);
        let storage = create_test_storage(configs.clone());
        let builder = TimelineBuilder::new(configs, storage);

        let result = builder.build().await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unsupported timeline layout version: 3"));
    }
}
