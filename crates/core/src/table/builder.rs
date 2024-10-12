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

use crate::config::{HudiConfigs, HUDI_CONF_DIR};
use crate::storage::utils::{parse_config_data, parse_uri};
use crate::storage::Storage;
use crate::table::fs_view::FileSystemView;
use crate::table::timeline::Timeline;
use crate::table::Table;
use anyhow::Context;
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use url::Url;

#[derive(Debug)]
pub struct TableBuilder {
    base_url: Url,
    hudi_options: Option<HashMap<String, String>>,
    storage_options: Option<HashMap<String, String>>,
}

impl TableBuilder {
    pub fn from_uri(uri: &str) -> Self {
        let base_url = parse_uri(uri).unwrap(); // TODO: handle err
        TableBuilder {
            base_url,
            storage_options: None,
            hudi_options: None,
        }
    }

    pub fn with_options<I, K, V>(mut self,  all_options: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        let mut hudi_options = HashMap::new();
        let mut storage_options = HashMap::new();

        for (k, v) in all_options {
            if k.as_ref().starts_with("hoodie.") {
                hudi_options.insert(k.as_ref().to_string(), v.into());
            } else {
                storage_options.insert(k.as_ref().to_string(), v.into());
            }
        }

        self.storage_options = Some(storage_options);
        self.hudi_options = Some(hudi_options);
        self
    }

    pub fn with_hudi_options(mut self, hudi_options: HashMap<String, String>) -> Self {
        self.hudi_options = Some(hudi_options);
        self
    }

    pub fn with_storage_options(mut self, storage_options: HashMap<String, String>) -> Self {
        self.storage_options = Some(storage_options);
        self
    }

    pub async fn build(self) -> anyhow::Result<Table> {
        let base_url = Arc::new(self.base_url);

        let hudi_options = self.hudi_options.unwrap_or_default().clone();
        let mut storage_options = self.storage_options.unwrap_or_default().clone();

        Self::load_storage_options(&mut storage_options);

        let hudi_configs =
            Self::load_hudi_configs(base_url.clone(), hudi_options, &storage_options)
                .await
                .context("Failed to load table properties")?;

        let hudi_configs = Arc::from(hudi_configs);
        let storage_options = Arc::from(storage_options);

        let timeline = Timeline::new(
            base_url.clone(),
            storage_options.clone(),
            hudi_configs.clone(),
        )
        .await
        .context("Failed to load timeline")?;

        let file_system_view = FileSystemView::new(
            base_url.clone(),
            storage_options.clone(),
            hudi_configs.clone(),
        )
        .await
        .context("Failed to load file system view")?;

        Ok(Table {
            base_url,
            configs: hudi_configs,
            extra_options: storage_options,
            timeline,
            file_system_view,
        })
    }

    fn load_storage_options(storage_options: &mut HashMap<String, String>) {
        Self::imbue_cloud_env_vars(storage_options);
    }

    async fn load_hudi_configs(
        base_url: Arc<Url>,
        mut hudi_options: HashMap<String, String>,
        storage_configs: &HashMap<String, String>,
    ) -> anyhow::Result<HudiConfigs> {
        let storage = Storage::new(base_url, storage_configs)?;

        Self::imbue_table_properties(&mut hudi_options, storage.clone()).await?;

        Self::imbue_global_hudi_configs(&mut hudi_options, storage.clone()).await?;

        let hudi_configs = HudiConfigs::new(hudi_options);

        Table::validate_configs(&hudi_configs).expect("Hudi configs are not valid.");
        Ok(hudi_configs)
    }

    fn imbue_cloud_env_vars(options: &mut HashMap<String, String>) {
        const PREFIXES: [&str; 3] = ["AWS_", "AZURE_", "GOOGLE_"];

        for (key, value) in env::vars() {
            if PREFIXES.iter().any(|prefix| key.starts_with(prefix))
                && !options.contains_key(&key.to_ascii_lowercase())
            {
                options.insert(key.to_ascii_lowercase(), value);
            }
        }
    }

    async fn imbue_table_properties(
        options: &mut HashMap<String, String>,
        storage: Arc<Storage>,
    ) -> anyhow::Result<()> {
        let bytes = storage.get_file_data(".hoodie/hoodie.properties").await?;
        let table_properties = parse_config_data(&bytes, "=").await?;

        // TODO: handle the case where the same key is present in both table properties and options
        for (k, v) in table_properties {
            options.insert(k.to_string(), v.to_string());
        }

        Ok(())
    }

    async fn imbue_global_hudi_configs(
        options: &mut HashMap<String, String>,
        storage: Arc<Storage>,
    ) -> anyhow::Result<()> {
        let global_config_path = env::var(HUDI_CONF_DIR)
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("/etc/hudi/conf"))
            .join("hudi-defaults.conf");

        if let Ok(bytes) = storage
            .get_file_data_from_absolute_path(global_config_path.to_str().unwrap())
            .await
        {
            if let Ok(global_configs) = parse_config_data(&bytes, " \t=").await {
                for (key, value) in global_configs {
                    if key.starts_with("hoodie.") && !options.contains_key(&key) {
                        options.insert(key.to_string(), value.to_string());
                    }
                }
            }
        }

        Ok(())
    }
}
