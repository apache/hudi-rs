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

use crate::config::table::HudiTableConfig;
use crate::config::utils::{parse_data_for_options, split_hudi_options_from_others};
use crate::config::{HudiConfigs, HUDI_CONF_DIR};
use crate::storage::Storage;
use crate::table::fs_view::FileSystemView;
use crate::table::timeline::Timeline;
use crate::table::Table;
use anyhow::Context;
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct TableBuilder {
    base_uri: String,
    hudi_options: Option<HashMap<String, String>>,
    storage_options: Option<HashMap<String, String>>,
}

impl TableBuilder {
    pub fn from_base_uri(base_uri: &str) -> Self {
        TableBuilder {
            base_uri: base_uri.to_string(),
            storage_options: None,
            hudi_options: None,
        }
    }

    pub fn with_options<I, K, V>(self, all_options: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        let mut hudi_options = HashMap::new();
        let mut storage_options = HashMap::new();

        let (hudi_opts, others) = split_hudi_options_from_others(all_options);
        hudi_options.extend(hudi_opts);
        storage_options.extend(others);

        self.with_hudi_options(hudi_options)
            .with_storage_options(storage_options)
    }

    pub fn with_hudi_options(mut self, hudi_options: HashMap<String, String>) -> Self {
        match self.hudi_options {
            None => self.hudi_options = Some(hudi_options),
            Some(options) => self.hudi_options = Some(Self::merge(options, hudi_options)),
        }
        self
    }

    pub fn with_storage_options(mut self, storage_options: HashMap<String, String>) -> Self {
        match self.storage_options {
            None => self.storage_options = Some(storage_options),
            Some(options) => self.storage_options = Some(Self::merge(options, storage_options)),
        }
        self
    }

    pub async fn build(self) -> anyhow::Result<Table> {
        let hudi_options = self.hudi_options.unwrap_or_default().clone();
        let mut storage_options = self.storage_options.unwrap_or_default().clone();

        Self::load_storage_options(&mut storage_options);

        let hudi_configs =
            Self::load_hudi_configs(self.base_uri.clone(), hudi_options, &storage_options)
                .await
                .context("Failed to load table properties")?;

        let hudi_configs = Arc::from(hudi_configs);
        let storage_options = Arc::from(storage_options);

        let timeline = Timeline::new(hudi_configs.clone(), storage_options.clone())
            .await
            .context("Failed to load timeline")?;

        let file_system_view = FileSystemView::new(hudi_configs.clone(), storage_options.clone())
            .await
            .context("Failed to load file system view")?;

        Ok(Table {
            hudi_configs,
            storage_options,
            timeline,
            file_system_view,
        })
    }

    fn load_storage_options(storage_options: &mut HashMap<String, String>) {
        Self::imbue_cloud_env_vars(storage_options);
    }

    async fn load_hudi_configs(
        base_uri: String,
        mut hudi_options: HashMap<String, String>,
        storage_options: &HashMap<String, String>,
    ) -> anyhow::Result<HudiConfigs> {
        hudi_options.insert(
            HudiTableConfig::BasePath.as_ref().to_string(),
            base_uri.to_string(),
        );

        // create a [Storage] instance to load properties from storage layer.
        let storage = Storage::new(
            Arc::new(storage_options.clone()),
            Arc::new(HudiConfigs::new(&hudi_options)),
        )?;

        Self::imbue_table_properties(&mut hudi_options, storage.clone()).await?;

        Self::imbue_global_hudi_configs_if_not_present(&mut hudi_options, storage.clone()).await?;

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
        let table_properties = parse_data_for_options(&bytes, "=")?;

        // TODO: handle the case where the same key is present in both table properties and options
        for (k, v) in table_properties {
            options.insert(k.to_string(), v.to_string());
        }

        Ok(())
    }

    async fn imbue_global_hudi_configs_if_not_present(
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
            if let Ok(global_configs) = parse_data_for_options(&bytes, " \t=") {
                for (key, value) in global_configs {
                    if key.starts_with("hoodie.") && !options.contains_key(&key) {
                        options.insert(key.to_string(), value.to_string());
                    }
                }
            }
        }

        Ok(())
    }

    fn merge(
        map1: HashMap<String, String>,
        map2: HashMap<String, String>,
    ) -> HashMap<String, String> {
        map1.into_iter().chain(map2).collect()
    }
}

#[cfg(test)]
mod tests {
    use crate::table::TableBuilder;
    use std::collections::HashMap;
    #[test]
    fn test_build_from_mixed_options() {
        let options = vec![
            ("hoodie.option1", "value1"),
            ("AWS_REGION", "us-east-1"),
            ("hoodie.option3", "value3"),
            ("AWS_ENDPOINT", "s3.us-east-1.amazonaws.com"),
        ];
        let builder = TableBuilder::from_base_uri("./tmp").with_options(options);
        let hudi_options = builder.hudi_options.clone().unwrap();
        let storage_options = builder.storage_options.clone().unwrap();
        assert_eq!(hudi_options.len(), 2);
        assert_eq!(hudi_options["hoodie.option1"], "value1");
        assert_eq!(hudi_options["hoodie.option3"], "value3");
        assert_eq!(storage_options.len(), 2);
        assert_eq!(storage_options["AWS_REGION"], "us-east-1");
        assert_eq!(
            storage_options["AWS_ENDPOINT"],
            "s3.us-east-1.amazonaws.com"
        );
    }

    #[test]
    fn test_build_from_explicit_options() {
        let hudi_options = HashMap::from([
            ("hoodie.option1".to_string(), "value1".to_string()),
            ("hoodie.option3".to_string(), "value3".to_string()),
        ]);
        let storage_options = HashMap::from([
            ("AWS_REGION".to_string(), "us-east-1".to_string()),
            (
                "AWS_ENDPOINT".to_string(),
                "s3.us-east-1.amazonaws.com".to_string(),
            ),
        ]);
        let builder = TableBuilder::from_base_uri("./tmp")
            .with_hudi_options(hudi_options)
            .with_storage_options(storage_options);
        let hudi_options = builder.hudi_options.clone().unwrap();
        let storage_options = builder.storage_options.clone().unwrap();
        assert_eq!(hudi_options.len(), 2);
        assert_eq!(hudi_options["hoodie.option1"], "value1");
        assert_eq!(hudi_options["hoodie.option3"], "value3");
        assert_eq!(storage_options.len(), 2);
        assert_eq!(storage_options["AWS_REGION"], "us-east-1");
        assert_eq!(
            storage_options["AWS_ENDPOINT"],
            "s3.us-east-1.amazonaws.com"
        );
    }

    #[test]
    fn test_build_from_explicit_options_chained() {
        let builder = TableBuilder::from_base_uri("./tmp")
            .with_hudi_options(HashMap::from([(
                "hoodie.option1".to_string(),
                "value1".to_string(),
            )]))
            .with_hudi_options(HashMap::from([(
                "hoodie.option3".to_string(),
                "value3".to_string(),
            )]))
            .with_storage_options(HashMap::from([(
                "AWS_REGION".to_string(),
                "us-east-1".to_string(),
            )]))
            .with_storage_options(HashMap::from([(
                "AWS_ENDPOINT".to_string(),
                "s3.us-east-1.amazonaws.com".to_string(),
            )]));
        let hudi_options = builder.hudi_options.clone().unwrap();
        let storage_options = builder.storage_options.clone().unwrap();
        assert_eq!(hudi_options.len(), 2);
        assert_eq!(hudi_options["hoodie.option1"], "value1");
        assert_eq!(hudi_options["hoodie.option3"], "value3");
        assert_eq!(storage_options.len(), 2);
        assert_eq!(storage_options["AWS_REGION"], "us-east-1");
        assert_eq!(
            storage_options["AWS_ENDPOINT"],
            "s3.us-east-1.amazonaws.com"
        );
    }
}
