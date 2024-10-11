use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use anyhow::{Context};
use url::Url;
use crate::config::{HudiConfigs, HUDI_CONF_DIR};
use crate::storage::Storage;
use crate::storage::utils::{parse_config_data, parse_uri};
use crate::table::fs_view::FileSystemView;
use crate::table::Table;
use crate::table::timeline::Timeline;

#[derive(Debug)]
pub struct TableBuilder {
    base_url: Url,
    hudi_options: Option<HashMap<String, String>>,
    storage_options: Option<HashMap<String, String>>
}

impl TableBuilder {

    pub fn from_uri(uri: &str) -> Self {
        let base_url = parse_uri(uri).unwrap(); // TODO: handle err
        TableBuilder {
            base_url: base_url.into(),
            storage_options: None,
            hudi_options: None
        }
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

        let hudi_options = self.hudi_options.unwrap_or(Default::default()).clone();
        let mut storage_options = self.storage_options.unwrap_or(Default::default()).clone();

        Self::load_storage_configs(&mut storage_options);

        let hudi_configs = Self::load_hudi_configs(base_url.clone(), hudi_options, &storage_options).await
            .context("Failed to load table properties")?;

        let timeline = Timeline::new(base_url.clone(), Arc::from(storage_options.clone()), Arc::from(hudi_configs.clone()))
            .await
            .context("Failed to load timeline")?;

        let file_system_view =
            FileSystemView::new(base_url.clone(), Arc::from(storage_options.clone()), Arc::from(hudi_configs.clone()))
                .await
                .context("Failed to load file system view")?;

        Ok(Table {
            base_url,
            configs: Arc::from(hudi_configs),
            extra_options: Arc::from(storage_options),
            timeline,
            file_system_view,
        })
    }
    
    fn load_storage_configs(
        mut storage_options: &mut HashMap<String, String>
    ) {
        Self::imbue_cloud_env_vars(&mut storage_options);
    }
    
    async fn load_hudi_configs(
        base_url: Arc<Url>,
        mut hudi_options: HashMap<String, String>,
        storage_configs: &HashMap<String, String>,
    ) -> anyhow::Result<HudiConfigs>
    {
        let storage = Storage::new(base_url, &storage_configs)?;

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

#[cfg(test)]
mod tests {

    use std::collections::{HashMap};
    use std::fs::canonicalize;
    use std::path::PathBuf;
    use url::Url;

    use crate::table::builder::TableBuilder;
    use crate::table::Table;

    /// Test helper to create a new `Table` instance without validating the configuration.
    ///
    /// # Arguments
    ///
    /// * `table_dir_name` - Name of the table root directory; all under `crates/core/tests/data/`.
    #[cfg(test)]
    async fn build_test_table_without_validation(table_dir_name: &str) -> Table {
        let base_url = Url::from_file_path(
            canonicalize(PathBuf::from("tests").join("data").join(table_dir_name)).unwrap(),
        )
            .unwrap();

        TableBuilder::from_uri(base_url.as_str())
            .with_hudi_options(HashMap::from([("hoodie.internal.skip.config.validation".to_string(), "true".to_string())]))
            .build()
            .await
            .unwrap()
    }
}