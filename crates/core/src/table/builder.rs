use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use object_store::DynObjectStore;
use url::Url;
use crate::table::Table;

pub struct TableBuilder {
    base_uri: str,
    hudi_options: Option<HashMap<String, String>>,
    storage_option: Option<HashMap<String, String>>,
    storage_backend: Option<(Arc<DynObjectStore>, Url)>
}

impl TableBuilder {
    pub fn with_hudi_options(mut self, _hudi_options: HashMap<String, String>) -> Self {
        self
    }

    pub fn with_storage_options(mut self, _storage_options: HashMap<String, String>) -> Self {
        self
    }

    pub async fn build(self) -> anyhow::Result<Table> {
        let mut table = Table::new(&self.base_uri).await;
        table
    }
}