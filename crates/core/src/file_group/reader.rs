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
use crate::config::HudiConfigs;
use crate::file_group::FileSlice;
use crate::storage::Storage;
use anyhow::Result;
use arrow_array::RecordBatch;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FileGroupReader {
    storage: Arc<Storage>,
}

impl FileGroupReader {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self { storage }
    }

    pub fn new_with_properties(
        base_url: Arc<url::Url>,
        options: Arc<HashMap<String, String>>,
        hudi_configs: Arc<HudiConfigs>,
    ) -> Result<Self> {
        let storage = Storage::new_with_properties(base_url.clone(), options, hudi_configs)?;
        Ok(Self { storage })
    }

    pub async fn read_file_slice_by_path_unchecked(
        &self,
        relative_path: &str,
    ) -> Result<RecordBatch> {
        self.storage.get_parquet_file_data(relative_path).await
    }

    pub async fn read_file_slice_unchecked(&self, file_slice: &FileSlice) -> Result<RecordBatch> {
        self.read_file_slice_by_path_unchecked(&file_slice.base_file_relative_path())
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::table::HudiTableConfig;
    use std::collections::HashMap;
    use url::Url;

    fn create_test_storage() -> Arc<Storage> {
        let base_uri = "file:///tmp/table";
        let base_url = Arc::new(Url::parse(base_uri).unwrap());
        let options = HashMap::from([("foo".to_string(), "bar".to_string())]);
        let hudi_configs = HudiConfigs::new(HashMap::from_iter(vec![(
            HudiTableConfig::BasePath.as_ref().to_string(),
            base_uri.to_string(),
        )]));
        Storage::new_with_properties(base_url, Arc::new(options), Arc::new(hudi_configs)).unwrap()
    }

    #[test]
    fn test_file_group_reader_json_serialization() {
        let storage = create_test_storage();
        let reader = FileGroupReader { storage };

        // Serialize to JSON
        let serialized = serde_json::to_string(&reader).unwrap();

        // Deserialize from JSON
        let deserialized: FileGroupReader = serde_json::from_str(&serialized).unwrap();

        assert_eq!(reader.storage.base_url, deserialized.storage.base_url);
        assert_eq!(reader.storage.options, deserialized.storage.options);
        assert_eq!(
            reader.storage.hudi_configs,
            deserialized.storage.hudi_configs
        );
    }
}
