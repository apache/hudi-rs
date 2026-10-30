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

use std::collections::{HashMap, HashSet};

use arrow::array::StringArray;

use crate::Result;
use crate::error::CoreError;
use crate::index::{HoodieIndex, HoodieKey, RecordLocation};
use crate::keygen::record_key::record_keys_for_batch;
use crate::metadata::meta_field::MetaField;
use crate::table::{ReadOptions, Table};

/// A single-node index that scans latest file slices to locate record keys.
#[derive(Clone, Debug, Default)]
pub struct SimpleIndex;

impl HoodieIndex for SimpleIndex {
    async fn tag_location(
        &self,
        table: &Table,
        keys: &[HoodieKey],
    ) -> Result<HashMap<HoodieKey, Option<RecordLocation>>> {
        let requested = keys
            .iter()
            .map(|key| key.record_key.clone())
            .collect::<HashSet<_>>();
        let mut locations = keys
            .iter()
            .cloned()
            .map(|key| (key, None))
            .collect::<HashMap<_, _>>();
        if requested.is_empty() {
            return Ok(locations);
        }
        let reader = table
            .create_file_group_reader_with_options(None, std::iter::empty::<(&str, &str)>())
            .await?;
        let mut by_record_key: HashMap<String, RecordLocation> = HashMap::new();
        for slice in table.get_file_slices(&ReadOptions::new()).await? {
            let batch = reader.read_file_slice(&slice, &ReadOptions::new()).await?;
            let location = RecordLocation {
                file_id: slice.file_id().to_string(),
                instant_time: table.timeline.get_latest_commit_timestamp()?,
                partition_path: slice.partition_path.clone(),
            };
            let mut tag = |key: &str| {
                if requested.contains(key) {
                    by_record_key.insert(key.to_string(), location.clone());
                }
            };
            match batch.column_by_name(MetaField::RecordKey.as_ref()) {
                Some(column) => column
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        CoreError::Unsupported(format!(
                            "record key field '{}' must be Utf8",
                            MetaField::RecordKey.as_ref()
                        ))
                    })?
                    .iter()
                    .flatten()
                    .for_each(tag),
                None => record_keys_for_batch(&table.hudi_configs, &batch)?
                    .iter()
                    .for_each(|key| tag(key)),
            }
        }
        for (hoodie_key, slot) in locations.iter_mut() {
            if let Some(location) = by_record_key.get(&hoodie_key.record_key) {
                *slot = Some(location.clone());
            }
        }
        Ok(locations)
    }
}
