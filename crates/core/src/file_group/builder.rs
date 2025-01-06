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
use crate::error::CoreError;
use crate::file_group::FileGroup;
use crate::Result;
use serde_json::{Map, Value};
use std::collections::HashSet;
use std::path::Path;

pub fn build_file_groups(commit_metadata: &Map<String, Value>) -> Result<HashSet<FileGroup>> {
    let partition_stats = commit_metadata
        .get("partitionToWriteStats")
        .and_then(|v| v.as_object())
        .ok_or(CoreError::CommitMetadata("Invalid or missing partitionToWriteStats object".into()))?;

    let mut file_groups = HashSet::new();

    for (partition, write_stats_array) in partition_stats {
        let write_stats = write_stats_array
            .as_array()
            .ok_or(CoreError::CommitMetadata("Invalid write stats array".into()))?;

        let partition = (!partition.is_empty()).then(|| partition.to_string());

        for stat in write_stats {
            let file_group_id = stat
                .get("fileId")
                .and_then(|v| v.as_str())
                .ok_or(CoreError::CommitMetadata("Invalid fileId in write stats".into()))?;

            let path = stat
                .get("path")
                .and_then(|v| v.as_str())
                .ok_or(CoreError::CommitMetadata("Invalid path in write stats".into()))?;

            let file_name = Path::new(path)
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or(CoreError::CommitMetadata("Invalid file name in path".into()))?;

            let file_group = FileGroup::new_with_base_file_name(
                file_group_id.to_string(),
                partition.clone(),
                file_name,
            )?;
            file_groups.insert(file_group);
        }
    }

    Ok(file_groups)
}

pub fn build_replaced_file_groups(
    commit_metadata: &Map<String, Value>,
) -> Result<HashSet<FileGroup>> {
    let partition_to_replaced = commit_metadata
        .get("partitionToReplaceFileIds")
        .and_then(|v| v.as_object())
        .ok_or(CoreError::CommitMetadata(
            "Invalid or missing partitionToReplaceFileIds object".into(),
        ))?;

    let mut file_groups = HashSet::new();

    for (partition, file_group_ids_value) in partition_to_replaced {
        let file_group_ids = file_group_ids_value
            .as_array()
            .ok_or(CoreError::CommitMetadata("Invalid file group ids array".into()))?;

        let partition = (!partition.is_empty()).then(|| partition.to_string());

        for file_group_id in file_group_ids {
            let id = file_group_id
                .as_str()
                .ok_or(CoreError::CommitMetadata("Invalid file group id string".into()))?;

            let file_group = FileGroup::new(id.to_string(), partition.clone());
            file_groups.insert(file_group);
        }
    }

    Ok(file_groups)
}
