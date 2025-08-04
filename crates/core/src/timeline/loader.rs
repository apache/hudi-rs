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
use crate::metadata::HUDI_METADATA_DIR;
use crate::storage::Storage;
use crate::timeline::instant::Instant;
use crate::timeline::selector::TimelineSelector;
use crate::Result;
use log::debug;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum TimelineLoader {
    LayoutOneActive(Arc<Storage>),
    LayoutOneArchived(Arc<Storage>),
    LayoutTwoActive(Arc<Storage>),
    LayoutTwoCompacted(Arc<Storage>),
}

impl TimelineLoader {
    pub async fn load_instants(
        &self,
        selector: &TimelineSelector,
        desc: bool,
    ) -> Result<Vec<Instant>> {
        match self {
            TimelineLoader::LayoutOneActive(storage) => {
                let files = storage.list_files(Some(HUDI_METADATA_DIR)).await?;
                let mut instants = Vec::with_capacity(files.len() / 3);

                for file_info in files {
                    match selector.try_create_instant(file_info.name.as_str()) {
                        Ok(instant) => instants.push(instant),
                        Err(e) => {
                            debug!(
                                "Instant not created from file {:?} due to: {:?}",
                                file_info, e
                            );
                        }
                    }
                }

                instants.sort_unstable();
                instants.shrink_to_fit();

                if desc {
                    Ok(instants.into_iter().rev().collect())
                } else {
                    Ok(instants)
                }
            }
            _ => Err(CoreError::Unsupported(
                "Loading from this timeline layout is not implemented yet.".to_string(),
            )),
        }
    }

    pub async fn load_archived_instants(
        &self,
        _selector: &TimelineSelector,
        _desc: bool,
    ) -> Result<Vec<Instant>> {
        match self {
            TimelineLoader::LayoutOneArchived(_) => Err(CoreError::Unsupported(
                "Archived timeline for layout v1 not implemented yet".to_string(),
            )),
            TimelineLoader::LayoutTwoCompacted(_) => Err(CoreError::Unsupported(
                "Compacted timeline for layout v2 not implemented yet".to_string(),
            )),
            _ => Ok(Vec::new()), // Active loaders don't have archived parts.
        }
    }
}
