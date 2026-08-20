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
//! Record-location indexes used by write operations.

mod record;
mod simple;

pub use record::{RecordIndex, is_record_index_enabled};
pub use simple::SimpleIndex;

use std::collections::HashMap;

use crate::Result;
use crate::table::Table;

/// A record key and its partition path.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct HoodieKey {
    pub record_key: String,
    pub partition_path: String,
}

/// Location of a record in the latest visible file slice.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordLocation {
    pub file_id: String,
    pub instant_time: String,
    pub partition_path: String,
}

/// Tags incoming record keys with their latest visible location.
#[allow(async_fn_in_trait)]
pub trait HoodieIndex {
    async fn tag_location(
        &self,
        table: &Table,
        keys: &[HoodieKey],
    ) -> Result<HashMap<HoodieKey, Option<RecordLocation>>>;
}

/// Selected write-path index: RLI when enabled, otherwise SimpleIndex.
pub enum TableIndex {
    Record(RecordIndex),
    Simple(SimpleIndex),
}

impl HoodieIndex for TableIndex {
    async fn tag_location(
        &self,
        table: &Table,
        keys: &[HoodieKey],
    ) -> Result<HashMap<HoodieKey, Option<RecordLocation>>> {
        match self {
            Self::Record(index) => index.tag_location(table, keys).await,
            Self::Simple(index) => index.tag_location(table, keys).await,
        }
    }
}

/// Select the write-path index: RLI when enabled, otherwise SimpleIndex.
pub fn for_table(table: &Table) -> TableIndex {
    if is_record_index_enabled(table) {
        TableIndex::Record(RecordIndex)
    } else {
        TableIndex::Simple(SimpleIndex)
    }
}

/// Like [`for_table`], but also confirms the record index actually has file
/// groups before choosing it — an advertised-but-unpopulated index answers
/// every lookup with "not found", which turns updates into duplicate inserts.
/// Java applies the same two-part check before tagging.
pub async fn for_table_checked(table: &Table) -> TableIndex {
    if record::record_index_has_file_groups(table).await {
        TableIndex::Record(RecordIndex)
    } else {
        TableIndex::Simple(SimpleIndex)
    }
}
