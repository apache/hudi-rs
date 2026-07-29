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

mod simple;

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
