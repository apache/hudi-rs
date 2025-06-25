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
use crate::config::HudiConfigs;
use crate::error::CoreError;
use crate::schema::delete::{transform_delete_batch_schema, transform_delete_record_batch};
use crate::Result;
use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use arrow_select::concat::concat_batches;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct RecordBatches {
    pub(crate) data_batches: Vec<RecordBatch>,
    pub(crate) delete_batches: Vec<(RecordBatch, String)>,
    num_data_rows: usize,
    num_delete_rows: usize,
}

impl Default for RecordBatches {
    fn default() -> Self {
        Self::new()
    }
}

impl RecordBatches {
    pub fn new() -> Self {
        Self {
            data_batches: Vec::new(),
            delete_batches: Vec::new(),
            num_data_rows: 0,
            num_delete_rows: 0,
        }
    }

    pub fn new_with_capacity(num_data_batches: usize, num_delete_batches: usize) -> Self {
        Self {
            data_batches: Vec::with_capacity(num_data_batches),
            delete_batches: Vec::with_capacity(num_delete_batches),
            num_data_rows: 0,
            num_delete_rows: 0,
        }
    }

    pub fn new_with_data_batches<I>(data_batches: I) -> Self
    where
        I: IntoIterator<Item = RecordBatch>,
    {
        let mut record_batches = Self::new();
        for batch in data_batches {
            record_batches.push_data_batch(batch);
        }
        record_batches
    }

    pub fn num_data_batches(&self) -> usize {
        self.data_batches.len()
    }

    pub fn num_delete_batches(&self) -> usize {
        self.delete_batches.len()
    }

    pub fn num_data_rows(&self) -> usize {
        self.num_data_rows
    }

    pub fn num_delete_rows(&self) -> usize {
        self.num_delete_rows
    }

    pub fn push_data_batch(&mut self, batch: RecordBatch) {
        self.num_data_rows += batch.num_rows();
        self.data_batches.push(batch);
    }

    pub fn push_delete_batch(&mut self, batch: RecordBatch, instant_time: String) {
        self.num_delete_rows += batch.num_rows();
        self.delete_batches.push((batch, instant_time));
    }

    pub fn extend(&mut self, other: RecordBatches) {
        self.num_data_rows += other.num_data_rows;
        self.data_batches.extend(other.data_batches);
        self.num_delete_rows += other.num_delete_rows;
        self.delete_batches.extend(other.delete_batches);
    }

    pub fn concat_data_batches(&self, schema: SchemaRef) -> Result<RecordBatch> {
        if self.num_data_rows == 0 {
            return Ok(RecordBatch::new_empty(schema));
        }

        concat_batches(&schema, &self.data_batches).map_err(CoreError::ArrowError)
    }

    pub fn concat_delete_batches_transformed(
        &self,
        hudi_configs: Arc<HudiConfigs>,
    ) -> Result<RecordBatch> {
        let ordering_field = hudi_configs
            .get(HudiTableConfig::PrecombineField)?
            .to::<String>();

        if self.num_delete_rows == 0 {
            return Ok(RecordBatch::new_empty(SchemaRef::from(Schema::empty())));
        }

        let mut delete_batches = Vec::with_capacity(self.delete_batches.len());
        for (batch, instant_time) in &self.delete_batches {
            let batch = transform_delete_record_batch(batch, instant_time, &ordering_field)?;
            delete_batches.push(batch);
        }

        let delete_schema =
            transform_delete_batch_schema(self.data_batches[0].schema(), &ordering_field);
        concat_batches(&delete_schema, &delete_batches).map_err(CoreError::ArrowError)
    }
}
