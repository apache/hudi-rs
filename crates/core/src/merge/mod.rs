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
#[cfg(test)]
mod conformance_tests;
mod ordering;
pub mod record_batch_merger;

use crate::Result;
use crate::config::HudiConfigs;
use crate::config::error;
use crate::config::error::ConfigError;
use crate::config::error::ConfigError::InvalidValue;
use crate::config::error::Result as ConfigResult;
use crate::config::read::HudiReadConfig;
use crate::config::table::HudiTableConfig::{
    OrderingFields, PopulatesMetaFields, RecordMergeStrategy,
};
use crate::error::CoreError;
use crate::file_group::record_batches::RecordBatches;
use crate::merge::record_batch_merger::RecordBatchMerger;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use std::str::FromStr;
use std::sync::Arc;
use strum_macros::AsRefStr;

/// Merges materialized data and delete batches into a single output batch.
///
/// Implementations are configured when constructed. Callers provide the union of
/// base-file and log-file data batches plus delete batches; the merger returns a
/// batch using [`Self::output_schema`]. Errors are returned for invalid
/// merge configuration, missing required fields, or Arrow compute failures.
pub trait RecordMerger: Send + Sync + std::fmt::Debug {
    /// Merges the provided record batches into one output batch.
    ///
    /// The input must contain batches compatible with this merger's output
    /// schema and configured Hudi merge strategy. Depending on that strategy,
    /// implementations may apply deletes or deduplicate by record key using
    /// ordering values. The returned batch uses [`Self::output_schema`].
    fn merge(&self, inputs: RecordBatches) -> Result<RecordBatch>;

    /// Returns the schema used by batches produced by this merger.
    fn output_schema(&self) -> &SchemaRef;
}

/// Default record merger implementation name.
pub const DEFAULT_RECORD_MERGER_IMPL: &str = "record_batch";

/// Creates a record merger from the configured merger implementation name.
pub fn create_record_merger(
    schema: SchemaRef,
    hudi_configs: Arc<HudiConfigs>,
) -> Result<Arc<dyn RecordMerger>> {
    let impl_name: String = hudi_configs
        .get_or_default(HudiReadConfig::RecordMergerImpl)
        .into();
    let normalized_impl_name = impl_name.to_ascii_lowercase();
    match normalized_impl_name.as_str() {
        DEFAULT_RECORD_MERGER_IMPL => Ok(Arc::new(RecordBatchMerger::new(schema, hudi_configs))),
        _ => Err(CoreError::Config(ConfigError::InvalidValue(format!(
            "unknown record_merger_impl: {impl_name}"
        )))),
    }
}

/// Validates merge-related Hudi configs shared by all record merger implementations.
pub fn validate_configs(hudi_configs: &HudiConfigs) -> ConfigResult<()> {
    let merge_strategy: String = hudi_configs.get_or_default(RecordMergeStrategy).into();
    let merge_strategy = RecordMergeStrategyValue::from_str(&merge_strategy)?;

    let populate_meta_fields: bool = hudi_configs.get_or_default(PopulatesMetaFields).into();
    if !populate_meta_fields && merge_strategy != RecordMergeStrategyValue::AppendOnly {
        return Err(ConfigError::InvalidValue(format!(
            "When {:?} is false, {:?} must be {:?}.",
            PopulatesMetaFields,
            RecordMergeStrategy,
            RecordMergeStrategyValue::AppendOnly
        )));
    }

    let precombine_field = hudi_configs.try_get(OrderingFields)?;
    if precombine_field.is_none() && merge_strategy == RecordMergeStrategyValue::OverwriteWithLatest
    {
        return Err(ConfigError::InvalidValue(format!(
            "When {:?} is {:?}, {:?} must be set.",
            RecordMergeStrategy,
            RecordMergeStrategyValue::OverwriteWithLatest,
            OrderingFields
        )));
    }

    Ok(())
}

/// Config value for [crate::config::table::HudiTableConfig::RecordMergeStrategy].
#[derive(Clone, Debug, PartialEq, AsRefStr)]
pub enum RecordMergeStrategyValue {
    #[strum(serialize = "append_only")]
    AppendOnly,
    #[strum(serialize = "overwrite_with_latest")]
    OverwriteWithLatest,
}

impl FromStr for RecordMergeStrategyValue {
    type Err = ConfigError;

    fn from_str(s: &str) -> error::Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "append_only" => Ok(Self::AppendOnly),
            "overwrite_with_latest" => Ok(Self::OverwriteWithLatest),
            v => Err(InvalidValue(v.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int32Array;
    use arrow_schema::{DataType, Field, Schema};

    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
    }

    fn create_test_batch(schema: SchemaRef) -> RecordBatch {
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2]))]).unwrap()
    }

    #[test]
    fn test_create_record_merger_default_merges_small_input() {
        let schema = create_test_schema();
        let merger = create_record_merger(schema.clone(), Arc::new(HudiConfigs::empty())).unwrap();
        let batch = create_test_batch(schema.clone());

        let merged = merger
            .merge(RecordBatches::new_with_data_batches([batch]))
            .unwrap();

        assert_eq!(merger.output_schema(), &schema);
        assert_eq!(merged.num_rows(), 2);
    }

    #[test]
    fn test_create_record_merger_unknown_impl_returns_config_error() {
        let schema = create_test_schema();
        let configs = HudiConfigs::new([(HudiReadConfig::RecordMergerImpl, "bogus")]);

        let err = create_record_merger(schema, Arc::new(configs)).unwrap_err();

        assert!(matches!(
            err,
            CoreError::Config(ConfigError::InvalidValue(msg))
                if msg == "unknown record_merger_impl: bogus"
        ));
    }
}
