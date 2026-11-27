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
//! Partition pruning and partition path helpers.

use crate::Result;
use crate::config::HudiConfigs;
use crate::config::table::HudiTableConfig;
use crate::error::CoreError::{self, InvalidPartitionPath};
use crate::expr::filter::{Filter, SchemableFilter};
use crate::keygen::timestamp_based::TimestampBasedKeyGenerator;
use crate::keygen::{DEFAULT_PARTITION_PATH, KeyGeneratorFilterTransformer};

use arrow_array::{ArrayRef, Scalar};
use arrow_schema::{DataType, Field, Schema};

use crate::config::table::HudiTableConfig::{KeyGeneratorClass, KeyGeneratorType, PartitionFields};
use crate::keygen::is_timestamp_based_keygen;
use crate::metadata::meta_field::MetaField;
use std::collections::HashMap;
use std::sync::Arc;

/// Name of the metadata file in each partition directory.
pub const PARTITION_METAFIELD_PREFIX: &str = ".hoodie_partition_metadata";
/// The partition path of an unpartitioned table.
pub const EMPTY_PARTITION_PATH: &str = "";

/// Build the partition [Schema] in the order declared by `partition_field_names`.
///
/// The resulting schema order must match the on-disk partition path segment order
/// (which follows the `hoodie.table.partition.fields` config), not the arbitrary
/// column order of the underlying Parquet schema. Returns an error if any declared
/// partition field is not present in `table_schema`: silently dropping such a field
/// would make the schema and on-disk path lengths disagree, which `parse_segments`
/// rejects and `should_include` then treats as fail-open (full-table scan).
pub(crate) fn project_partition_schema(
    table_schema: &Schema,
    partition_field_names: &[String],
) -> Result<Schema> {
    let fields: Vec<Arc<Field>> = partition_field_names
        .iter()
        .map(|name| {
            table_schema
                .field_with_name(name)
                .map(|f| Arc::new(f.clone()))
                .map_err(|_| {
                    CoreError::Schema(format!(
                        "Partition field `{name}` declared in \
                         `hoodie.table.partition.fields` is not present in the table schema"
                    ))
                })
        })
        .collect::<Result<_>>()?;
    Ok(Schema::new(fields))
}

/// Whether the table has partition fields and does not use a non-partitioned key generator.
pub fn is_table_partitioned(hudi_configs: &HudiConfigs) -> Result<bool> {
    let has_partition_fields = {
        let partition_fields: Vec<String> = hudi_configs.get_or_default(PartitionFields).into();
        !partition_fields.is_empty()
    };

    let uses_non_partitioned_key_gen = hudi_configs
        .try_get(KeyGeneratorClass)?
        .map(|key_gen| {
            let key_gen_str: String = key_gen.into();
            key_gen_str == "org.apache.hudi.keygen.NonpartitionedKeyGenerator"
        })
        .unwrap_or(false);

    let uses_non_partitioned_type = hudi_configs
        .try_get(KeyGeneratorType)?
        .map(|v| {
            let s: String = v.into();
            let upper = s.to_uppercase();
            upper == "NON_PARTITION" || upper == "NON_PARTITION_AVRO"
        })
        .unwrap_or(false);

    Ok(has_partition_fields && !uses_non_partitioned_key_gen && !uses_non_partitioned_type)
}

/// A partition pruner that filters partitions based on the partition path and its filters.
#[derive(Debug, Clone)]
pub struct PartitionPruner {
    schema: Arc<Schema>,
    is_hive_style: bool,
    is_url_encoded: bool,
    is_partitioned: bool,
    and_filters: Vec<SchemableFilter>,
    /// Filters that reached the pruner but could not be bound to the partition schema, and
    /// so prune nothing. Retained so a caller can tell "no partition matches your
    /// predicate" apart from "your predicate was discarded and every partition was
    /// scanned" — otherwise indistinguishable, since neither returns an error.
    unapplied_filters: Vec<Filter>,
    /// True when the table declares a key generator that needs filter rewriting but whose
    /// configuration could not be loaded, so no rewriting happened.
    keygen_unavailable: bool,
}

impl PartitionPruner {
    /// Creates a pruner from the filters that apply to `partition_schema`. Filters on other
    /// columns, and filters whose value cannot be cast to the column type, are ignored.
    pub fn new(
        and_filters: &[Filter],
        partition_schema: &Schema,
        hudi_configs: &HudiConfigs,
    ) -> Result<Self> {
        // Transform filters based on key generator configuration
        let (transformed_filters, keygen_unavailable) =
            Self::transform_filters_for_keygen(and_filters, partition_schema, hudi_configs)?;

        // A filter that does not bind to the partition schema is dropped here, which is
        // correct for a genuine data-column predicate but is also how a partition predicate
        // silently disappears when its key generator could not rewrite it. Dropping it
        // quietly then reads as a successful unfiltered scan, so record every drop.
        let mut unapplied_filters = Vec::new();
        let mut and_filters: Vec<SchemableFilter> = Vec::new();
        for filter in &transformed_filters {
            match SchemableFilter::try_from((filter.clone(), partition_schema)) {
                Ok(bound) => and_filters.push(bound),
                Err(e) => {
                    log::warn!(
                        "Filter on `{}` does not apply to the partition schema ({e}); it will \
                         not prune any partition{}.",
                        filter.field,
                        if keygen_unavailable {
                            ", because this table's key-generator configuration could not be \
                             loaded and the filter was therefore never rewritten to a \
                             partition-path predicate"
                        } else {
                            ""
                        }
                    );
                    unapplied_filters.push(filter.clone());
                }
            }
        }

        let schema = Arc::new(partition_schema.clone());
        let is_hive_style: bool = hudi_configs
            .get_or_default(HudiTableConfig::IsHiveStylePartitioning)
            .into();
        let is_url_encoded: bool = hudi_configs
            .get_or_default(HudiTableConfig::IsPartitionPathUrlencoded)
            .into();
        let is_partitioned = is_table_partitioned(hudi_configs)?;
        Ok(PartitionPruner {
            schema,
            is_hive_style,
            is_url_encoded,
            is_partitioned,
            and_filters,
            unapplied_filters,
            keygen_unavailable,
        })
    }

    /// Creates an empty partition pruner that does not filter any partitions.
    pub fn empty() -> Self {
        PartitionPruner {
            schema: Arc::new(Schema::empty()),
            is_hive_style: false,
            is_url_encoded: false,
            is_partitioned: false,
            and_filters: Vec::new(),
            unapplied_filters: Vec::new(),
            keygen_unavailable: false,
        }
    }

    /// Filters that reached the pruner but prune nothing, because they could not be bound
    /// to the partition schema.
    ///
    /// A non-empty result means the returned partitions are wider than the caller's
    /// predicate asked for. That is not in itself an error — a predicate on a non-partition
    /// data column belongs here and is enforced when rows are read — but a caller that
    /// requires partition pruning to happen, rather than degrade to a full scan, should
    /// check this and treat a partition-column entry as a failure.
    pub fn unapplied_filters(&self) -> &[Filter] {
        &self.unapplied_filters
    }

    /// Whether this table declares a key generator that needs filter rewriting but whose
    /// configuration could not be loaded.
    ///
    /// The `hoodie.keygen.timebased.*` options are writer-side and are not guaranteed to be
    /// persisted into `hoodie.properties`, so a table can name `TimestampBasedKeyGenerator`
    /// while carrying nothing that says which timestamp type, timezone or output format it
    /// used. There is no safe way to guess a granularity from the class name alone, so
    /// pruning is skipped rather than approximated — and reported here rather than passed
    /// off as a successful scan.
    pub fn is_keygen_config_unavailable(&self) -> bool {
        self.keygen_unavailable
    }

    /// Returns `true` if the partition pruner does not have any filters.
    pub fn is_empty(&self) -> bool {
        self.and_filters.is_empty()
    }

    /// Returns `true` if the table is partitioned.
    pub fn is_table_partitioned(&self) -> bool {
        self.is_partitioned
    }

    /// Returns `true` if the partition path should be included based on the filters.
    pub fn should_include(&self, partition_path: &str) -> bool {
        let segments = match self.parse_segments(partition_path) {
            Ok(s) => s,
            Err(_) => return true, // Include the partition regardless of parsing error
        };

        self.and_filters.iter().all(|filter| {
            match segments.get(filter.field.name()) {
                Some(Some(segment_value)) => {
                    match filter.apply_comparison(segment_value) {
                        Ok(scalar) => scalar.value(0),
                        Err(_) => true, // Include the partition when comparison error occurs
                    }
                }
                // A null partition value satisfies no comparison.
                Some(None) => false,
                None => true, // Include the partition when filtering field does not match any field in the partition
            }
        })
    }

    /// Transforms user filters on data columns to filters on partition path columns
    /// based on the configured key generator.
    ///
    /// Returns the (possibly rewritten) filters and whether a key generator was declared but
    /// could not be built. The second value matters because failing to build one is not
    /// benign: the untransformed filter still names the source data column, which is absent
    /// from the partition schema, so it is dropped when bound and the table is scanned in
    /// full. Reporting it lets the caller say so instead of returning a silent full scan.
    fn transform_filters_for_keygen(
        filters: &[Filter],
        _partition_schema: &Schema,
        hudi_configs: &HudiConfigs,
    ) -> Result<(Vec<Filter>, bool)> {
        if is_timestamp_based_keygen(hudi_configs)? {
            match TimestampBasedKeyGenerator::from_configs(hudi_configs) {
                Ok(transformer) => {
                    return Ok((
                        Self::apply_transformer_to_filters(filters, &transformer),
                        false,
                    ));
                }
                Err(e) => {
                    log::warn!(
                        "Table declares TimestampBasedKeyGenerator but one could not be built \
                         ({e}); the hoodie.keygen.timebased.* options are writer-side and may \
                         not be present in hoodie.properties. Filters on the timestamp source \
                         column cannot be rewritten into partition-path predicates and will \
                         not prune; every partition will be read."
                    );
                    return Ok((filters.to_vec(), true));
                }
            }
        }

        Ok((filters.to_vec(), false))
    }

    /// Rewrites each filter through the key generator.
    ///
    /// A filter the generator cannot rewrite — typically a literal it cannot parse — is
    /// kept as it was rather than turned into an error. Pruning is an optimization, and
    /// being unable to compute it is no reason to refuse the query: the untransformed
    /// filter still names the source data column, which is absent from the partition
    /// schema, so it fails to bind, prunes nothing, is reported through
    /// [`PartitionPruner::unapplied_filters`], and is enforced per row like any other
    /// data-column predicate.
    fn apply_transformer_to_filters(
        filters: &[Filter],
        transformer: &dyn KeyGeneratorFilterTransformer,
    ) -> Vec<Filter> {
        let mut transformed = Vec::new();
        for filter in filters {
            match transformer.transform_filter(filter) {
                Ok(partition_filters) => transformed.extend(partition_filters),
                Err(e) => {
                    log::warn!(
                        "Filter on `{}` could not be rewritten into a partition-path \
                         predicate ({e}); it will not prune any partition and is enforced \
                         per row instead.",
                        filter.field
                    );
                    transformed.push(filter.clone());
                }
            }
        }
        transformed
    }

    /// Partition values by field; `None` is a null value.
    fn parse_segments(
        &self,
        partition_path: &str,
    ) -> Result<HashMap<String, Option<Scalar<ArrayRef>>>> {
        let decode = |value: &str| -> Result<String> {
            Ok(if self.is_url_encoded {
                percent_encoding::percent_decode(value.as_bytes())
                    .decode_utf8()?
                    .into_owned()
            } else {
                value.to_string()
            })
        };

        // Special case: single _hoodie_partition_path field uses the whole path as-is
        if self.schema.fields().len() == 1
            && self.schema.field(0).name() == MetaField::PartitionPath.as_ref()
        {
            let scalar = SchemableFilter::cast_value(
                &[decode(partition_path)?.as_str()],
                &arrow_schema::DataType::Utf8,
            )?;
            return Ok(HashMap::from([(
                MetaField::PartitionPath.as_ref().to_string(),
                Some(scalar),
            )]));
        }

        let parts: Vec<&str> = partition_path.split('/').collect();

        if parts.len() != self.schema.fields().len() {
            return Err(InvalidPartitionPath(format!(
                "Partition path should have {} part(s) but got {}",
                self.schema.fields().len(),
                parts.len()
            )));
        }

        self.schema
            .fields()
            .iter()
            .zip(parts)
            .map(|(field, part)| {
                let value = if self.is_hive_style {
                    let (name, value) = part.split_once('=').ok_or(InvalidPartitionPath(
                        format!("Partition path should be hive-style but got {part}"),
                    ))?;
                    if name != field.name() {
                        return Err(InvalidPartitionPath(format!(
                            "Partition path should contain {} but got {}",
                            field.name(),
                            name
                        )));
                    }
                    value
                } else {
                    part
                };
                // Java writes nulls here, and empty strings too for a string
                // field, which a filter evaluates as `''`. The row mask cannot
                // correct this decision when partition columns are dropped.
                if value == DEFAULT_PARTITION_PATH {
                    let empty = matches!(
                        field.data_type(),
                        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                    )
                    .then(|| SchemableFilter::cast_value(&[""], field.data_type()))
                    .transpose()?;
                    return Ok((field.name().to_string(), empty));
                }
                let value = decode(value)?;
                let scalar = SchemableFilter::cast_value(&[value.as_str()], field.data_type())?;
                Ok((field.name().to_string(), Some(scalar)))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::table::HudiTableConfig::{
        IsHiveStylePartitioning, IsPartitionPathUrlencoded,
    };
    use crate::expr::ExprOperator;

    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_array::{BooleanArray, Date32Array, Int64Array, RecordBatch};
    use std::str::FromStr;

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Field::new("date", DataType::Date32, false),
            Field::new("category", DataType::Utf8, false),
            Field::new("count", DataType::Int32, false),
        ])
    }

    fn create_hudi_configs(is_hive_style: bool, is_url_encoded: bool) -> HudiConfigs {
        HudiConfigs::new([
            (IsHiveStylePartitioning, is_hive_style.to_string()),
            (IsPartitionPathUrlencoded, is_url_encoded.to_string()),
        ])
    }

    #[test]
    fn project_partition_schema_preserves_config_order() {
        let table_schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("integration_id", DataType::Utf8, false),
            Field::new("resource_type", DataType::Utf8, false),
            Field::new("org", DataType::Utf8, false),
            Field::new("payload", DataType::Utf8, false),
        ]);
        let partition_field_names = vec![
            "org".to_string(),
            "resource_type".to_string(),
            "integration_id".to_string(),
        ];

        let projected = project_partition_schema(&table_schema, &partition_field_names).unwrap();

        assert_eq!(projected.fields().len(), 3);
        assert_eq!(projected.field(0).name(), "org");
        assert_eq!(projected.field(1).name(), "resource_type");
        assert_eq!(projected.field(2).name(), "integration_id");
    }

    #[test]
    fn project_partition_schema_errors_when_field_missing_from_table_schema() {
        let table_schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("org", DataType::Utf8, false),
        ]);
        let partition_field_names = vec!["org".to_string(), "not_in_schema".to_string()];

        let err = project_partition_schema(&table_schema, &partition_field_names).unwrap_err();

        assert!(matches!(err, CoreError::Schema(_)));
        assert!(err.to_string().contains("not_in_schema"));
    }

    #[test]
    fn project_partition_schema_empty_config_returns_empty_schema() {
        let table_schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
        let projected = project_partition_schema(&table_schema, &[]).unwrap();
        assert_eq!(projected.fields().len(), 0);
    }

    #[test]
    fn test_partition_pruner_new() {
        let schema = create_test_schema();
        let configs = create_hudi_configs(true, false);

        let filter_gt_date = Filter::try_from(("date", ">", "2023-01-01")).unwrap();
        let filter_eq_a = Filter::try_from(("category", "=", "A")).unwrap();

        let pruner = PartitionPruner::new(&[filter_gt_date, filter_eq_a], &schema, &configs);
        assert!(pruner.is_ok());

        let pruner = pruner.unwrap();
        assert_eq!(pruner.and_filters.len(), 2);
        assert!(pruner.is_hive_style);
        assert!(!pruner.is_url_encoded);
    }

    #[test]
    fn test_partition_pruner_empty() {
        let pruner = PartitionPruner::empty();
        assert!(pruner.is_empty());
        assert!(!pruner.is_hive_style);
        assert!(!pruner.is_url_encoded);
    }

    #[test]
    fn test_partition_pruner_is_empty() {
        let schema = create_test_schema();
        let configs = create_hudi_configs(false, false);

        let pruner_empty = PartitionPruner::new(&[], &schema, &configs).unwrap();
        assert!(pruner_empty.is_empty());

        let filter_gt_date = Filter::try_from(("date", ">", "2023-01-01")).unwrap();
        let pruner_non_empty = PartitionPruner::new(&[filter_gt_date], &schema, &configs).unwrap();
        assert!(!pruner_non_empty.is_empty());
    }

    #[test]
    fn test_partition_pruner_should_include() {
        let schema = create_test_schema();
        let configs = create_hudi_configs(true, false);

        let filter_gt_date = Filter::try_from(("date", ">", "2023-01-01")).unwrap();
        let filter_eq_a = Filter::try_from(("category", "=", "A")).unwrap();
        let filter_lte_100 = Filter::try_from(("count", "<=", "100")).unwrap();

        let pruner = PartitionPruner::new(
            &[filter_gt_date, filter_eq_a, filter_lte_100],
            &schema,
            &configs,
        )
        .unwrap();

        assert!(pruner.should_include("date=2023-02-01/category=A/count=10"));
        assert!(pruner.should_include("date=2023-02-01/category=A/count=100"));
        assert!(!pruner.should_include("date=2022-12-31/category=A/count=10"));
        assert!(!pruner.should_include("date=2023-02-01/category=B/count=10"));
    }

    #[test]
    fn test_hoodie_partition_path_whole_path_pruning() {
        // A timestamp key generator rewrites a source-column predicate into a predicate on
        // the opaque whole-path meta field, so the pruner holds a single-field schema and
        // must compare the path as one string rather than split it into segments.
        let configs = HudiConfigs::new([
            ("hoodie.table.partition.fields", "ts_str"),
            (
                "hoodie.table.keygenerator.class",
                "org.apache.hudi.keygen.TimestampBasedKeyGenerator",
            ),
            ("hoodie.keygen.timebased.timestamp.type", "DATE_STRING"),
            (
                "hoodie.keygen.timebased.input.dateformat",
                "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
            ),
            ("hoodie.keygen.timebased.output.dateformat", "yyyy/MM/dd/HH"),
            ("hoodie.datasource.write.hive_style_partitioning", "true"),
            ("hoodie.datasource.write.partitionpath.urlencode", "false"),
        ]);
        let partition_schema = Schema::new(vec![Field::new(
            MetaField::PartitionPath.as_ref(),
            DataType::Utf8,
            false,
        )]);
        let filter = Filter::try_from(("ts_str", "=", "2023-04-01T12:01:00.123Z")).unwrap();

        let pruner = PartitionPruner::new(&[filter], &partition_schema, &configs).unwrap();
        assert!(!pruner.is_empty());
        assert!(pruner.should_include("ts_str=2023/04/01/12"));
        assert!(!pruner.should_include("ts_str=2023/05/01/08"));
    }

    #[test]
    fn test_hoodie_partition_path_whole_path_pruning_url_encoded() {
        let configs = HudiConfigs::new([
            ("hoodie.table.partition.fields", "ts_str"),
            (
                "hoodie.table.keygenerator.class",
                "org.apache.hudi.keygen.TimestampBasedKeyGenerator",
            ),
            ("hoodie.keygen.timebased.timestamp.type", "DATE_STRING"),
            (
                "hoodie.keygen.timebased.input.dateformat",
                "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
            ),
            ("hoodie.keygen.timebased.output.dateformat", "yyyy/MM/dd/HH"),
            ("hoodie.datasource.write.hive_style_partitioning", "true"),
            ("hoodie.datasource.write.partitionpath.urlencode", "true"),
        ]);
        let partition_schema = Schema::new(vec![Field::new(
            MetaField::PartitionPath.as_ref(),
            DataType::Utf8,
            false,
        )]);
        let filter = Filter::try_from(("ts_str", "=", "2023-04-01T12:01:00.123Z")).unwrap();

        let pruner = PartitionPruner::new(&[filter], &partition_schema, &configs).unwrap();
        // Hudi url-encodes the value before adding the hive prefix, so the path on
        // storage is `ts_str=2023%2F04%2F01%2F12`.
        assert!(pruner.should_include("ts_str=2023%2F04%2F01%2F12"));
        assert!(!pruner.should_include("ts_str=2023%2F05%2F01%2F08"));
    }

    #[test]
    fn test_partition_pruner_parse_segments() {
        let schema = create_test_schema();
        let configs = create_hudi_configs(true, false);
        let pruner = PartitionPruner::new(&[], &schema, &configs).unwrap();

        let segments = pruner
            .parse_segments("date=2023-02-01/category=A/count=10")
            .unwrap();
        assert_eq!(segments.len(), 3);
        assert!(segments.contains_key("date"));
        assert!(segments.contains_key("category"));
        assert!(segments.contains_key("count"));
    }

    #[test]
    fn test_partition_pruner_url_encoded() {
        let schema = create_test_schema();
        let configs = create_hudi_configs(true, true);
        let pruner = PartitionPruner::new(&[], &schema, &configs).unwrap();

        // Java escapes each value, so an encoded '/' or '=' stays inside its segment.
        let path = "date=2023-02-01/category=A%2FB%3DC/count=10";
        let segments = pruner.parse_segments(path).unwrap();
        assert_eq!(segments.len(), 3);
        let matching = Filter::try_from(("category", "=", "A/B=C")).unwrap();
        let other = Filter::try_from(("category", "=", "A")).unwrap();
        let pruner = PartitionPruner::new(&[matching], &schema, &configs).unwrap();
        assert!(pruner.should_include(path));
        let pruner = PartitionPruner::new(&[other], &schema, &configs).unwrap();
        assert!(!pruner.should_include(path));
    }

    #[test]
    fn test_partition_pruner_default_partition_is_empty_or_null() {
        // Java writes nulls to the default partition, and empty strings too
        // for a string field.
        let schema = create_test_schema();
        let configs = create_hudi_configs(true, false);
        let default_category = "date=2023-02-01/category=__HIVE_DEFAULT_PARTITION__/count=10";
        let default_count = "date=2023-02-01/category=A/count=__HIVE_DEFAULT_PARTITION__";
        let includes = |filter: (&str, &str, &str), path: &str| {
            let filter = Filter::try_from(filter).unwrap();
            PartitionPruner::new(&[filter], &schema, &configs)
                .unwrap()
                .should_include(path)
        };
        assert!(includes(("category", "=", ""), default_category));
        assert!(includes(("category", "<", "B"), default_category));
        assert!(!includes(("category", "=", "A"), default_category));
        assert!(!includes(
            ("category", "=", ""),
            "date=2023-02-01/category=A/count=10"
        ));
        // Only nulls fill a non-string default partition, and no comparison
        // matches a null.
        assert!(!includes(("count", "=", "10"), default_count));
        assert!(!includes(("count", "!=", "10"), default_count));
        assert!(includes(("category", "=", "A"), default_count));
        // Filters on the other fields still prune.
        assert!(!includes(("date", ">", "2024-01-01"), default_category));
    }

    /// A value containing a structural character is escaped by the writer precisely so the
    /// split stays unambiguous, so each decoded value must keep its escaped `/`, `=`, space
    /// and `%` inside the one segment.
    #[test]
    fn test_partition_pruner_url_encoded_value_contains_separators() {
        let schema = Schema::new(vec![Field::new("category", DataType::Utf8, false)]);
        let configs = create_hudi_configs(true, true);
        let pruner = PartitionPruner::new(&[], &schema, &configs).unwrap();

        for (encoded, decoded) in [
            ("category=a%2Fb", "a/b"),
            ("category=a%3Db", "a=b"),
            ("category=a%20b", "a b"),
            ("category=a%25b", "a%b"),
        ] {
            let segments = pruner.parse_segments(encoded).unwrap();
            let expected = SchemableFilter::cast_value(&[decoded], &DataType::Utf8).unwrap();
            assert_eq!(
                segments["category"]
                    .as_ref()
                    .unwrap()
                    .clone()
                    .into_inner()
                    .as_ref(),
                expected.into_inner().as_ref(),
                "decoding {encoded}"
            );
        }
    }

    /// Decoding happens exactly once: a literal `%2F` in the source value is written as
    /// `%252F`, and one decode pass must yield `%2F`, not `/`.
    #[test]
    fn test_partition_pruner_url_decodes_exactly_once() {
        let schema = Schema::new(vec![Field::new("category", DataType::Utf8, false)]);
        let configs = create_hudi_configs(true, true);
        let pruner = PartitionPruner::new(&[], &schema, &configs).unwrap();

        let segments = pruner.parse_segments("category=a%252Fb").unwrap();
        let expected = SchemableFilter::cast_value(&["a%2Fb"], &DataType::Utf8).unwrap();
        assert_eq!(
            segments["category"]
                .as_ref()
                .unwrap()
                .clone()
                .into_inner()
                .as_ref(),
            expected.into_inner().as_ref()
        );
    }

    /// With url-encoding off, a `%` in a value is a literal `%` and must survive untouched.
    #[test]
    fn test_partition_pruner_no_decode_when_urlencode_disabled() {
        let schema = Schema::new(vec![Field::new("category", DataType::Utf8, false)]);
        let configs = create_hudi_configs(true, false);
        let pruner = PartitionPruner::new(&[], &schema, &configs).unwrap();

        let segments = pruner.parse_segments("category=a%2Fb").unwrap();
        let expected = SchemableFilter::cast_value(&["a%2Fb"], &DataType::Utf8).unwrap();
        assert_eq!(
            segments["category"]
                .as_ref()
                .unwrap()
                .clone()
                .into_inner()
                .as_ref(),
            expected.into_inner().as_ref()
        );
    }

    /// The `_hoodie_partition_path` meta field takes the whole path as a single
    /// opaque value with no segment structure to preserve, so it is decoded in one
    /// pass — the separators inside it are part of the value, not delimiters the
    /// decode has to stay clear of.
    #[test]
    fn partition_path_meta_field_decodes_the_whole_path_at_once() {
        let schema = Schema::new(vec![Field::new(
            MetaField::PartitionPath.as_ref(),
            DataType::Utf8,
            false,
        )]);
        let pruner = PartitionPruner::new(&[], &schema, &create_hudi_configs(true, true)).unwrap();

        let segments = pruner
            .parse_segments("date%3D2023-02-01%2Fcategory%3DA")
            .unwrap();
        let expected =
            SchemableFilter::cast_value(&["date=2023-02-01/category=A"], &DataType::Utf8).unwrap();
        assert_eq!(
            segments[MetaField::PartitionPath.as_ref()]
                .as_ref()
                .unwrap()
                .clone()
                .into_inner()
                .as_ref(),
            expected.into_inner().as_ref()
        );
    }

    #[test]
    fn test_partition_pruner_invalid_path() {
        let schema = create_test_schema();
        let configs = create_hudi_configs(true, false);
        let pruner = PartitionPruner::new(&[], &schema, &configs).unwrap();

        let result = pruner.parse_segments("date=2023-02-01/category=A/count=10/extra");
        assert!(matches!(result.unwrap_err(), InvalidPartitionPath(_)));

        let result = pruner.parse_segments("date=2023-02-01/category=A/10");
        assert!(matches!(result.unwrap_err(), InvalidPartitionPath(_)));

        let result = pruner.parse_segments("date=2023-02-01/category=A/non_exist_field=10");
        assert!(matches!(result.unwrap_err(), InvalidPartitionPath(_)));
    }

    #[test]
    fn test_partition_filter_try_from_valid() {
        let schema = create_test_schema();
        let filter = Filter {
            field: "date".to_string(),
            operator: ExprOperator::Eq,
            values: vec!["2023-01-01".to_string()],
        };

        let partition_filter = SchemableFilter::try_from((filter, &schema)).unwrap();
        assert_eq!(partition_filter.field.name(), "date");
        assert_eq!(partition_filter.operator, ExprOperator::Eq);

        let value_inner = partition_filter.values[0].clone().into_inner();

        let date_array = value_inner.as_any().downcast_ref::<Date32Array>().unwrap();

        let date_value = date_array.value_as_date(0).unwrap();
        assert_eq!(date_value.to_string(), "2023-01-01");
    }

    #[test]
    fn test_partition_filter_try_from_invalid_field() {
        let schema = create_test_schema();
        let filter = Filter {
            field: "invalid_field".to_string(),
            operator: ExprOperator::Eq,
            values: vec!["2023-01-01".to_string()],
        };
        let result = SchemableFilter::try_from((filter, &schema));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Field invalid_field not found in schema")
        );
    }

    #[test]
    fn test_partition_filter_try_from_invalid_value() {
        let schema = create_test_schema();
        let filter = Filter {
            field: "count".to_string(),
            operator: ExprOperator::Eq,
            values: vec!["not_a_number".to_string()],
        };
        let result = SchemableFilter::try_from((filter, &schema));
        assert!(result.is_err());
    }

    #[test]
    fn test_partition_filter_try_from_all_operators() {
        let schema = create_test_schema();
        for (op, op_enum) in ExprOperator::TOKEN_OP_PAIRS {
            let filter = Filter::new("count".to_string(), op_enum, vec!["5".to_string()]).unwrap();
            let partition_filter = SchemableFilter::try_from((filter, &schema));
            let filter = partition_filter.unwrap();
            assert_eq!(filter.field.name(), "count");
            assert_eq!(filter.operator, ExprOperator::from_str(op).unwrap());
        }
    }

    #[test]
    fn test_transform_filters_for_keygen_timestamp_based() {
        let partition_schema = Schema::new(vec![Field::new(
            MetaField::PartitionPath.as_ref(),
            DataType::Utf8,
            false,
        )]);

        // Range filter: DATE_STRING Gte → single _hoodie_partition_path Gte
        let configs = HudiConfigs::new([
            ("hoodie.table.partition.fields", "ts_str"),
            (
                "hoodie.table.keygenerator.class",
                "org.apache.hudi.keygen.TimestampBasedKeyGenerator",
            ),
            ("hoodie.keygen.timebased.timestamp.type", "DATE_STRING"),
            (
                "hoodie.keygen.timebased.input.dateformat",
                "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
            ),
            ("hoodie.keygen.timebased.output.dateformat", "yyyy/MM/dd"),
            ("hoodie.datasource.write.hive_style_partitioning", "true"),
        ]);

        let user_filter = Filter {
            field: "ts_str".to_string(),
            operator: ExprOperator::Gte,
            values: vec!["2023-04-15T12:00:00.000Z".to_string()],
        };

        let (transformed, _) = PartitionPruner::transform_filters_for_keygen(
            &[user_filter],
            &partition_schema,
            &configs,
        )
        .unwrap();

        assert_eq!(transformed.len(), 1);
        assert_eq!(transformed[0].field, MetaField::PartitionPath.as_ref());
        assert_eq!(transformed[0].operator, ExprOperator::Gte);
        assert_eq!(transformed[0].values[0], "ts_str=2023/04/15");

        // Equality filter: UNIX_TIMESTAMP Eq → single path
        let configs = HudiConfigs::new([
            ("hoodie.table.partition.fields", "event_time"),
            (
                "hoodie.table.keygenerator.class",
                "org.apache.hudi.keygen.TimestampBasedKeyGenerator",
            ),
            ("hoodie.keygen.timebased.timestamp.type", "UNIX_TIMESTAMP"),
            ("hoodie.keygen.timebased.output.dateformat", "yyyy/MM/dd"),
            ("hoodie.datasource.write.hive_style_partitioning", "false"),
        ]);

        // 2024-01-25 00:00:00 UTC = 1706140800 seconds
        let user_filter = Filter {
            field: "event_time".to_string(),
            operator: ExprOperator::Eq,
            values: vec!["1706140800".to_string()],
        };

        let (transformed, _) = PartitionPruner::transform_filters_for_keygen(
            &[user_filter],
            &partition_schema,
            &configs,
        )
        .unwrap();

        assert_eq!(transformed.len(), 1);
        assert_eq!(transformed[0].field, MetaField::PartitionPath.as_ref());
        assert_eq!(transformed[0].values[0], "2024/01/25");

        // v8 detection via keygenerator.type=TIMESTAMP (no keygenerator.class)
        let configs = HudiConfigs::new([
            ("hoodie.table.partition.fields", "ts_str"),
            ("hoodie.table.keygenerator.type", "TIMESTAMP"),
            ("hoodie.keygen.timebased.timestamp.type", "DATE_STRING"),
            (
                "hoodie.keygen.timebased.input.dateformat",
                "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
            ),
            ("hoodie.keygen.timebased.output.dateformat", "yyyy/MM/dd"),
            ("hoodie.datasource.write.hive_style_partitioning", "true"),
        ]);

        let user_filter = Filter {
            field: "ts_str".to_string(),
            operator: ExprOperator::Eq,
            values: vec!["2023-04-15T12:00:00.000Z".to_string()],
        };

        let (transformed, _) = PartitionPruner::transform_filters_for_keygen(
            &[user_filter],
            &partition_schema,
            &configs,
        )
        .unwrap();

        assert_eq!(transformed.len(), 1);
        assert_eq!(transformed[0].values[0], "ts_str=2023/04/15");
    }

    #[test]
    fn test_transform_filters_for_keygen_no_transformation() {
        let configs = HudiConfigs::new([
            ("hoodie.table.partition.fields", "region"),
            (
                "hoodie.table.keygenerator.class",
                "org.apache.hudi.keygen.SimpleKeyGenerator",
            ),
            ("hoodie.datasource.write.hive_style_partitioning", "true"),
        ]);

        let partition_schema = Schema::new(vec![Field::new("region", DataType::Utf8, false)]);

        let user_filter = Filter {
            field: "region".to_string(),
            operator: ExprOperator::Eq,
            values: vec!["us-west".to_string()],
        };

        let (transformed, _) = PartitionPruner::transform_filters_for_keygen(
            std::slice::from_ref(&user_filter),
            &partition_schema,
            &configs,
        )
        .unwrap();

        assert_eq!(transformed.len(), 1);
        assert_eq!(transformed[0].field, user_filter.field);
        assert_eq!(transformed[0].values[0], user_filter.values[0]);
    }

    #[test]
    fn test_timestamp_negative_filters_keep_matching_rows() {
        // Excluding midnight must not hide the noon record in the same partition.
        let batch = RecordBatch::try_from_iter([(
            "ts",
            Arc::new(Int64Array::from(vec![
                1706140800, 1706184000, 1706227200, 1706270400,
            ])) as ArrayRef,
        )])
        .unwrap();
        let partition_schema = Schema::new(vec![Field::new(
            MetaField::PartitionPath.as_ref(),
            DataType::Utf8,
            false,
        )]);
        let configs = HudiConfigs::new([
            ("hoodie.table.partition.fields", "ts"),
            (
                "hoodie.table.keygenerator.class",
                "org.apache.hudi.keygen.TimestampBasedKeyGenerator",
            ),
            ("hoodie.keygen.timebased.timestamp.type", "UNIX_TIMESTAMP"),
            ("hoodie.keygen.timebased.output.dateformat", "yyyy/MM/dd"),
        ]);
        let paths = ["2024/01/25", "2024/01/26"];
        for (operator, excluded, expected) in [
            (
                ExprOperator::Ne,
                vec!["1706140800"],
                vec![false, true, true, true],
            ),
            (
                ExprOperator::NotIn,
                vec!["1706140800", "1706227200"],
                vec![false, true, false, true],
            ),
        ] {
            let filter = Filter::new(
                "ts".to_string(),
                operator,
                excluded.into_iter().map(str::to_string).collect(),
            )
            .unwrap();
            let filters = std::slice::from_ref(&filter);
            let pruner = PartitionPruner::new(filters, &partition_schema, &configs).unwrap();
            for path in paths {
                assert!(pruner.should_include(path), "{operator} must retain {path}");
            }
            assert_eq!(
                crate::expr::filter::filters_to_row_mask(filters, &batch).unwrap(),
                BooleanArray::from(expected),
            );
            let bound = Filter::try_from(("ts", ">=", "1706227200")).unwrap();
            let bounded =
                PartitionPruner::new(&[filter, bound], &partition_schema, &configs).unwrap();
            assert!(!bounded.should_include(paths[0]));
            assert!(bounded.should_include(paths[1]));
        }
    }

    #[test]
    fn test_partition_pruner_with_timestamp_keygen() {
        let configs = HudiConfigs::new([
            ("hoodie.table.partition.fields", "ts"),
            (
                "hoodie.table.keygenerator.class",
                "org.apache.hudi.keygen.TimestampBasedKeyGenerator",
            ),
            ("hoodie.keygen.timebased.timestamp.type", "DATE_STRING"),
            (
                "hoodie.keygen.timebased.input.dateformat",
                "yyyy-MM-dd'T'HH:mm:ssZ",
            ),
            ("hoodie.keygen.timebased.output.dateformat", "yyyy/MM/dd"),
            ("hoodie.datasource.write.hive_style_partitioning", "true"),
            ("hoodie.datasource.write.partitionpath.urlencode", "false"),
        ]);

        let partition_schema = Schema::new(vec![Field::new(
            MetaField::PartitionPath.as_ref(),
            DataType::Utf8,
            false,
        )]);

        let user_filter = Filter {
            field: "ts".to_string(),
            operator: ExprOperator::Gte,
            values: vec!["2024-01-15T00:00:00Z".to_string()],
        };

        let pruner = PartitionPruner::new(&[user_filter], &partition_schema, &configs).unwrap();

        assert!(!pruner.is_empty());

        // Full path string comparison on _hoodie_partition_path
        assert!(pruner.should_include("ts=2024/01/15"));
        assert!(pruner.should_include("ts=2024/06/30"));
        assert!(pruner.should_include("ts=2025/01/01"));

        // Should exclude partitions < 2024/01/15
        assert!(!pruner.should_include("ts=2023/12/31"));
        assert!(!pruner.should_include("ts=2022/01/01"));

        // Non-hive-style: verify the same logic works
        let configs = HudiConfigs::new([
            ("hoodie.table.partition.fields", "ts"),
            (
                "hoodie.table.keygenerator.class",
                "org.apache.hudi.keygen.TimestampBasedKeyGenerator",
            ),
            ("hoodie.keygen.timebased.timestamp.type", "DATE_STRING"),
            (
                "hoodie.keygen.timebased.input.dateformat",
                "yyyy-MM-dd'T'HH:mm:ssZ",
            ),
            ("hoodie.keygen.timebased.output.dateformat", "yyyy/MM/dd"),
            ("hoodie.datasource.write.hive_style_partitioning", "false"),
        ]);

        let user_filter = Filter {
            field: "ts".to_string(),
            operator: ExprOperator::Eq,
            values: vec!["2024-01-15T00:00:00Z".to_string()],
        };

        let pruner = PartitionPruner::new(&[user_filter], &partition_schema, &configs).unwrap();
        assert!(pruner.should_include("2024/01/15"));
        assert!(!pruner.should_include("2024/01/16"));
        assert!(!pruner.should_include("2023/12/31"));
    }

    // ---------------------------------------------------------------------------------
    // Hive-style timestamp-keygen pruning. Every timestamp-keygen fixture in
    // crates/test/data is non-hive-style, so this branch previously had no table-backed
    // coverage at all.
    // ---------------------------------------------------------------------------------

    fn hive_style_ts_configs(output_dateformat: &str) -> HudiConfigs {
        HudiConfigs::new([
            ("hoodie.table.partition.fields", "ts"),
            (
                "hoodie.table.keygenerator.class",
                "org.apache.hudi.keygen.TimestampBasedKeyGenerator",
            ),
            ("hoodie.keygen.timebased.timestamp.type", "DATE_STRING"),
            (
                "hoodie.keygen.timebased.input.dateformat",
                "yyyy-MM-dd'T'HH:mm:ssZ",
            ),
            (
                "hoodie.keygen.timebased.output.dateformat",
                output_dateformat,
            ),
            ("hoodie.datasource.write.hive_style_partitioning", "true"),
            ("hoodie.datasource.write.partitionpath.urlencode", "false"),
        ])
    }

    fn partition_path_schema() -> Schema {
        Schema::new(vec![Field::new(
            MetaField::PartitionPath.as_ref(),
            DataType::Utf8,
            false,
        )])
    }

    fn ts_filter(operator: ExprOperator, value: &str) -> Filter {
        Filter {
            field: "ts".to_string(),
            operator,
            values: vec![value.to_string()],
        }
    }

    /// The hive prefix is the declared partition field, applied once to the whole formatted
    /// value. It never comes from the format tokens.
    #[test]
    fn hive_style_path_uses_declared_field_name_not_format_tokens() {
        for (output_dateformat, expected) in [
            ("yyyy-MM-dd", "ts=2024-03-01"),
            ("yyyy/MM/dd", "ts=2024/03/01"),
            ("yyyy-MM-dd-HH", "ts=2024-03-01-08"),
            ("yyyyMMdd", "ts=20240301"),
        ] {
            let keygen =
                TimestampBasedKeyGenerator::from_configs(&hive_style_ts_configs(output_dateformat))
                    .unwrap();
            let transformed = keygen
                .transform_filter(&ts_filter(ExprOperator::Eq, "2024-03-01T08:00:00Z"))
                .unwrap();
            assert_eq!(
                transformed[0].values[0], expected,
                "output.dateformat {output_dateformat}"
            );
        }
    }

    /// A predicate no partition can satisfy must exclude every partition.
    #[test]
    fn impossible_predicate_excludes_every_day_partition() {
        let pruner = PartitionPruner::new(
            &[ts_filter(ExprOperator::Eq, "1900-01-01T00:00:00Z")],
            &partition_path_schema(),
            &hive_style_ts_configs("yyyy-MM-dd"),
        )
        .unwrap();

        for path in ["ts=2024-03-01", "ts=2024-03-02", "ts=2023-12-31"] {
            assert!(!pruner.should_include(path), "{path} should be excluded");
        }
    }

    #[test]
    fn day_granularity_range_selects_only_days_in_range() {
        let pruner = PartitionPruner::new(
            &[
                ts_filter(ExprOperator::Gte, "2024-03-01T00:00:00Z"),
                ts_filter(ExprOperator::Lt, "2024-03-04T00:00:00Z"),
            ],
            &partition_path_schema(),
            &hive_style_ts_configs("yyyy-MM-dd"),
        )
        .unwrap();

        for included in ["ts=2024-03-01", "ts=2024-03-02", "ts=2024-03-03"] {
            assert!(pruner.should_include(included), "{included}");
        }
        for excluded in ["ts=2024-02-29", "ts=2024-03-05", "ts=2025-01-01"] {
            assert!(!pruner.should_include(excluded), "{excluded}");
        }
    }

    /// A half-open upper bound widens from `<` to `<=` at partition granularity, so the
    /// boundary partition is retained. That is deliberate: `ts < 2024-03-02T06:00:00Z`
    /// genuinely has matching rows inside `ts=2024-03-02`, and the transform has already
    /// discarded the time of day, so the partition cannot be ruled out here. Row-level
    /// evaluation removes the rows that do not match.
    #[test]
    fn half_open_upper_bound_retains_the_boundary_partition() {
        let pruner = PartitionPruner::new(
            &[
                ts_filter(ExprOperator::Gte, "2024-03-01T00:00:00Z"),
                ts_filter(ExprOperator::Lt, "2024-03-02T00:00:00Z"),
            ],
            &partition_path_schema(),
            &hive_style_ts_configs("yyyy-MM-dd"),
        )
        .unwrap();

        assert!(pruner.should_include("ts=2024-03-01"));
        assert!(pruner.should_include("ts=2024-03-02"));
        assert!(!pruner.should_include("ts=2024-03-03"));
        assert!(!pruner.should_include("ts=2024-02-29"));
    }

    #[test]
    fn hour_granularity_range_spans_a_day_boundary() {
        let pruner = PartitionPruner::new(
            &[
                ts_filter(ExprOperator::Gte, "2024-03-01T22:00:00Z"),
                ts_filter(ExprOperator::Lte, "2024-03-02T01:00:00Z"),
            ],
            &partition_path_schema(),
            &hive_style_ts_configs("yyyy-MM-dd-HH"),
        )
        .unwrap();

        for included in [
            "ts=2024-03-01-22",
            "ts=2024-03-01-23",
            "ts=2024-03-02-00",
            "ts=2024-03-02-01",
        ] {
            assert!(pruner.should_include(included), "{included}");
        }
        for excluded in ["ts=2024-03-01-21", "ts=2024-03-02-02"] {
            assert!(!pruner.should_include(excluded), "{excluded}");
        }
    }

    /// Leap day and year rollover are ordinary calendar arithmetic, but a format that
    /// zero-padded inconsistently would break the ordered string comparison, so pin them.
    #[test]
    fn leap_day_and_year_rollover_compare_correctly() {
        let configs = hive_style_ts_configs("yyyy-MM-dd");
        let pruner = PartitionPruner::new(
            &[
                ts_filter(ExprOperator::Gte, "2024-02-28T00:00:00Z"),
                ts_filter(ExprOperator::Lte, "2024-03-01T00:00:00Z"),
            ],
            &partition_path_schema(),
            &configs,
        )
        .unwrap();
        assert!(pruner.should_include("ts=2024-02-29"), "leap day");
        assert!(!pruner.should_include("ts=2023-02-28"));

        let pruner = PartitionPruner::new(
            &[
                ts_filter(ExprOperator::Gte, "2024-12-31T00:00:00Z"),
                ts_filter(ExprOperator::Lte, "2025-01-01T00:00:00Z"),
            ],
            &partition_path_schema(),
            &configs,
        )
        .unwrap();
        assert!(pruner.should_include("ts=2024-12-31"));
        assert!(pruner.should_include("ts=2025-01-01"));
        assert!(!pruner.should_include("ts=2024-12-30"));
        assert!(!pruner.should_include("ts=2025-01-02"));
    }

    /// A table can name TimestampBasedKeyGenerator while carrying none of the writer-side
    /// `hoodie.keygen.timebased.*` options that say which granularity, timezone or format
    /// it used. Guessing is unsafe, so pruning is skipped — but the caller must be able to
    /// see that. Previously the filter was discarded without a trace and the resulting full
    /// scan was indistinguishable from a successful one.
    #[test]
    fn missing_timebased_config_is_reported_rather_than_silently_ignored() {
        let configs = HudiConfigs::new([
            ("hoodie.table.partition.fields", "ts"),
            (
                "hoodie.table.keygenerator.class",
                "org.apache.hudi.keygen.TimestampBasedKeyGenerator",
            ),
            ("hoodie.datasource.write.hive_style_partitioning", "true"),
        ]);
        let pruner = PartitionPruner::new(
            &[ts_filter(ExprOperator::Eq, "1900-01-01T00:00:00Z")],
            &partition_path_schema(),
            &configs,
        )
        .unwrap();

        assert!(
            pruner.is_keygen_config_unavailable(),
            "the reader must report that it could not load the key-generator config"
        );
        assert_eq!(
            pruner.unapplied_filters().len(),
            1,
            "the discarded predicate must be reported, not dropped in silence"
        );
        assert_eq!(pruner.unapplied_filters()[0].field, "ts");

        // Pruning is deliberately skipped rather than approximated, so every partition is
        // still returned; the point is that the accessors above make that visible.
        assert!(pruner.should_include("ts=2024-03-01"));
    }

    /// A predicate on a plain data column legitimately prunes nothing and is reported as
    /// unapplied, but it must not be mistaken for a broken key generator.
    #[test]
    fn data_column_filter_is_unapplied_without_blaming_the_keygen() {
        let pruner = PartitionPruner::new(
            &[Filter {
                field: "amount".to_string(),
                operator: ExprOperator::Gt,
                values: vec!["100".to_string()],
            }],
            &partition_path_schema(),
            &hive_style_ts_configs("yyyy-MM-dd"),
        )
        .unwrap();

        assert_eq!(pruner.unapplied_filters().len(), 1);
        assert!(!pruner.is_keygen_config_unavailable());
        assert!(pruner.should_include("ts=2024-03-01"));
    }

    /// When the key generator is usable, nothing is left unapplied.
    #[test]
    fn a_usable_keygen_leaves_no_unapplied_filter() {
        let pruner = PartitionPruner::new(
            &[ts_filter(ExprOperator::Eq, "2024-03-01T00:00:00Z")],
            &partition_path_schema(),
            &hive_style_ts_configs("yyyy-MM-dd"),
        )
        .unwrap();

        assert!(pruner.unapplied_filters().is_empty());
        assert!(!pruner.is_keygen_config_unavailable());
    }

    /// Pruning is an optimization. A literal the key generator cannot turn into a
    /// partition path is no reason to refuse the query: the filter is left untransformed,
    /// fails to bind to the partition schema, prunes nothing, and is reported — the same
    /// route an unbuildable key generator takes — while the read goes ahead and enforces
    /// the predicate per row.
    #[test]
    fn unparseable_literal_is_reported_as_unapplied_instead_of_failing_the_read() {
        let pruner = PartitionPruner::new(
            &[
                ts_filter(ExprOperator::Eq, "yesterday"),
                ts_filter(ExprOperator::Gte, "2024-03-01T00:00:00Z"),
            ],
            &partition_path_schema(),
            &hive_style_ts_configs("yyyy-MM-dd"),
        )
        .expect("an unparseable literal must not fail construction");

        assert_eq!(pruner.unapplied_filters().len(), 1);
        assert_eq!(pruner.unapplied_filters()[0].field, "ts");
        assert_eq!(pruner.unapplied_filters()[0].values, vec!["yesterday"]);
        assert!(
            !pruner.is_keygen_config_unavailable(),
            "the key generator was built; only this literal was unusable"
        );

        // The filter that could be rewritten still prunes.
        assert!(pruner.should_include("ts=2024-03-01"));
        assert!(!pruner.should_include("ts=2024-02-29"));
    }
}
