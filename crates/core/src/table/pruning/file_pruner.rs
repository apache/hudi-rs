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

//! File-level pruner for filtering files based on column statistics.

use crate::Result;
use crate::expr::filter::{Filter, SchemableFilter};
use crate::statistics::StatisticsContainer;

use arrow_schema::Schema;
use std::collections::HashSet;

use super::StatsPruner;

/// A file-level pruner that filters files based on column statistics.
///
/// This pruner uses min/max statistics from Parquet files to determine if a file
/// can be skipped (pruned) based on query predicates. A file is pruned if its
/// statistics prove that no rows in the file can match the predicate.
#[derive(Debug, Clone)]
pub struct FilePruner {
    /// Filters that apply to non-partition columns
    and_filters: Vec<SchemableFilter>,
}

impl FilePruner {
    /// Creates a new file pruner with filters on non-partition columns.
    ///
    /// Filters on partition columns are excluded since they are handled by PartitionPruner.
    ///
    /// # Arguments
    /// * `and_filters` - List of filters to apply (AND semantics)
    /// * `table_schema` - The table's data schema
    /// * `partition_schema` - The partition schema (filters on these columns are excluded)
    pub fn new(
        and_filters: &[Filter],
        table_schema: &Schema,
        partition_schema: &Schema,
    ) -> Result<Self> {
        // Get partition column names to exclude from file pruning
        let partition_columns: HashSet<&str> = partition_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();

        // Only keep filters on non-partition columns that exist in the table schema
        let and_filters: Vec<SchemableFilter> = and_filters
            .iter()
            .filter(|filter| !partition_columns.contains(filter.field_name.as_str()))
            .filter_map(|filter| SchemableFilter::try_from((filter.clone(), table_schema)).ok())
            .collect();

        Ok(FilePruner { and_filters })
    }

    /// Creates an empty file pruner that does not filter any files.
    pub fn empty() -> Self {
        FilePruner {
            and_filters: Vec::new(),
        }
    }

    /// Returns `true` if the pruner does not have any filters.
    pub fn is_empty(&self) -> bool {
        self.and_filters.is_empty()
    }

    /// Returns the names of columns used in the filter predicates.
    pub fn filter_column_names(&self) -> Vec<&str> {
        self.and_filters
            .iter()
            .map(|f| f.field.name().as_str())
            .collect()
    }

    /// Returns a clone of the filters for use in partition stats pruning.
    pub fn filters(&self) -> Vec<SchemableFilter> {
        self.and_filters.clone()
    }

    /// Returns `true` if the file should be included based on its statistics.
    ///
    /// A file is included if ANY of its rows MIGHT match all the filters.
    /// A file is excluded (pruned) only if we can prove that NO rows can match.
    /// If statistics are missing or incomplete, the file is included (safe default).
    pub fn should_include(&self, stats: &StatisticsContainer) -> bool {
        // If no filters, include everything
        if self.and_filters.is_empty() {
            return true;
        }

        // All filters must pass (AND semantics).
        // If any filter definitively excludes the file, return false.
        for filter in &self.and_filters {
            let col_name = filter.field.name();
            let Some(col_stats) = stats.columns.get(col_name) else {
                // No stats for this column, cannot prune - include the file
                continue;
            };

            if StatsPruner::can_prune_by_filter(filter, col_stats) {
                return false; // File can be pruned
            }
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::statistics::{ColumnStatistics, StatsGranularity};
    use arrow_array::{ArrayRef, Int64Array, StringArray};
    use arrow_schema::{DataType, Field};
    use std::sync::Arc;

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
            Field::new("date", DataType::Date32, false),
        ])
    }

    fn create_partition_schema() -> Schema {
        Schema::new(vec![Field::new("date", DataType::Date32, false)])
    }

    fn create_stats_with_int_range(col_name: &str, min: i64, max: i64) -> StatisticsContainer {
        let mut stats = StatisticsContainer::new(StatsGranularity::File);
        stats.columns.insert(
            col_name.to_string(),
            ColumnStatistics {
                column_name: col_name.to_string(),
                data_type: DataType::Int64,
                min_value: Some(Arc::new(Int64Array::from(vec![min])) as ArrayRef),
                max_value: Some(Arc::new(Int64Array::from(vec![max])) as ArrayRef),
            },
        );
        stats
    }

    fn create_stats_with_string_range(col_name: &str, min: &str, max: &str) -> StatisticsContainer {
        let mut stats = StatisticsContainer::new(StatsGranularity::File);
        stats.columns.insert(
            col_name.to_string(),
            ColumnStatistics {
                column_name: col_name.to_string(),
                data_type: DataType::Utf8,
                min_value: Some(Arc::new(StringArray::from(vec![min])) as ArrayRef),
                max_value: Some(Arc::new(StringArray::from(vec![max])) as ArrayRef),
            },
        );
        stats
    }

    #[test]
    fn test_empty_pruner() {
        let pruner = FilePruner::empty();
        assert!(pruner.is_empty());

        let stats = create_stats_with_int_range("id", 1, 100);
        assert!(pruner.should_include(&stats));
    }

    #[test]
    fn test_pruner_excludes_partition_columns() {
        let table_schema = create_test_schema();
        let partition_schema = create_partition_schema();

        let filters = vec![Filter::try_from(("date", "=", "2024-01-01")).unwrap()];

        let pruner = FilePruner::new(&filters, &table_schema, &partition_schema).unwrap();
        assert!(pruner.is_empty());
    }

    #[test]
    fn test_pruner_keeps_non_partition_columns() {
        let table_schema = create_test_schema();
        let partition_schema = create_partition_schema();

        let filters = vec![Filter::try_from(("id", ">", "50")).unwrap()];

        let pruner = FilePruner::new(&filters, &table_schema, &partition_schema).unwrap();
        assert!(!pruner.is_empty());
    }

    #[test]
    fn test_eq_filter_prunes_when_value_below_min() {
        let table_schema = create_test_schema();
        let partition_schema = Schema::empty();

        let filters = vec![Filter::try_from(("id", "=", "5")).unwrap()];
        let pruner = FilePruner::new(&filters, &table_schema, &partition_schema).unwrap();

        let stats = create_stats_with_int_range("id", 10, 100);
        assert!(!pruner.should_include(&stats));
    }

    #[test]
    fn test_eq_filter_prunes_when_value_above_max() {
        let table_schema = create_test_schema();
        let partition_schema = Schema::empty();

        let filters = vec![Filter::try_from(("id", "=", "200")).unwrap()];
        let pruner = FilePruner::new(&filters, &table_schema, &partition_schema).unwrap();

        let stats = create_stats_with_int_range("id", 10, 100);
        assert!(!pruner.should_include(&stats));
    }

    #[test]
    fn test_eq_filter_includes_when_value_in_range() {
        let table_schema = create_test_schema();
        let partition_schema = Schema::empty();

        let filters = vec![Filter::try_from(("id", "=", "50")).unwrap()];
        let pruner = FilePruner::new(&filters, &table_schema, &partition_schema).unwrap();

        let stats = create_stats_with_int_range("id", 10, 100);
        assert!(pruner.should_include(&stats));
    }

    #[test]
    fn test_ne_filter_prunes_when_all_equal() {
        let table_schema = create_test_schema();
        let partition_schema = Schema::empty();

        let filters = vec![Filter::try_from(("id", "!=", "50")).unwrap()];
        let pruner = FilePruner::new(&filters, &table_schema, &partition_schema).unwrap();

        let stats = create_stats_with_int_range("id", 50, 50);
        assert!(!pruner.should_include(&stats));
    }

    #[test]
    fn test_ne_filter_includes_when_range_exists() {
        let table_schema = create_test_schema();
        let partition_schema = Schema::empty();

        let filters = vec![Filter::try_from(("id", "!=", "50")).unwrap()];
        let pruner = FilePruner::new(&filters, &table_schema, &partition_schema).unwrap();

        let stats = create_stats_with_int_range("id", 10, 100);
        assert!(pruner.should_include(&stats));
    }

    #[test]
    fn test_lt_filter_prunes_when_min_gte_value() {
        let table_schema = create_test_schema();
        let partition_schema = Schema::empty();

        let filters = vec![Filter::try_from(("id", "<", "10")).unwrap()];
        let pruner = FilePruner::new(&filters, &table_schema, &partition_schema).unwrap();

        let stats = create_stats_with_int_range("id", 10, 100);
        assert!(!pruner.should_include(&stats));
    }

    #[test]
    fn test_lt_filter_includes_when_min_lt_value() {
        let table_schema = create_test_schema();
        let partition_schema = Schema::empty();

        let filters = vec![Filter::try_from(("id", "<", "50")).unwrap()];
        let pruner = FilePruner::new(&filters, &table_schema, &partition_schema).unwrap();

        let stats = create_stats_with_int_range("id", 10, 100);
        assert!(pruner.should_include(&stats));
    }

    #[test]
    fn test_lte_filter_prunes_when_min_gt_value() {
        let table_schema = create_test_schema();
        let partition_schema = Schema::empty();

        let filters = vec![Filter::try_from(("id", "<=", "5")).unwrap()];
        let pruner = FilePruner::new(&filters, &table_schema, &partition_schema).unwrap();

        let stats = create_stats_with_int_range("id", 10, 100);
        assert!(!pruner.should_include(&stats));
    }

    #[test]
    fn test_gt_filter_prunes_when_max_lte_value() {
        let table_schema = create_test_schema();
        let partition_schema = Schema::empty();

        let filters = vec![Filter::try_from(("id", ">", "100")).unwrap()];
        let pruner = FilePruner::new(&filters, &table_schema, &partition_schema).unwrap();

        let stats = create_stats_with_int_range("id", 10, 100);
        assert!(!pruner.should_include(&stats));
    }

    #[test]
    fn test_gt_filter_includes_when_max_gt_value() {
        let table_schema = create_test_schema();
        let partition_schema = Schema::empty();

        let filters = vec![Filter::try_from(("id", ">", "50")).unwrap()];
        let pruner = FilePruner::new(&filters, &table_schema, &partition_schema).unwrap();

        let stats = create_stats_with_int_range("id", 10, 100);
        assert!(pruner.should_include(&stats));
    }

    #[test]
    fn test_gte_filter_prunes_when_max_lt_value() {
        let table_schema = create_test_schema();
        let partition_schema = Schema::empty();

        let filters = vec![Filter::try_from(("id", ">=", "150")).unwrap()];
        let pruner = FilePruner::new(&filters, &table_schema, &partition_schema).unwrap();

        let stats = create_stats_with_int_range("id", 10, 100);
        assert!(!pruner.should_include(&stats));
    }

    #[test]
    fn test_lte_filter_includes_when_value_in_range() {
        let table_schema = create_test_schema();
        let partition_schema = Schema::empty();

        let filters = vec![Filter::try_from(("id", "<=", "50")).unwrap()];
        let pruner = FilePruner::new(&filters, &table_schema, &partition_schema).unwrap();

        let stats = create_stats_with_int_range("id", 10, 100);
        assert!(pruner.should_include(&stats));
    }

    #[test]
    fn test_gte_filter_includes_when_value_in_range() {
        let table_schema = create_test_schema();
        let partition_schema = Schema::empty();

        let filters = vec![Filter::try_from(("id", ">=", "50")).unwrap()];
        let pruner = FilePruner::new(&filters, &table_schema, &partition_schema).unwrap();

        let stats = create_stats_with_int_range("id", 10, 100);
        assert!(pruner.should_include(&stats));
    }

    #[test]
    fn test_string_filter() {
        let table_schema = create_test_schema();
        let partition_schema = Schema::empty();

        let filters = vec![Filter::try_from(("name", "=", "zebra")).unwrap()];
        let pruner = FilePruner::new(&filters, &table_schema, &partition_schema).unwrap();

        let stats = create_stats_with_string_range("name", "apple", "banana");
        assert!(!pruner.should_include(&stats));

        let stats2 = create_stats_with_string_range("name", "apple", "zebra");
        assert!(pruner.should_include(&stats2));
    }

    #[test]
    fn test_missing_column_stats_includes_file() {
        let table_schema = create_test_schema();
        let partition_schema = Schema::empty();

        let filters = vec![Filter::try_from(("id", "=", "50")).unwrap()];
        let pruner = FilePruner::new(&filters, &table_schema, &partition_schema).unwrap();

        let stats = create_stats_with_int_range("other_column", 1, 10);
        assert!(pruner.should_include(&stats));
    }

    #[test]
    fn test_filter_on_column_with_no_stats() {
        let table_schema = create_test_schema();
        let partition_schema = Schema::empty();

        let filters = vec![Filter::try_from(("id", "=", "50")).unwrap()];
        let pruner = FilePruner::new(&filters, &table_schema, &partition_schema).unwrap();

        let mut stats = StatisticsContainer::new(StatsGranularity::File);
        stats.columns.insert(
            "id".to_string(),
            ColumnStatistics {
                column_name: "id".to_string(),
                data_type: DataType::Int64,
                min_value: None,
                max_value: None,
            },
        );

        assert!(pruner.should_include(&stats));
    }

    #[test]
    fn test_multiple_filters_all_must_pass() {
        let table_schema = create_test_schema();
        let partition_schema = Schema::empty();

        let filters = vec![
            Filter::try_from(("id", ">", "0")).unwrap(),
            Filter::try_from(("id", "<", "5")).unwrap(),
        ];
        let pruner = FilePruner::new(&filters, &table_schema, &partition_schema).unwrap();

        let stats = create_stats_with_int_range("id", 10, 100);
        assert!(!pruner.should_include(&stats));
    }
}
