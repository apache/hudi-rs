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

//! Pruning modules for filtering files and partitions based on statistics.
//!
//! This module provides common statistics-based pruning logic that can be used
//! at different granularities (file-level, partition-level).

mod file_pruner;
mod partition_pruner;

pub use file_pruner::FilePruner;
pub use partition_pruner::PartitionPruner;

use arrow_array::ArrayRef;
use arrow_ord::cmp;

use crate::expr::ExprOperator;
use crate::expr::filter::SchemableFilter;
use crate::statistics::ColumnStatistics;

/// Common statistics-based pruning logic.
///
/// These functions determine if a data range can be pruned based on filter predicates
/// and column min/max statistics. They are used by both file-level and partition-level
/// pruners.
pub struct StatsPruner;

impl StatsPruner {
    /// Determines if a data range can be pruned based on a filter and column stats.
    ///
    /// Returns `true` if the range can definitely be pruned (no rows can match).
    pub fn can_prune_by_filter(filter: &SchemableFilter, col_stats: &ColumnStatistics) -> bool {
        let Some(filter_value) = Self::extract_filter_array(filter) else {
            return false; // Cannot extract value, don't prune
        };

        let min = &col_stats.min_value;
        let max = &col_stats.max_value;

        match filter.operator {
            ExprOperator::Eq => Self::can_prune_eq(&filter_value, min, max),
            ExprOperator::Ne => Self::can_prune_ne(&filter_value, min, max),
            ExprOperator::Lt => Self::can_prune_lt(&filter_value, min),
            ExprOperator::Lte => Self::can_prune_lte(&filter_value, min),
            ExprOperator::Gt => Self::can_prune_gt(&filter_value, max),
            ExprOperator::Gte => Self::can_prune_gte(&filter_value, max),
        }
    }

    /// Extracts the filter value as an ArrayRef.
    fn extract_filter_array(filter: &SchemableFilter) -> Option<ArrayRef> {
        use arrow_array::Datum;
        let (array, is_scalar) = filter.value.get();
        if array.is_empty() {
            return None;
        }
        if is_scalar || array.len() == 1 {
            Some(array.slice(0, 1))
        } else {
            None
        }
    }

    /// Prune for `col = value`: prune if value < min OR value > max
    fn can_prune_eq(value: &ArrayRef, min: &Option<ArrayRef>, max: &Option<ArrayRef>) -> bool {
        // Need both min and max to make this decision
        let Some(min_val) = min else { return false };
        let Some(max_val) = max else { return false };

        // Prune if value is outside the [min, max] range
        let value_lt_min = cmp::lt(value, min_val).map(|r| r.value(0)).unwrap_or(false);
        let value_gt_max = cmp::gt(value, max_val).map(|r| r.value(0)).unwrap_or(false);

        value_lt_min || value_gt_max
    }

    /// Prune for `col != value`: prune if min = max = value (all rows equal the excluded value)
    fn can_prune_ne(value: &ArrayRef, min: &Option<ArrayRef>, max: &Option<ArrayRef>) -> bool {
        // Need both min and max to make this decision
        let Some(min_val) = min else { return false };
        let Some(max_val) = max else { return false };

        // Prune only if all rows have the excluded value (min = max = value)
        let min_eq_max = cmp::eq(min_val, max_val)
            .map(|r| r.value(0))
            .unwrap_or(false);
        let min_eq_value = cmp::eq(min_val, value).map(|r| r.value(0)).unwrap_or(false);

        min_eq_max && min_eq_value
    }

    /// Prune for `col < value`: prune if min >= value (all values are >= the threshold)
    fn can_prune_lt(value: &ArrayRef, min: &Option<ArrayRef>) -> bool {
        let Some(min_val) = min else { return false };
        cmp::gt_eq(min_val, value)
            .map(|r| r.value(0))
            .unwrap_or(false)
    }

    /// Prune for `col <= value`: prune if min > value (all values are > the threshold)
    fn can_prune_lte(value: &ArrayRef, min: &Option<ArrayRef>) -> bool {
        let Some(min_val) = min else { return false };
        cmp::gt(min_val, value).map(|r| r.value(0)).unwrap_or(false)
    }

    /// Prune for `col > value`: prune if max <= value (all values are <= the threshold)
    fn can_prune_gt(value: &ArrayRef, max: &Option<ArrayRef>) -> bool {
        let Some(max_val) = max else { return false };
        cmp::lt_eq(max_val, value)
            .map(|r| r.value(0))
            .unwrap_or(false)
    }

    /// Prune for `col >= value`: prune if max < value (all values are < the threshold)
    fn can_prune_gte(value: &ArrayRef, max: &Option<ArrayRef>) -> bool {
        let Some(max_val) = max else { return false };
        cmp::lt(max_val, value).map(|r| r.value(0)).unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::DataType;
    use std::sync::Arc;

    fn make_int_stats(min: i64, max: i64) -> ColumnStatistics {
        ColumnStatistics {
            column_name: "col".to_string(),
            data_type: DataType::Int64,
            min_value: Some(Arc::new(Int64Array::from(vec![min])) as ArrayRef),
            max_value: Some(Arc::new(Int64Array::from(vec![max])) as ArrayRef),
        }
    }

    #[test]
    fn test_stats_pruner_eq() {
        let schema = arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "col",
            DataType::Int64,
            false,
        )]);

        // value=5, range=[10,100] -> prune (5 < 10)
        let filter = crate::expr::filter::Filter::try_from(("col", "=", "5")).unwrap();
        let schemable = SchemableFilter::try_from((filter, &schema)).unwrap();
        let stats = make_int_stats(10, 100);
        assert!(StatsPruner::can_prune_by_filter(&schemable, &stats));

        // value=50, range=[10,100] -> include
        let filter = crate::expr::filter::Filter::try_from(("col", "=", "50")).unwrap();
        let schemable = SchemableFilter::try_from((filter, &schema)).unwrap();
        assert!(!StatsPruner::can_prune_by_filter(&schemable, &stats));
    }

    #[test]
    fn test_stats_pruner_gt() {
        let schema = arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "col",
            DataType::Int64,
            false,
        )]);

        // col > 100, range=[10,100] -> prune (max <= 100)
        let filter = crate::expr::filter::Filter::try_from(("col", ">", "100")).unwrap();
        let schemable = SchemableFilter::try_from((filter, &schema)).unwrap();
        let stats = make_int_stats(10, 100);
        assert!(StatsPruner::can_prune_by_filter(&schemable, &stats));

        // col > 50, range=[10,100] -> include
        let filter = crate::expr::filter::Filter::try_from(("col", ">", "50")).unwrap();
        let schemable = SchemableFilter::try_from((filter, &schema)).unwrap();
        assert!(!StatsPruner::can_prune_by_filter(&schemable, &stats));
    }
}
