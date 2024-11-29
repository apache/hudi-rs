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

use crate::exprs::HudiOperator;

use anyhow::{Context, Result};
use arrow_array::{ArrayRef, Scalar, StringArray};
use arrow_cast::{cast_with_options, CastOptions};
use arrow_schema::{DataType, Field, Schema};
use std::str::FromStr;

/// A partition filter that represents a filter expression for partition pruning.
#[derive(Debug, Clone)]
pub struct PartitionFilter {
    pub field: Field,
    pub operator: HudiOperator,
    pub value: Scalar<ArrayRef>,
}

impl TryFrom<((&str, &str, &str), &Schema)> for PartitionFilter {
    type Error = anyhow::Error;

    fn try_from((filter, partition_schema): ((&str, &str, &str), &Schema)) -> Result<Self> {
        let (field_name, operator_str, value_str) = filter;

        let field: &Field = partition_schema
            .field_with_name(field_name)
            .with_context(|| format!("Field '{}' not found in partition schema", field_name))?;

        let operator = HudiOperator::from_str(operator_str)
            .with_context(|| format!("Unsupported operator: {}", operator_str))?;

        let value = &[value_str];
        let value = Self::cast_value(value, field.data_type())
            .with_context(|| format!("Unable to cast {:?} as {:?}", value, field.data_type()))?;

        let field = field.clone();
        Ok(PartitionFilter {
            field,
            operator,
            value,
        })
    }
}

impl PartitionFilter {
    pub fn cast_value(value: &[&str; 1], data_type: &DataType) -> Result<Scalar<ArrayRef>> {
        let cast_options = CastOptions {
            safe: false,
            format_options: Default::default(),
        };

        let value = StringArray::from(Vec::from(value));

        Ok(Scalar::new(cast_with_options(
            &value,
            data_type,
            &cast_options,
        )?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exprs::HudiOperator;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_array::Datum;
    use std::str::FromStr;

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Field::new("date", DataType::Date32, false),
            Field::new("category", DataType::Utf8, false),
            Field::new("count", DataType::Int32, false),
        ])
    }

    #[test]
    fn test_partition_filter_try_from_valid() {
        let schema = create_test_schema();
        let filter_tuple = ("date", "=", "2023-01-01");
        let filter = PartitionFilter::try_from((filter_tuple, &schema));
        assert!(filter.is_ok());
        let filter = filter.unwrap();
        assert_eq!(filter.field.name(), "date");
        assert_eq!(filter.operator, HudiOperator::Eq);
        assert_eq!(filter.value.get().0.len(), 1);

        let filter_tuple = ("category", "!=", "foo");
        let filter = PartitionFilter::try_from((filter_tuple, &schema));
        assert!(filter.is_ok());
        let filter = filter.unwrap();
        assert_eq!(filter.field.name(), "category");
        assert_eq!(filter.operator, HudiOperator::Ne);
        assert_eq!(filter.value.get().0.len(), 1);
        assert_eq!(
            StringArray::from(filter.value.into_inner().to_data()).value(0),
            "foo"
        )
    }

    #[test]
    fn test_partition_filter_try_from_invalid_field() {
        let schema = create_test_schema();
        let filter_tuple = ("invalid_field", "=", "2023-01-01");
        let filter = PartitionFilter::try_from((filter_tuple, &schema));
        assert!(filter.is_err());
        assert!(filter
            .unwrap_err()
            .to_string()
            .contains("not found in partition schema"));
    }

    #[test]
    fn test_partition_filter_try_from_invalid_operator() {
        let schema = create_test_schema();
        let filter_tuple = ("date", "??", "2023-01-01");
        let filter = PartitionFilter::try_from((filter_tuple, &schema));
        assert!(filter.is_err());
        assert!(filter
            .unwrap_err()
            .to_string()
            .contains("Unsupported operator: ??"));
    }

    #[test]
    fn test_partition_filter_try_from_invalid_value() {
        let schema = create_test_schema();
        let filter_tuple = ("count", "=", "not_a_number");
        let filter = PartitionFilter::try_from((filter_tuple, &schema));
        assert!(filter.is_err());
        assert!(filter.unwrap_err().to_string().contains("Unable to cast"));
    }

    #[test]
    fn test_partition_filter_try_from_all_operators() {
        let schema = create_test_schema();
        for (op, _) in HudiOperator::TOKEN_OP_PAIRS {
            let filter_tuple = ("count", op, "10");
            let filter = PartitionFilter::try_from((filter_tuple, &schema));
            assert!(filter.is_ok(), "Failed for operator: {}", op);
            let filter = filter.unwrap();
            assert_eq!(filter.field.name(), "count");
            assert_eq!(filter.operator, HudiOperator::from_str(op).unwrap());
        }
    }
}
