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

use crate::Result;
use crate::error::CoreError;
use crate::expr::ExprOperator;
use arrow_arith::boolean;
use arrow_array::{ArrayRef, BooleanArray, Datum, Scalar, StringArray};
use arrow_cast::{CastOptions, cast_with_options};
use arrow_ord::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use arrow_schema::{DataType, Field, Schema};
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct Filter {
    pub field_name: String,
    pub operator: ExprOperator,
    pub field_value: String,
    /// For IN/NOT IN operators only. Empty for other operators.
    pub field_values: Vec<String>,
}

impl Filter {
    /// Create a filter with a single value (for =, !=, <, <=, >, >=)
    pub fn new_single(field_name: String, operator: ExprOperator, field_value: String) -> Self {
        Self {
            field_name,
            operator,
            field_value,
            field_values: Vec::new(),
        }
    }

    /// Create a filter with multiple values (for IN/NOT IN)
    pub fn new_multi(field_name: String, operator: ExprOperator, field_values: Vec<String>) -> Self {
        Self {
            field_name,
            operator,
            field_value: String::new(), // Not used for IN/NOT IN
            field_values,
        }
    }
}

impl Filter {
    pub fn negate(&self) -> Option<Self> {
        self.operator.negate().map(|op| Self {
            operator: op,
            ..self.clone()
        })
    }
}

impl From<Filter> for (String, String, String) {
    fn from(filter: Filter) -> Self {
        let value_str = if filter.field_values.is_empty() {
            filter.field_value
        } else {
            // For IN/NOT IN, join values with comma
            filter.field_values.join(",")
        };
        (
            filter.field_name,
            filter.operator.to_string(),
            value_str,
        )
    }
}

impl TryFrom<(&str, &str, &str)> for Filter {
    type Error = CoreError;

    fn try_from(binary_expr_tuple: (&str, &str, &str)) -> Result<Self, Self::Error> {
        let (field_name, operator_str, field_value) = binary_expr_tuple;

        let field_name = field_name.to_string();

        let operator = ExprOperator::from_str(operator_str)?;

        let field_value = field_value.to_string();

        Ok(Filter {
            field_name,
            operator,
            field_value,
            field_values: Vec::new(),
        })
    }
}

pub fn from_str_tuples<I, S>(tuples: I) -> Result<Vec<Filter>>
where
    I: IntoIterator<Item = (S, S, S)>,
    S: AsRef<str>,
{
    tuples
        .into_iter()
        .map(|t| Filter::try_from((t.0.as_ref(), t.1.as_ref(), t.2.as_ref())))
        .collect()
}

pub struct FilterField {
    pub name: String,
}

impl FilterField {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn eq(&self, value: impl Into<String>) -> Filter {
        Filter {
            field_name: self.name.clone(),
            operator: ExprOperator::Eq,
            field_value: value.into(),
            field_values: Vec::new(),
        }
    }

    pub fn ne(&self, value: impl Into<String>) -> Filter {
        Filter {
            field_name: self.name.clone(),
            operator: ExprOperator::Ne,
            field_value: value.into(),
            field_values: Vec::new(),
        }
    }

    pub fn lt(&self, value: impl Into<String>) -> Filter {
        Filter {
            field_name: self.name.clone(),
            operator: ExprOperator::Lt,
            field_value: value.into(),
            field_values: Vec::new(),
        }
    }

    pub fn lte(&self, value: impl Into<String>) -> Filter {
        Filter {
            field_name: self.name.clone(),
            operator: ExprOperator::Lte,
            field_value: value.into(),
            field_values: Vec::new(),
        }
    }

    pub fn gt(&self, value: impl Into<String>) -> Filter {
        Filter {
            field_name: self.name.clone(),
            operator: ExprOperator::Gt,
            field_value: value.into(),
            field_values: Vec::new(),
        }
    }

    pub fn gte(&self, value: impl Into<String>) -> Filter {
        Filter {
            field_name: self.name.clone(),
            operator: ExprOperator::Gte,
            field_value: value.into(),
            field_values: Vec::new(),
        }
    }

    pub fn in_list<I, S>(&self, values: I) -> Filter
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Filter {
            field_name: self.name.clone(),
            operator: ExprOperator::In,
            field_value: String::new(),
            field_values: values.into_iter().map(|v| v.into()).collect(),
        }
    }

    pub fn not_in_list<I, S>(&self, values: I) -> Filter
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Filter {
            field_name: self.name.clone(),
            operator: ExprOperator::NotIn,
            field_value: String::new(),
            field_values: values.into_iter().map(|v| v.into()).collect(),
        }
    }
}

pub fn col(name: impl Into<String>) -> FilterField {
    FilterField::new(name)
}

#[derive(Debug, Clone)]
pub struct SchemableFilter {
    pub field: Field,
    pub operator: ExprOperator,
    pub value: Scalar<ArrayRef>,
    /// For IN/NOT IN operators only. Empty for other operators.
    pub values: Vec<Scalar<ArrayRef>>,
}

impl TryFrom<(Filter, &Schema)> for SchemableFilter {
    type Error = CoreError;

    fn try_from((filter, schema): (Filter, &Schema)) -> Result<Self, Self::Error> {
        let field_name = filter.field_name.clone();
        let field: &Field = schema.field_with_name(&field_name).map_err(|e| {
            CoreError::Schema(format!("Field {field_name} not found in schema: {e:?}"))
        })?;

        let operator = filter.operator;

        // Handle IN/NOT IN separately
        let (value, values) = match operator {
            ExprOperator::In | ExprOperator::NotIn => {
                // For IN/NOT IN, use field_values
                if filter.field_values.is_empty() {
                    return Err(CoreError::Schema(format!(
                        "IN/NOT IN operator requires non-empty field_values for field '{field_name}'"
                    )));
                }
                let values: Result<Vec<_>> = filter.field_values
                    .iter()
                    .map(|v| Self::cast_value(&[v.as_str()], field.data_type()))
                    .collect();
                let values = values?;
                // Use first value as placeholder for compatibility
                let first_value = values[0].clone();
                (first_value, values)
            }
            _ => {
                // For other operators, use field_value
                let value = &[filter.field_value.as_str()];
                let value = Self::cast_value(value, field.data_type())?;
                (value, Vec::new())
            }
        };

        let field = field.clone();
        Ok(SchemableFilter {
            field,
            operator,
            value,
            values,
        })
    }
}

impl SchemableFilter {
    pub fn cast_value(value: &[&str; 1], data_type: &DataType) -> Result<Scalar<ArrayRef>> {
        let cast_options = CastOptions {
            safe: false,
            format_options: Default::default(),
        };

        let value = StringArray::from(Vec::from(value));

        Ok(Scalar::new(
            cast_with_options(&value, data_type, &cast_options)
                .map_err(|e| CoreError::Schema(format!("Unable to cast {data_type:?}: {e:?}")))?,
        ))
    }

    pub fn apply_comparison(&self, value: &dyn Datum) -> Result<BooleanArray> {
        match self.operator {
            ExprOperator::Eq => eq(value, &self.value),
            ExprOperator::Ne => neq(value, &self.value),
            ExprOperator::Lt => lt(value, &self.value),
            ExprOperator::Lte => lt_eq(value, &self.value),
            ExprOperator::Gt => gt(value, &self.value),
            ExprOperator::Gte => gt_eq(value, &self.value),
            ExprOperator::In => {
                // IN: value == values[0] OR value == values[1] OR ...
                let mut result = eq(value, &self.values[0])?;
                for filter_value in &self.values[1..] {
                    let comparison = eq(value, filter_value)?;
                    result = boolean::or(&result, &comparison)?;
                }
                Ok(result)
            }
            ExprOperator::NotIn => {
                // NOT IN: value != values[0] AND value != values[1] AND ...
                let mut result = neq(value, &self.values[0])?;
                for filter_value in &self.values[1..] {
                    let comparison = neq(value, filter_value)?;
                    result = boolean::and(&result, &comparison)?;
                }
                Ok(result)
            }
        }
        .map_err(|e| e.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Field::new("string_col", DataType::Utf8, false),
            Field::new("int_col", DataType::Int64, false),
        ])
    }

    #[test]
    fn test_schemable_filter_try_from() -> Result<()> {
        let schema = create_test_schema();

        // Test string column filter creation
        let string_filter = Filter {
            field_name: "string_col".to_string(),
            operator: ExprOperator::Eq,
            field_value: "test_value".to_string(),
            field_values: Vec::new(),
        };

        let schemable = SchemableFilter::try_from((string_filter, &schema))?;
        assert_eq!(schemable.field.name(), "string_col");
        assert_eq!(schemable.field.data_type(), &DataType::Utf8);
        assert_eq!(schemable.operator, ExprOperator::Eq);

        // Test integer column filter creation
        let int_filter = Filter {
            field_name: "int_col".to_string(),
            operator: ExprOperator::Gt,
            field_value: "42".to_string(),
            field_values: Vec::new(),
        };

        let schemable = SchemableFilter::try_from((int_filter, &schema))?;
        assert_eq!(schemable.field.name(), "int_col");
        assert_eq!(schemable.field.data_type(), &DataType::Int64);
        assert_eq!(schemable.operator, ExprOperator::Gt);

        // Test error case - non-existent column
        let invalid_filter = Filter {
            field_name: "non_existent".to_string(),
            operator: ExprOperator::Eq,
            field_value: "value".to_string(),
            field_values: Vec::new(),
        };

        assert!(SchemableFilter::try_from((invalid_filter, &schema)).is_err());

        Ok(())
    }

    #[test]
    fn test_schemable_filter_in_empty_values_error() -> Result<()> {
        let schema = Schema::new(vec![Field::new("int_col", DataType::Int64, true)]);

        // IN operator with empty field_values should error
        let filter = Filter::new_multi("int_col".to_string(), ExprOperator::In, vec![]);
        let result = SchemableFilter::try_from((filter, &schema));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("non-empty field_values"));

        // NOT IN operator with empty field_values should also error
        let filter = Filter::new_multi("int_col".to_string(), ExprOperator::NotIn, vec![]);
        let result = SchemableFilter::try_from((filter, &schema));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("non-empty field_values"));

        Ok(())
    }

    #[test]
    fn test_schemable_filter_cast_value() -> Result<()> {
        // Test casting to string
        let string_value = SchemableFilter::cast_value(&["test"], &DataType::Utf8)?;
        assert_eq!(string_value.get().0.len(), 1);

        // Test casting to integer
        let int_value = SchemableFilter::cast_value(&["42"], &DataType::Int64)?;
        assert_eq!(int_value.get().0.len(), 1);

        // Test invalid integer cast
        let result = SchemableFilter::cast_value(&["not_a_number"], &DataType::Int64);
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_schemable_filter_apply_comparison() -> Result<()> {
        let schema = create_test_schema();

        // Test string equality comparison
        let eq_filter = Filter {
            field_name: "string_col".to_string(),
            operator: ExprOperator::Eq,
            field_value: "test".to_string(),
            field_values: Vec::new(),
        };
        let schemable = SchemableFilter::try_from((eq_filter, &schema))?;

        let test_array = StringArray::from(vec!["test", "other", "test"]);
        let result = schemable.apply_comparison(&test_array)?;
        assert_eq!(result, BooleanArray::from(vec![true, false, true]));

        // Test integer greater than comparison
        let gt_filter = Filter {
            field_name: "int_col".to_string(),
            operator: ExprOperator::Gt,
            field_value: "50".to_string(),
            field_values: Vec::new(),
        };
        let schemable = SchemableFilter::try_from((gt_filter, &schema))?;

        let test_array = Int64Array::from(vec![40, 50, 60]);
        let result = schemable.apply_comparison(&test_array)?;
        assert_eq!(result, BooleanArray::from(vec![false, false, true]));

        Ok(())
    }

    #[test]
    fn test_schemable_filter_all_operators() -> Result<()> {
        let schema = create_test_schema();
        let test_array = Int64Array::from(vec![40, 50, 60]);

        let test_cases = vec![
            (ExprOperator::Eq, "50", vec![false, true, false]),
            (ExprOperator::Ne, "50", vec![true, false, true]),
            (ExprOperator::Lt, "50", vec![true, false, false]),
            (ExprOperator::Lte, "50", vec![true, true, false]),
            (ExprOperator::Gt, "50", vec![false, false, true]),
            (ExprOperator::Gte, "50", vec![false, true, true]),
        ];

        for (operator, value, expected) in test_cases {
            let filter = Filter {
                field_name: "int_col".to_string(),
                operator,
                field_value: value.to_string(),
                field_values: Vec::new(),
            };

            let schemable = SchemableFilter::try_from((filter, &schema))?;
            let result = schemable.apply_comparison(&test_array)?;
            assert_eq!(
                result,
                BooleanArray::from(expected),
                "Failed for operator {operator:?} with value {value}"
            );
        }

        Ok(())
    }

    #[test]
    fn test_schemable_filter_in() -> Result<()> {
        let schema = create_test_schema();

        let in_filter = Filter::new_multi(
            "string_col".to_string(),
            ExprOperator::In,
            vec!["foo".to_string(), "bar".to_string()],
        );

        let schemable = SchemableFilter::try_from((in_filter, &schema))?;
        let test_array = StringArray::from(vec!["foo", "baz", "bar", "qux"]);
        let result = schemable.apply_comparison(&test_array)?;
        assert_eq!(result, BooleanArray::from(vec![true, false, true, false]));

        Ok(())
    }

    #[test]
    fn test_schemable_filter_not_in() -> Result<()> {
        let schema = create_test_schema();

        let not_in_filter = Filter::new_multi(
            "string_col".to_string(),
            ExprOperator::NotIn,
            vec!["foo".to_string(), "bar".to_string()],
        );

        let schemable = SchemableFilter::try_from((not_in_filter, &schema))?;
        let test_array = StringArray::from(vec!["foo", "baz", "bar", "qux"]);
        let result = schemable.apply_comparison(&test_array)?;
        assert_eq!(result, BooleanArray::from(vec![false, true, false, true]));

        Ok(())
    }

    #[test]
    fn test_schemable_filter_in_with_integers() -> Result<()> {
        let schema = create_test_schema();

        let in_filter = Filter::new_multi(
            "int_col".to_string(),
            ExprOperator::In,
            vec!["40".to_string(), "60".to_string()],
        );

        let schemable = SchemableFilter::try_from((in_filter, &schema))?;
        let test_array = Int64Array::from(vec![40, 50, 60]);
        let result = schemable.apply_comparison(&test_array)?;
        assert_eq!(result, BooleanArray::from(vec![true, false, true]));

        Ok(())
    }
}
