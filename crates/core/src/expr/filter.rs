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
use crate::expr::ExprOperator;
use crate::Result;
use arrow_array::{ArrayRef, BooleanArray, Datum, Scalar, StringArray};
use arrow_cast::{cast_with_options, CastOptions};
use arrow_ord::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use arrow_schema::{DataType, Field, Schema};
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct Filter {
    pub field_name: String,
    pub operator: ExprOperator,
    pub field_value: String,
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
        (
            filter.field_name,
            filter.operator.to_string(),
            filter.field_value,
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
        }
    }

    pub fn ne(&self, value: impl Into<String>) -> Filter {
        Filter {
            field_name: self.name.clone(),
            operator: ExprOperator::Ne,
            field_value: value.into(),
        }
    }

    pub fn lt(&self, value: impl Into<String>) -> Filter {
        Filter {
            field_name: self.name.clone(),
            operator: ExprOperator::Lt,
            field_value: value.into(),
        }
    }

    pub fn lte(&self, value: impl Into<String>) -> Filter {
        Filter {
            field_name: self.name.clone(),
            operator: ExprOperator::Lte,
            field_value: value.into(),
        }
    }

    pub fn gt(&self, value: impl Into<String>) -> Filter {
        Filter {
            field_name: self.name.clone(),
            operator: ExprOperator::Gt,
            field_value: value.into(),
        }
    }

    pub fn gte(&self, value: impl Into<String>) -> Filter {
        Filter {
            field_name: self.name.clone(),
            operator: ExprOperator::Gte,
            field_value: value.into(),
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
}

impl TryFrom<(Filter, &Schema)> for SchemableFilter {
    type Error = CoreError;

    fn try_from((filter, schema): (Filter, &Schema)) -> Result<Self, Self::Error> {
        let field_name = filter.field_name.clone();
        let field: &Field = schema.field_with_name(&field_name).map_err(|e| {
            CoreError::Schema(format!("Field {} not found in schema: {:?}", field_name, e))
        })?;

        let operator = filter.operator;
        let value = &[filter.field_value.as_str()];
        let value = Self::cast_value(value, field.data_type())?;

        let field = field.clone();
        Ok(SchemableFilter {
            field,
            operator,
            value,
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
            cast_with_options(&value, data_type, &cast_options).map_err(|e| {
                CoreError::Schema(format!("Unable to cast {:?}: {:?}", data_type, e))
            })?,
        ))
    }

    pub fn apply_comparsion(&self, value: &dyn Datum) -> Result<BooleanArray> {
        match self.operator {
            ExprOperator::Eq => eq(value, &self.value),
            ExprOperator::Ne => neq(value, &self.value),
            ExprOperator::Lt => lt(value, &self.value),
            ExprOperator::Lte => lt_eq(value, &self.value),
            ExprOperator::Gt => gt(value, &self.value),
            ExprOperator::Gte => gt_eq(value, &self.value),
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
        };

        assert!(SchemableFilter::try_from((invalid_filter, &schema)).is_err());

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
        };
        let schemable = SchemableFilter::try_from((eq_filter, &schema))?;

        let test_array = StringArray::from(vec!["test", "other", "test"]);
        let result = schemable.apply_comparsion(&test_array)?;
        assert_eq!(result, BooleanArray::from(vec![true, false, true]));

        // Test integer greater than comparison
        let gt_filter = Filter {
            field_name: "int_col".to_string(),
            operator: ExprOperator::Gt,
            field_value: "50".to_string(),
        };
        let schemable = SchemableFilter::try_from((gt_filter, &schema))?;

        let test_array = Int64Array::from(vec![40, 50, 60]);
        let result = schemable.apply_comparsion(&test_array)?;
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
            };

            let schemable = SchemableFilter::try_from((filter, &schema))?;
            let result = schemable.apply_comparsion(&test_array)?;
            assert_eq!(
                result,
                BooleanArray::from(expected),
                "Failed for operator {:?} with value {}",
                operator,
                value
            );
        }

        Ok(())
    }
}
