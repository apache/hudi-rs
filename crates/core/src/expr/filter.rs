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
use arrow_array::{ArrayRef, Scalar, StringArray};
use arrow_cast::{cast_with_options, CastOptions};
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
}
