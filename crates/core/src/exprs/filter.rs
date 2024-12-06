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

use crate::error::CoreError::Unsupported;
use crate::exprs::ExprOperator;
use crate::Result;
use arrow_array::StringArray;
use arrow_array::{ArrayRef, Scalar};
use arrow_cast::{cast_with_options, CastOptions};
use arrow_schema::DataType;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct Filter {
    pub field_name: String,
    pub operator: ExprOperator,
    pub value: String,
}

impl Filter {
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

impl TryFrom<(&str, &str, &str)> for Filter {
    type Error = crate::error::CoreError;

    fn try_from(filter: (&str, &str, &str)) -> Result<Self> {
        let (field_name, operator_str, value_str) = filter;

        let field_name = field_name.to_string();

        let operator = ExprOperator::from_str(operator_str)
            .map_err(|_| Unsupported(format!("Unsupported operator: {}", operator_str)))?;

        let value = value_str.to_string();

        Ok(Filter {
            field_name,
            operator,
            value,
        })
    }
}
