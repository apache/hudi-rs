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
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct Filter {
    pub field_name: String,
    pub operator: ExprOperator,
    pub field_value: String,
}

impl Filter {}

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
