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

pub mod filter;

use crate::error::CoreError;
use crate::error::CoreError::Unsupported;

use std::cmp::PartialEq;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::str::FromStr;

/// An operator that represents a comparison operation used in a partition filter expression.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ExprOperator {
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
}

impl Display for ExprOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            // Binary Operators
            ExprOperator::Eq => write!(f, "="),
            ExprOperator::Ne => write!(f, "!="),
            ExprOperator::Lt => write!(f, "<"),
            ExprOperator::Lte => write!(f, "<="),
            ExprOperator::Gt => write!(f, ">"),
            ExprOperator::Gte => write!(f, ">="),
        }
    }
}

impl ExprOperator {
    pub const TOKEN_OP_PAIRS: [(&'static str, ExprOperator); 6] = [
        ("=", ExprOperator::Eq),
        ("!=", ExprOperator::Ne),
        ("<", ExprOperator::Lt),
        ("<=", ExprOperator::Lte),
        (">", ExprOperator::Gt),
        (">=", ExprOperator::Gte),
    ];

    /// Negates the operator.
    pub fn negate(&self) -> Option<ExprOperator> {
        match self {
            ExprOperator::Eq => Some(ExprOperator::Ne),
            ExprOperator::Ne => Some(ExprOperator::Eq),
            ExprOperator::Lt => Some(ExprOperator::Gte),
            ExprOperator::Lte => Some(ExprOperator::Gt),
            ExprOperator::Gt => Some(ExprOperator::Lte),
            ExprOperator::Gte => Some(ExprOperator::Lt),
        }
    }
}

impl FromStr for ExprOperator {
    type Err = CoreError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        ExprOperator::TOKEN_OP_PAIRS
            .iter()
            .find_map(|&(token, op)| {
                if token.eq_ignore_ascii_case(s) {
                    Some(op)
                } else {
                    None
                }
            })
            .ok_or_else(|| Unsupported(format!("Unsupported operator: {}", s)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_operator_from_str() {
        assert_eq!(ExprOperator::from_str("=").unwrap(), ExprOperator::Eq);
        assert_eq!(ExprOperator::from_str("!=").unwrap(), ExprOperator::Ne);
        assert_eq!(ExprOperator::from_str("<").unwrap(), ExprOperator::Lt);
        assert_eq!(ExprOperator::from_str("<=").unwrap(), ExprOperator::Lte);
        assert_eq!(ExprOperator::from_str(">").unwrap(), ExprOperator::Gt);
        assert_eq!(ExprOperator::from_str(">=").unwrap(), ExprOperator::Gte);
        assert!(ExprOperator::from_str("??").is_err());
    }

    #[test]
    fn test_operator_display() {
        assert_eq!(ExprOperator::Eq.to_string(), "=");
        assert_eq!(ExprOperator::Ne.to_string(), "!=");
        assert_eq!(ExprOperator::Lt.to_string(), "<");
        assert_eq!(ExprOperator::Lte.to_string(), "<=");
        assert_eq!(ExprOperator::Gt.to_string(), ">");
        assert_eq!(ExprOperator::Gte.to_string(), ">=");
    }
}
