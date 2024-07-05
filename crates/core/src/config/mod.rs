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
use std::collections::HashMap;

use anyhow::Result;

pub mod read;
pub mod table;

pub trait OptionsParser {
    type Output;

    fn parse_value(&self, options: &HashMap<String, String>) -> Result<Self::Output>;

    fn parse_value_or_default(&self, options: &HashMap<String, String>) -> Self::Output;
}

#[derive(Debug)]
pub enum HudiConfigValue {
    Integer(isize),
}

impl HudiConfigValue {
    pub fn cast<T: 'static + TryFrom<isize> + TryFrom<usize> + std::fmt::Debug>(&self) -> T {
        match self {
            HudiConfigValue::Integer(value) => T::try_from(*value).unwrap_or_else(|_| {
                panic!("Failed to convert isize to {}", std::any::type_name::<T>())
            }),
        }
    }
}
