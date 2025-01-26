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
pub mod arrow;

pub trait StrTupleRef {
    fn as_strs(&self) -> Vec<(&str, &str, &str)>;
}

impl StrTupleRef for Vec<(String, String, String)> {
    fn as_strs(&self) -> Vec<(&str, &str, &str)> {
        self.iter()
            .map(|(s1, s2, s3)| (s1.as_str(), s2.as_str(), s3.as_str()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_vec_of_string_to_vec_of_str_slice() {
        let vec_of_strings = vec![
            (
                String::from("date"),
                String::from("="),
                String::from("2022-01-02"),
            ),
            (
                String::from("foo"),
                String::from("bar"),
                String::from("baz"),
            ),
        ];
        let binding = vec_of_strings.as_strs();
        let str_slice = &binding[..];
        assert_eq!(
            str_slice,
            [("date", "=", "2022-01-02"), ("foo", "bar", "baz")]
        );
    }
}
