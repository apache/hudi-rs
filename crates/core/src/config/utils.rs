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

pub fn empty_options<'a>() -> std::iter::Empty<(&'a str, &'a str)> {
    std::iter::empty::<(&str, &str)>()
}

pub fn split_hudi_options_from_others<I, K, V>(
    all_options: I,
) -> (HashMap<String, String>, HashMap<String, String>)
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: Into<String>,
{
    let mut hudi_options = HashMap::new();
    let mut others = HashMap::new();
    for (k, v) in all_options {
        if k.as_ref().starts_with("hoodie.") {
            hudi_options.insert(k.as_ref().to_string(), v.into());
        } else {
            others.insert(k.as_ref().to_string(), v.into());
        }
    }

    (hudi_options, others)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_options() {
        let empty = empty_options();
        assert_eq!(empty.count(), 0);
    }

    #[test]
    fn test_split_hudi_options_from_others_empty() {
        let (hudi, others) = split_hudi_options_from_others(Vec::<(&str, &str)>::new());
        assert!(hudi.is_empty());
        assert!(others.is_empty());
    }

    #[test]
    fn test_split_hudi_options_from_others_mixed() {
        let options = vec![
            ("hoodie.option1", "value1"),
            ("option2", "value2"),
            ("hoodie.option3", "value3"),
            ("option4", "value4"),
        ];
        let (hudi, others) = split_hudi_options_from_others(options);
        assert_eq!(hudi.len(), 2);
        assert_eq!(hudi["hoodie.option1"], "value1");
        assert_eq!(hudi["hoodie.option3"], "value3");
        assert_eq!(others.len(), 2);
        assert_eq!(others["option2"], "value2");
        assert_eq!(others["option4"], "value4");
    }
}
