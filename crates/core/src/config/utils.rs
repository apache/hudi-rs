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

use anyhow::{Context, Result};
use bytes::Bytes;
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Cursor};

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
            hudi_options.insert(k.as_ref().into(), v.into());
        } else {
            others.insert(k.as_ref().into(), v.into());
        }
    }

    (hudi_options, others)
}

pub fn parse_data_for_options(data: &Bytes, split_chars: &str) -> Result<HashMap<String, String>> {
    let cursor = Cursor::new(data);
    let lines = BufReader::new(cursor).lines();
    let mut options = HashMap::new();

    for line in lines {
        let line = line.context("Failed to read line")?;
        let trimmed_line = line.trim();
        if trimmed_line.is_empty() || trimmed_line.starts_with('#') {
            continue;
        }
        let mut parts = trimmed_line.splitn(2, |c| split_chars.contains(c));
        let key = parts
            .next()
            .context("Missing key in config line")?
            .trim()
            .to_owned();
        let value = parts.next().unwrap_or("").trim().to_owned();
        options.insert(key, value);
    }

    Ok(options)
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

    #[test]
    fn test_parse_data_comprehensive() {
        let data = Bytes::from(
            "key1=value1\n\
             key2:value2\n\
             key3 value3\n\
             key4\tvalue4\n\
             key5=value with spaces\n\
             key6: another spaced value \n\
             key7\tvalue with tab\n\
             key8=value with = in it\n\
             key9:value:with:colons\n\
             empty_key=\n\
             #comment line\n\
             \n\
             key10==double equals",
        );

        let result = parse_data_for_options(&data, "=: \t").unwrap();

        let mut expected = HashMap::new();
        expected.insert("key1".to_string(), "value1".to_string());
        expected.insert("key2".to_string(), "value2".to_string());
        expected.insert("key3".to_string(), "value3".to_string());
        expected.insert("key4".to_string(), "value4".to_string());
        expected.insert("key5".to_string(), "value with spaces".to_string());
        expected.insert("key6".to_string(), "another spaced value".to_string());
        expected.insert("key7".to_string(), "value with tab".to_string());
        expected.insert("key8".to_string(), "value with = in it".to_string());
        expected.insert("key9".to_string(), "value:with:colons".to_string());
        expected.insert("empty_key".to_string(), "".to_string());
        expected.insert("key10".to_string(), "=double equals".to_string());

        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_data_with_comments_and_empty_lines() {
        let data = Bytes::from("key1=value1\n# This is a comment\n\nkey2:value2");
        let result = parse_data_for_options(&data, "=:").unwrap();
        let mut expected = HashMap::new();
        expected.insert("key1".to_string(), "value1".to_string());
        expected.insert("key2".to_string(), "value2".to_string());
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_data_with_missing_value() {
        let data = Bytes::from("key1=value1\nkey2=\nkey3:value3");
        let result = parse_data_for_options(&data, "=:").unwrap();
        let mut expected = HashMap::new();
        expected.insert("key1".to_string(), "value1".to_string());
        expected.insert("key2".to_string(), "".to_string());
        expected.insert("key3".to_string(), "value3".to_string());
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_data_with_trailing_spaces() {
        let data = Bytes::from("key1 = value1  \n  key2:  value2  ");
        let result = parse_data_for_options(&data, "=:").unwrap();
        let mut expected = HashMap::new();
        expected.insert("key1".to_string(), "value1".to_string());
        expected.insert("key2".to_string(), "value2".to_string());
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_data_empty_input() {
        let data = Bytes::from("");
        let result = parse_data_for_options(&data, "=:").unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_parse_data_invalid_line() {
        let data = Bytes::from("key1=value1\ninvalid_line\nkey2=value2");
        let result = parse_data_for_options(&data, "=").unwrap();
        let mut expected = HashMap::new();
        expected.insert("key1".to_string(), "value1".to_string());
        expected.insert("invalid_line".to_string(), "".to_string());
        expected.insert("key2".to_string(), "value2".to_string());
        assert_eq!(result, expected);
    }
}
