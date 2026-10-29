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
//! Codec for `hoodie.properties`, matching Java `Properties.load(InputStream)`
//! and `Properties.store(OutputStream)`.

use crate::config::{Result, error::ConfigError::ParseLine};
use std::collections::HashMap;

pub(crate) fn parse_properties(bytes: &[u8]) -> Result<HashMap<String, String>> {
    // The byte-stream format is ISO-8859-1; other characters arrive as \uXXXX escapes.
    let text: String = bytes.iter().map(|b| char::from(*b)).collect();
    let text = text.replace("\r\n", "\n").replace('\r', "\n");
    let mut properties = HashMap::new();
    let mut logical = String::new();
    for line in text.split('\n') {
        let line = line.trim_start_matches([' ', '\t', '\u{c}']);
        // Java treats a blank or comment line as such whenever no logical-line
        // content has accumulated, including right after a lone `\` line.
        if logical.is_empty() && (line.is_empty() || line.starts_with(['#', '!'])) {
            continue;
        }
        logical.push_str(line);
        if logical.bytes().rev().take_while(|b| *b == b'\\').count() % 2 == 1 {
            logical.pop();
            continue;
        }
        parse_logical_line(&logical, &mut properties)?;
        logical.clear();
    }
    if !logical.is_empty() {
        parse_logical_line(&logical, &mut properties)?;
    }
    Ok(properties)
}

fn parse_logical_line(line: &str, properties: &mut HashMap<String, String>) -> Result<()> {
    let mut escaped = false;
    let mut end = line.len();
    for (i, ch) in line.char_indices() {
        if !escaped && matches!(ch, '=' | ':' | ' ' | '\t' | '\u{c}') {
            end = i;
            break;
        }
        escaped = ch == '\\' && !escaped;
    }
    let mut value = line[end..].trim_start_matches([' ', '\t', '\u{c}']);
    if value.starts_with(['=', ':']) {
        value = &value[1..];
    }
    value = value.trim_start_matches([' ', '\t', '\u{c}']);
    properties.insert(unescape(&line[..end])?, unescape(value)?);
    Ok(())
}

fn unescape(input: &str) -> Result<String> {
    let mut units = Vec::with_capacity(input.len());
    let mut chars = input.chars();
    while let Some(ch) = chars.next() {
        let ch = if ch == '\\' {
            match chars.next() {
                Some('u') => {
                    let mut code = 0u16;
                    for _ in 0..4 {
                        let digit = chars.next().and_then(|c| c.to_digit(16)).ok_or_else(|| {
                            ParseLine("malformed \\uxxxx escape in properties".to_string())
                        })?;
                        code = (code << 4) | digit as u16;
                    }
                    units.push(code);
                    continue;
                }
                Some('t') => '\t',
                Some('n') => '\n',
                Some('r') => '\r',
                Some('f') => '\u{c}',
                Some(ch) => ch,
                None => break,
            }
        } else {
            ch
        };
        let mut encoded = [0; 2];
        units.extend_from_slice(ch.encode_utf16(&mut encoded));
    }
    String::from_utf16(&units).map_err(|e| ParseLine(format!("invalid UTF-16 in properties: {e}")))
}

/// Escapes a key or value the way `Properties.store` does, so the output is ASCII.
pub(crate) fn escape_property(input: &str, is_key: bool) -> String {
    let mut out = String::with_capacity(input.len());
    for (i, ch) in input.chars().enumerate() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\u{c}' => out.push_str("\\f"),
            ' ' if is_key || i == 0 => out.push_str("\\ "),
            '=' | ':' | '#' | '!' => {
                out.push('\\');
                out.push(ch);
            }
            ch if !(' '..='~').contains(&ch) => {
                let mut units = [0; 2];
                for unit in ch.encode_utf16(&mut units) {
                    out.push_str(&format!("\\u{unit:04X}"));
                }
            }
            ch => out.push(ch),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_java_store_output() {
        let props = parse_properties(include_bytes!(
            "../../tests/fixtures/properties/java-v9.properties"
        ))
        .unwrap();
        assert_eq!(props["hoodie.table.name"], "café");
        assert_eq!(
            props["hoodie.table.create.schema"],
            r#"{"type":"record","name":"row","fields":[{"name":"id","type":"string","default":"line\nnext"}]}"#
        );
        assert_eq!(props["hoodie.table.partition.fields"], "");
        assert_eq!(
            props["hoodie.test.special"],
            " leading café =:#!\\literal\nactual-newline"
        );
    }

    #[test]
    fn test_parse_java_load_grammar() {
        let props = parse_properties(include_bytes!(
            "../../tests/fixtures/properties/java-byte-stream.properties"
        ))
        .unwrap();
        assert_eq!(props["hoodie.test.iso"], "é");
        assert_eq!(props["hoodie.test.utf8"], "Ã©");
        assert_eq!(props["hoodie.test.continued"], "onetwo");
        assert_eq!(props["hoodie.test.supplementary"], "😀");
        assert_eq!(props["hoodie.test.key with spaces"], "value=colon:");

        // Values checked against Java 17 Properties.load.
        let props = parse_properties(b"a=1\\\n\\\n# comment\nb=2\n\\\n  ! also\nempty\n").unwrap();
        assert_eq!(props["a"], "1# comment");
        assert_eq!(props["b"], "2");
        assert_eq!(props["empty"], "");
        assert_eq!(props.len(), 3);

        assert!(parse_properties(br"bad=\uNOT0").is_err());
        assert!(parse_properties(br"bad=\uD800").is_err());
    }

    #[test]
    fn test_escape_matches_java_store() {
        // Expected strings are Java 17 Properties.store output.
        assert_eq!(escape_property(" key:=\\", true), r"\ key\:\=\\");
        assert_eq!(escape_property(" key:=\\", false), r"\ key\:\=\\");
        assert_eq!(escape_property("=:#!", false), r"\=\:\#\!");
        assert_eq!(escape_property("line\nnext\t", false), r"line\nnext\t");
        assert_eq!(escape_property("table😀", false), r"table\uD83D\uDE00");
        assert_eq!(escape_property("café", false), r"caf\u00E9");
    }

    #[test]
    fn test_escape_round_trips_and_is_ascii() {
        let key = " key:=\\";
        for value in [
            "café",
            "😀",
            " leading and trailing ",
            "line\nnext\t",
            r"path\next",
            "=:#!",
            "",
        ] {
            let line = format!(
                "{}={}\n",
                escape_property(key, true),
                escape_property(value, false)
            );
            assert!(line.is_ascii(), "{line}");
            assert_eq!(parse_properties(line.as_bytes()).unwrap()[key], value);
        }
    }
}
