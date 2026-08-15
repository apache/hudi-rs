// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.

//! Output rendering for both human (table) and machine (JSON) consumption.

use crate::OutputFormat;
use comfy_table::{Cell, Color, Table};

/// A renderable command output that supports both table and JSON formats.
pub struct CommandOutput {
    /// Section title (displayed in table mode, used as JSON key)
    pub sections: Vec<OutputSection>,
}

pub struct OutputSection {
    pub title: String,
    pub headers: Vec<String>,
    pub rows: Vec<Vec<String>>,
    /// Extra key-value metadata (shown above the table)
    pub metadata: Vec<(String, String)>,
}

impl CommandOutput {
    pub fn single(section: OutputSection) -> Self {
        Self {
            sections: vec![section],
        }
    }

    pub fn multi(sections: Vec<OutputSection>) -> Self {
        Self { sections }
    }

    pub fn render(&self, fmt: &OutputFormat) {
        match fmt {
            OutputFormat::Table => self.render_table(),
            OutputFormat::Json => self.render_json(),
            OutputFormat::Jsonl => self.render_jsonl(),
        }
    }

    fn render_table(&self) {
        for (i, section) in self.sections.iter().enumerate() {
            if i > 0 {
                println!();
            }
            if !section.title.is_empty() {
                println!("=== {} ===", section.title);
            }

            // Print metadata key-value pairs
            for (key, value) in &section.metadata {
                println!("  {}: {}", key, value);
            }
            if !section.metadata.is_empty() && !section.rows.is_empty() {
                println!();
            }

            // Print table if there are rows
            if !section.rows.is_empty() {
                let mut table = Table::new();
                table.set_header(
                    section
                        .headers
                        .iter()
                        .map(|h| Cell::new(h).fg(Color::Cyan))
                        .collect::<Vec<_>>(),
                );
                for row in &section.rows {
                    table.add_row(row.iter().map(|c| Cell::new(c)).collect::<Vec<_>>());
                }
                println!("{table}");
            }
        }
    }

    fn render_json(&self) {
        let json = self.to_json_value();
        println!("{}", serde_json::to_string_pretty(&json).unwrap());
    }

    fn render_jsonl(&self) {
        for section in &self.sections {
            for row in &section.rows {
                let obj: serde_json::Map<String, serde_json::Value> = section
                    .headers
                    .iter()
                    .zip(row.iter())
                    .map(|(h, v)| (h.clone(), serde_json::Value::String(v.clone())))
                    .collect();
                println!("{}", serde_json::to_string(&obj).unwrap());
            }
        }
    }

    fn to_json_value(&self) -> serde_json::Value {
        if self.sections.len() == 1 {
            section_to_json(&self.sections[0])
        } else {
            let mut map = serde_json::Map::new();
            for section in &self.sections {
                let key = section
                    .title
                    .to_lowercase()
                    .replace(' ', "_")
                    .replace('/', "_");
                map.insert(key, section_to_json(section));
            }
            serde_json::Value::Object(map)
        }
    }
}

fn section_to_json(section: &OutputSection) -> serde_json::Value {
    let mut result = serde_json::Map::new();

    // Add metadata
    for (key, value) in &section.metadata {
        let json_key = key.to_lowercase().replace(' ', "_").replace('/', "_");
        result.insert(json_key, serde_json::Value::String(value.clone()));
    }

    // Add rows as array of objects
    if !section.rows.is_empty() {
        let rows: Vec<serde_json::Value> = section
            .rows
            .iter()
            .map(|row| {
                let obj: serde_json::Map<String, serde_json::Value> = section
                    .headers
                    .iter()
                    .zip(row.iter())
                    .map(|(h, v)| {
                        let key = h.to_lowercase().replace(' ', "_");
                        // Try to parse as number for JSON output
                        let val = if let Ok(n) = v.parse::<i64>() {
                            serde_json::Value::Number(n.into())
                        } else if let Ok(f) = v.parse::<f64>() {
                            serde_json::json!(f)
                        } else {
                            serde_json::Value::String(v.clone())
                        };
                        (key, val)
                    })
                    .collect();
                serde_json::Value::Object(obj)
            })
            .collect();
        result.insert("data".to_string(), serde_json::Value::Array(rows));
    }

    serde_json::Value::Object(result)
}

/// Helper to format byte sizes into human-readable strings.
pub fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;
    const TB: u64 = 1024 * GB;

    if bytes >= TB {
        format!("{:.1} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} B")
    }
}
