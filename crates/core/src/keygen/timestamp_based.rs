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

use crate::config::error::ConfigError;
use crate::config::table::HudiTableConfig;
use crate::config::HudiConfigs;
use crate::error::CoreError;
use crate::expr::filter::Filter;
use crate::expr::ExprOperator;
use crate::keygen::KeyGeneratorFilterTransformer;
use crate::Result;
use chrono::{DateTime, Datelike, NaiveDateTime, TimeZone, Timelike, Utc};
use std::collections::HashMap;

/// Configuration for TimestampBasedKeyGenerator.
///
/// This key generator transforms timestamp values into hierarchical partition paths
/// based on date/time components (year, month, day, hour, etc.).
///
/// # Example Configuration
/// ```text
/// hoodie.table.keygenerator.class = org.apache.hudi.keygen.TimestampBasedKeyGenerator
/// hoodie.keygen.timebased.timestamp.type = DATE_STRING
/// hoodie.keygen.timebased.input.dateformat = yyyy-MM-dd'T'HH:mm:ss.SSSZ
/// hoodie.keygen.timebased.output.dateformat = yyyy/MM/dd/HH
/// hoodie.datasource.write.hive_style_partitioning = false
/// ```
#[derive(Debug, Clone)]
pub struct TimestampBasedKeyGenerator {
    /// The source field name in the data (e.g., "ts_str", "event_timestamp")
    source_field: String,

    /// Type of timestamp in the source field (e.g., UNIX_TIMESTAMP, DATE_STRING)
    timestamp_type: TimestampType,

    /// Input date format (only for DATE_STRING type)
    input_dateformat: Option<String>,

    /// Output date format that determines partition structure (e.g., "yyyy/MM/dd/HH")
    output_dateformat: String,

    /// Whether partitions use Hive-style naming (e.g., year=2024 vs 2024)
    #[allow(dead_code)]
    is_hive_style: bool,

    /// Partition field names derived from output format (e.g., ["year", "month", "day", "hour"])
    partition_fields: Vec<String>,
}

/// Type of timestamp value in the source field.
#[derive(Debug, Clone, PartialEq)]
pub enum TimestampType {
    /// Unix timestamp in milliseconds since epoch
    UnixTimestamp,
    /// Date string that needs to be parsed
    DateString,
}

impl TimestampBasedKeyGenerator {
    /// Creates a new TimestampBasedKeyGenerator from Hudi table configurations.
    ///
    /// # Required Configurations
    /// - `hoodie.table.partition.fields` - The source field name
    /// - `hoodie.keygen.timebased.timestamp.type` - "UNIX_TIMESTAMP" or "DATE_STRING"
    /// - `hoodie.keygen.timebased.output.dateformat` - Output format like "yyyy/MM/dd"
    ///
    /// # Optional Configurations
    /// - `hoodie.keygen.timebased.input.dateformat` - Required if timestamp.type is DATE_STRING
    /// - `hoodie.datasource.write.hive_style_partitioning` - Default is false
    pub fn from_configs(hudi_configs: &HudiConfigs) -> Result<Self> {
        // Get the partition field (source field)
        let partition_fields: Vec<String> = hudi_configs
            .get_or_default(HudiTableConfig::PartitionFields)
            .into();

        if partition_fields.is_empty() {
            return Err(CoreError::Config(ConfigError::NotFound(
                "No partition fields configured for TimestampBasedKeyGenerator".to_string(),
            )));
        }

        if partition_fields.len() > 1 {
            return Err(CoreError::Config(ConfigError::InvalidValue(
                "TimestampBasedKeyGenerator only supports a single partition field".to_string(),
            )));
        }

        let source_field = partition_fields[0].clone();

        // Get timestamp type
        let timestamp_type_str =
            Self::get_config_value(hudi_configs, "hoodie.keygen.timebased.timestamp.type")?;

        let timestamp_type = match timestamp_type_str.to_uppercase().as_str() {
            "UNIX_TIMESTAMP" => TimestampType::UnixTimestamp,
            "DATE_STRING" => TimestampType::DateString,
            _ => {
                return Err(CoreError::Config(ConfigError::InvalidValue(format!(
                    "Unsupported timestamp type: {}. Must be UNIX_TIMESTAMP or DATE_STRING",
                    timestamp_type_str
                ))))
            }
        };

        // Get input date format (required for DATE_STRING)
        let input_dateformat = if timestamp_type == TimestampType::DateString {
            Some(Self::get_config_value(
                hudi_configs,
                "hoodie.keygen.timebased.input.dateformat",
            )?)
        } else {
            None
        };

        // Get output date format (required)
        let output_dateformat =
            Self::get_config_value(hudi_configs, "hoodie.keygen.timebased.output.dateformat")?;

        // Get hive style partitioning setting
        let is_hive_style: bool = hudi_configs
            .get_or_default(HudiTableConfig::IsHiveStylePartitioning)
            .into();

        // Parse partition fields from output format
        let partition_fields = Self::parse_partition_fields(&output_dateformat, is_hive_style);

        Ok(Self {
            source_field,
            timestamp_type,
            input_dateformat,
            output_dateformat,
            is_hive_style,
            partition_fields,
        })
    }

    /// Helper to get a config value as String
    fn get_config_value(hudi_configs: &HudiConfigs, key: &str) -> Result<String> {
        hudi_configs.as_options().get(key).cloned().ok_or_else(|| {
            CoreError::Config(ConfigError::NotFound(format!(
                "Missing required configuration: {}",
                key
            )))
        })
    }

    /// Parses the output date format to determine partition field names.
    ///
    /// # Arguments
    /// * `output_format` - Format like "yyyy/MM/dd/HH"
    /// * `is_hive_style` - Whether to use Hive-style naming
    ///
    /// # Returns
    /// Vector of partition field names in order.
    ///
    /// # Examples
    /// ```text
    /// "yyyy/MM/dd/HH" + hive_style=true  → ["year", "month", "day", "hour"]
    /// "yyyy/MM/dd/HH" + hive_style=false → ["yyyy", "MM", "dd", "HH"]
    /// "yyyyMMdd"      + hive_style=true  → ["date"]
    /// ```
    fn parse_partition_fields(output_format: &str, is_hive_style: bool) -> Vec<String> {
        if !is_hive_style {
            // Non-hive style: use the format segments as-is
            return output_format.split('/').map(|s| s.to_string()).collect();
        }

        // Hive style: map format segments to semantic names
        let segments: Vec<&str> = output_format.split('/').collect();
        segments
            .iter()
            .map(|segment| Self::format_segment_to_field_name(segment))
            .collect()
    }

    /// Maps a date format segment to a Hive-style field name.
    fn format_segment_to_field_name(segment: &str) -> String {
        match segment {
            "yyyy" => "year".to_string(),
            "MM" => "month".to_string(),
            "dd" => "day".to_string(),
            "HH" => "hour".to_string(),
            "mm" => "minute".to_string(),
            "ss" => "second".to_string(),
            _ => segment.to_string(), // Fallback to the segment itself
        }
    }

    /// Parses a timestamp value into a DateTime<Utc>.
    fn parse_timestamp(&self, value: &str) -> Result<DateTime<Utc>> {
        match self.timestamp_type {
            TimestampType::UnixTimestamp => {
                let millis: i64 = value.parse().map_err(|e| {
                    CoreError::TimestampParsingError(format!(
                        "Failed to parse unix timestamp '{}': {}",
                        value, e
                    ))
                })?;

                DateTime::from_timestamp_millis(millis).ok_or_else(|| {
                    CoreError::TimestampParsingError(format!("Invalid unix timestamp: {}", millis))
                })
            }
            TimestampType::DateString => {
                let input_format = self.input_dateformat.as_ref().ok_or_else(|| {
                    CoreError::Config(ConfigError::NotFound(
                        "Input date format is required for DATE_STRING type".to_string(),
                    ))
                })?;

                // Convert Java date format to chrono format
                let chrono_format = Self::java_to_chrono_format(input_format);

                // Try parsing with timezone first
                if let Ok(dt) = DateTime::parse_from_str(value, &chrono_format) {
                    return Ok(dt.with_timezone(&Utc));
                }

                // Try parsing as naive datetime and assume UTC
                let naive_dt =
                    NaiveDateTime::parse_from_str(value, &chrono_format).map_err(|e| {
                        CoreError::TimestampParsingError(format!(
                            "Failed to parse date string '{}' with format '{}': {}",
                            value, chrono_format, e
                        ))
                    })?;

                Ok(Utc.from_utc_datetime(&naive_dt))
            }
        }
    }

    /// Converts Java SimpleDateFormat to chrono format string.
    /// This is a simplified conversion for common patterns.
    fn java_to_chrono_format(java_format: &str) -> String {
        java_format
            .replace("yyyy", "%Y")
            .replace("MM", "%m")
            .replace("dd", "%d")
            .replace("HH", "%H")
            .replace("mm", "%M")
            .replace("ss", "%S")
            .replace("SSS", "%3f")
            .replace("Z", "%#z") // %#z handles both Z and numeric offsets
            .replace("'T'", "T")
    }

    /// Extracts partition values from a datetime based on output format.
    ///
    /// # Returns
    /// HashMap mapping partition field names to their values.
    fn extract_partition_values(&self, dt: &DateTime<Utc>) -> HashMap<String, String> {
        let mut values = HashMap::new();

        let segments: Vec<&str> = self.output_dateformat.split('/').collect();

        for (i, segment) in segments.iter().enumerate() {
            let field_name = &self.partition_fields[i];
            let value = match *segment {
                "yyyy" => format!("{:04}", dt.year()),
                "MM" => format!("{:02}", dt.month()),
                "dd" => format!("{:02}", dt.day()),
                "HH" => format!("{:02}", dt.hour()),
                "mm" => format!("{:02}", dt.minute()),
                "ss" => format!("{:02}", dt.second()),
                _ => segment.to_string(),
            };
            values.insert(field_name.clone(), value);
        }

        values
    }
}

impl KeyGeneratorFilterTransformer for TimestampBasedKeyGenerator {
    fn source_field(&self) -> &str {
        &self.source_field
    }

    fn transform_filter(&self, filter: &Filter) -> Result<Vec<Filter>> {
        // Only transform filters on the source field
        if filter.field_name != self.source_field {
            return Ok(vec![filter.clone()]);
        }

        let dt = self.parse_timestamp(&filter.field_value)?;
        let partition_values = self.extract_partition_values(&dt);

        let mut filters = Vec::new();

        match filter.operator {
            ExprOperator::Eq => {
                // Exact match: all partition fields must match
                for field_name in &self.partition_fields {
                    if let Some(value) = partition_values.get(field_name) {
                        filters.push(Filter {
                            field_name: field_name.clone(),
                            operator: ExprOperator::Eq,
                            field_value: value.clone(),
                        });
                    }
                }
            }
            ExprOperator::Gte | ExprOperator::Gt => {
                // Only compares the first partition field for simplicity
                // Trade-off: may scan more partitions than necessary but avoids complex logic
                if let Some(first_field) = self.partition_fields.first() {
                    if let Some(value) = partition_values.get(first_field) {
                        filters.push(Filter {
                            field_name: first_field.clone(),
                            operator: ExprOperator::Gte, // Use >= for both > and >=
                            field_value: value.clone(),
                        });
                    }
                }
            }
            ExprOperator::Lte | ExprOperator::Lt => {
                // Less than: similar to greater than
                if let Some(first_field) = self.partition_fields.first() {
                    if let Some(value) = partition_values.get(first_field) {
                        filters.push(Filter {
                            field_name: first_field.clone(),
                            operator: ExprOperator::Lte, // Use <= for both < and <=
                            field_value: value.clone(),
                        });
                    }
                }
            }
            ExprOperator::Ne => {
                return Err(CoreError::Config(ConfigError::InvalidValue(format!(
                    "Not-equal (!=) operator is not supported for timestamp-based key generator \
                     filter transformation on field '{}'. Consider rewriting the query without \
                     != on partition columns.",
                    filter.field_name
                ))));
            }
        }

        Ok(filters)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_configs_date_string() -> HudiConfigs {
        HudiConfigs::new([
            ("hoodie.table.partition.fields", "ts_str"),
            (
                "hoodie.table.keygenerator.class",
                "org.apache.hudi.keygen.TimestampBasedKeyGenerator",
            ),
            ("hoodie.keygen.timebased.timestamp.type", "DATE_STRING"),
            (
                "hoodie.keygen.timebased.input.dateformat",
                "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
            ),
            ("hoodie.keygen.timebased.output.dateformat", "yyyy/MM/dd/HH"),
            ("hoodie.datasource.write.hive_style_partitioning", "true"),
        ])
    }

    fn create_test_configs_unix_timestamp() -> HudiConfigs {
        HudiConfigs::new([
            ("hoodie.table.partition.fields", "event_timestamp"),
            (
                "hoodie.table.keygenerator.class",
                "org.apache.hudi.keygen.TimestampBasedKeyGenerator",
            ),
            ("hoodie.keygen.timebased.timestamp.type", "UNIX_TIMESTAMP"),
            ("hoodie.keygen.timebased.output.dateformat", "yyyy/MM/dd"),
            ("hoodie.datasource.write.hive_style_partitioning", "false"),
        ])
    }

    #[test]
    fn test_parse_partition_fields_hive_style() {
        let fields = TimestampBasedKeyGenerator::parse_partition_fields("yyyy/MM/dd/HH", true);
        assert_eq!(fields, vec!["year", "month", "day", "hour"]);
    }

    #[test]
    fn test_parse_partition_fields_non_hive_style() {
        let fields = TimestampBasedKeyGenerator::parse_partition_fields("yyyy/MM/dd", false);
        assert_eq!(fields, vec!["yyyy", "MM", "dd"]);
    }

    #[test]
    fn test_java_to_chrono_format() {
        let input = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
        let expected = "%Y-%m-%dT%H:%M:%S.%3f%#z";
        assert_eq!(
            TimestampBasedKeyGenerator::java_to_chrono_format(input),
            expected
        );
    }

    #[test]
    fn test_from_configs_date_string() {
        let configs = create_test_configs_date_string();
        let keygen = TimestampBasedKeyGenerator::from_configs(&configs).unwrap();

        assert_eq!(keygen.source_field, "ts_str");
        assert_eq!(keygen.timestamp_type, TimestampType::DateString);
        assert_eq!(keygen.output_dateformat, "yyyy/MM/dd/HH");
        assert_eq!(
            keygen.partition_fields,
            vec!["year", "month", "day", "hour"]
        );
        assert!(keygen.is_hive_style);
    }

    #[test]
    fn test_from_configs_unix_timestamp() {
        let configs = create_test_configs_unix_timestamp();
        let keygen = TimestampBasedKeyGenerator::from_configs(&configs).unwrap();

        assert_eq!(keygen.source_field, "event_timestamp");
        assert_eq!(keygen.timestamp_type, TimestampType::UnixTimestamp);
        assert_eq!(keygen.output_dateformat, "yyyy/MM/dd");
        assert_eq!(keygen.partition_fields, vec!["yyyy", "MM", "dd"]);
        assert!(!keygen.is_hive_style);
    }

    #[test]
    fn test_from_configs_missing_partition_field() {
        let configs = HudiConfigs::new([
            (
                "hoodie.table.keygenerator.class",
                "org.apache.hudi.keygen.TimestampBasedKeyGenerator",
            ),
            ("hoodie.keygen.timebased.timestamp.type", "UNIX_TIMESTAMP"),
            ("hoodie.keygen.timebased.output.dateformat", "yyyy/MM/dd"),
        ]);

        let result = TimestampBasedKeyGenerator::from_configs(&configs);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_timestamp_unix() {
        let configs = create_test_configs_unix_timestamp();
        let keygen = TimestampBasedKeyGenerator::from_configs(&configs).unwrap();

        // 2024-01-25 00:00:00 UTC = 1706140800000 milliseconds
        let dt = keygen.parse_timestamp("1706140800000").unwrap();
        assert_eq!(dt.year(), 2024);
        assert_eq!(dt.month(), 1);
        assert_eq!(dt.day(), 25);
    }

    #[test]
    fn test_parse_timestamp_date_string() {
        let configs = create_test_configs_date_string();
        let keygen = TimestampBasedKeyGenerator::from_configs(&configs).unwrap();

        let dt = keygen.parse_timestamp("2023-04-01T12:01:00.123Z").unwrap();
        assert_eq!(dt.year(), 2023);
        assert_eq!(dt.month(), 4);
        assert_eq!(dt.day(), 1);
        assert_eq!(dt.hour(), 12);
    }

    #[test]
    fn test_extract_partition_values() {
        let configs = create_test_configs_date_string();
        let keygen = TimestampBasedKeyGenerator::from_configs(&configs).unwrap();

        let dt = keygen.parse_timestamp("2023-04-15T18:30:00.000Z").unwrap();
        let values = keygen.extract_partition_values(&dt);

        assert_eq!(values.get("year"), Some(&"2023".to_string()));
        assert_eq!(values.get("month"), Some(&"04".to_string()));
        assert_eq!(values.get("day"), Some(&"15".to_string()));
        assert_eq!(values.get("hour"), Some(&"18".to_string()));
    }

    #[test]
    fn test_transform_filter_equality() {
        let configs = create_test_configs_date_string();
        let keygen = TimestampBasedKeyGenerator::from_configs(&configs).unwrap();

        let filter = Filter {
            field_name: "ts_str".to_string(),
            operator: ExprOperator::Eq,
            field_value: "2023-04-01T12:01:00.123Z".to_string(),
        };

        let transformed = keygen.transform_filter(&filter).unwrap();

        // Should generate exact match filters for all partition fields
        assert_eq!(transformed.len(), 4);
        assert_eq!(transformed[0].field_name, "year");
        assert_eq!(transformed[0].operator, ExprOperator::Eq);
        assert_eq!(transformed[0].field_value, "2023");

        assert_eq!(transformed[1].field_name, "month");
        assert_eq!(transformed[1].field_value, "04");

        assert_eq!(transformed[2].field_name, "day");
        assert_eq!(transformed[2].field_value, "01");

        assert_eq!(transformed[3].field_name, "hour");
        assert_eq!(transformed[3].field_value, "12");
    }

    #[test]
    fn test_transform_filter_greater_than() {
        let configs = create_test_configs_unix_timestamp();
        let keygen = TimestampBasedKeyGenerator::from_configs(&configs).unwrap();

        let filter = Filter {
            field_name: "event_timestamp".to_string(),
            operator: ExprOperator::Gte,
            field_value: "1706140800000".to_string(), // 2024-01-25
        };

        let transformed = keygen.transform_filter(&filter).unwrap();

        // Should generate filter on first partition field (year)
        assert_eq!(transformed.len(), 1);
        assert_eq!(transformed[0].field_name, "yyyy");
        assert_eq!(transformed[0].operator, ExprOperator::Gte);
        assert_eq!(transformed[0].field_value, "2024");
    }

    #[test]
    fn test_transform_filter_less_than() {
        let configs = create_test_configs_date_string();
        let keygen = TimestampBasedKeyGenerator::from_configs(&configs).unwrap();

        let filter = Filter {
            field_name: "ts_str".to_string(),
            operator: ExprOperator::Lt,
            field_value: "2023-12-31T23:59:59.999Z".to_string(),
        };

        let transformed = keygen.transform_filter(&filter).unwrap();

        // Should generate filter on first partition field
        assert_eq!(transformed.len(), 1);
        assert_eq!(transformed[0].field_name, "year");
        assert_eq!(transformed[0].operator, ExprOperator::Lte);
        assert_eq!(transformed[0].field_value, "2023");
    }

    #[test]
    fn test_transform_filter_wrong_field() {
        let configs = create_test_configs_date_string();
        let keygen = TimestampBasedKeyGenerator::from_configs(&configs).unwrap();

        // Filter on a different field
        let filter = Filter {
            field_name: "other_field".to_string(),
            operator: ExprOperator::Eq,
            field_value: "value".to_string(),
        };

        let transformed = keygen.transform_filter(&filter).unwrap();

        // Should return the original filter unchanged
        assert_eq!(transformed.len(), 1);
        assert_eq!(transformed[0].field_name, "other_field");
        assert_eq!(transformed[0].field_value, "value");
    }

    #[test]
    fn test_transform_filter_not_equal_returns_error() {
        let configs = create_test_configs_date_string();
        let keygen = TimestampBasedKeyGenerator::from_configs(&configs).unwrap();

        let filter = Filter {
            field_name: "ts_str".to_string(),
            operator: ExprOperator::Ne,
            field_value: "2023-04-01T12:01:00.123Z".to_string(),
        };

        let result = keygen.transform_filter(&filter);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Not-equal (!=) operator is not supported"));
    }

    #[test]
    fn test_source_field() {
        let configs = create_test_configs_date_string();
        let keygen = TimestampBasedKeyGenerator::from_configs(&configs).unwrap();

        assert_eq!(keygen.source_field(), "ts_str");
    }
}
