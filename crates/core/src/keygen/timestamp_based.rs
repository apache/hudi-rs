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

use crate::Result;
use crate::config::HudiConfigs;
use crate::config::error::ConfigError;
use crate::config::table::HudiTableConfig;
use crate::error::CoreError;
use crate::expr::ExprOperator;
use crate::expr::filter::Filter;
use crate::keygen::KeyGeneratorFilterTransformer;
use chrono::{DateTime, Datelike, NaiveDateTime, TimeZone, Timelike, Utc};
use chrono_tz::Tz;
use std::collections::HashMap;

/// Configuration for TimestampBasedKeyGenerator.
///
/// This key generator transforms timestamp values into hierarchical partition paths
/// based on date/time components (year, month, day, hour, etc.).
#[derive(Debug, Clone)]
pub struct TimestampBasedKeyGenerator {
    /// The source field name in the data (e.g., "ts_str", "event_timestamp")
    source_field: String,

    /// Type of timestamp in the source field
    timestamp_type: TimestampType,

    /// Input date format (only for DATE_STRING/MIXED type)
    input_dateformat: Option<String>,

    /// Input timezone for interpreting DATE_STRING values without embedded offset
    input_timezone: Option<Tz>,

    /// Time unit for SCALAR timestamp type
    scalar_time_unit: Option<ScalarTimeUnit>,

    /// Output date format that determines partition structure (e.g., "yyyy/MM/dd/HH")
    output_dateformat: String,

    /// Output timezone for converting timestamps to partition values
    output_timezone: Tz,

    /// Whether partitions use Hive-style naming (e.g., year=2024 vs 2024)
    #[allow(dead_code)]
    is_hive_style: bool,

    /// Partition field names derived from output format (e.g., ["year", "month", "day", "hour"])
    partition_fields: Vec<String>,
}

/// Type of timestamp value in the source field.
#[derive(Debug, Clone, PartialEq)]
pub enum TimestampType {
    /// Unix timestamp in seconds since epoch
    UnixTimestamp,
    /// Epoch milliseconds since epoch
    EpochMilliseconds,
    /// Epoch microseconds since epoch
    EpochMicroseconds,
    /// Date string that needs to be parsed
    DateString,
    /// Numeric value in configurable time units
    Scalar,
    /// Mixed types (treated like DATE_STRING)
    Mixed,
}

/// Time unit for SCALAR timestamp type.
#[derive(Debug, Clone, PartialEq)]
enum ScalarTimeUnit {
    Nanoseconds,
    Microseconds,
    Milliseconds,
    Seconds,
    Minutes,
    Hours,
    Days,
}

impl ScalarTimeUnit {
    fn from_str(s: &str) -> Result<Self> {
        match s.to_uppercase().as_str() {
            "NANOSECONDS" => Ok(Self::Nanoseconds),
            "MICROSECONDS" => Ok(Self::Microseconds),
            "MILLISECONDS" => Ok(Self::Milliseconds),
            "SECONDS" => Ok(Self::Seconds),
            "MINUTES" => Ok(Self::Minutes),
            "HOURS" => Ok(Self::Hours),
            "DAYS" => Ok(Self::Days),
            _ => Err(CoreError::Config(ConfigError::InvalidValue(format!(
                "Unsupported scalar time unit: {s}"
            )))),
        }
    }

    fn to_millis(&self, value: i64) -> i64 {
        match self {
            Self::Nanoseconds => value / 1_000_000,
            Self::Microseconds => value / 1_000,
            Self::Milliseconds => value,
            Self::Seconds => value * 1_000,
            Self::Minutes => value * 60_000,
            Self::Hours => value * 3_600_000,
            Self::Days => value * 86_400_000,
        }
    }
}

impl TimestampBasedKeyGenerator {
    /// Config key prefix for timestamp-based key generator properties.
    const CONFIG_PREFIX: &'static str = "hoodie.keygen.timebased.";

    /// Legacy config key prefix (deltastreamer).
    const OLD_CONFIG_PREFIX: &'static str = "hoodie.deltastreamer.keygen.timebased.";

    /// Creates a new TimestampBasedKeyGenerator from Hudi table configurations.
    ///
    /// # Required Configurations
    /// - `hoodie.table.partition.fields` - The source field name
    /// - `hoodie.keygen.timebased.timestamp.type` - Timestamp type
    /// - `hoodie.keygen.timebased.output.dateformat` - Output format like "yyyy/MM/dd"
    ///
    /// # Optional Configurations
    /// - `hoodie.keygen.timebased.input.dateformat` - Required if type is DATE_STRING/MIXED
    /// - `hoodie.keygen.timebased.input.timezone` - Input timezone (default: UTC)
    /// - `hoodie.keygen.timebased.output.timezone` - Output timezone (default: UTC)
    /// - `hoodie.keygen.timebased.timezone` - Fallback timezone for both input/output
    /// - `hoodie.keygen.timebased.timestamp.scalar.time.unit` - Time unit for SCALAR type
    /// - `hoodie.datasource.write.hive_style_partitioning` - Default is false
    pub fn from_configs(hudi_configs: &HudiConfigs) -> Result<Self> {
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

        let timestamp_type_str = Self::get_config_value_with_alt(hudi_configs, "timestamp.type")?;

        let timestamp_type = match timestamp_type_str.to_uppercase().as_str() {
            "UNIX_TIMESTAMP" => TimestampType::UnixTimestamp,
            "EPOCHMILLISECONDS" => TimestampType::EpochMilliseconds,
            "EPOCHMICROSECONDS" => TimestampType::EpochMicroseconds,
            "DATE_STRING" => TimestampType::DateString,
            "SCALAR" => TimestampType::Scalar,
            "MIXED" => TimestampType::Mixed,
            _ => {
                return Err(CoreError::Config(ConfigError::InvalidValue(format!(
                    "Unsupported timestamp type: {timestamp_type_str}. Must be one of: \
                     UNIX_TIMESTAMP, EPOCHMILLISECONDS, EPOCHMICROSECONDS, DATE_STRING, SCALAR, MIXED"
                ))));
            }
        };

        let input_dateformat = if timestamp_type == TimestampType::DateString
            || timestamp_type == TimestampType::Mixed
        {
            Some(Self::get_config_value_with_alt(
                hudi_configs,
                "input.dateformat",
            )?)
        } else {
            None
        };

        let scalar_time_unit = if timestamp_type == TimestampType::Scalar {
            let unit_str = Self::resolve_option_with_alt(
                &hudi_configs.as_options(),
                "timestamp.scalar.time.unit",
            )
            .unwrap_or_else(|| "SECONDS".to_string());
            Some(ScalarTimeUnit::from_str(&unit_str)?)
        } else {
            None
        };

        let output_dateformat = Self::get_config_value_with_alt(hudi_configs, "output.dateformat")?;

        let options = hudi_configs.as_options();

        // Read input timezone: hoodie.keygen.timebased.timezone → input.timezone → None
        let input_tz_str = Self::resolve_option_with_alt(&options, "timezone")
            .or_else(|| Self::resolve_option_with_alt(&options, "input.timezone"));

        let input_timezone = match input_tz_str {
            Some(tz_str) if !tz_str.trim().is_empty() => {
                Some(tz_str.parse::<Tz>().map_err(|_| {
                    CoreError::Config(ConfigError::InvalidValue(format!(
                        "Invalid input timezone: {tz_str}"
                    )))
                })?)
            }
            _ => None,
        };

        // Read output timezone: hoodie.keygen.timebased.timezone → output.timezone → UTC
        let output_tz_str = Self::resolve_option_with_alt(&options, "timezone")
            .or_else(|| Self::resolve_option_with_alt(&options, "output.timezone"))
            .unwrap_or_else(|| "UTC".to_string());

        let output_timezone: Tz = output_tz_str.parse().map_err(|_| {
            CoreError::Config(ConfigError::InvalidValue(format!(
                "Invalid output timezone: {output_tz_str}"
            )))
        })?;

        let is_hive_style: bool = hudi_configs
            .get_or_default(HudiTableConfig::IsHiveStylePartitioning)
            .into();

        let partition_fields = Self::parse_partition_fields(&output_dateformat, is_hive_style);

        Ok(Self {
            source_field,
            timestamp_type,
            input_dateformat,
            input_timezone,
            scalar_time_unit,
            output_dateformat,
            output_timezone,
            is_hive_style,
            partition_fields,
        })
    }

    /// Reads a config value, trying both the standard prefix and the legacy deltastreamer prefix.
    fn get_config_value_with_alt(hudi_configs: &HudiConfigs, suffix: &str) -> Result<String> {
        let options = hudi_configs.as_options();
        let key = format!("{}{suffix}", Self::CONFIG_PREFIX);
        let alt_key = format!("{}{suffix}", Self::OLD_CONFIG_PREFIX);

        options
            .get(&key)
            .or_else(|| options.get(&alt_key))
            .cloned()
            .ok_or_else(|| {
                CoreError::Config(ConfigError::NotFound(format!(
                    "Missing required configuration: {key}"
                )))
            })
    }

    /// Resolves an option value from the options map, trying both prefixes.
    fn resolve_option_with_alt(options: &HashMap<String, String>, suffix: &str) -> Option<String> {
        let key = format!("{}{suffix}", Self::CONFIG_PREFIX);
        let alt_key = format!("{}{suffix}", Self::OLD_CONFIG_PREFIX);
        options.get(&key).or_else(|| options.get(&alt_key)).cloned()
    }

    /// Parses the output date format to determine partition field names.
    ///
    /// For hive-style: "yyyy/MM/dd/HH" → ["year", "month", "day", "hour"]
    /// For non-hive-style: "yyyy/MM/dd" → ["yyyy", "MM", "dd"]
    fn parse_partition_fields(output_format: &str, is_hive_style: bool) -> Vec<String> {
        if !is_hive_style {
            return output_format.split('/').map(|s| s.to_string()).collect();
        }

        output_format
            .split('/')
            .map(Self::format_segment_to_field_name)
            .collect()
    }

    fn format_segment_to_field_name(segment: &str) -> String {
        match segment {
            "yyyy" => "year".to_string(),
            "MM" => "month".to_string(),
            "dd" => "day".to_string(),
            "HH" => "hour".to_string(),
            "mm" => "minute".to_string(),
            "ss" => "second".to_string(),
            _ => segment.to_string(),
        }
    }

    /// Parses a timestamp value into a DateTime<Utc>.
    fn parse_timestamp(&self, value: &str) -> Result<DateTime<Utc>> {
        match self.timestamp_type {
            TimestampType::UnixTimestamp => {
                let secs: i64 = value.parse().map_err(|e| {
                    CoreError::TimestampParsingError(format!(
                        "Failed to parse unix timestamp '{value}': {e}"
                    ))
                })?;

                DateTime::from_timestamp(secs, 0).ok_or_else(|| {
                    CoreError::TimestampParsingError(format!("Invalid unix timestamp: {secs}"))
                })
            }
            TimestampType::EpochMilliseconds => {
                let millis: i64 = value.parse().map_err(|e| {
                    CoreError::TimestampParsingError(format!(
                        "Failed to parse epoch milliseconds '{value}': {e}"
                    ))
                })?;

                DateTime::from_timestamp_millis(millis).ok_or_else(|| {
                    CoreError::TimestampParsingError(format!(
                        "Invalid epoch milliseconds: {millis}"
                    ))
                })
            }
            TimestampType::EpochMicroseconds => {
                let micros: i64 = value.parse().map_err(|e| {
                    CoreError::TimestampParsingError(format!(
                        "Failed to parse epoch microseconds '{value}': {e}"
                    ))
                })?;

                DateTime::from_timestamp_micros(micros).ok_or_else(|| {
                    CoreError::TimestampParsingError(format!(
                        "Invalid epoch microseconds: {micros}"
                    ))
                })
            }
            TimestampType::Scalar => {
                let scalar_val: i64 = value.parse().map_err(|e| {
                    CoreError::TimestampParsingError(format!(
                        "Failed to parse scalar timestamp '{value}': {e}"
                    ))
                })?;

                let unit = self.scalar_time_unit.as_ref().ok_or_else(|| {
                    CoreError::Config(ConfigError::NotFound(
                        "Scalar time unit not configured for SCALAR type".to_string(),
                    ))
                })?;
                let millis = unit.to_millis(scalar_val);

                DateTime::from_timestamp_millis(millis).ok_or_else(|| {
                    CoreError::TimestampParsingError(format!(
                        "Invalid scalar timestamp: {scalar_val}"
                    ))
                })
            }
            TimestampType::DateString | TimestampType::Mixed => {
                let input_format = self.input_dateformat.as_ref().ok_or_else(|| {
                    CoreError::Config(ConfigError::NotFound(
                        "Input date format is required for DATE_STRING/MIXED type".to_string(),
                    ))
                })?;

                let chrono_format = Self::java_to_chrono_format(input_format);

                // Try parsing with embedded timezone first
                if let Ok(dt) = DateTime::parse_from_str(value, &chrono_format) {
                    return Ok(dt.with_timezone(&Utc));
                }

                // Try parsing as naive datetime, apply input timezone if configured
                let naive_dt =
                    NaiveDateTime::parse_from_str(value, &chrono_format).map_err(|e| {
                        CoreError::TimestampParsingError(format!(
                            "Failed to parse date string '{value}' with format '{chrono_format}': {e}"
                        ))
                    })?;

                if let Some(input_tz) = &self.input_timezone {
                    // Apply input timezone: interpret naive datetime as being in input_tz
                    Ok(input_tz
                        .from_local_datetime(&naive_dt)
                        .single()
                        .ok_or_else(|| {
                            CoreError::TimestampParsingError(format!(
                                "Ambiguous or invalid datetime '{value}' in timezone '{input_tz}'"
                            ))
                        })?
                        .with_timezone(&Utc))
                } else {
                    // No input timezone configured: assume UTC
                    Ok(Utc.from_utc_datetime(&naive_dt))
                }
            }
        }
    }

    /// Converts Java SimpleDateFormat to chrono format string.
    fn java_to_chrono_format(java_format: &str) -> String {
        // Longer tokens must be replaced before their substrings
        // (e.g., SSS before ss, to avoid partial replacement in formats like "HHmmssSSS")
        java_format
            .replace("yyyy", "%Y")
            .replace("SSS", "%3f")
            .replace("MM", "%m")
            .replace("dd", "%d")
            .replace("HH", "%H")
            .replace("mm", "%M")
            .replace("ss", "%S")
            .replace("Z", "%#z")
            .replace("'T'", "T")
    }

    /// Extracts partition values from a datetime based on output format,
    /// applying the configured output timezone.
    fn extract_partition_values(&self, dt: &DateTime<Utc>) -> HashMap<String, String> {
        let local_dt = dt.with_timezone(&self.output_timezone);
        let mut values = HashMap::new();

        let segments: Vec<&str> = self.output_dateformat.split('/').collect();

        for (i, segment) in segments.iter().enumerate() {
            let field_name = &self.partition_fields[i];
            let value = match *segment {
                "yyyy" => format!("{:04}", local_dt.year()),
                "MM" => format!("{:02}", local_dt.month()),
                "dd" => format!("{:02}", local_dt.day()),
                "HH" => format!("{:02}", local_dt.hour()),
                "mm" => format!("{:02}", local_dt.minute()),
                "ss" => format!("{:02}", local_dt.second()),
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
        if filter.field_name != self.source_field {
            return Ok(vec![filter.clone()]);
        }

        // Check for unsupported operators first before parsing
        match filter.operator {
            ExprOperator::Ne => {
                return Err(CoreError::Config(ConfigError::InvalidValue(format!(
                    "Not-equal (!=) operator is not supported for timestamp-based partition \
                     pruning on field '{}'. Rewrite the query without != on partition columns.",
                    filter.field_name
                ))));
            }
            ExprOperator::In | ExprOperator::NotIn => {
                return Err(CoreError::Config(ConfigError::InvalidValue(format!(
                    "IN/NOT IN operators are not supported for timestamp-based partition \
                     pruning on field '{}'. Rewrite the query using equality/range comparisons.",
                    filter.field_name
                ))));
            }
            _ => {}
        }

        let dt = self.parse_timestamp(&filter.values[0])?;
        let partition_values = self.extract_partition_values(&dt);

        let mut filters = Vec::new();

        match filter.operator {
            ExprOperator::Eq => {
                for field_name in &self.partition_fields {
                    if let Some(value) = partition_values.get(field_name) {
                        filters.push(Filter {
                            field_name: field_name.clone(),
                            operator: ExprOperator::Eq,
                            values: vec![value.clone()],
                        });
                    }
                }
            }
            ExprOperator::Gte | ExprOperator::Gt => {
                // Only compare the first partition field for simplicity.
                // May scan more partitions than necessary but avoids complex multi-field range logic.
                if let Some(first_field) = self.partition_fields.first() {
                    if let Some(value) = partition_values.get(first_field) {
                        filters.push(Filter {
                            field_name: first_field.clone(),
                            operator: ExprOperator::Gte,
                            values: vec![value.clone()],
                        });
                    }
                }
            }
            ExprOperator::Lte | ExprOperator::Lt => {
                if let Some(first_field) = self.partition_fields.first() {
                    if let Some(value) = partition_values.get(first_field) {
                        filters.push(Filter {
                            field_name: first_field.clone(),
                            operator: ExprOperator::Lte,
                            values: vec![value.clone()],
                        });
                    }
                }
            }
            // Ne, In, NotIn are already handled above before parsing
            ExprOperator::Ne | ExprOperator::In | ExprOperator::NotIn => {
                unreachable!("Unsupported operators should have been caught earlier")
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
    fn test_utility_functions() {
        // Java SimpleDateFormat → chrono format conversion
        assert_eq!(
            TimestampBasedKeyGenerator::java_to_chrono_format("yyyy-MM-dd'T'HH:mm:ss.SSSZ"),
            "%Y-%m-%dT%H:%M:%S.%3f%#z"
        );

        // Hive-style: format segments → semantic names
        assert_eq!(
            TimestampBasedKeyGenerator::parse_partition_fields("yyyy/MM/dd/HH", true),
            vec!["year", "month", "day", "hour"]
        );

        // Non-hive-style: format segments used as-is
        assert_eq!(
            TimestampBasedKeyGenerator::parse_partition_fields("yyyy/MM/dd", false),
            vec!["yyyy", "MM", "dd"]
        );
    }

    #[test]
    fn test_construction_and_parsing() {
        // DATE_STRING: hive-style with timezone in input
        let keygen =
            TimestampBasedKeyGenerator::from_configs(&create_test_configs_date_string()).unwrap();
        assert_eq!(keygen.source_field, "ts_str");
        assert_eq!(keygen.timestamp_type, TimestampType::DateString);
        assert_eq!(
            keygen.partition_fields,
            vec!["year", "month", "day", "hour"]
        );
        assert!(keygen.is_hive_style);
        let dt = keygen.parse_timestamp("2023-04-01T12:01:00.123Z").unwrap();
        assert_eq!(
            (dt.year(), dt.month(), dt.day(), dt.hour()),
            (2023, 4, 1, 12)
        );

        // DATE_STRING: without timezone — falls back to NaiveDateTime
        let configs = HudiConfigs::new([
            ("hoodie.table.partition.fields", "ts"),
            ("hoodie.keygen.timebased.timestamp.type", "DATE_STRING"),
            (
                "hoodie.keygen.timebased.input.dateformat",
                "yyyy-MM-dd HH:mm:ss",
            ),
            ("hoodie.keygen.timebased.output.dateformat", "yyyy/MM/dd"),
            ("hoodie.datasource.write.hive_style_partitioning", "true"),
        ]);
        let keygen = TimestampBasedKeyGenerator::from_configs(&configs).unwrap();
        let dt = keygen.parse_timestamp("2023-04-15 18:30:00").unwrap();
        assert_eq!(
            (dt.year(), dt.month(), dt.day(), dt.hour()),
            (2023, 4, 15, 18)
        );

        // DATE_STRING: with input timezone — naive datetime interpreted in that timezone
        let configs = HudiConfigs::new([
            ("hoodie.table.partition.fields", "ts"),
            ("hoodie.keygen.timebased.timestamp.type", "DATE_STRING"),
            (
                "hoodie.keygen.timebased.input.dateformat",
                "yyyy-MM-dd HH:mm:ss",
            ),
            ("hoodie.keygen.timebased.input.timezone", "Asia/Tokyo"),
            ("hoodie.keygen.timebased.output.dateformat", "yyyy/MM/dd"),
            ("hoodie.datasource.write.hive_style_partitioning", "true"),
        ]);
        let keygen = TimestampBasedKeyGenerator::from_configs(&configs).unwrap();
        // 2023-04-15 18:30:00 JST = 2023-04-15 09:30:00 UTC
        let dt = keygen.parse_timestamp("2023-04-15 18:30:00").unwrap();
        assert_eq!(
            (dt.year(), dt.month(), dt.day(), dt.hour()),
            (2023, 4, 15, 9)
        );

        // UNIX_TIMESTAMP: non-hive-style, seconds since epoch
        let keygen =
            TimestampBasedKeyGenerator::from_configs(&create_test_configs_unix_timestamp())
                .unwrap();
        assert_eq!(keygen.timestamp_type, TimestampType::UnixTimestamp);
        assert_eq!(keygen.partition_fields, vec!["yyyy", "MM", "dd"]);
        assert!(!keygen.is_hive_style);
        // 2024-01-25 00:00:00 UTC = 1706140800 seconds
        let dt = keygen.parse_timestamp("1706140800").unwrap();
        assert_eq!((dt.year(), dt.month(), dt.day()), (2024, 1, 25));

        // EPOCHMILLISECONDS
        let configs = HudiConfigs::new([
            ("hoodie.table.partition.fields", "event_time"),
            (
                "hoodie.keygen.timebased.timestamp.type",
                "EPOCHMILLISECONDS",
            ),
            ("hoodie.keygen.timebased.output.dateformat", "yyyy/MM/dd"),
            ("hoodie.datasource.write.hive_style_partitioning", "true"),
        ]);
        let keygen = TimestampBasedKeyGenerator::from_configs(&configs).unwrap();
        assert_eq!(keygen.timestamp_type, TimestampType::EpochMilliseconds);
        // 2024-01-25 00:00:00 UTC = 1706140800000 milliseconds
        let dt = keygen.parse_timestamp("1706140800000").unwrap();
        assert_eq!((dt.year(), dt.month(), dt.day()), (2024, 1, 25));

        // EPOCHMICROSECONDS
        let configs = HudiConfigs::new([
            ("hoodie.table.partition.fields", "event_time"),
            (
                "hoodie.keygen.timebased.timestamp.type",
                "EPOCHMICROSECONDS",
            ),
            ("hoodie.keygen.timebased.output.dateformat", "yyyy/MM/dd"),
            ("hoodie.datasource.write.hive_style_partitioning", "true"),
        ]);
        let keygen = TimestampBasedKeyGenerator::from_configs(&configs).unwrap();
        assert_eq!(keygen.timestamp_type, TimestampType::EpochMicroseconds);
        // 2024-01-25 00:00:00 UTC = 1706140800000000 microseconds
        let dt = keygen.parse_timestamp("1706140800000000").unwrap();
        assert_eq!((dt.year(), dt.month(), dt.day()), (2024, 1, 25));

        // MIXED: treated like DATE_STRING
        let configs = HudiConfigs::new([
            ("hoodie.table.partition.fields", "ts"),
            ("hoodie.keygen.timebased.timestamp.type", "MIXED"),
            (
                "hoodie.keygen.timebased.input.dateformat",
                "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
            ),
            ("hoodie.keygen.timebased.output.dateformat", "yyyy/MM/dd"),
            ("hoodie.datasource.write.hive_style_partitioning", "true"),
        ]);
        let keygen = TimestampBasedKeyGenerator::from_configs(&configs).unwrap();
        assert_eq!(keygen.timestamp_type, TimestampType::Mixed);
        let dt = keygen.parse_timestamp("2023-04-01T12:01:00.123Z").unwrap();
        assert_eq!((dt.year(), dt.month(), dt.day()), (2023, 4, 1));

        // SCALAR: default time unit is SECONDS
        let configs = HudiConfigs::new([
            ("hoodie.table.partition.fields", "ts"),
            ("hoodie.keygen.timebased.timestamp.type", "SCALAR"),
            ("hoodie.keygen.timebased.output.dateformat", "yyyy/MM/dd"),
            ("hoodie.datasource.write.hive_style_partitioning", "false"),
        ]);
        let keygen = TimestampBasedKeyGenerator::from_configs(&configs).unwrap();
        assert_eq!(keygen.timestamp_type, TimestampType::Scalar);
        // 1706140800 seconds = 2024-01-25 00:00:00 UTC
        let dt = keygen.parse_timestamp("1706140800").unwrap();
        assert_eq!((dt.year(), dt.month(), dt.day()), (2024, 1, 25));

        // SCALAR: explicit MILLISECONDS time unit
        let configs = HudiConfigs::new([
            ("hoodie.table.partition.fields", "ts"),
            ("hoodie.keygen.timebased.timestamp.type", "SCALAR"),
            (
                "hoodie.keygen.timebased.timestamp.scalar.time.unit",
                "MILLISECONDS",
            ),
            ("hoodie.keygen.timebased.output.dateformat", "yyyy/MM/dd"),
            ("hoodie.datasource.write.hive_style_partitioning", "false"),
        ]);
        let keygen = TimestampBasedKeyGenerator::from_configs(&configs).unwrap();
        let dt = keygen.parse_timestamp("1706140800000").unwrap();
        assert_eq!((dt.year(), dt.month(), dt.day()), (2024, 1, 25));

        // Legacy deltastreamer prefix
        let configs = HudiConfigs::new([
            ("hoodie.table.partition.fields", "ts"),
            (
                "hoodie.deltastreamer.keygen.timebased.timestamp.type",
                "UNIX_TIMESTAMP",
            ),
            (
                "hoodie.deltastreamer.keygen.timebased.output.dateformat",
                "yyyy/MM/dd",
            ),
            ("hoodie.datasource.write.hive_style_partitioning", "false"),
        ]);
        let keygen = TimestampBasedKeyGenerator::from_configs(&configs).unwrap();
        assert_eq!(keygen.timestamp_type, TimestampType::UnixTimestamp);
    }

    #[test]
    fn test_from_configs_errors() {
        // Missing partition fields
        let configs = HudiConfigs::new([
            ("hoodie.keygen.timebased.timestamp.type", "UNIX_TIMESTAMP"),
            ("hoodie.keygen.timebased.output.dateformat", "yyyy/MM/dd"),
        ]);
        assert!(TimestampBasedKeyGenerator::from_configs(&configs).is_err());

        // Multiple partition fields
        let configs = HudiConfigs::new([
            ("hoodie.table.partition.fields", "field1,field2"),
            ("hoodie.keygen.timebased.timestamp.type", "UNIX_TIMESTAMP"),
            ("hoodie.keygen.timebased.output.dateformat", "yyyy/MM/dd"),
        ]);
        assert!(
            TimestampBasedKeyGenerator::from_configs(&configs)
                .unwrap_err()
                .to_string()
                .contains("single partition field")
        );

        // Unsupported timestamp type
        let configs = HudiConfigs::new([
            ("hoodie.table.partition.fields", "ts"),
            ("hoodie.keygen.timebased.timestamp.type", "INVALID_TYPE"),
            ("hoodie.keygen.timebased.output.dateformat", "yyyy/MM/dd"),
        ]);
        assert!(
            TimestampBasedKeyGenerator::from_configs(&configs)
                .unwrap_err()
                .to_string()
                .contains("Unsupported timestamp type")
        );

        // Invalid timezone
        let configs = HudiConfigs::new([
            ("hoodie.table.partition.fields", "ts"),
            ("hoodie.keygen.timebased.timestamp.type", "UNIX_TIMESTAMP"),
            ("hoodie.keygen.timebased.output.dateformat", "yyyy/MM/dd"),
            (
                "hoodie.keygen.timebased.output.timezone",
                "Invalid/Timezone",
            ),
        ]);
        assert!(TimestampBasedKeyGenerator::from_configs(&configs).is_err());
    }

    #[test]
    fn test_timezone_config_and_partition_values() {
        // output.timezone shifts date components
        let configs = HudiConfigs::new([
            ("hoodie.table.partition.fields", "ts"),
            ("hoodie.keygen.timebased.timestamp.type", "UNIX_TIMESTAMP"),
            ("hoodie.keygen.timebased.output.dateformat", "yyyy/MM/dd"),
            (
                "hoodie.keygen.timebased.output.timezone",
                "America/New_York",
            ),
            ("hoodie.datasource.write.hive_style_partitioning", "true"),
        ]);
        let keygen = TimestampBasedKeyGenerator::from_configs(&configs).unwrap();

        // 2024-01-25 03:00:00 UTC = 2024-01-24 22:00:00 EST → day=24
        let dt = keygen.parse_timestamp("1706151600").unwrap();
        let values = keygen.extract_partition_values(&dt);
        assert_eq!(values.get("day"), Some(&"24".to_string()));

        // Fallback: hoodie.keygen.timebased.timezone used when output.timezone absent
        let configs = HudiConfigs::new([
            ("hoodie.table.partition.fields", "ts"),
            ("hoodie.keygen.timebased.timestamp.type", "UNIX_TIMESTAMP"),
            ("hoodie.keygen.timebased.output.dateformat", "yyyy/MM/dd"),
            ("hoodie.keygen.timebased.timezone", "Asia/Tokyo"),
            ("hoodie.datasource.write.hive_style_partitioning", "true"),
        ]);
        let keygen = TimestampBasedKeyGenerator::from_configs(&configs).unwrap();
        assert_eq!(keygen.output_timezone, chrono_tz::Asia::Tokyo);

        // 2024-01-25 20:00:00 UTC = 2024-01-26 05:00:00 JST → day=26
        let dt = keygen.parse_timestamp("1706212800").unwrap();
        let values = keygen.extract_partition_values(&dt);
        assert_eq!(values.get("day"), Some(&"26".to_string()));

        // Precedence: deprecated shared `timezone` wins over specific `output.timezone`
        let configs = HudiConfigs::new([
            ("hoodie.table.partition.fields", "ts"),
            ("hoodie.keygen.timebased.timestamp.type", "UNIX_TIMESTAMP"),
            ("hoodie.keygen.timebased.output.dateformat", "yyyy/MM/dd"),
            (
                "hoodie.keygen.timebased.output.timezone",
                "America/New_York",
            ),
            ("hoodie.keygen.timebased.timezone", "Asia/Tokyo"),
            ("hoodie.datasource.write.hive_style_partitioning", "true"),
        ]);
        let keygen = TimestampBasedKeyGenerator::from_configs(&configs).unwrap();
        assert_eq!(keygen.output_timezone, chrono_tz::Asia::Tokyo);
    }

    #[test]
    fn test_transform_filter() {
        // Equality: DATE_STRING → expands to all partition fields
        let keygen =
            TimestampBasedKeyGenerator::from_configs(&create_test_configs_date_string()).unwrap();

        let filter = Filter {
            field_name: "ts_str".to_string(),
            operator: ExprOperator::Eq,
            values: vec!["2023-04-01T12:01:00.123Z".to_string()],
        };
        let transformed = keygen.transform_filter(&filter).unwrap();
        assert_eq!(transformed.len(), 4);
        assert_eq!(
            (
                transformed[0].field_name.as_str(),
                transformed[0].values[0].as_str()
            ),
            ("year", "2023")
        );
        assert_eq!(
            (
                transformed[1].field_name.as_str(),
                transformed[1].values[0].as_str()
            ),
            ("month", "04")
        );
        assert_eq!(
            (
                transformed[2].field_name.as_str(),
                transformed[2].values[0].as_str()
            ),
            ("day", "01")
        );
        assert_eq!(
            (
                transformed[3].field_name.as_str(),
                transformed[3].values[0].as_str()
            ),
            ("hour", "12")
        );

        // Range operators: Gt/Gte → Gte, Lt/Lte → Lte (safe widening for partition boundaries)
        let keygen =
            TimestampBasedKeyGenerator::from_configs(&create_test_configs_unix_timestamp())
                .unwrap();
        for (input_op, expected_op) in [
            (ExprOperator::Gt, ExprOperator::Gte),
            (ExprOperator::Gte, ExprOperator::Gte),
            (ExprOperator::Lt, ExprOperator::Lte),
            (ExprOperator::Lte, ExprOperator::Lte),
        ] {
            let filter = Filter {
                field_name: "event_timestamp".to_string(),
                operator: input_op,
                values: vec!["1706140800".to_string()],
            };
            let transformed = keygen.transform_filter(&filter).unwrap();
            assert_eq!(transformed.len(), 1, "Expected 1 filter for {input_op:?}");
            assert_eq!(transformed[0].field_name, "yyyy");
            assert_eq!(
                transformed[0].operator, expected_op,
                "{input_op:?} should coerce to {expected_op:?}"
            );
        }

        // Non-source field passes through unchanged
        let keygen =
            TimestampBasedKeyGenerator::from_configs(&create_test_configs_date_string()).unwrap();
        let filter = Filter {
            field_name: "other_field".to_string(),
            operator: ExprOperator::Eq,
            values: vec!["value".to_string()],
        };
        let transformed = keygen.transform_filter(&filter).unwrap();
        assert_eq!(transformed.len(), 1);
        assert_eq!(transformed[0].field_name, "other_field");

        // Not-equal operator is rejected
        let filter = Filter {
            field_name: "ts_str".to_string(),
            operator: ExprOperator::Ne,
            values: vec!["2023-04-01T12:01:00.123Z".to_string()],
        };
        assert!(
            keygen
                .transform_filter(&filter)
                .unwrap_err()
                .to_string()
                .contains("Not-equal (!=) operator is not supported")
        );

        // Invalid timestamp value produces error, not panic
        let unix_keygen =
            TimestampBasedKeyGenerator::from_configs(&create_test_configs_unix_timestamp())
                .unwrap();
        let filter = Filter {
            field_name: "event_timestamp".to_string(),
            operator: ExprOperator::Eq,
            values: vec!["not_a_number".to_string()],
        };
        assert!(unix_keygen.transform_filter(&filter).is_err());
    }

    #[test]
    fn test_in_not_in_operators_return_error() {
        let keygen =
            TimestampBasedKeyGenerator::from_configs(&create_test_configs_date_string()).unwrap();

        // IN operator should return error
        let filter = Filter::new(
            "ts_str".to_string(),
            ExprOperator::In,
            vec!["2023-04-01T12:00:00.000Z".to_string()],
        )
        .unwrap();
        let result = keygen.transform_filter(&filter);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("IN/NOT IN operators are not supported")
        );

        // NOT IN operator should also return error
        let filter = Filter::new(
            "ts_str".to_string(),
            ExprOperator::NotIn,
            vec!["2023-04-01T12:00:00.000Z".to_string()],
        )
        .unwrap();
        let result = keygen.transform_filter(&filter);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("IN/NOT IN operators are not supported")
        );
    }
}
