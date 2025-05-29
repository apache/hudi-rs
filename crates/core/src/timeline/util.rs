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
use crate::config::table::TimelineTimezoneValue;
use crate::error::CoreError;
use crate::Result;
use chrono::{DateTime, Datelike, Local, TimeZone, Timelike, Utc};
use std::str::FromStr;
use CoreError::TimestampParsingError;

/// Parse various timestamp formats and convert to Hudi timeline format
///
/// Supported formats:
/// - Hudi timeline format: `yyyyMMddHHmmSSSSS` or `yyyyMMddHHmmSS`
/// - Epoch time (seconds, milliseconds, microseconds, nanoseconds)
/// - RFC3339 format like `2024-03-15T14:25:30Z` or `2024-03-15T14:25:30+00:00` or `2024-03-15`
///
/// # Arguments
/// * `ts_str` - The timestamp str to parse
/// * `timezone` - The timezone config value to interpret the timestamp in (UTC or Local)
pub fn format_timestamp(ts_str: &str, timezone: &str) -> Result<String> {
    let ts_str = ts_str.trim();

    let mut parse_errors: Vec<CoreError> = vec![];

    match parse_timeline_format(ts_str) {
        Ok(formatted_ts) => return Ok(formatted_ts),
        Err(e) => parse_errors.push(e),
    }

    let timezone = TimelineTimezoneValue::from_str(timezone)?;

    if let Ok(formatted_ts) = parse_epoch_time(ts_str, &timezone) {
        return Ok(formatted_ts);
    }
    match parse_epoch_time(ts_str, &timezone) {
        Ok(formatted_ts) => return Ok(formatted_ts),
        Err(e) => parse_errors.push(e),
    }

    match parse_rfc3339_format(ts_str, &timezone) {
        Ok(formatted_ts) => return Ok(formatted_ts),
        Err(e) => parse_errors.push(e),
    }

    Err(TimestampParsingError(format!(
        "Unable to parse timestamp: {} due to errors: {:?}",
        ts_str, parse_errors
    )))
}

fn parse_timeline_format(ts_str: &str) -> Result<String> {
    if matches!(ts_str.len(), 14 | 17) && ts_str.chars().all(|c| c.is_ascii_digit()) {
        return Ok(ts_str.to_string());
    }

    Err(TimestampParsingError(
        "Not a Hudi timeline format".to_string(),
    ))
}

fn parse_epoch_time(ts_str: &str, timezone: &TimelineTimezoneValue) -> Result<String> {
    let ts: i64 = ts_str.parse().map_err(|e| {
        TimestampParsingError(format!("Invalid epoch time: {} due to {:?}", ts_str, e))
    })?;
    if ts < 0 {
        return Err(TimestampParsingError(format!(
            "Epoch time must be non-negative: {}",
            ts_str
        )));
    }

    let datetime = if ts_str.len() <= 10 {
        // Seconds
        DateTime::from_timestamp(ts, 0)
    } else if ts_str.len() <= 13 {
        // Milliseconds
        DateTime::from_timestamp(ts / 1000, ((ts % 1000) * 1_000_000) as u32)
    } else if ts_str.len() <= 16 {
        // Microseconds
        DateTime::from_timestamp(ts / 1_000_000, ((ts % 1_000_000) * 1000) as u32)
    } else {
        // Nanoseconds
        DateTime::from_timestamp(ts / 1_000_000_000, (ts % 1_000_000_000) as u32)
    };

    match datetime {
        Some(dt) => match timezone {
            TimelineTimezoneValue::UTC => Ok(datetime_to_timeline_format(&dt)),
            TimelineTimezoneValue::Local => {
                let local_dt = dt.with_timezone(&Local);
                Ok(datetime_to_timeline_format(&local_dt))
            }
        },
        None => Err(TimestampParsingError(format!(
            "Invalid epoch time: {}",
            ts_str
        ))),
    }
}

fn parse_rfc3339_format(ts_str: &str, timezone: &TimelineTimezoneValue) -> Result<String> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(ts_str) {
        return match timezone {
            TimelineTimezoneValue::UTC => Ok(datetime_to_timeline_format(&dt.with_timezone(&Utc))),
            TimelineTimezoneValue::Local => {
                let local_dt = dt.with_timezone(&Local);
                Ok(datetime_to_timeline_format(&local_dt))
            }
        };
    }

    Err(TimestampParsingError(
        "Not a valid RFC3339 format".to_string(),
    ))
}

fn datetime_to_timeline_format(dt: &DateTime<impl TimeZone>) -> String {
    let year = dt.year();
    let month = dt.month();
    let day = dt.day();
    let hour = dt.hour();
    let minute = dt.minute();
    let second = dt.second();
    let timestamp_subsec_millis = dt.timestamp_subsec_millis();
    format!(
        "{:04}{:02}{:02}{:02}{:02}{:02}{:03}",
        year, month, day, hour, minute, second, timestamp_subsec_millis
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use hudi_test::util::{reset_timezone, set_fixed_timezone};

    fn set_singapore_timezone() {
        set_fixed_timezone("Asia/Singapore");
    }

    #[test]
    fn test_parse_timeline_format() {
        // Valid 14-digit format
        let result = parse_timeline_format("20240315142530").unwrap();
        assert_eq!(result, "20240315142530");

        // Valid 17-digit format
        let result = parse_timeline_format("20240315142530123").unwrap();
        assert_eq!(result, "20240315142530123");

        // Invalid formats
        assert!(parse_timeline_format("2024031514253").is_err()); // Wrong length
        assert!(parse_timeline_format("202403151425301234").is_err()); // Wrong length
        assert!(parse_timeline_format("2024031514253a").is_err()); // Non-digits
        assert!(parse_timeline_format("").is_err()); // Empty
    }

    #[test]
    fn test_parse_epoch_time() {
        // Test epoch time in seconds (10 digits or fewer)
        let result = parse_epoch_time("1710512730", &TimelineTimezoneValue::UTC).unwrap();
        // March 15, 2024 14:25:30 UTC
        assert_eq!(result, "20240315142530000");

        // Test with Local timezone (Singapore UTC+8)
        set_singapore_timezone();
        let result = parse_epoch_time("1710512730", &TimelineTimezoneValue::Local).unwrap();
        // March 15, 2024 14:25:30 UTC = March 15, 2024 22:25:30 Singapore time
        assert_eq!(result, "20240315222530000");
        reset_timezone();

        // Test milliseconds
        let result = parse_epoch_time("1710512730123", &TimelineTimezoneValue::UTC).unwrap();
        assert_eq!(result, "20240315142530123");

        // Test microseconds
        let result = parse_epoch_time("1710512730123456", &TimelineTimezoneValue::UTC).unwrap();
        assert_eq!(result, "20240315142530123");

        // Test nanoseconds
        let result = parse_epoch_time("1710512730123456789", &TimelineTimezoneValue::UTC).unwrap();
        assert_eq!(result, "20240315142530123");

        // Invalid epoch time
        assert!(parse_epoch_time("not_a_number", &TimelineTimezoneValue::UTC).is_err());
        assert!(parse_epoch_time("-1", &TimelineTimezoneValue::UTC).is_err());
    }

    #[test]
    fn test_parse_rfc3339_format() {
        // RFC3339 with timezone offset
        let result =
            parse_rfc3339_format("2024-03-15T14:25:30+00:00", &TimelineTimezoneValue::UTC).unwrap();
        assert_eq!(result, "20240315142530000");

        // RFC3339 with Z (UTC)
        let result =
            parse_rfc3339_format("2024-03-15T14:25:30Z", &TimelineTimezoneValue::UTC).unwrap();
        assert_eq!(result, "20240315142530000");

        // RFC3339 with milliseconds
        let result =
            parse_rfc3339_format("2024-03-15T14:25:30.123Z", &TimelineTimezoneValue::UTC).unwrap();
        assert_eq!(result, "20240315142530123");

        // Test conversion to local timezone (Singapore)
        set_singapore_timezone();
        let result =
            parse_rfc3339_format("2024-03-15T14:25:30Z", &TimelineTimezoneValue::Local).unwrap();
        assert_eq!(result, "20240315222530000"); // Convert UTC to Singapore time

        // RFC3339 with different timezone offset
        let result =
            parse_rfc3339_format("2024-03-15T20:25:30+06:00", &TimelineTimezoneValue::Local)
                .unwrap();
        assert_eq!(result, "20240315222530000"); // +06:00 = UTC+6, so 20:25:30+06:00 = 14:25:30Z = 22:25:30 Singapore
        reset_timezone();

        // Invalid RFC3339 format
        assert!(parse_rfc3339_format("2024-03-15 14:25:30", &TimelineTimezoneValue::UTC).is_err());
        assert!(parse_rfc3339_format("2024/03/15T14:25:30Z", &TimelineTimezoneValue::UTC).is_err());
        assert!(parse_rfc3339_format("not-a-date", &TimelineTimezoneValue::UTC).is_err());
    }

    #[test]
    fn test_datetime_to_timeline_format() {
        // Test with UTC timezone
        let dt = Utc
            .with_ymd_and_hms(2024, 3, 15, 14, 25, 30)
            .unwrap()
            .with_nanosecond(123_000_000)
            .unwrap();
        let result = datetime_to_timeline_format(&dt);
        assert_eq!(result, "20240315142530123");

        // Test with zero milliseconds
        let dt = Utc.with_ymd_and_hms(2024, 3, 15, 14, 25, 30).unwrap();
        let result = datetime_to_timeline_format(&dt);
        assert_eq!(result, "20240315142530000");

        // Test with single-digit values
        let dt = Utc.with_ymd_and_hms(2024, 1, 5, 9, 8, 7).unwrap();
        let result = datetime_to_timeline_format(&dt);
        assert_eq!(result, "20240105090807000");
    }

    #[test]
    fn test_format_timestamp_timeline_format() {
        // Already in timeline format should be returned as-is
        let result = format_timestamp("20240315142530", "UTC").unwrap();
        assert_eq!(result, "20240315142530");

        let result = format_timestamp("20240315142530123", "UTC").unwrap();
        assert_eq!(result, "20240315142530123");

        // Should work with Local timezone too (no conversion needed)
        let result = format_timestamp("20240315142530", "Local").unwrap();
        assert_eq!(result, "20240315142530");
    }

    #[test]
    fn test_format_timestamp_epoch_time() {
        set_singapore_timezone();

        // Epoch time with UTC
        let result = format_timestamp("1710512730", "UTC").unwrap();
        assert_eq!(result, "20240315142530000");

        // Epoch time with Local (Singapore)
        let result = format_timestamp("1710512730", "Local").unwrap();
        assert_eq!(result, "20240315222530000");

        // Epoch time with milliseconds
        let result = format_timestamp("1710512730123", "UTC").unwrap();
        assert_eq!(result, "20240315142530123");

        let result = format_timestamp("1710512730123", "Local").unwrap();
        assert_eq!(result, "20240315222530123");

        reset_timezone();
    }

    #[test]
    fn test_format_timestamp_rfc3339() {
        set_singapore_timezone();

        // RFC3339 with timezone info
        let result = format_timestamp("2024-03-15T14:25:30Z", "UTC").unwrap();
        assert_eq!(result, "20240315142530000");

        let result = format_timestamp("2024-03-15T14:25:30Z", "Local").unwrap();
        assert_eq!(result, "20240315222530000"); // Convert UTC to Singapore

        // RFC3339 with offset
        let result = format_timestamp("2024-03-15T14:25:30+00:00", "UTC").unwrap();
        assert_eq!(result, "20240315142530000");

        let result = format_timestamp("2024-03-15T14:25:30+00:00", "Local").unwrap();
        assert_eq!(result, "20240315222530000");

        // RFC3339 with milliseconds
        let result = format_timestamp("2024-03-15T14:25:30.123Z", "UTC").unwrap();
        assert_eq!(result, "20240315142530123");

        let result = format_timestamp("2024-03-15T14:25:30.123Z", "Local").unwrap();
        assert_eq!(result, "20240315222530123");

        reset_timezone();
    }

    #[test]
    fn test_format_timestamp_comprehensive() {
        set_singapore_timezone();

        // Test all supported formats
        let test_cases = vec![
            // Timeline format (no conversion)
            ("20240315142530", "UTC", "20240315142530"),
            ("20240315142530", "Local", "20240315142530"),
            ("20240315142530123", "UTC", "20240315142530123"),
            ("20240315142530123", "Local", "20240315142530123"),
            // Epoch times
            ("1710512730", "UTC", "20240315142530000"), // 14:25:30 UTC
            ("1710512730", "Local", "20240315222530000"), // 22:25:30 Singapore
            ("1710512730123", "UTC", "20240315142530123"),
            ("1710512730123", "Local", "20240315222530123"),
            // RFC3339 with timezone
            ("2024-03-15T14:25:30Z", "UTC", "20240315142530000"),
            ("2024-03-15T14:25:30Z", "Local", "20240315222530000"),
            ("2024-03-15T14:25:30+00:00", "UTC", "20240315142530000"),
            ("2024-03-15T14:25:30+00:00", "Local", "20240315222530000"),
            ("2024-03-15T14:25:30.123Z", "UTC", "20240315142530123"),
            ("2024-03-15T14:25:30.123Z", "Local", "20240315222530123"),
        ];

        for (input, timezone, expected) in test_cases {
            let result = format_timestamp(input, timezone).unwrap();
            assert_eq!(
                result, expected,
                "Failed for input: {} with timezone: {}",
                input, timezone
            );
        }

        reset_timezone();
    }

    #[test]
    fn test_format_timestamp_parsing_order() {
        // Test that the timeline format is checked first (should not be parsed as epoch)
        let result = format_timestamp("20240315142530", "UTC").unwrap();
        assert_eq!(
            result, "20240315142530",
            "Should be treated as the timeline format, not epoch"
        );

        // Test that epoch is checked before RFC3339
        let result = format_timestamp("1710512730", "UTC").unwrap();
        assert_eq!(
            result, "20240315142530000",
            "Should be treated as epoch time"
        );
    }

    #[test]
    fn test_format_timestamp_error_collection() {
        // Test that invalid input collects all parsing errors
        let result = format_timestamp("invalid-input", "UTC");
        assert!(result.is_err());

        let error_msg = format!("{:?}", result.unwrap_err());
        assert!(error_msg.contains("Unable to parse timestamp: invalid-input"));
        assert!(error_msg.contains("due to errors"));
    }

    #[test]
    fn test_format_timestamp_invalid_inputs() {
        let invalid_inputs = vec![
            "invalid-timestamp",
            "2024-13-45T14:25:30Z", // Invalid date in RFC3339
            "",
            "2024/03/15",     // Wrong format
            "abcdefghijklmn", // Wrong length for timeline
        ];

        for input in invalid_inputs {
            let result = format_timestamp(input, "UTC");
            assert!(result.is_err(), "Should fail for input: {}", input);
        }
    }

    #[test]
    fn test_format_timestamp_invalid_timezone() {
        let result = format_timestamp("2024-03-15T14:25:30Z", "InvalidTimezone");
        assert!(result.is_err());
    }

    #[test]
    fn test_whitespace_handling() {
        // Test that whitespace is properly trimmed
        let result = format_timestamp("  20240315142530  ", "UTC").unwrap();
        assert_eq!(result, "20240315142530");

        let result = format_timestamp("  2024-03-15T14:25:30Z  ", "UTC").unwrap();
        assert_eq!(result, "20240315142530000");

        let result = format_timestamp("  1710512730  ", "UTC").unwrap();
        assert_eq!(result, "20240315142530000");
    }

    #[test]
    fn test_timezone_conversion_consistency() {
        set_singapore_timezone();

        // Test that the same timestamp produces consistent results with different input formats
        let timestamp_epoch = "1710512730"; // March 15, 2024 14:25:30 UTC
        let timestamp_rfc3339 = "2024-03-15T14:25:30Z";

        let epoch_utc = format_timestamp(timestamp_epoch, "UTC").unwrap();
        let epoch_local = format_timestamp(timestamp_epoch, "Local").unwrap();
        let rfc3339_utc = format_timestamp(timestamp_rfc3339, "UTC").unwrap();
        let rfc3339_local = format_timestamp(timestamp_rfc3339, "Local").unwrap();

        // All UTC results should be the same
        assert_eq!(epoch_utc, rfc3339_utc);
        assert_eq!(epoch_utc, "20240315142530000");

        // All Local results should be the same
        assert_eq!(epoch_local, rfc3339_local);
        assert_eq!(epoch_local, "20240315222530000"); // Singapore time

        reset_timezone();
    }

    #[test]
    fn test_edge_cases() {
        set_singapore_timezone();

        // Test leap year date
        let result = format_timestamp("2024-02-29T12:00:00Z", "UTC").unwrap();
        assert_eq!(result, "20240229120000000");

        let result = format_timestamp("2024-02-29T12:00:00Z", "Local").unwrap();
        assert_eq!(result, "20240229200000000"); // Singapore time

        // Test end of year
        let result = format_timestamp("2024-12-31T23:59:59Z", "UTC").unwrap();
        assert_eq!(result, "20241231235959000");

        // Test beginning of year
        let result = format_timestamp("2024-01-01T00:00:00Z", "UTC").unwrap();
        assert_eq!(result, "20240101000000000");

        reset_timezone();
    }
}
