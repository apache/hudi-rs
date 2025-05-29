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
use chrono::{DateTime, Datelike, Local, NaiveDate, NaiveDateTime, TimeZone, Timelike, Utc};
use std::str::FromStr;

/// Parse various timestamp formats and convert to Hudi timeline format
///
/// Supported formats:
/// - Hudi timeline format: `yyyyMMddHHmmSSSSS` or `yyyyMMddHHmmSS`
/// - Epoch time (seconds, milliseconds, microseconds, nanoseconds)
/// - ISO8601 formats with various precisions
/// - Date-only format: `YYYY-MM-DD`
///
/// # Arguments
/// * `ts_str` - The timestamp str to parse
/// * `timezone` - The timezone config value to interpret the timestamp in (UTC or Local)
pub fn format_timestamp(ts_str: &str, timezone: &str) -> Result<String> {
    let ts_str = ts_str.trim();

    if let Ok(formatted_ts) = parse_timeline_format(ts_str) {
        return Ok(formatted_ts);
    }

    let timezone = TimelineTimezoneValue::from_str(timezone)?;

    if let Ok(formatted_ts) = parse_epoch_time(ts_str, &timezone) {
        return Ok(formatted_ts);
    }

    if let Ok(formatted_ts) = parse_iso8601_format(ts_str, &timezone) {
        return Ok(formatted_ts);
    }

    if let Ok(formatted_ts) = parse_date_only_format(ts_str, &timezone) {
        return Ok(formatted_ts);
    }

    Err(CoreError::TimestampParsingError(format!(
        "Unable to parse timestamp: {}",
        ts_str
    )))
}

/// Parse the timestamp for timeline format
fn parse_timeline_format(ts_str: &str) -> Result<String> {
    if matches!(ts_str.len(), 14 | 17) && ts_str.chars().all(|c| c.is_ascii_digit()) {
        return Ok(ts_str.to_string());
    }

    Err(CoreError::TimestampParsingError(
        "Not a Hudi timeline format".to_string(),
    ))
}

/// Parse epoch time in various precisions
fn parse_epoch_time(ts_str: &str, timezone: &TimelineTimezoneValue) -> Result<String> {
    let ts: i64 = ts_str
        .parse()
        .map_err(|_| CoreError::TimestampParsingError(format!("Invalid epoch time: {}", ts_str)))?;

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
        None => Err(CoreError::TimestampParsingError(format!(
            "Invalid epoch time: {}",
            ts_str
        ))),
    }
}

/// Parse ISO8601 format timestamps
fn parse_iso8601_format(ts_str: &str, timezone: &TimelineTimezoneValue) -> Result<String> {
    // List of ISO8601 formats to try, ordered from the most specific to the least specific
    let formats = [
        // With timezone offset
        "%Y-%m-%dT%H:%M:%S%.9f%:z", // nanoseconds with timezone
        "%Y-%m-%dT%H:%M:%S%.6f%:z", // microseconds with timezone
        "%Y-%m-%dT%H:%M:%S%.3f%:z", // milliseconds with timezone
        "%Y-%m-%dT%H:%M:%S%:z",     // seconds with timezone
        // With Z (UTC)
        "%Y-%m-%dT%H:%M:%S%.9fZ", // nanoseconds with Z
        "%Y-%m-%dT%H:%M:%S%.6fZ", // microseconds with Z
        "%Y-%m-%dT%H:%M:%S%.3fZ", // milliseconds with Z
        "%Y-%m-%dT%H:%M:%SZ",     // seconds with Z
        // Without timezone (ambiguous - use mode to determine interpretation)
        "%Y-%m-%dT%H:%M:%S%.9f", // nanoseconds
        "%Y-%m-%dT%H:%M:%S%.6f", // microseconds
        "%Y-%m-%dT%H:%M:%S%.3f", // milliseconds
        "%Y-%m-%dT%H:%M:%S",     // seconds
    ];

    // Try parsing with timezone-aware formats first (these ignore the timezone config)
    for format in &formats[..8] {
        if let Ok(dt) = DateTime::parse_from_str(ts_str, format) {
            return match timezone {
                TimelineTimezoneValue::UTC => {
                    Ok(datetime_to_timeline_format(&dt.with_timezone(&Utc)))
                }
                TimelineTimezoneValue::Local => {
                    let local_dt = dt.with_timezone(&Local);
                    Ok(datetime_to_timeline_format(&local_dt))
                }
            };
        }
    }

    // Try parsing as naive datetime and interpret it according to the timezone config
    for format in &formats[8..] {
        if let Ok(naive_dt) = NaiveDateTime::parse_from_str(ts_str, format) {
            return format_naive_date_time(naive_dt, timezone);
        }
    }

    Err(CoreError::TimestampParsingError(
        "Not a valid ISO8601 format".to_string(),
    ))
}

/// Parse date-only format (YYYY-MM-DD)
fn parse_date_only_format(ts_str: &str, timezone: &TimelineTimezoneValue) -> Result<String> {
    if let Ok(date) = NaiveDate::parse_from_str(ts_str, "%Y-%m-%d") {
        // Convert to datetime at start of day
        let datetime = date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| CoreError::TimestampParsingError("Invalid date".to_string()))?;

        return format_naive_date_time(datetime, timezone);
    }

    Err(CoreError::TimestampParsingError(
        "Not a valid date format".to_string(),
    ))
}

fn format_naive_date_time(
    date_time: NaiveDateTime,
    timezone: &TimelineTimezoneValue,
) -> Result<String> {
    match timezone {
        TimelineTimezoneValue::UTC => {
            let dt = Utc.from_utc_datetime(&date_time);
            Ok(datetime_to_timeline_format(&dt))
        }
        TimelineTimezoneValue::Local => {
            let dt = Local
                .from_local_datetime(&date_time)
                .single()
                .ok_or_else(|| {
                    CoreError::TimestampParsingError("Ambiguous local datetime".to_string())
                })?;
            Ok(datetime_to_timeline_format(&dt))
        }
    }
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
