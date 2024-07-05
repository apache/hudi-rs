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

use arrow::record_batch::RecordBatch;
use arrow_array::{Array, BooleanArray, Int32Array, StringArray};

#[macro_export]
macro_rules! assert_not {
    ($cond:expr) => {
        if $cond {
            panic!("assertion failed: condition is true");
        }
    };
    ($cond:expr, $($arg:tt)+) => {
        if $cond {
            panic!("assertion failed: condition is true: {}", format_args!($($arg)+));
        }
    };
}

#[macro_export]
macro_rules! assert_approx_eq {
    ($a:expr, $b:expr, $delta:expr) => {{
        let a = $a;
        let b = $b;
        let delta = $delta;
        let diff = if a > b { a - b } else { b - a };

        assert!(
            diff <= delta,
            "assertion failed: `(left ≈ right)`\n  left: `{:?}`,\n right: `{:?}`,\n delta: `{:?}`",
            a, b, delta
        );
    }};
}

pub fn get_str_column<'a>(record_batch: &'a RecordBatch, name: &str) -> Vec<&'a str> {
    record_batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|s| s.unwrap())
        .collect::<Vec<_>>()
}

pub fn get_i32_column(record_batch: &RecordBatch, name: &str) -> Vec<i32> {
    record_batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .iter()
        .map(|s| s.unwrap())
        .collect::<Vec<_>>()
}

pub fn get_bool_column(record_batch: &RecordBatch, name: &str) -> Vec<bool> {
    record_batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .iter()
        .map(|s| s.unwrap())
        .collect::<Vec<_>>()
}
