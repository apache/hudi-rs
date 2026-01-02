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

/// Check if the given path is a metadata table path.
///
/// Detection is based on the path ending with `.hoodie/metadata`.
pub fn is_metadata_table_path(path: &str) -> bool {
    path.trim_end_matches('/').ends_with(".hoodie/metadata")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_metadata_table_path() {
        assert!(is_metadata_table_path("/data/my_table/.hoodie/metadata"));
        assert!(is_metadata_table_path("/data/my_table/.hoodie/metadata/"));
        assert!(is_metadata_table_path("s3://bucket/table/.hoodie/metadata"));
        assert!(!is_metadata_table_path("/data/my_table"));
        assert!(!is_metadata_table_path("/data/my_table/.hoodie"));
        assert!(!is_metadata_table_path("/data/.hoodie/metadata/files"));
    }
}
