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
use std::hash::Hash;

pub fn extend_if_absent<K, V>(target: &mut HashMap<K, V>, source: &HashMap<K, V>)
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    for (key, value) in source {
        target.entry(key.clone()).or_insert_with(|| value.clone());
    }
}

/// Split a vector into approximately equal chunks based on the specified number of splits.
///
/// # Arguments
/// * `items` - The vector to split
/// * `num_splits` - The desired number of chunks (will be clamped to at least 1)
pub fn split_into_chunks<T: Clone>(items: Vec<T>, num_splits: usize) -> Vec<Vec<T>> {
    if items.is_empty() {
        return Vec::new();
    }

    let num_splits = std::cmp::max(1, num_splits);
    let chunk_size = items.len().div_ceil(num_splits);

    items
        .chunks(chunk_size)
        .map(|chunk| chunk.to_vec())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_into_chunks_zero_splits() {
        let items = vec![1, 2, 3, 4, 5];
        let result = split_into_chunks(items.clone(), 0);
        assert_eq!(result.len(), 1, "Zero splits should be clamped to 1");
        assert_eq!(result[0], items, "All items should be in single chunk");
    }

    #[test]
    fn test_split_into_chunks_empty_input() {
        let items: Vec<i32> = vec![];
        let result = split_into_chunks(items, 2);
        assert!(result.is_empty(), "Empty input should return empty result");
    }

    #[test]
    fn test_split_into_chunks_more_splits_than_items() {
        let items = vec![1, 2, 3];
        let result = split_into_chunks(items, 5);
        assert_eq!(result.len(), 3, "Should return 3 chunks (one per item)");
        assert_eq!(result[0], vec![1]);
        assert_eq!(result[1], vec![2]);
        assert_eq!(result[2], vec![3]);
    }

    #[test]
    fn test_split_into_chunks_normal_case() {
        let items = vec![1, 2, 3, 4, 5];
        let result = split_into_chunks(items, 2);
        assert_eq!(result.len(), 2, "Should return 2 chunks");
        assert_eq!(result[0], vec![1, 2, 3], "First chunk should have 3 items");
        assert_eq!(result[1], vec![4, 5], "Second chunk should have 2 items");
    }
}
