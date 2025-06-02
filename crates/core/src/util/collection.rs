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
