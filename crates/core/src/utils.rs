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

use std::path::{Path, PathBuf};
use std::{fs, io};

pub fn get_leaf_dirs(path: &Path) -> Result<Vec<PathBuf>, io::Error> {
    let mut leaf_dirs = Vec::new();
    let mut is_leaf_dir = true;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if entry.path().is_dir() {
            is_leaf_dir = false;
            let curr_sub_dir = entry.path();
            let curr = get_leaf_dirs(&curr_sub_dir)?;
            leaf_dirs.extend(curr);
        }
    }
    if is_leaf_dir {
        leaf_dirs.push(path.to_path_buf())
    }
    Ok(leaf_dirs)
}
