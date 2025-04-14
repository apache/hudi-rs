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

#pragma once

#include <memory>
#include <string>
#include <vector>
#include "arrow/c/abi.h"
#include "hudi/src/lib.rs.h" // Generated from cxxbridge
#include "rust/cxx.h" // For Rust C++ bridge

namespace hudi {

// C++ wrapper class for Rust's FileGroupReader
class FileGroupReader {
public:
    FileGroupReader(const std::string& base_uri,
                    const std::vector<std::string>& options = {});
    ~FileGroupReader();

    // Reads a file slice by its base file path
    // Returns an ArrowArrayStream which must be released by the caller
    struct ArrowArrayStream* readFileSliceByBaseFilePath(const std::string& relative_path);

private:
    std::unique_ptr<HudiFileGroupReader> reader_;
};

} // namespace hudi