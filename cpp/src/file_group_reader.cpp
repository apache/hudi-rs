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

#include "hudi/file_group_reader.h"
#include "lib.rs.h" // Generated from cxxbridge
#include <stdexcept>

namespace hudi {

FileGroupReader::FileGroupReader(const std::string& base_uri,
                                 const std::vector<std::string>& options) {
    // Convert options to the format expected by Rust
    rust::Vec<rust::String> rust_options;
    for (const auto& opt : options) {
        rust_options.push_back(rust::String(opt));
    }

    try {
        // Directly call the Rust function exposed by cxx bridge
        reader_ = new_file_group_reader_with_options(
            rust::String(base_uri),
            rust::Slice<const rust::String>(rust_options.data(), rust_options.size()));
    } catch (const std::exception& e) {
        throw std::runtime_error(std::string("Failed to create file group reader: ") + e.what());
    }
}

FileGroupReader::~FileGroupReader() = default;

struct ArrowArrayStream* FileGroupReader::readFileSliceByBaseFilePath(const std::string& relative_path) {
    try {
        return reader_->read_file_slice_by_base_file_path(rust::String(relative_path));
    } catch (const std::exception& e) {
        throw std::runtime_error(std::string("Failed to read file slice: ") + e.what());
    }
}

} // namespace hudi