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
#include <cstdlib>
#include <exception>
#include <iostream>
#include <string>
#include <vector>

#include "cxx.h"
#include "src/lib.rs.h"

int main() {
    const std::vector<std::string> logs;
    try {
        const auto slice = new_file_slice_from_file_names(
            "", "f00000000-0000-0000-0000-000000000000-0_0-1-0_20250101000000000.parquet", logs);
    } catch (const std::exception& error) {
        std::cerr << "Valid file-slice construction failed: " << error.what() << '\n';
        return EXIT_FAILURE;
    }

    try {
        const auto slice = new_file_slice_from_file_names("", "invalid.parquet", logs);
    } catch (const rust::Error& error) {
        if (std::string(error.what()).empty()) {
            std::cerr << "rust::Error did not preserve its error message\n";
            return EXIT_FAILURE;
        }
        return EXIT_SUCCESS;
    } catch (const std::exception& error) {
        std::cerr << "Expected rust::Error, got: " << error.what() << '\n';
        return EXIT_FAILURE;
    }

    std::cerr << "Invalid file name did not throw rust::Error\n";
    return EXIT_FAILURE;
}
