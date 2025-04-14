# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

cmake_minimum_required(VERSION 3.15)
project(hudi_cpp_examples)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find the installed hudi library
find_library(HUDI_LIBRARY hudi REQUIRED)

# Set the include directories for Hudi
set(HUDI_INCLUDE_DIR "/usr/local/include")

# Add the example executable
add_executable(file_group_api_cpp
  map.cpp
  ${HUDI_INCLUDE_DIR}/hudi/src/lib.rs.cc
)

# Setup include directories
target_include_directories(hudi_cpp_example PRIVATE
  ${HUDI_INCLUDE_DIR}/hudi
  ${HUDI_INCLUDE_DIR}/rust
)

# Link against libraries
target_link_libraries(hudi_cpp_example
  ${HUDI_LIBRARY}
)

# Output configuration
message(STATUS "Using HUDI_LIBRARY: ${HUDI_LIBRARY}")
