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
project(hudi_cpp_example)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_subdirectory(../../../../cpp hudi-cpp)

add_executable(file_group_api_cpp
  main.cpp
)

# Get the cxxbridge directory and add the generated source file
get_target_property(HUDI_LIB_PATH hudi INTERFACE_HUDI_LIBRARY_PATH)
get_filename_component(TARGET_DIR ${HUDI_LIB_PATH} DIRECTORY)
get_filename_component(TARGET_DIR ${TARGET_DIR} DIRECTORY)  # Go up one level to get target directory
set(CXX_BRIDGE_DIR "${TARGET_DIR}/cxxbridge")

target_sources(file_group_api_cpp PRIVATE
  ${CXX_BRIDGE_DIR}/hudi/src/lib.rs.cc
)

# Make sure the executable depends on the rust build completing
add_dependencies(file_group_api_cpp rust_build)

# Link against the hudi library target
target_link_libraries(file_group_api_cpp
  Hudi::Hudi
)

# Get the actual library path and link it directly
get_target_property(HUDI_LIB_PATH hudi INTERFACE_HUDI_LIBRARY_PATH)
if(HUDI_LIB_PATH)
    target_link_libraries(file_group_api_cpp ${HUDI_LIB_PATH})
endif()

# Output configuration
get_target_property(HUDI_INCLUDE_DIRS hudi INTERFACE_INCLUDE_DIRECTORIES)
message(STATUS "Using Hudi include directories: ${HUDI_INCLUDE_DIRS}")
message(STATUS "Using Hudi library path: ${HUDI_LIB_PATH}")
