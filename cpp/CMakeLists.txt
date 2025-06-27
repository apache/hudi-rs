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

cmake_minimum_required(VERSION 3.20)
project(hudi-cpp VERSION 0.4.0-dev)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find the cargo metadata to locate the project paths
execute_process(
    COMMAND cargo metadata --format-version=1
    OUTPUT_VARIABLE CARGO_METADATA
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/..
)
string(JSON CARGO_WORKSPACE_ROOT GET ${CARGO_METADATA} "workspace_root")
string(JSON TARGET_DIRECTORY GET ${CARGO_METADATA} "target_directory")

# Set the default build type if not specified
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Build type" FORCE)
endif()

# Determine library extension based on platform
if(WIN32)
    set(LIB_EXTENSION ".dll")
elseif(APPLE)
    set(LIB_EXTENSION ".dylib")
else()
    set(LIB_EXTENSION ".so")
endif()

# Determine library path based on build type
if(CMAKE_BUILD_TYPE MATCHES "Debug")
    set(RUST_LIB_PATH "${TARGET_DIRECTORY}/debug/libhudi${LIB_EXTENSION}")
    set(CARGO_BUILD_COMMAND "cargo" "build")
else()
    set(RUST_LIB_PATH "${TARGET_DIRECTORY}/release/libhudi${LIB_EXTENSION}")
    set(CARGO_BUILD_COMMAND "cargo" "build" "--release")
endif()

# Include directories for the build
set(CXX_GENERATED_DIR "${TARGET_DIRECTORY}/cxxbridge")
set(INCLUDE_DIRS
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
    "${CXX_GENERATED_DIR}"
)

# Custom command to build the rust library
add_custom_command(
    OUTPUT ${RUST_LIB_PATH}
    COMMAND ${CARGO_BUILD_COMMAND}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Building Rust library with cargo"
    VERBATIM
)

# Create a custom target that depends on the Rust library
add_custom_target(rust_build ALL DEPENDS ${RUST_LIB_PATH})

# Create the main library target
add_library(hudi INTERFACE)
target_include_directories(hudi INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CXX_GENERATED_DIR}/rust
    ${CXX_GENERATED_DIR}/hudi
)

# Store the library path as a property for consumers
set_target_properties(hudi PROPERTIES
    INTERFACE_HUDI_LIBRARY_PATH ${RUST_LIB_PATH}
)

# Make sure the Rust library is built before anyone uses this target
add_dependencies(hudi rust_build)

# Add an alias target to match exported name convention
add_library(Hudi::Hudi ALIAS hudi)

# Output configuration summary
message(STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")
message(STATUS "Rust library path: ${RUST_LIB_PATH}")
message(STATUS "Generated C++ path: ${CXX_GENERATED_DIR}")
message(STATUS "Hudi target created for add_subdirectory usage")
