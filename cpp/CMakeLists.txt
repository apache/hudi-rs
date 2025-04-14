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
project(hudi-cpp VERSION 0.4.0)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Include GNUInstallDirs for standard installation directories
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

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

# Determine library path based on build type
if(CMAKE_BUILD_TYPE MATCHES "Debug")
    set(RUST_LIB_PATH "${TARGET_DIRECTORY}/debug/libhudi.dylib")
    set(CARGO_BUILD_COMMAND "cargo" "build")
else()
    set(RUST_LIB_PATH "${TARGET_DIRECTORY}/release/libhudi.dylib")
    set(CARGO_BUILD_COMMAND "cargo" "build" "--release")
endif()

# Include directories for the build
set(CXX_GENERATED_DIR "${TARGET_DIRECTORY}/cxxbridge")
set(INCLUDE_DIRS
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
    "${CXX_GENERATED_DIR}"
)

# Create the cmake directory for config files
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/cmake)

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

# Create a wrapper target for the rust library
add_library(hudi INTERFACE)
target_include_directories(hudi INTERFACE
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
    "$<BUILD_INTERFACE:${CXX_GENERATED_DIR}>"
    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/hudi>"
    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>"
)
# Store the library path for later use in install rules
set_target_properties(hudi PROPERTIES
    INTERFACE_HUDI_LIBRARY_PATH ${RUST_LIB_PATH}
)
add_dependencies(hudi rust_build)

# Add an alias target to match the exported name
add_library(Hudi::Hudi ALIAS hudi)

# Install the library
install(FILES ${RUST_LIB_PATH}
    DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RENAME "libhudi${CMAKE_SHARED_LIBRARY_SUFFIX}"
)

# Install the header files
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/hudi
    FILES_MATCHING PATTERN "*.h"
)

# Install the generated headers
add_custom_target(copy_generated_headers ALL
    DEPENDS rust_build
    COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_CURRENT_BINARY_DIR}/include_copy
    COMMAND ${CMAKE_COMMAND} -E copy_directory ${CXX_GENERATED_DIR} ${CMAKE_CURRENT_BINARY_DIR}/include_copy
    COMMENT "Copying generated headers to staging directory"
)

install(DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/include_copy/
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    FILES_MATCHING
        PATTERN "*.h"
        PATTERN "*.cc"
)

# Install the FindHudi.cmake module
install(FILES
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/FindHudi.cmake"
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/hudi
)

# Configure and install HudiConfig.cmake
configure_package_config_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/HudiConfig.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/cmake/HudiConfig.cmake"
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/hudi
)

# Write the version file
write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/cmake/HudiConfigVersion.cmake"
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)

# Install the config files
install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/cmake/HudiConfig.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/cmake/HudiConfigVersion.cmake"
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/hudi
)

# Create the target export
install(TARGETS hudi
    EXPORT HudiTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

# Install the target export file
install(EXPORT HudiTargets
    FILE HudiTargets.cmake
    NAMESPACE Hudi::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/hudi
)

# Export the targets for use in the build tree
export(EXPORT HudiTargets
    NAMESPACE Hudi::
    FILE "${CMAKE_CURRENT_BINARY_DIR}/cmake/HudiTargets.cmake"
)

# Output configuration summary
message(STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")
message(STATUS "Rust library path: ${RUST_LIB_PATH}")
message(STATUS "Generated C++ path: ${CXX_GENERATED_DIR}")
message(STATUS "Install prefix: ${CMAKE_INSTALL_PREFIX}")