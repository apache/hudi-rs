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

# HudiConfig.cmake - Config file for Hudi C++ library
# Exports:
#  Hudi::Hudi - Imported target for the Hudi library

@PACKAGE_INIT@

# Define the library path
set(HUDI_LIBRARY_PATH "@CMAKE_INSTALL_PREFIX@/@CMAKE_INSTALL_LIBDIR@/libhudi@CMAKE_SHARED_LIBRARY_SUFFIX@")

# Include the exported targets file
include("${CMAKE_CURRENT_LIST_DIR}/HudiTargets.cmake")

# Check if the imported target exists
if(NOT TARGET Hudi::Hudi)
  message(FATAL_ERROR "Hudi::Hudi target not found in HudiTargets.cmake")
endif()

# Get the include directories
get_target_property(HUDI_INCLUDE_DIRS Hudi::Hudi INTERFACE_INCLUDE_DIRECTORIES)

# Set other required variables
set(HUDI_LIBRARIES Hudi::Hudi)
set(HUDI_LIBRARY ${HUDI_LIBRARY_PATH})
set(HUDI_FOUND TRUE)
set(Hudi_FOUND TRUE)
set(HUDI_VERSION "@PROJECT_VERSION@")
set(Hudi_VERSION "@PROJECT_VERSION@")

# Check all required components
check_required_components(Hudi)