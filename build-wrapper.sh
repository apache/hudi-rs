#!/bin/bash
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

# Build environment wrapper for macOS: sets SDK environment variables on
# macOS 26 (Tahoe) and later, and optimizes dependencies in dev builds on arm64.

set -e

# Detect macOS version
if [[ "$OSTYPE" == "darwin"* ]]; then
    MACOS_VERSION=$(sw_vers -productVersion | cut -d. -f1)
    
    if [[ "$MACOS_VERSION" -ge 26 ]]; then
        echo "Detected macOS ${MACOS_VERSION} (Tahoe or later)"
        echo "Setting SDK environment variables for build compatibility..."
        
        # Export SDK environment variables
        export SDKROOT="$(xcrun --show-sdk-path)"
        export MACOSX_DEPLOYMENT_TARGET="14.0"
        export CXXFLAGS="-isysroot ${SDKROOT}"
        export CFLAGS="-isysroot ${SDKROOT}"
        
        echo "SDK configuration:"
        echo "  SDKROOT=${SDKROOT}"
        echo "  MACOSX_DEPLOYMENT_TARGET=${MACOSX_DEPLOYMENT_TARGET}"
    fi
fi

# On macOS arm64, dependencies are compiled at opt-level 1 in dev builds.
# hudi-core links two arrow majors while Lance stays on its own, and at
# opt-level 0 that puts the code section of the hudi-datafusion test binaries
# and the Python extension past the arm64 +/-128MB branch range, where the
# linker cannot reach `ctor`'s startup section (pulled in through
# lance-io -> opendal). Workspace crates stay at opt-level 0 for debugging.
# The override is a global cargo flag, so it reaches cargo's built-in
# subcommands and maturin, but not external ones such as `cargo tarpaulin`.
# Remove with the Lance arrow bridge: https://github.com/apache/hudi-rs/issues/844
if [[ "$OSTYPE" == "darwin"* && "$(uname -m)" == "arm64" ]]; then
    DEP_OPT_LEVEL='profile.dev.package."*".opt-level=1'
    case "$1" in
        cargo)
            echo "Detected macOS arm64: dependencies build at opt-level 1"
            set -- "$1" --config "$DEP_OPT_LEVEL" "${@:2}"
            ;;
        maturin)
            if [[ "$2" == "build" || "$2" == "develop" ]]; then
                echo "Detected macOS arm64: dependencies build at opt-level 1"
                set -- "$1" "$2" --config "$DEP_OPT_LEVEL" "${@:3}"
            fi
            ;;
    esac
fi

# Run the command passed as arguments
exec "$@"
