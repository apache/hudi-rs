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

# Setup script for macOS 26 (Tahoe) build environment
# This script detects macOS version and sets SDK environment variables if needed

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

# Run the command passed as arguments
exec "$@"
