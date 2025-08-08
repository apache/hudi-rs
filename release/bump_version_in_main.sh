#!/bin/bash
#
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
#

bump_version() {
    local current_version=$1
    local bump_type=$2

    IFS='.' read -ra version_parts <<< "$current_version"
    local major=${version_parts[0]}
    local minor=${version_parts[1]}
    local patch=${version_parts[2]}

    case $bump_type in
        major)
            major=$((major + 1))
            minor=0
            patch=0
            ;;
        minor)
            minor=$((minor + 1))
            patch=0
            ;;
        *)
            echo "Error: Invalid bump type. Use 'major' or 'minor'."
            exit 1
            ;;
    esac

    echo "$major.$minor.$patch-dev"
}

# Check if cargo-edit is installed
if ! cargo set-version --version &> /dev/null; then
    echo "Error: cargo-edit is not installed. Please install it with 'cargo install cargo-edit'."
    exit 1
fi

# Check if current branch is main
current_branch=$(git rev-parse --abbrev-ref HEAD)
if [ "$current_branch" != "main" ]; then
    echo "Error: Not on main branch. Please checkout the main branch before running this script."
    exit 1
fi

# Define the crate to update
crate="crates/hudi"

# Prompt user for bump type
read -p "Enter bump type (major/minor): " bump_type

# Validate bump type
if [[ "$bump_type" != "major" && "$bump_type" != "minor" ]]; then
    echo "Error: Invalid bump type. Use 'major' or 'minor'."
    exit 1
fi

# Get the current version
current_version=$(cargo pkgid --manifest-path "$crate/Cargo.toml" | cut -d# -f2)

if [ -z "$current_version" ]; then
    echo "Error: Unable to find current version for $crate"
    exit 1
fi

echo "Current version of $crate: $current_version"

# Calculate new version
new_version=$(bump_version "$current_version" "$bump_type")

echo "New version for $crate: $new_version"

# Update version using cargo set-version
cargo set-version "$new_version" --manifest-path "$crate/Cargo.toml"

if [ $? -ne 0 ]; then
    echo "Error: Failed to update version for $crate"
    exit 1
fi

echo "Updated version of $crate to $new_version"

# Update C++ CMakeLists.txt version
cpp_cmake_file="cpp/CMakeLists.txt"
if [ -f "$cpp_cmake_file" ]; then
    # Use sed to update the project version line (drop -dev suffix for CMakeLists.txt)
    cmake_version=${new_version%-dev}
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS sed requires empty string after -i
        sed -i '' "s/^project(hudi-cpp VERSION [0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*)/project(hudi-cpp VERSION $cmake_version)/" "$cpp_cmake_file"
    else
        # Linux sed
        sed -i "s/^project(hudi-cpp VERSION [0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*)/project(hudi-cpp VERSION $cmake_version)/" "$cpp_cmake_file"
    fi
    
    if [ $? -ne 0 ]; then
        echo "Error: Failed to update C++ version in $cpp_cmake_file"
        exit 1
    fi
    
    echo "Updated C++ version in $cpp_cmake_file to $cmake_version"
else
    echo "Warning: $cpp_cmake_file not found, skipping C++ version update"
fi

# Create a new branch
branch_name="bump-${new_version%-dev}"
git checkout -b "$branch_name"

if [ $? -ne 0 ]; then
    echo "Error: Failed to create new branch $branch_name"
    exit 1
fi

echo "Created new branch: $branch_name"

# Commit the changes
git add .
commit_message="build(release): bump version to $new_version"
git commit -m "$commit_message"

if [ $? -ne 0 ]; then
    echo "Error: Failed to commit changes"
    exit 1
fi

echo "Changes committed with message: $commit_message"

echo "Version bump complete. New branch '$branch_name' created with updated versions."
