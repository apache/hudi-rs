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

set -o errexit
set -o nounset

if [ "$#" -lt 1 ]; then
  echo "❌  Insufficient arguments - please specify <hudi version>"
  exit 1
fi

hudi_version=$1
dist_repo_subpath=hudi-rs-$hudi_version

echo "❗️  Publishing $dist_repo_subpath"

work_dir="$TMPDIR$(date +'%Y-%m-%d-%H-%M-%S')"

svn_dev_url="https://dist.apache.org/repos/dist/dev/hudi/$dist_repo_subpath"
svn_dev_dir="$work_dir/$dist_repo_subpath"
echo ">>> Checking out dev src dist to $svn_dev_dir"
svn co -q "$svn_dev_url" "$svn_dev_dir"

svn_release_url="https://dist.apache.org/repos/dist/release/hudi"
svn_release_dir="$work_dir/svn_release"
echo ">>> Checking out release src dist to $svn_release_dir"
svn co -q $svn_release_url --depth=immediates "$svn_release_dir"
echo ">>> Checking if $dist_repo_subpath already exists"
subpath="$svn_release_dir/$dist_repo_subpath"
if [ -d "$subpath" ]; then
  echo "❌  Version $dist_repo_subpath already exists!"
  exit 1
fi

echo ">>> Copying $dist_repo_subpath to $subpath"
pushd "$svn_dev_dir"
find . -name "$dist_repo_subpath.src.*" -exec cp '{}' "$subpath" \;
echo "✅  SUCCESS! Placed source release at $subpath"
for i in $(ls -a "$subpath"); do echo "|___$i"; done
popd

echo ">>> Publishing to $svn_release_url"
pushd "$svn_release_dir"
svn add "$dist_repo_subpath"
svn commit -m "add $dist_repo_subpath"
echo "✅  SUCCESS! Published $dist_repo_subpath to $svn_release_url"
popd
