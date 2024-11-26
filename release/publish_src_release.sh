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
dist_dirname=hudi-rs-$hudi_version

echo "❗️  Publishing $dist_dirname"

work_dir="$TMPDIR$(date +'%Y-%m-%d-%H-%M-%S')"

svn_dev_url="https://dist.apache.org/repos/dist/dev/hudi/$dist_dirname"
svn_dev_dir="$work_dir/$dist_dirname"
echo ">>> Checking out dev src dist '/$dist_dirname' to $svn_dev_dir"
svn co -q "$svn_dev_url" "$svn_dev_dir"

svn_release_url="https://dist.apache.org/repos/dist/release/hudi"
svn_release_dir="$work_dir/svn_release"
echo ">>> Checking out release src dist to $svn_release_dir"
svn co -q $svn_release_url --depth=immediates "$svn_release_dir"
echo ">>> Checking if $dist_dirname already exists"
release_dist_dir="$svn_release_dir/$dist_dirname"
if [ -d "$release_dist_dir" ]; then
  echo "❌  Version $dist_dirname already exists in the src release repo!"
  exit 1
fi

echo ">>> Copying $dist_dirname to $release_dist_dir"
pushd "$svn_dev_dir"
mkdir "$release_dist_dir"
find . -type f -name "$dist_dirname.src.*" -exec cp {} "$release_dist_dir" \;
echo "✅  SUCCESS! Placed source release at $release_dist_dir"
for i in $(ls -a "$release_dist_dir"); do echo "|___$i"; done
popd

echo ">>> Publishing to $svn_release_url"
pushd "$svn_release_dir"
svn add "$dist_dirname"
svn commit -m "add $dist_dirname"
echo "✅  SUCCESS! Published $dist_dirname to $svn_release_url"
popd
