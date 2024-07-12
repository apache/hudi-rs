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

if [[ $# -ne 2 ]]; then
  echo "Usage: $0 <dev|release> <hudi_version>"
  exit 1
fi

repo=$1
hudi_version=$2

dev_pattern="^[0-9]+\.[0-9]+\.[0-9]+-(alpha|beta|rc)\.[0-9]+$"
release_pattern="^[0-9]+\.[0-9]+\.[0-9]+$"

if [[ "$repo" == "dev" ]]; then
  if [[ ! "$hudi_version" =~ $dev_pattern ]]; then
    echo "ERROR: For 'dev' repo, version must be in format X.Y.Z-[alpha|beta|rc].N (e.g., 0.1.0-rc.1)"
    exit 1
  fi
elif [[ "$repo" == "release" ]]; then
  if [[ ! "$hudi_version" =~ $release_pattern ]]; then
    echo "ERROR: For 'release' repo, version must be in format X.Y.Z (e.g., 0.1.0)"
    exit 1
  fi
else
  echo "ERROR: Invalid repository type. Use 'dev' or 'release'."
  exit 1
fi

work_dir="$TMPDIR$(date +'%Y-%m-%d-%H-%M-%S')/svn_dir"
hudi_artifact="hudi-rs-$hudi_version"
svn_url="https://dist.apache.org/repos/dist/$repo/hudi/$hudi_artifact"
echo "Checking out src release from $svn_url to $work_dir"
svn co -q "$svn_url" "$work_dir"

cd "$work_dir"
src="$hudi_artifact.src.tgz"
pub_key="$src.asc"
checksum="$src.sha512"
echo ">>> Verifying artifacts exist..."
artifacts=($src $pub_key $checksum)
for artifact in "${artifacts[@]}"; do
  if [ ! -f "$artifact" ]; then
    echo "ERROR: Artifact $artifact does not exist."
    exit 1
  fi
done
echo "<<< OK"

echo ">>> Verifying checksum..."
if [ "$(uname)" == "Darwin" ]; then
  SHASUM="shasum -a 512"
else
  SHASUM="sha512sum"
fi
$SHASUM "$src" >"$work_dir/src.sha512"
diff -u "$checksum" "$work_dir/src.sha512"
echo "<<< OK"

echo ">>> Verifying signature..."
curl -s "https://dist.apache.org/repos/dist/$repo/hudi/KEYS" >"$work_dir/KEYS"
gpg -q --import "$work_dir/KEYS"
gpg --verify "$pub_key" "$src"
echo "<<< OK"

echo "Un-tarring the source release artifact"
mkdir "$hudi_artifact"
tar -xzf "$src" -C "$hudi_artifact"
cd "$hudi_artifact"

echo ">>> Verifying no DISCLAIMER..."
if [ -f "./DISCLAIMER" ]; then
  echo "ERROR: DISCLAIMER file should not be present."
  exit 1
fi
echo "<<< OK"

echo ">>> Verifying LICENSE file present..."
if [ ! -f "./LICENSE" ]; then
  echo "ERROR: LICENSE file is missing."
  exit 1
fi
echo "<<< OK"

echo ">>> Verifying NOTICE file present..."
if [ ! -f "./NOTICE" ]; then
  echo "ERROR: NOTICE file is missing."
  exit 1
fi
echo "<<< OK"

echo ">>> Verifying licenses..."
docker run -it --rm -v $(pwd):/github/workspace apache/skywalking-eyes header check
echo "<<< OK"

echo ">>> Verifying no binary files..."
find_binary_files() {
  find . -type f \
    -not -path "*/tests/data/*" \
    -not -path "*/tests/table/*" \
    -not -name "*.json" -not -name "*.xml" \
    -exec file -I '{}' \; |
    grep -viE 'directory|text/'
}
numBinaryFiles=$(find_binary_files | wc -l)
if ((numBinaryFiles > 0)); then
  echo "ERROR: There were non-text files in the source release. Please check:"
  find_binary_files
  exit 1
fi
echo "<<< OK"
