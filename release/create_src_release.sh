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

set -e

# check git branch
curr_branch=$(git rev-parse --abbrev-ref HEAD)
if [[ ! $curr_branch == release/* ]]; then
  echo "Branch does not start with 'release/'"
  exit 1
else
  echo "❗️  On branch $curr_branch"
fi

# check release version
hudi_version=${HUDI_VERSION}
if [ -z "${hudi_version}" ]; then
	echo "HUDI_VERSION is not set; Please specify the target hudi version to release, e.g., 0.1.0-rc.1"
	exit 1
else
	echo "❗️  Releasing Hudi version $hudi_version"
fi

# check release version against git tag
curr_tag=$(git describe --exact-match --tags)
if [ ! "$curr_tag" == "release-$hudi_version" ]; then
  echo "Tag '$curr_tag' does not match with release version $hudi_version"
else
  echo "❗️  On tag $curr_tag"
fi

# make sure a desired key is specified
gpg_user_id=${GPG_USER_ID}
if [ -z "${gpg_user_id}" ]; then
	echo "GPG_USER_ID is not set; run 'gpg --list-secret-keys --keyid-format=long' to choose a key."
	exit 1
else
	echo "❗️  Signing using key '$gpg_user_id'"
fi


work_dir="$TMPDIR$(date +'%Y-%m-%d-%H-%M-%S')"
hudi_src_rel_dir="$work_dir/hudi-rs-$hudi_version"
echo ">>> Archiving branch $curr_branch to $hudi_src_rel_dir"
mkdir -p "$hudi_src_rel_dir"
git archive --format=tar.gz --output="$hudi_src_rel_dir/hudi-rs-$hudi_version.src.tar.gz" "$curr_branch"
echo "Done archiving."

pushd "$hudi_src_rel_dir"
echo ">>> Generating signature"
for i in *.tar.gz; do
	echo "$i"
	gpg --local-user "$gpg_user_id" --armor --output "$i.asc" --detach-sig "$i"
done
echo ">>> Checking signature"
for i in *.tar.gz; do
	echo "$i"
	gpg --local-user "$gpg_user_id" --verify "$i.asc" "$i"
done
echo ">>> Generating sha512sum"
if [ "$(uname)" == "Darwin" ]; then
    SHASUM="shasum -a 512"
else
    SHASUM="sha512sum"
fi
for i in *.tar.gz; do
	echo "$i"
	$SHASUM "$i" >"$i.sha512"
done
echo ">>> Checking sha512sum"
for i in *.tar.gz; do
	echo "$i"
	$SHASUM --check "$i.sha512"
done

echo "✅  SUCCESS! Created source release at $hudi_src_rel_dir"
for i in $(ls -a "$hudi_src_rel_dir"); do echo "|___$i"; done;
popd

svn_dev_url="https://dist.apache.org/repos/dist/dev/hudi"
svn_dir="$work_dir/svn_dev"
echo ">>> Checking out svn dev to $svn_dir"
svn checkout $svn_dev_url --depth=immediates "$svn_dir"
echo ">>> Copying source release files to $svn_dir"
cp "$hudi_src_rel_dir" "$svn_dir/"

echo "❗️  [ACTION REQUIRED] Manually commit the source release to SVN."
echo "1️⃣  cd $svn_dir"
echo "2️⃣  svn add hudi-rs-$hudi_version"
echo "3️⃣  svn commit"
