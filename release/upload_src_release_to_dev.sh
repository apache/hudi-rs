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

# ensure it's release branch
curr_branch=$(git rev-parse --abbrev-ref HEAD)
if [[ ! $curr_branch == release/* ]]; then
  echo "Branch does not start with 'release/'"
  exit 1
else
  echo "❗️  On branch $curr_branch"
fi

if [ "$#" -lt 2 ]; then
  echo "❌  Insufficient arguments - please specify <hudi version> <gpg key to sign>"
  echo "Run 'gpg --list-secret-keys --keyid-format=long' to choose a signing key."
  exit 1
fi

hudi_version=$1
signing_key=$2
repo=dev

echo "❗️  Releasing Hudi version $hudi_version"
echo "❗️  Uploading to https://dist.apache.org/repos/dist/$repo/hudi/"
echo "❗️  Using signing key: $signing_key"

# ensure the git tag is matched with the releasing version
curr_tag=$(git describe --exact-match --tags)
if [[ ! "$curr_tag" == "release-$hudi_version" ]]; then
  echo "❌  Tag $curr_tag does not match with the version release-$hudi_version"
  exit 1
else
  echo "❗️  On tag $curr_tag"
fi

dist_repo_subpath=hudi-rs-$hudi_version

work_dir="$TMPDIR$(date +'%Y-%m-%d-%H-%M-%S')"
src_rel_dir="$work_dir/$dist_repo_subpath"
echo ">>> Archiving branch $curr_branch to $src_rel_dir"
mkdir -p "$src_rel_dir"
git archive --format=tgz --output="$src_rel_dir/hudi-rs-$hudi_version.src.tgz" "$curr_branch"
echo "Done archiving."

pushd "$src_rel_dir"
echo ">>> Generating signature"
for i in *.tgz; do
  echo "$i"
  gpg --local-user "$signing_key" --armor --output "$i.asc" --detach-sig "$i"
done
echo ">>> Checking signature"
for i in *.tgz; do
  echo "$i"
  gpg --local-user "$signing_key" --verify "$i.asc" "$i"
done
echo ">>> Generating sha512sum"
if [ "$(uname)" == "Darwin" ]; then
  SHASUM="shasum -a 512"
else
  SHASUM="sha512sum"
fi
for i in *.tgz; do
  echo "$i"
  $SHASUM "$i" >"$i.sha512"
done
echo ">>> Checking sha512sum"
for i in *.tgz; do
  echo "$i"
  $SHASUM --check "$i.sha512"
done

echo "✅  SUCCESS! Created source release at $src_rel_dir"
for i in $(ls -a "$src_rel_dir"); do echo "|___$i"; done
popd

svn_dev_url="https://dist.apache.org/repos/dist/dev/hudi"
svn_dev_dir="$work_dir/svn_dev"
echo ">>> Checking out svn dev to $svn_dev_dir"
svn co -q $svn_dev_url --depth=immediates "$svn_dev_dir"
echo ">>> Checking if the same version dir exists in svn"
src_rel_svn_dir="$svn_dev_dir/$dist_repo_subpath"
if [ -d "$src_rel_svn_dir" ]; then
  echo "❌  $dist_repo_subpath already exists!"
  exit 1
fi

echo ">>> Copying source release files to $svn_dev_dir"
cp -r "$src_rel_dir" "$svn_dev_dir/"
echo "✅  SUCCESS! Placed source release at $src_rel_svn_dir"
for i in $(ls -a "$src_rel_svn_dir"); do echo "|___$i"; done

echo ">>> Publishing to $svn_dev_url"
pushd "$svn_dev_dir"
svn add "$dist_repo_subpath"
svn commit -m "add $dist_repo_subpath"
echo "✅  SUCCESS! Published $dist_repo_subpath to $svn_dev_url"
popd
