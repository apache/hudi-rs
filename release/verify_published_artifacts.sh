#!/usr/bin/env bash
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
# Verify the artifacts a release tag published to crates.io and pypi.org.
#
# verify_src_release.sh covers the source release, which is the ASF release
# proper. This covers the convenience binaries, which are what almost everyone
# actually installs, and which the source checks say nothing about: whether
# every platform is present, whether the wheel imports, and whether it reads a
# table once loaded.
#
# Run it after the publish workflow finishes and before starting the VOTE
# thread. A publish job that fails after its siblings have uploaded leaves a
# version that looks published and is missing a platform, and crates.io will
# not take that version again, so the gap has to be found before anyone votes.
#
# Run it from the release branch: the functional test reads a table fixture out
# of the checkout, and a fixture written for a newer table version than the
# release supports fails for that reason rather than for anything about the
# artifact.

set -euo pipefail

usage() {
  echo "Usage: $0 <version> [--skip-functional]"
  echo
  echo "  version           release version, e.g. 0.5.0 or 0.5.0-rc.2"
  echo "  --skip-functional skip the container install-and-read test (needs docker)"
  exit 1
}

[ "$#" -ge 1 ] || usage

version=$1
skip_functional=false
[ "${2:-}" = "--skip-functional" ] && skip_functional=true

version_pattern="^[0-9]+\.[0-9]+\.[0-9]+(-(alpha|beta|rc)\.[0-9]+)?$"
if [[ ! "$version" =~ $version_pattern ]]; then
  echo "ERROR: version must be X.Y.Z or X.Y.Z-{alpha|beta|rc}.W"
  exit 1
fi

# PyPI normalizes the pre-release suffix: 0.5.0-rc.1 is published as 0.5.0rc1.
pypi_version=$(echo "$version" |
  sed -E 's/-alpha\.([0-9]+)$/a\1/; s/-beta\.([0-9]+)$/b\1/; s/-rc\.([0-9]+)$/rc\1/')

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
failures=0
note_failure() {
  echo "    $1"
  failures=$((failures + 1))
}

echo "Verifying published artifacts for $version (pypi: $pypi_version)"

# ---------------------------------------------------------------- crates.io --
# The three crates publish in one cargo invocation, so a partial set means the
# publish died part-way and the version can never be re-published.
echo ">>> Verifying crates.io..."
for crate in hudi-core hudi-datafusion hudi; do
  if curl -sf -H 'User-Agent: hudi-rs release verification' \
      "https://crates.io/api/v1/crates/$crate/$version" |
      grep -q "\"num\":\"$version\""; then
    echo "    $crate $version: present"
  else
    note_failure "$crate $version: MISSING from crates.io"
  fi
done

# -------------------------------------------------------------------- PyPI --
# Every platform the release workflow builds has to be here. A missing wheel
# sends that platform to the sdist, which needs protoc and a C++ toolchain, so
# in practice it is an install failure rather than a slow install.
echo ">>> Verifying pypi.org..."
pypi_json=$(curl -sf "https://pypi.org/pypi/hudi/$pypi_version/json" || true)
if [ -z "$pypi_json" ]; then
  note_failure "hudi $pypi_version: NOT FOUND on pypi.org"
else
  filenames=$(echo "$pypi_json" | python3 -c \
    'import json,sys; print("\n".join(f["filename"] for f in json.load(sys.stdin)["urls"]))')

  # Substrings rather than exact names: the abi3 tag tracks the python floor and
  # the manylinux tag tracks the build image, and neither should fail this check
  # when it changes deliberately.
  for expected in \
    "macosx_.*_x86_64\.whl" \
    "macosx_.*_arm64\.whl" \
    "win_amd64\.whl" \
    "manylinux.*_x86_64\.whl" \
    "manylinux.*_aarch64\.whl" \
    "\.tar\.gz"; do
    if echo "$filenames" | grep -qE "$expected"; then
      echo "    $(echo "$filenames" | grep -E "$expected" | head -n 1)"
    else
      note_failure "no artifact matching '$expected'"
    fi
  done
fi

# -------------------------------------------------------------- functional --
# Installing in a clean image is the part that catches a wheel that builds and
# then does not load: the rocksdb bindings come from the build container's
# libclang, and bad bindings surface on import or first read, not at compile
# time. Reading a table exercises the merge path rather than just the module.
if [ "$skip_functional" = true ]; then
  echo ">>> Skipping the functional test (--skip-functional)"
elif ! command -v docker >/dev/null 2>&1; then
  echo ">>> Skipping the functional test (docker not found)"
else
  echo ">>> Verifying the wheels install and read a table..."
  work_dir=$(mktemp -d)
  trap 'rm -rf "$work_dir"' EXIT

  fixture=$repo_root/crates/test/data/quickstart_trips_table/mor/avro/v8_trips_8i3u1d.zip
  if [ ! -f "$fixture" ]; then
    note_failure "table fixture not found at $fixture"
  else
    unzip -q "$fixture" -d "$work_dir/tables"

    cat >"$work_dir/check.py" <<'PY'
import glob
import os
import sys

import hudi

tables = [p for p in sorted(glob.glob("/data/*")) if os.path.isdir(f"{p}/.hoodie")]
if not tables:
    sys.exit("no table fixture mounted")

for path in tables:
    table = hudi.HudiTable(path)
    rows = sum(batch.num_rows for batch in table.read())
    if rows == 0:
        sys.exit(f"{os.path.basename(path)}: read returned no rows")
    print(f"    {os.path.basename(path)}: {table.table_type}, {rows} rows, "
          f"{len(table.get_schema())} columns")
PY

    # Both linux wheels, not just the host's. One of them runs emulated and is
    # slow, but a wheel is per-architecture and testing only the native one
    # leaves the other exactly as unverified as it was before this script.
    # A stock python image, not the image the wheel was built in: that is what
    # makes this a test of the platform tag rather than of the build container.
    for docker_platform in linux/amd64 linux/arm64; do
      echo "  $docker_platform:"
      if docker run --rm --platform "$docker_platform" \
          -v "$work_dir/tables:/data:ro" \
          -v "$work_dir/check.py:/check.py:ro" \
          python:3.11-slim bash -c "
            set -e
            pip install --quiet --only-binary=:all: 'hudi==$pypi_version'
            python -c 'import hudi; print(\"    import: ok\")'
            python /check.py
          "; then
        echo "    installs and reads: ok"
      else
        note_failure "$docker_platform: the wheel failed to install, import, or read a table"
      fi
    done
  fi

  # The macOS wheels can only be exercised on macOS, and the windows wheel not
  # at all from here. Say so rather than letting the linux results read as
  # coverage of everything the presence check above lists.
  if [ "$(uname -s)" = "Darwin" ] && command -v python3 >/dev/null 2>&1; then
    echo "  macos/$(uname -m):"
    venv=$work_dir/venv
    python3 -m venv "$venv"
    if "$venv/bin/pip" install --quiet --only-binary=:all: "hudi==$pypi_version" &&
        "$venv/bin/python" -c 'import hudi; print("    import: ok")' &&
        DATA="$work_dir/tables" "$venv/bin/python" -c '
import glob, os, sys
import hudi
for path in sorted(glob.glob(os.environ["DATA"] + "/*")):
    if not os.path.isdir(f"{path}/.hoodie"):
        continue
    table = hudi.HudiTable(path)
    rows = sum(batch.num_rows for batch in table.read())
    if rows == 0:
        sys.exit(f"{os.path.basename(path)}: read returned no rows")
    print(f"    {os.path.basename(path)}: {table.table_type}, {rows} rows, "
          f"{len(table.get_schema())} columns")
'; then
      echo "    installs and reads: ok"
    else
      note_failure "macos/$(uname -m): the wheel failed to install, import, or read a table"
    fi
  else
    echo "  macos: not tested (run this on macOS to cover it)"
  fi
  echo "  windows: not tested (no way to exercise it from here)"
fi

echo
if [ "$failures" -ne 0 ]; then
  echo "FAILED: $failures problem(s) found. Do not start the VOTE thread."
  exit 1
fi
echo "OK: published artifacts for $version look complete and usable."
