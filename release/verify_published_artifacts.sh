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
  echo "  --skip-functional skip the install-and-read tests"
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
work_root=$(mktemp -d)
trap 'rm -rf "$work_root"' EXIT

failures=0
note_failure() {
  echo "    $1"
  failures=$((failures + 1))
}

# A check that could not run is not a broken release. Counted separately so the
# summary can say the run was incomplete without sending anyone to cut a new RC.
unknowns=0
note_unknown() {
  echo "    $1"
  unknowns=$((unknowns + 1))
}

echo "Verifying published artifacts for $version (pypi: $pypi_version)"

# ---------------------------------------------------------------- crates.io --
# The three crates publish in one cargo invocation, so a partial set means the
# publish died part-way and the version can never be re-published.
echo ">>> Verifying crates.io..."
for crate in hudi-core hudi-datafusion hudi; do
  # Distinguish a registry we could not reach from a version that is not there.
  # Only the second means the release is broken, and the remedy for that is a
  # whole new release candidate.
  body=$work_root/crates-$crate.json
  http_code=$(curl -s -o "$body" -w '%{http_code}' \
    -H 'User-Agent: hudi-rs release verification' \
    "https://crates.io/api/v1/crates/$crate/$version" || true)
  case "$http_code" in
    200)
      if grep -q "\"num\":\"$version\"" "$body"; then
        echo "    $crate $version: present"
      else
        note_failure "$crate $version: MISSING from crates.io"
      fi
      ;;
    404)
      note_failure "$crate $version: MISSING from crates.io"
      ;;
    *)
      note_unknown "$crate $version: not checked (crates.io returned $http_code)"
      ;;
  esac
done

# -------------------------------------------------------------------- PyPI --
# Every platform the release workflow builds has to be here. A missing wheel
# sends that platform to the sdist, which needs protoc and a C++ toolchain, so
# in practice it is an install failure rather than a slow install.
echo ">>> Verifying pypi.org..."
pypi_body=$work_root/pypi.json
pypi_code=$(curl -s -o "$pypi_body" -w '%{http_code}' \
  "https://pypi.org/pypi/hudi/$pypi_version/json" || true)
if [ "$pypi_code" = "404" ]; then
  note_failure "hudi $pypi_version: NOT FOUND on pypi.org"
elif [ "$pypi_code" != "200" ]; then
  note_unknown "hudi $pypi_version: not checked (pypi.org returned ${pypi_code:-no response})"
else
  pypi_json=$(cat "$pypi_body")
  filenames=$(echo "$pypi_json" | python3 -c \
    'import json,sys; print("\n".join(f["filename"] for f in json.load(sys.stdin)["urls"]))')
  # The floor the wheels were built against, so a leg can tell "this interpreter
  # is too old" apart from "this wheel is broken".
  pypi_requires_python=$(echo "$pypi_json" | python3 -c \
    'import json,sys; print(json.load(sys.stdin)["info"].get("requires_python") or "")')

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
# Installing in a clean environment is the part that catches a wheel that builds
# and then does not load: the rocksdb bindings come from the build container's
# libclang, and bad bindings surface on import or first read, not at compile
# time. Reading a table exercises the merge path rather than just the module.
#
# A wheel is per-platform, so each one has to be installed on its own platform.
# A leg that cannot run here says so: it must not be reported as passing, and an
# environment that cannot run a leg must not be reported as a broken release.
if [ "$skip_functional" = true ]; then
  echo ">>> Skipping the install-and-read tests (--skip-functional)"
else
  echo ">>> Verifying the wheels install and read a table..."
  work_dir=$work_root
  have_fixture=true
  fixture=$repo_root/crates/test/data/quickstart_trips_table/mor/avro/v8_trips_8i3u1d.zip
  if [ ! -f "$fixture" ]; then
    note_failure "table fixture not found at $fixture"
    have_fixture=false
  else
    unzip -q "$fixture" -d "$work_dir/tables"
  fi

  # One copy of the read, run by every leg. Two copies drift, and the check that
  # a leg read anything at all is the one worth not losing to that.
  check_py=$work_dir/check.py
  cat >"$check_py" <<'CHECK_PY'
import glob
import os
import sys

import hudi

root = sys.argv[1]
tables = [p for p in sorted(glob.glob(f"{root}/*")) if os.path.isdir(f"{p}/.hoodie")]
if not tables:
    sys.exit(f"no table fixture found under {root}")

for path in tables:
    table = hudi.HudiTable(path)
    rows = sum(batch.num_rows for batch in table.read())
    if rows == 0:
        sys.exit(f"{os.path.basename(path)}: read returned no rows")
    print(f"    {os.path.basename(path)}: {table.table_type}, {rows} rows, "
          f"{len(table.get_schema())} columns")
CHECK_PY

  if [ "$have_fixture" != true ]; then
    echo "  linux: not tested (no table fixture)"
  elif ! command -v docker >/dev/null 2>&1; then
    echo "  linux: not tested (docker not found)"
  elif ! docker info >/dev/null 2>&1; then
    # Without this, the per-architecture probe below swallows the daemon error
    # and reports both wheels as an architecture this host cannot run, which is
    # both untrue and silent about the two artifacts that motivated this script.
    echo "  linux: not tested (docker found but the daemon is not reachable)"
  else
    # A stock python image, not the image the wheel was built in: that is what
    # makes this a test of the platform tag rather than of the build container.
    for docker_platform in linux/amd64 linux/arm64; do
      # An architecture this host cannot execute is a missing binfmt handler,
      # not a bad wheel. The two must not reach the same verdict, because the
      # remedy for a bad wheel is an entire new release candidate.
      if ! docker run --rm --platform "$docker_platform" python:3.11-slim true >/dev/null 2>&1; then
        echo "  $docker_platform: not tested (this host cannot run that architecture)"
        continue
      fi
      echo "  $docker_platform:"
      if docker run --rm --platform "$docker_platform" \
          -v "$work_dir/tables:/data:ro" \
          -v "$check_py:/check.py:ro" \
          python:3.11-slim bash -c "
            set -e
            pip install --quiet --only-binary=:all: 'hudi==$pypi_version'
            python -c 'import hudi; print(\"    import: ok\")'
            python /check.py /data
          "; then
        echo "    installs and reads: ok"
      else
        note_failure "$docker_platform: the wheel failed to install, import, or read a table"
      fi
    done
  fi

  # The macOS wheel needs no container, so it must not sit behind the docker
  # check: on a mac without docker this is the one leg that can still run.
  if [ "$have_fixture" != true ]; then
    echo "  macos: not tested (no table fixture)"
  elif [ "$(uname -s)" != "Darwin" ]; then
    echo "  macos: not tested (run this on macOS to cover it)"
  elif ! command -v python3 >/dev/null 2>&1; then
    echo "  macos: not tested (python3 not found)"
  elif ! python3 -c "
import sys
spec = '''${pypi_requires_python:-}'''
floor = spec.split('>=')[-1].strip() if '>=' in spec else ''
sys.exit(0 if not floor else
         0 if sys.version_info >= tuple(int(p) for p in floor.split('.')) else 1)
" >/dev/null 2>&1; then
    # The container leg pins its interpreter; this one takes what PATH gives it,
    # and macOS still ships 3.9. Below the wheel's abi3 floor pip reports no
    # matching distribution, which is the interpreter being too old rather than
    # anything wrong with the wheel.
    echo "  macos: not tested (python3 is $(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))'), the wheel needs ${pypi_requires_python:-a newer python})"
  else
    echo "  macos/$(uname -m):"
    venv=$work_dir/venv
    python3 -m venv "$venv"
    if "$venv/bin/pip" install --quiet --only-binary=:all: "hudi==$pypi_version" &&
        "$venv/bin/python" -c 'import hudi; print("    import: ok")' &&
        "$venv/bin/python" "$check_py" "$work_dir/tables"; then
      echo "    installs and reads: ok"
    else
      note_failure "macos/$(uname -m): the wheel failed to install, import, or read a table"
    fi
  fi

  # Name what is still uncovered, so the legs that did run are not read as
  # standing in for the whole set the presence check above lists.
  if [ "$(uname -s)" = "Darwin" ]; then
    if [ "$(uname -m)" = "arm64" ]; then
      echo "  macos/x86_64: not tested (needs an intel mac)"
    else
      echo "  macos/arm64: not tested (needs an apple silicon mac)"
    fi
  fi
  echo "  windows: not tested (no way to exercise it from here)"
fi

echo
if [ "$failures" -ne 0 ]; then
  echo "FAILED: $failures problem(s) found. Do not start the VOTE thread."
  exit 1
fi
if [ "$unknowns" -ne 0 ]; then
  echo "INCOMPLETE: $unknowns check(s) could not run. Nothing looks broken, but"
  echo "this run did not cover everything; re-run it before starting the VOTE thread."
  exit 2
fi
echo "OK: published artifacts for $version look complete and usable."
