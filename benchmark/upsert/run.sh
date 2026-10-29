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

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
VENV="$SCRIPT_DIR/.venv"
PY="$VENV/bin/python"

usage() {
  cat <<USAGE
Usage: $(basename "$0") <command> [bench.py options]

Commands:
  setup         Create the Python 3.13 venv at benchmark/upsert/.venv and
                install the pinned packages.
  install-hudi  Build the hudi Python binding from this checkout into the venv
                (maturin, release profile), or install the wheel paths given.
                The write API comes from apache/hudi-rs#878, which is not on
                main yet: build from a checkout that includes it, or pass a
                wheel built from one. Fails if the installed hudi has no
                HudiTable.create.
  smoke         The scenario's smoke scale, every leg; writes
                <work-dir>/smoke-<scenario>.json. A busy machine only warns.
  full          The scenario's default scale; pass --scenario/--legs/--out as needed.
  summarize     Print a table from results files (pass --markdown for the README).

Examples:
  $(basename "$0") setup
  $(basename "$0") smoke --scenario rag-chunks
  $(basename "$0") full --scenario uuid --legs hudi-mor --out results/uuid-hudi-mor-10M.json
  $(basename "$0") summarize results/*.json --markdown
USAGE
}

require_venv() {
  if [[ ! -x "$PY" ]]; then
    echo "no venv at $VENV; run: $0 setup" >&2
    exit 1
  fi
}

cmd="${1:-}"
[[ -n "$cmd" ]] && shift
case "$cmd" in
  setup)
    uv venv --python 3.13 "$VENV"
    uv pip install --python "$PY" -q -r "$SCRIPT_DIR/requirements.txt"
    "$PY" -c 'import importlib.metadata as m
for d in ("pyarrow", "numpy", "psutil", "duckdb", "pytest", "hudi"):
    try:
        print(d, m.version(d))
    except m.PackageNotFoundError:
        print(d, "not installed")'
    ;;
  install-hudi)
    require_venv
    if [[ $# -gt 0 ]]; then
      # Only hudi is reinstalled; its dependencies keep their pinned versions.
      uv pip install --python "$PY" --reinstall-package hudi "$@"
    else
      uv pip install --python "$PY" -q 'maturin>=1.5,<2.0'  # python/pyproject.toml's range
      (cd "$REPO_ROOT" && VIRTUAL_ENV="$VENV" PATH="$VENV/bin:$PATH" \
        ./build-wrapper.sh maturin develop --release -m python/Cargo.toml)
    fi
    if ! "$PY" -c 'import sys, hudi
print("hudi", hudi.__version__)
sys.exit(0 if hasattr(hudi.HudiTable, "create") else 1)'; then
      echo "the installed hudi has no write API (HudiTable.create); build from a" \
        "checkout that includes apache/hudi-rs#878 or pass a wheel built from one" >&2
      exit 1
    fi
    ;;
  smoke)
    require_venv
    "$PY" "$SCRIPT_DIR/bench.py" --smoke "$@"
    ;;
  full)
    require_venv
    "$PY" "$SCRIPT_DIR/bench.py" "$@"
    ;;
  summarize)
    require_venv
    "$PY" "$SCRIPT_DIR/summarize.py" "$@"
    ;;
  ""|-h|--help|help)
    usage
    ;;
  *)
    echo "unknown command: $cmd" >&2
    usage >&2
    exit 1
    ;;
esac
