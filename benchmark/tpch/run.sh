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

DEFAULT_SCALE_FACTOR=1
DOCKER_IMAGE="tpch-spark-hudi"
TPCH_BIN="$REPO_ROOT/target/release/tpch"

build_tpch() {
  echo "Building TPC-H tool..."
  cargo build -p tpch --release --manifest-path "$REPO_ROOT/Cargo.toml"
}

build_docker() {
  echo "Building Spark + Hudi Docker image..."
  docker build -t "$DOCKER_IMAGE" "$SCRIPT_DIR/infra/spark"
}

# Read spark-submit args from tpch binary (one token per line) into SPARK_ARGS array.
read_spark_args() {
  SPARK_ARGS=()
  while IFS= read -r line; do
    SPARK_ARGS+=("$line")
  done < <("$TPCH_BIN" spark-args "$@")
}

usage() {
  cat <<EOF
Usage: $0 <command> [options]

Commands:
  generate          Generate TPC-H parquet data
  create-tables     Create Hudi COW tables from parquet via Spark (Docker)
  bench-spark       Run TPC-H queries against Hudi tables via Spark SQL (Docker)
  clean             Remove generated data

Options:
  --scale-factor N  TPC-H scale factor (default: $DEFAULT_SCALE_FACTOR)
  --queries Q       Comma-separated query numbers for bench-spark (default: all 22)
  --iterations N    Number of measured iterations per query (default: 3)
  --warmup N        Number of unmeasured warmup iterations per query (default: 2)

Examples:
  $0 generate --scale-factor 1
  $0 create-tables --scale-factor 1
  $0 bench-spark --scale-factor 1 --queries 1,3,6 --iterations 3 --warmup 2
  $0 clean
EOF
}

# --- Commands ---

cmd_generate() {
  local sf="$DEFAULT_SCALE_FACTOR"
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --scale-factor) sf="$2"; shift 2 ;;
      *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
    esac
  done

  build_tpch
  "$TPCH_BIN" generate --scale-factor "$sf"
}

cmd_create_tables() {
  local sf="$DEFAULT_SCALE_FACTOR"
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --scale-factor) sf="$2"; shift 2 ;;
      *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
    esac
  done

  local parquet_dir="$SCRIPT_DIR/data/sf$sf-parquet"
  if [ ! -d "$parquet_dir" ]; then
    echo "Error: parquet data not found at $parquet_dir. Run 'generate' first." >&2
    exit 1
  fi

  build_tpch
  build_docker
  mkdir -p "$SCRIPT_DIR/data/sf$sf-hudi"

  local sql_file
  sql_file="$(mktemp)"
  "$TPCH_BIN" render-ctas --scale-factor "$sf" \
    --parquet-base /opt/parquet --hudi-base /opt/hudi > "$sql_file"

  read_spark_args --scale-factor "$sf" --command create-tables

  echo "Creating Hudi COW tables from parquet (sf$sf)..."
  local docker_exit=0
  docker run --rm \
    -v "$parquet_dir:/opt/parquet:ro" \
    -v "$SCRIPT_DIR/data/sf$sf-hudi:/opt/hudi" \
    -v "$sql_file:/opt/spark/work-dir/create_hudi_tables.sql:ro" \
    "$DOCKER_IMAGE" \
    /opt/spark/bin/spark-sql "${SPARK_ARGS[@]}" \
    -f /opt/spark/work-dir/create_hudi_tables.sql \
    || docker_exit=$?

  rm -f "$sql_file"
  if [ $docker_exit -ne 0 ]; then
    echo "Error: Spark SQL failed with exit code $docker_exit" >&2
    return $docker_exit
  fi
  echo "Hudi COW tables created at: $SCRIPT_DIR/data/sf$sf-hudi"
}

cmd_bench_spark() {
  local sf="$DEFAULT_SCALE_FACTOR"
  local queries=""
  local iterations=3
  local warmup=2
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --scale-factor) sf="$2"; shift 2 ;;
      --queries) queries="$2"; shift 2 ;;
      --iterations) iterations="$2"; shift 2 ;;
      --warmup) warmup="$2"; shift 2 ;;
      *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
    esac
  done

  local hudi_dir="$SCRIPT_DIR/data/sf$sf-hudi"
  if [ ! -d "$hudi_dir" ]; then
    echo "Error: Hudi data not found at $hudi_dir. Run 'create-tables' first." >&2
    exit 1
  fi

  build_tpch
  build_docker

  read_spark_args --scale-factor "$sf" --command bench

  local output_file
  output_file="$(mktemp)"

  local bench_args=(
    --hudi-base /opt/hudi
    --query-dir /opt/queries
    --scale-factor "$sf"
    --warmup "$warmup"
    --iterations "$iterations"
  )
  if [ -n "$queries" ]; then
    bench_args+=(--queries "$queries")
  fi

  echo "Running Spark SQL benchmark..."
  local docker_exit=0
  docker run --rm \
    -v "$hudi_dir:/opt/hudi:ro" \
    -v "$SCRIPT_DIR/queries:/opt/queries:ro" \
    -v "$SCRIPT_DIR/infra/spark/bench.py:/opt/spark/work-dir/bench.py:ro" \
    "$DOCKER_IMAGE" \
    /opt/spark/bin/spark-submit "${SPARK_ARGS[@]}" \
    /opt/spark/work-dir/bench.py \
    "${bench_args[@]}" \
    > "$output_file" \
    || docker_exit=$?

  if [ $docker_exit -ne 0 ]; then
    echo "Error: Spark SQL benchmark failed with exit code $docker_exit" >&2
    rm -f "$output_file"
    return $docker_exit
  fi

  echo ""
  "$TPCH_BIN" parse-spark-output --input "$output_file"
  rm -f "$output_file"
}

cmd_clean() {
  if [ -d "$SCRIPT_DIR/data" ]; then
    echo "Removing generated data..."
    rm -rf "$SCRIPT_DIR/data"
  fi
  echo "Clean complete."
}

# --- Main ---

if [[ $# -lt 1 ]]; then
  usage
  exit 1
fi

COMMAND="$1"
shift

case "$COMMAND" in
  generate)      cmd_generate "$@" ;;
  create-tables) cmd_create_tables "$@" ;;
  bench-spark)   cmd_bench_spark "$@" ;;
  clean)         cmd_clean "$@" ;;
  *)
    echo "Unknown command: $COMMAND" >&2
    usage
    exit 1
    ;;
esac
