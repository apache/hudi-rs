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
TPCH_BIN="$REPO_ROOT/target/release/tpch"

build_tpch() {
  echo "Building TPC-H tool..."
  cargo build -p tpch --release --manifest-path "$REPO_ROOT/Cargo.toml"
}

# Read spark-submit args from tpch binary (one token per line) into SPARK_ARGS array.
read_spark_args() {
  SPARK_ARGS=()
  while IFS= read -r line; do
    SPARK_ARGS+=("$line")
  done < <("$TPCH_BIN" spark-args "$@")
}

# Setup Spark config files.
setup_spark() {
  if [ -z "${SPARK_HOME:-}" ]; then
    echo "Error: SPARK_HOME is not set. Set it to your Spark installation directory." >&2
    exit 1
  fi
  if [ ! -x "$SPARK_HOME/bin/spark-submit" ]; then
    echo "Error: $SPARK_HOME/bin/spark-submit not found or not executable." >&2
    exit 1
  fi

  echo "Configuring Spark at $SPARK_HOME..."
  cp "$SCRIPT_DIR/infra/spark/spark-defaults.conf" "$SPARK_HOME/conf/spark-defaults.conf"
  cp "$SCRIPT_DIR/infra/spark/log4j2.properties" "$SPARK_HOME/conf/log4j2.properties"
}

is_cloud_url() {
  case "$1" in
    s3://*|s3a://*|gs://*|wasb://*|wasbs://*|az://*) return 0 ;;
    *) return 1 ;;
  esac
}

usage() {
  cat <<EOF
Usage: $0 <command> [options]

Commands:
  generate          Generate TPC-H parquet data
  create-tables     Create Hudi COW tables from parquet via Spark SQL
  bench-spark       Run TPC-H queries against Hudi tables via Spark SQL
  bench-datafusion  Run TPC-H queries against Hudi tables via DataFusion
  compare           Compare persisted benchmark results with bar charts

Options:
  --scale-factor N  TPC-H scale factor (default: $DEFAULT_SCALE_FACTOR)
  --format F        Table format: hudi or parquet (default: auto)
  --hudi-dir D      Hudi data directory or cloud URL (default: data/sf{N}-hudi)
  --parquet-dir D   Parquet data directory or cloud URL (default: data/sf{N}-parquet)
  --queries Q       Comma-separated query numbers (default: all 22)
  --iterations N    Number of measured iterations per query (from config)
  --warmup N        Number of unmeasured warmup iterations per query (from config)
  --output-dir D    Directory to persist results as JSON (bench commands only)
  --engines E       Comma-separated engine names to compare (compare command only)

Examples:
  $0 generate --scale-factor 1
  $0 create-tables --scale-factor 1
  $0 bench-spark --scale-factor 1 --queries 1,3,6
  $0 bench-datafusion --scale-factor 1 --queries 1,3,6
  $0 bench-datafusion --scale-factor 100 --hudi-dir gs://bucket/sf100-hudi
  $0 compare --scale-factor 1 --engines datafusion,spark --format hudi
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

  local parquet_dir="$SCRIPT_DIR/data/sf$sf-parquet"
  if [ -d "$parquet_dir" ]; then
    echo "Removing existing parquet data at $parquet_dir..."
    rm -rf "$parquet_dir"
  fi

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

  local hudi_dir="$SCRIPT_DIR/data/sf$sf-hudi"
  if [ -d "$hudi_dir" ]; then
    echo "Removing existing Hudi data at $hudi_dir..."
    rm -rf "$hudi_dir"
  fi

  build_tpch
  setup_spark
  mkdir -p "$hudi_dir"

  local sql_file
  sql_file="$(mktemp)"
  "$TPCH_BIN" render-ctas --scale-factor "$sf" \
    --parquet-base "$parquet_dir" --hudi-base "$hudi_dir" > "$sql_file"

  read_spark_args --scale-factor "$sf" --command create-tables

  echo "Creating Hudi COW tables from parquet (sf$sf)..."
  "$SPARK_HOME/bin/spark-sql" \
    --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.1.1 \
    "${SPARK_ARGS[@]}" \
    -f "$sql_file"

  rm -f "$sql_file"
  echo "Hudi COW tables created at: $hudi_dir"
}

cmd_bench_spark() {
  local sf="$DEFAULT_SCALE_FACTOR"
  local queries=""
  local iterations=""
  local warmup=""
  local output_dir=""
  local format="hudi"
  local custom_hudi_dir=""
  local custom_parquet_dir=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --scale-factor) sf="$2"; shift 2 ;;
      --queries) queries="$2"; shift 2 ;;
      --iterations) iterations="$2"; shift 2 ;;
      --warmup) warmup="$2"; shift 2 ;;
      --output-dir) output_dir="$2"; shift 2 ;;
      --format) format="$2"; shift 2 ;;
      --hudi-dir) custom_hudi_dir="$2"; shift 2 ;;
      --parquet-dir) custom_parquet_dir="$2"; shift 2 ;;
      *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
    esac
  done

  build_tpch

  # Read defaults from config via the tpch binary
  if [ -z "$warmup" ] || [ -z "$iterations" ]; then
    local defaults
    defaults=$("$TPCH_BIN" bench-defaults --scale-factor "$sf")
    local cfg_warmup cfg_iterations
    cfg_warmup=$(echo "$defaults" | awk '{print $1}')
    cfg_iterations=$(echo "$defaults" | awk '{print $2}')
    warmup="${warmup:-$cfg_warmup}"
    iterations="${iterations:-$cfg_iterations}"
  fi

  local hudi_dir="${custom_hudi_dir:-$SCRIPT_DIR/data/sf$sf-hudi}"
  local parquet_dir="${custom_parquet_dir:-$SCRIPT_DIR/data/sf$sf-parquet}"

  local data_dir=""
  local bench_data_arg=""
  case "$format" in
    hudi)
      data_dir="$hudi_dir"
      bench_data_arg="--hudi-base"
      ;;
    parquet)
      data_dir="$parquet_dir"
      bench_data_arg="--parquet-base"
      ;;
    *) echo "Error: unknown format '$format'. Use 'hudi' or 'parquet'." >&2; exit 1 ;;
  esac

  if ! is_cloud_url "$data_dir" && [ ! -d "$data_dir" ]; then
    echo "Error: $format data not found at $data_dir." >&2
    exit 1
  fi

  read_spark_args --scale-factor "$sf" --command bench
  setup_spark

  local tmp_dir
  tmp_dir="$(mktemp -d)"
  local output_file="$tmp_dir/results.jsonl"

  local bench_args=(
    $bench_data_arg "$data_dir"
    --query-dir "$SCRIPT_DIR/queries"
    --scale-factor "$sf"
    --warmup "$warmup"
    --iterations "$iterations"
    --output "$output_file"
  )
  if [ -n "$queries" ]; then
    bench_args+=(--queries "$queries")
  fi

  echo "Running Spark SQL benchmark ($format)..."
  "$SPARK_HOME/bin/spark-submit" \
    --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.1.1 \
    "${SPARK_ARGS[@]}" \
    "$SCRIPT_DIR/infra/spark/bench.py" \
    "${bench_args[@]}"

  echo ""
  local parse_args=(parse-spark-output --input "$output_file")
  if [ -n "$output_dir" ]; then
    mkdir -p "$output_dir"
    parse_args+=(--output-dir "$output_dir" --engine-label spark --format-label "$format" --display-name "spark+hudi" --scale-factor "$sf")
  fi
  "$TPCH_BIN" "${parse_args[@]}"
  rm -rf "$tmp_dir"
}

cmd_bench_datafusion() {
  local sf="$DEFAULT_SCALE_FACTOR"
  local format=""
  local queries=""
  local iterations=""
  local warmup=""
  local output_dir=""
  local custom_hudi_dir=""
  local custom_parquet_dir=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --scale-factor) sf="$2"; shift 2 ;;
      --format) format="$2"; shift 2 ;;
      --queries) queries="$2"; shift 2 ;;
      --iterations) iterations="$2"; shift 2 ;;
      --warmup) warmup="$2"; shift 2 ;;
      --output-dir) output_dir="$2"; shift 2 ;;
      --hudi-dir) custom_hudi_dir="$2"; shift 2 ;;
      --parquet-dir) custom_parquet_dir="$2"; shift 2 ;;
      *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
    esac
  done

  local hudi_dir="${custom_hudi_dir:-$SCRIPT_DIR/data/sf$sf-hudi}"
  local parquet_dir="${custom_parquet_dir:-$SCRIPT_DIR/data/sf$sf-parquet}"

  # Determine which formats to bench
  local use_hudi=false
  local use_parquet=false
  case "$format" in
    hudi)    use_hudi=true ;;
    parquet) use_parquet=true ;;
    "")
      # Default: for cloud URLs trust the user; for local paths check existence
      if is_cloud_url "$hudi_dir"; then
        use_hudi=true
      else
        [ -d "$hudi_dir" ] && use_hudi=true
      fi
      if is_cloud_url "$parquet_dir"; then
        use_parquet=true
      else
        [ -d "$parquet_dir" ] && use_parquet=true
      fi
      ;;
    *) echo "Error: unknown format '$format'. Use 'hudi' or 'parquet'." >&2; exit 1 ;;
  esac

  if [ "$use_hudi" = false ] && [ "$use_parquet" = false ]; then
    echo "Error: no data found for sf$sf. Run 'generate' and/or 'create-tables' first." >&2
    exit 1
  fi
  if [ "$use_hudi" = true ] && ! is_cloud_url "$hudi_dir" && [ ! -d "$hudi_dir" ]; then
    echo "Error: Hudi data not found at $hudi_dir. Run 'create-tables' first." >&2
    exit 1
  fi
  if [ "$use_parquet" = true ] && ! is_cloud_url "$parquet_dir" && [ ! -d "$parquet_dir" ]; then
    echo "Error: Parquet data not found at $parquet_dir. Run 'generate' first." >&2
    exit 1
  fi

  build_tpch

  local bench_args=(bench --scale-factor "$sf")
  [ "$use_hudi" = true ] && bench_args+=(--hudi-dir "$hudi_dir")
  [ "$use_parquet" = true ] && bench_args+=(--parquet-dir "$parquet_dir")
  [ -n "$queries" ] && bench_args+=(--queries "$queries")
  [ -n "$iterations" ] && bench_args+=(--iterations "$iterations")
  [ -n "$warmup" ] && bench_args+=(--warmup "$warmup")

  if [ -n "$output_dir" ]; then
    mkdir -p "$output_dir"
    output_dir="$(cd "$output_dir" && pwd)"
    bench_args+=(--output-dir "$output_dir" --engine-label datafusion --format-label "${format:-hudi}" --display-name "datafusion+hudi-rs")
  fi

  echo "Running DataFusion benchmark..."
  TPCH_CONFIG_DIR="$SCRIPT_DIR/config" \
  TPCH_QUERY_DIR="$SCRIPT_DIR/queries" \
  RUST_LOG="${RUST_LOG:-warn}" \
  "$TPCH_BIN" "${bench_args[@]}"
}

cmd_compare() {
  local sf="$DEFAULT_SCALE_FACTOR"
  local engines=""
  local format="hudi"
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --scale-factor) sf="$2"; shift 2 ;;
      --engines) engines="$2"; shift 2 ;;
      --format) format="$2"; shift 2 ;;
      *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
    esac
  done

  if [ -z "$engines" ]; then
    echo "Error: --engines is required (e.g., --engines datafusion,spark)" >&2
    exit 1
  fi

  # Convert "datafusion,spark" → "datafusion_hudi_sf1,spark_hudi_sf1"
  local runs=""
  IFS=',' read -ra engine_arr <<< "$engines"
  for e in "${engine_arr[@]}"; do
    [ -n "$runs" ] && runs+=","
    runs+="${e}_${format}_sf${sf}"
  done

  build_tpch
  "$TPCH_BIN" compare \
    --results-dir "$SCRIPT_DIR/results" \
    --runs "$runs"
}

# --- Main ---

if [[ $# -lt 1 ]]; then
  usage
  exit 1
fi

COMMAND="$1"
shift

case "$COMMAND" in
  generate)         cmd_generate "$@" ;;
  create-tables)    cmd_create_tables "$@" ;;
  bench-spark)      cmd_bench_spark "$@" ;;
  bench-datafusion) cmd_bench_datafusion "$@" ;;
  compare)          cmd_compare "$@" ;;
  *)
    echo "Unknown command: $COMMAND" >&2
    usage
    exit 1
    ;;
esac
