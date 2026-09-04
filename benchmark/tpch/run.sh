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
HUDI_SPARK_BUNDLE="org.apache.hudi:hudi-spark3.5-bundle_2.12:1.1.1"

# Record what the numbers were produced on. A timing is only comparable against
# another run on the same hardware, build and data, and none of that is
# recoverable from the results afterwards.
write_env_report() {
  local out_file="$1"
  local sf="$2"
  local data_dir="$3"

  local cpu_model cpu_count mem_total
  if [ -r /proc/cpuinfo ]; then
    # lscpu first: aarch64 /proc/cpuinfo carries no model name at all, only
    # implementer and part numbers.
    cpu_model=$(lscpu 2>/dev/null | awk -F': +' '/^Model name/ {print $2; exit}')
    [ -z "$cpu_model" ] && cpu_model=$(awk -F': +' '/^model name/ {print $2; exit}' /proc/cpuinfo)
    [ -z "$cpu_model" ] && cpu_model=$(uname -m)
    cpu_count=$(nproc)
    mem_total=$(awk '/^MemTotal/ {printf "%.0f GiB", $2/1048576}' /proc/meminfo)
  else
    cpu_model=$(sysctl -n machdep.cpu.brand_string 2>/dev/null)
    cpu_count=$(sysctl -n hw.ncpu 2>/dev/null)
    mem_total=$(sysctl -n hw.memsize 2>/dev/null | awk '{printf "%.0f GiB", $1/1073741824}')
  fi

  # Instance identity, when this is an EC2 box. IMDSv2, and short timeouts so a
  # non-EC2 machine costs nothing.
  local instance_type="" instance_region=""
  local imds_token
  imds_token=$(curl -fsS --max-time 1 -X PUT \
    -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' \
    http://169.254.169.254/latest/api/token 2>/dev/null) || true
  if [ -n "$imds_token" ]; then
    instance_type=$(curl -fsS --max-time 1 -H "X-aws-ec2-metadata-token: $imds_token" \
      http://169.254.169.254/latest/meta-data/instance-type 2>/dev/null) || true
    instance_region=$(curl -fsS --max-time 1 -H "X-aws-ec2-metadata-token: $imds_token" \
      http://169.254.169.254/latest/meta-data/placement/region 2>/dev/null) || true
  fi

  local git_rev git_dirty=""
  git_rev=$(git -C "$REPO_ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown)
  git -C "$REPO_ROOT" diff --quiet 2>/dev/null || git_dirty=" (modified)"

  # Where the data physically sits: an EBS root and an instance store give very
  # different read numbers for the same command.
  # Report the scheme without the bucket: the results are meant to be pasted
  # somewhere public, and the bucket name identifies private infrastructure.
  local data_location="$data_dir"
  local data_backing="n/a (object storage)"
  if is_cloud_url "$data_dir"; then
    data_location="${data_dir%%://*}:// (bucket omitted)"
  else
    data_backing=$(df -h "$data_dir" 2>/dev/null | awk 'NR==2 {print $1" "$2}')
  fi

  {
    echo "# Benchmark environment"
    echo "captured:        $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    echo "scale factor:    $sf"
    echo "data location:   $data_location"
    echo "data backing:    $data_backing"
    [ -n "$instance_type" ] && echo "instance:        $instance_type ($instance_region)"
    echo "cpu:             ${cpu_model:-unknown} x ${cpu_count:-?}"
    echo "memory:          ${mem_total:-unknown}"
    echo "os:              $(uname -srm)"
    echo "spark master:    local[*] on ${cpu_count:-?} cores"
    echo "hudi-rs commit:  ${git_rev}${git_dirty}"
    echo "rustc:           $(rustc --version 2>/dev/null || echo unknown)"
    echo "RUSTFLAGS:       ${RUSTFLAGS:-(unset, see ~/.cargo/config.toml)}"
    echo "cargo build:     release"
    echo "java:            $(java -version 2>&1 | head -1)"
    echo "spark:           $("$SPARK_HOME/bin/spark-submit" --version 2>&1 | awk '/version/ {print $NF; exit}')"
    echo "hudi bundle:     $HUDI_SPARK_BUNDLE"
    echo "config:          config/sf$sf.yaml"
  } > "$out_file"
}

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
  # A pip-installed PySpark ships without a conf directory.
  mkdir -p "$SPARK_HOME/conf"
  cp "$SCRIPT_DIR/infra/spark/spark-defaults.conf" "$SPARK_HOME/conf/spark-defaults.conf"
  cp "$SCRIPT_DIR/infra/spark/log4j2.properties" "$SPARK_HOME/conf/log4j2.properties"
}

is_cloud_url() {
  case "$1" in
    s3://*|s3a://*|gs://*|wasb://*|wasbs://*|az://*) return 0 ;;
    *) return 1 ;;
  esac
}

# A data dir symlinked to an instance store dangles once a stop/start wipes it.
# Name that, because the failure it otherwise produces is create_dir_all
# reporting EEXIST for a directory that does not exist.
require_usable_data_dir() {
  local data_root="$SCRIPT_DIR/data"
  if [ -L "$data_root" ] && [ ! -d "$data_root" ]; then
    echo "Error: $data_root links to $(readlink "$data_root"), which is missing." >&2
    echo "Recreate that directory, or remove the symlink to use local storage." >&2
    exit 1
  fi
}

# Fail with the missing table names rather than letting the engine report a
# missing path once the run is already under way.
require_hudi_tables() {
  local hudi_dir="$1"
  if ! "$TPCH_BIN" check-tables --hudi-base "$hudi_dir"; then
    echo "Error: run 'create-tables' first, or point --hudi-dir at existing tables." >&2
    exit 1
  fi
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

Options (per command):
  --scale-factor N  TPC-H scale factor [all commands] (default: $DEFAULT_SCALE_FACTOR)
  --format F        Table format: hudi or parquet [bench-*, compare] (default: auto)
  --recreate        Rebuild tables that already exist [create-tables] (default: reuse them)
  --hudi-dir D      Hudi data directory or cloud URL [create-tables, bench-*] (default: data/sf{N}-hudi)
  --parquet-dir D   Parquet data directory or cloud URL [create-tables, bench-*] (default: data/sf{N}-parquet)
  --queries Q       Comma-separated query numbers [bench-*] (default: all 22)
  --iterations N    Number of measured iterations per query [bench-*] (from config)
  --warmup N        Number of unmeasured warmup iterations per query [bench-*] (from config)
  --output-dir D    Directory to persist results as JSON [bench-*]
  --engines E       Comma-separated engine names to compare [compare]

Examples:
  $0 generate --scale-factor 1
  $0 create-tables --scale-factor 1
  $0 create-tables --scale-factor 100 --hudi-dir s3://bucket/sf100-hudi
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

  require_usable_data_dir

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
  local custom_hudi_dir=""
  local custom_parquet_dir=""
  local recreate=0
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --scale-factor) sf="$2"; shift 2 ;;
      --hudi-dir) custom_hudi_dir="$2"; shift 2 ;;
      --parquet-dir) custom_parquet_dir="$2"; shift 2 ;;
      --recreate) recreate=1; shift ;;
      *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
    esac
  done

  local hudi_dir="${custom_hudi_dir:-$SCRIPT_DIR/data/sf$sf-hudi}"

  build_tpch

  # Rebuilding is the expensive step at scale, so existing tables are reused
  # unless --recreate says otherwise.
  if [ "$recreate" -eq 0 ] && "$TPCH_BIN" check-tables --hudi-base "$hudi_dir" 2>/dev/null; then
    echo "Reusing existing Hudi tables at: $hudi_dir"
    echo "Pass --recreate to rebuild them."
    return 0
  fi

  local parquet_dir="${custom_parquet_dir:-$SCRIPT_DIR/data/sf$sf-parquet}"
  if ! is_cloud_url "$parquet_dir" && [ ! -d "$parquet_dir" ]; then
    echo "Error: parquet data not found at $parquet_dir. Run 'generate' first." >&2
    exit 1
  fi

  setup_spark

  # Spark creates the cloud prefix itself; only a local target needs clearing
  # and pre-creating, and only there would stale files survive a rerun.
  if ! is_cloud_url "$hudi_dir"; then
    if [ -d "$hudi_dir" ]; then
      echo "Removing existing Hudi data at $hudi_dir..."
      rm -rf "$hudi_dir"
    fi
    mkdir -p "$hudi_dir"
  fi

  local sql_file
  sql_file="$(mktemp)"
  "$TPCH_BIN" render-ctas --scale-factor "$sf" \
    --parquet-base "$parquet_dir" --hudi-base "$hudi_dir" > "$sql_file"

  read_spark_args --scale-factor "$sf" --command create-tables

  echo "Creating Hudi COW tables from parquet (sf$sf)..."
  "$SPARK_HOME/bin/spark-sql" \
    --packages "$HUDI_SPARK_BUNDLE" \
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
  if [ "$format" = "hudi" ]; then
    require_hudi_tables "$data_dir"
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
    --packages "$HUDI_SPARK_BUNDLE" \
    "${SPARK_ARGS[@]}" \
    "$SCRIPT_DIR/infra/spark/bench.py" \
    "${bench_args[@]}"

  echo ""
  local parse_args=(parse-spark-output --input "$output_file")
  if [ -n "$output_dir" ]; then
    mkdir -p "$output_dir"
    parse_args+=(--output-dir "$output_dir" --engine-label spark --format-label "$format" --display-name "spark+hudi" --scale-factor "$sf")
    write_env_report "$output_dir/environment.txt" "$sf" "$data_dir"
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

  if [ "$use_hudi" = true ]; then
    require_hudi_tables "$hudi_dir"
  fi

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
    write_env_report "$output_dir/environment.txt" "$sf" "$hudi_dir"
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

  # Print alongside the chart so a copied result carries the conditions that
  # produced it.
  local env_file="$SCRIPT_DIR/results/environment.txt"
  if [ -f "$env_file" ]; then
    echo ""
    cat "$env_file"
  fi
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
