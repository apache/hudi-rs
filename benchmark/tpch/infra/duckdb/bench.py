#!/usr/bin/env python3
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

"""TPC-H benchmark runner for DuckDB, reading Hudi tables via the hudi extension.

Measures wall-clock time around execute().fetchall() for each query iteration,
mirroring how the Spark and DataFusion benchmarks are measured, and writes the
same JSON lines the Spark runner does.

The Hudi tables are read through read_hudi() and the parquet through
read_parquet(), each behind one view per TPC-H table in its own schema, so the
query text is identical for both formats. With --validate, every query runs
against both schemas and the results are compared instead of timed.
"""

import argparse
import json
import math
import os
import signal
import subprocess
import sys
import time
from decimal import Decimal

import duckdb

TPCH_TABLES = [
    "customer", "lineitem", "nation", "orders",
    "part", "partsupp", "region", "supplier",
]


def load_query(query_dir, query_num, scale_factor):
    path = os.path.join(query_dir, f"q{query_num}.sql")
    with open(path) as f:
        sql = f.read()
    q11_fraction = f"{0.0001 / scale_factor:.10f}"
    sql = sql.replace("${Q11_FRACTION}", q11_fraction)
    sql = "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))
    return [s.strip() for s in sql.split(";") if s.strip()]


def sql_str(value):
    return "'" + value.replace("'", "''") + "'"


def connect(args):
    # The process keeps one database for the whole run: the hudi extension pins a
    # connection to the database it was first loaded into.
    config = {}
    if args.extension != "community":
        config["allow_unsigned_extensions"] = "true"
    if args.threads:
        config["threads"] = str(args.threads)
    if args.memory_limit:
        config["memory_limit"] = args.memory_limit
    con = duckdb.connect(config=config)

    if args.extension == "community":
        con.execute("INSTALL hudi FROM community")
        con.execute("LOAD hudi")
    else:
        con.execute(f"LOAD {sql_str(args.extension)}")

    bases = [b for b in (args.hudi_base, args.parquet_base) if b]
    if any(b.startswith(("s3://", "s3a://")) for b in bases):
        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")
        con.execute("CREATE OR REPLACE SECRET tpch_s3 (TYPE s3, PROVIDER credential_chain)")
    return con


def register_views(con, args):
    if args.hudi_base:
        con.execute("CREATE SCHEMA IF NOT EXISTS hudi")
        opt = f", hudi_option => {sql_str(args.hudi_option)}" if args.hudi_option else ""
        for table in TPCH_TABLES:
            uri = sql_str(f"{args.hudi_base}/{table}")
            con.execute(f"CREATE OR REPLACE VIEW hudi.{table} AS SELECT * FROM read_hudi({uri}{opt})")
    if args.parquet_base:
        con.execute("CREATE SCHEMA IF NOT EXISTS parquet")
        for table in TPCH_TABLES:
            glob = sql_str(f"{args.parquet_base}/{table}/*.parquet")
            con.execute(f"CREATE OR REPLACE VIEW parquet.{table} AS SELECT * FROM read_parquet({glob})")


def run_query(con, schema, statements):
    con.execute(f"USE {schema}")
    # Q15 wraps its query in CREATE VIEW / DROP VIEW; the answer is the SELECT's.
    rows = None
    for stmt in statements:
        result = con.execute(stmt).fetchall()
        if stmt.split(None, 1)[0].lower() in ("select", "with"):
            rows = result
    return rows


def values_match(a, b):
    # The same tolerance the DataFusion validator applies.
    if isinstance(a, float) or isinstance(b, float):
        if a is None or b is None:
            return a is b
        return math.isclose(float(a), float(b), rel_tol=1e-6, abs_tol=1e-10)
    return a == b


def sort_key(row):
    # Numbers sort as numbers whatever their type, so a float on one side and a
    # Decimal on the other still pair up.
    return tuple(
        (1, float(v), "") if isinstance(v, (int, float, Decimal)) and not isinstance(v, bool)
        else (0, 0.0, "") if v is None
        else (2, 0.0, repr(v))
        for v in row
    )


def rows_match(left, right):
    if len(left) != len(right):
        return False
    return all(
        len(lr) == len(rr) and all(values_match(x, y) for x, y in zip(lr, rr))
        for lr, rr in zip(left, right)
    )


def validate(con, args, query_nums):
    mismatched = []
    for qn in query_nums:
        statements = load_query(args.query_dir, qn, args.scale_factor)
        try:
            parquet_rows = run_query(con, "parquet", statements)
        except duckdb.Error as e:
            print(f"  Q{qn:02d} ERROR on parquet, cannot validate: {e}", flush=True)
            mismatched.append(qn)
            continue
        try:
            hudi_rows = run_query(con, "hudi", statements)
        except duckdb.Error as e:
            print(f"  Q{qn:02d} ERROR on hudi: {e}", flush=True)
            mismatched.append(qn)
            continue
        # Ties under ORDER BY may come back in either order; a sorted retry
        # separates that from a real difference.
        ok = rows_match(hudi_rows, parquet_rows) or rows_match(
            sorted(hudi_rows, key=sort_key), sorted(parquet_rows, key=sort_key))
        status = "OK" if ok else "DIFF"
        print(f"  Q{qn:02d} {status} ({len(hudi_rows)} hudi rows, {len(parquet_rows)} parquet rows)",
              flush=True)
        if not ok:
            mismatched.append(qn)
    return 1 if mismatched else 0


def bench(con, args, query_nums):
    schema = "hudi" if args.hudi_base else "parquet"
    total_runs = args.warmup + args.iterations
    failed = []
    with open(args.output, "a") as result_file:
        for qn in query_nums:
            statements = load_query(args.query_dir, qn, args.scale_factor)
            # Written only once every iteration has run, so a query that fails
            # part way leaves no partial timings for compare to average.
            timings = []
            for i in range(total_runs):
                is_warmup = i < args.warmup
                if is_warmup:
                    label = f"warmup {i + 1}/{args.warmup}"
                else:
                    label = f"iter {i - args.warmup + 1}/{args.iterations}"
                print(f"  Q{qn:02d} {label}...", end="", flush=True)

                start = time.perf_counter()
                try:
                    run_query(con, schema, statements)
                except duckdb.Error as e:
                    print(f" ERROR: {e}", flush=True)
                    failed.append(qn)
                    break
                elapsed_ms = (time.perf_counter() - start) * 1000.0

                print(f" {elapsed_ms:.1f}ms", flush=True)
                if not is_warmup:
                    timings.append(elapsed_ms)
            if qn not in failed:
                for elapsed_ms in timings:
                    result_file.write(json.dumps({"query": qn, "elapsed_ms": elapsed_ms}) + "\n")
                result_file.flush()
    return 2 if failed else 0


def run_isolated(args, query_nums):
    """Run each query in its own process, so one that crashes the process is
    recorded as a failure instead of ending the run."""
    if args.output:
        open(args.output, "w").close()
    else:
        print(f"Validating {len(query_nums)} queries, Hudi against parquet", flush=True)
    if not args.validate:
        print(f"Warmup: {args.warmup} iteration(s), Measured: {args.iterations} iteration(s)",
              flush=True)

    # Load the extension once up front, so a setup failure (no network for the
    # community install, a build for another DuckDB) is reported once rather
    # than by every query.
    base_cmd = [sys.executable, os.path.abspath(__file__), *sys.argv[1:]]
    probe = subprocess.run([*base_cmd, "--print-versions"], capture_output=True, text=True)
    if probe.returncode != 0:
        print("Error: could not load the hudi extension:", file=sys.stderr)
        print(probe.stderr.strip() or f"exit status {probe.returncode}", file=sys.stderr)
        return 1 if args.validate else 2

    failed = []
    for qn in query_nums:
        cmd = [*base_cmd, "--worker-query", str(qn)]
        rc = subprocess.run(cmd).returncode
        if rc < 0:
            print(f"  Q{qn:02d} CRASHED ({signal.Signals(-rc).name})", flush=True)
        if rc != 0:
            failed.append(qn)

    if failed:
        what = "differ from parquet or failed" if args.validate else "failed, no timings recorded"
        print(f"{', '.join(f'Q{q:02d}' for q in failed)}: {what}", file=sys.stderr)
        return 1 if args.validate else 2
    if args.validate:
        print("All queries match.")
    return 0


def main():
    parser = argparse.ArgumentParser(description="TPC-H DuckDB benchmark")
    parser.add_argument("--hudi-base", default=None)
    parser.add_argument("--parquet-base", default=None)
    parser.add_argument("--query-dir", required=True)
    parser.add_argument("--scale-factor", type=float, default=1.0)
    parser.add_argument("--queries", default=None, help="Comma-separated query numbers")
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument("--output", default=None, help="Output file for JSON results")
    parser.add_argument("--extension", default="community",
                        help="'community', or the path to a locally built hudi.duckdb_extension")
    parser.add_argument("--hudi-option", default=None,
                        help="Passed to read_hudi as hudi_option, e.g. 'k1=v1; k2=v2'")
    parser.add_argument("--threads", type=int, default=None)
    parser.add_argument("--memory-limit", default=None)
    parser.add_argument("--validate", action="store_true",
                        help="Compare Hudi results against parquet instead of timing")
    parser.add_argument("--print-versions", action="store_true",
                        help="Print the DuckDB and hudi extension versions and exit")
    parser.add_argument("--engine-label", default="duckdb",
                        help="Names the run in the --print-versions output")
    parser.add_argument("--worker-query", type=int, default=None, help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.print_versions:
        con = connect(args)
        ext = con.execute(
            "SELECT extension_version, install_mode FROM duckdb_extensions() "
            "WHERE extension_name = 'hudi'").fetchone()
        print(f"duckdb:          {duckdb.__version__}")
        print(f"duckdb-hudi [{args.engine_label}]: {ext[0]} ({ext[1]}, {args.extension})")
        threads = con.execute("SELECT current_setting('threads')").fetchone()[0]
        print(f"duckdb threads:  {threads}")
        return 0

    if args.validate:
        if not (args.hudi_base and args.parquet_base):
            parser.error("--validate needs both --hudi-base and --parquet-base")
    elif bool(args.hudi_base) == bool(args.parquet_base):
        parser.error("exactly one of --hudi-base or --parquet-base is required")
    elif not args.output:
        parser.error("--output is required unless --validate is given")

    if args.worker_query is None:
        query_nums = list(range(1, 23))
        if args.queries:
            query_nums = [int(q.strip()) for q in args.queries.split(",")]
        return run_isolated(args, query_nums)

    con = connect(args)
    register_views(con, args)
    query_nums = [args.worker_query]
    return validate(con, args, query_nums) if args.validate else bench(con, args, query_nums)


if __name__ == "__main__":
    sys.exit(main())
