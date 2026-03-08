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

"""TPC-H benchmark runner for Spark SQL with Hudi tables.

Measures wall-clock time around spark.sql().collect() for each query iteration,
mirroring how DataFusion benchmarks are measured.
"""

import argparse
import json
import os
import sys
import time

from pyspark.sql import SparkSession

TPCH_TABLES = [
    "customer", "lineitem", "nation", "orders",
    "part", "partsupp", "region", "supplier",
]


def load_query(query_dir, query_num, scale_factor):
    path = os.path.join(query_dir, f"q{query_num}.sql")
    with open(path) as f:
        sql = f.read()
    q11_fraction = f"{0.0001 / scale_factor:.10f}"
    return sql.replace("${Q11_FRACTION}", q11_fraction)


def main():
    parser = argparse.ArgumentParser(description="TPC-H Spark SQL benchmark")
    parser.add_argument("--hudi-base", required=True)
    parser.add_argument("--query-dir", required=True)
    parser.add_argument("--scale-factor", type=float, default=1.0)
    parser.add_argument("--queries", default=None, help="Comma-separated query numbers")
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--iterations", type=int, default=3)
    args = parser.parse_args()

    query_nums = list(range(1, 23))
    if args.queries:
        query_nums = [int(q.strip()) for q in args.queries.split(",")]

    total_runs = args.warmup + args.iterations

    spark = SparkSession.builder.getOrCreate()

    # Register Hudi tables
    for table in TPCH_TABLES:
        spark.sql(
            f"CREATE TABLE {table} USING hudi LOCATION '{args.hudi_base}/{table}'"
        )

    print(
        f"Warmup: {args.warmup} iteration(s), Measured: {args.iterations} iteration(s)",
        file=sys.stderr, flush=True,
    )

    for qn in query_nums:
        sql = load_query(args.query_dir, qn, args.scale_factor)
        statements = [s.strip() for s in sql.split(";") if s.strip()]

        for i in range(total_runs):
            is_warmup = i < args.warmup
            if is_warmup:
                label = f"warmup {i + 1}/{args.warmup}"
            else:
                label = f"iter {i - args.warmup + 1}/{args.iterations}"
            print(f"  Q{qn:02d} {label}...", end="", file=sys.stderr, flush=True)

            start = time.time()
            for stmt in statements:
                spark.sql(stmt).collect()
            elapsed_ms = (time.time() - start) * 1000.0

            print(f" {elapsed_ms:.1f}ms", file=sys.stderr, flush=True)

            if not is_warmup:
                print(json.dumps({"query": qn, "elapsed_ms": elapsed_ms}), flush=True)

    spark.stop()


if __name__ == "__main__":
    main()
