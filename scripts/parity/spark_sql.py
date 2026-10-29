#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
"""Run Spark SQL statements against a Hudi table registered in a database
(parity harness).

The table at --path is registered as <db>.<table> with `create table ... using
hudi location`, then each --sql statement runs in order. `{table}` in a
statement expands to the qualified name.
"""
import argparse
import sys

from pyspark.sql import SparkSession


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", required=True)
    parser.add_argument("--db", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--warehouse", required=True, help="spark.sql.warehouse.dir")
    parser.add_argument("--sql", action="append", default=[], help="statement (repeatable)")
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("parity-sql")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        .config("spark.sql.warehouse.dir", args.warehouse)
        .getOrCreate()
    )
    qualified = f"{args.db}.{args.table}"
    spark.sql(f"create database if not exists {args.db}")
    spark.sql(f"drop table if exists {qualified}")
    spark.sql(f"create table {qualified} using hudi location '{args.path}'")
    for statement in args.sql:
        spark.sql(statement.replace("{table}", qualified))
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
