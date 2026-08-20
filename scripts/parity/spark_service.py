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
"""Run a Hudi table service via Spark SQL CALL procedures (parity harness)."""
import argparse
import sys

from pyspark.sql import SparkSession


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", required=True)
    parser.add_argument(
        "--service", required=True, choices=["compact", "cluster", "clean"]
    )
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("parity-service")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
        )
        .getOrCreate()
    )
    if args.service == "compact":
        spark.sql(
            f"CALL run_compaction(op => 'scheduleandexecute', path => '{args.path}')"
        ).show(truncate=False)
    elif args.service == "cluster":
        spark.sql(
            f"CALL run_clustering(path => '{args.path}', op => 'scheduleandexecute')"
        ).show(truncate=False)
    elif args.service == "clean":
        # run_clean requires a catalog table name; register the path first.
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS parity_clean_target USING hudi LOCATION '{args.path}'"
        )
        spark.sql("CALL run_clean(table => 'parity_clean_target')").show(truncate=False)
        spark.sql("DROP TABLE IF EXISTS parity_clean_target")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
