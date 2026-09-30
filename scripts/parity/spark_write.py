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
"""Write rows to a Hudi table via the Spark datasource (parity harness).

Standard harness schema: id STRING (record key), part STRING (partition when
--partitioned), ts BIGINT (precombine), value BIGINT.
"""
import argparse
import json
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType

SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("part", StringType(), True),
        StructField("ts", LongType(), False),
        StructField("value", LongType(), False),
    ]
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", required=True)
    parser.add_argument(
        "--op",
        required=True,
        choices=["upsert", "insert", "bulk_insert", "delete", "insert_overwrite", "insert_overwrite_table"],
    )
    parser.add_argument("--table-type", default="cow", choices=["cow", "mor"])
    parser.add_argument("--table-name", default="parity")
    parser.add_argument("--partitioned", action="store_true")
    parser.add_argument("--rows", required=True, help="JSON array of row objects")
    parser.add_argument(
        "--write-option",
        action="append",
        default=[],
        help="extra hoodie write option as key=value (repeatable)",
    )
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("parity-write")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .getOrCreate()
    )
    rows = json.loads(args.rows)
    df = spark.createDataFrame(rows, SCHEMA)

    options = {
        "hoodie.table.name": args.table_name,
        "hoodie.datasource.write.recordkey.field": "id",
        "hoodie.datasource.write.precombine.field": "ts",
        "hoodie.datasource.write.operation": args.op,
        "hoodie.datasource.write.table.type": (
            "MERGE_ON_READ" if args.table_type == "mor" else "COPY_ON_WRITE"
        ),
    }
    if args.partitioned:
        options["hoodie.datasource.write.partitionpath.field"] = "part"
        options["hoodie.datasource.write.hive_style_partitioning"] = "true"
    else:
        options["hoodie.datasource.write.partitionpath.field"] = ""
        options["hoodie.datasource.write.keygenerator.class"] = (
            "org.apache.hudi.keygen.NonpartitionedKeyGenerator"
        )

    for opt in args.write_option:
        key, _, value = opt.partition("=")
        options[key] = value

    df.write.format("hudi").options(**options).mode("append").save(args.path)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
