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
"""Read a Hudi table via Spark and dump sorted rows as JSON.

Modes:
- snapshot (default): read twice, with and without metadata-table listing, and
  fail (exit 3) if the two disagree so MDT interop problems surface.
- timetravel: snapshot as of --as-of instant.
- incremental: changes in (--begin, --end] (end optional).
- cdc: incremental with the CDC format; dumps raw CDC rows.
"""
import argparse
import json
import sys

from pyspark.sql import SparkSession


def read_rows(spark, path, use_metadata, extra_options=None):
    reader = (
        spark.read.format("hudi")
        .option("hoodie.metadata.enable", "true" if use_metadata else "false")
    )
    for k, v in (extra_options or {}).items():
        reader = reader.option(k, v)
    df = reader.load(path)
    columns = [c for c in ["id", "part", "ts", "value"] if c in df.columns]
    rows = [
        {c: row[c] for c in columns}
        for row in df.select(*columns).collect()
    ]
    rows.sort(key=lambda r: (r.get("id") or "", r.get("part") or ""))
    return rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument(
        "--mode",
        default="snapshot",
        choices=["snapshot", "timetravel", "incremental", "cdc"],
    )
    parser.add_argument("--as-of", help="instant for timetravel mode")
    parser.add_argument("--begin", help="begin instant for incremental/cdc mode")
    parser.add_argument("--end", help="optional end instant for incremental/cdc mode")
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("parity-read")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .getOrCreate()
    )

    if args.mode == "snapshot":
        with_mdt = read_rows(spark, args.path, use_metadata=True)
        without_mdt = read_rows(spark, args.path, use_metadata=False)
        if with_mdt != without_mdt:
            print("MDT_MISMATCH", file=sys.stderr)
            print(
                json.dumps({"with_mdt": with_mdt, "without_mdt": without_mdt}),
                file=sys.stderr,
            )
            spark.stop()
            return 3
        rows = with_mdt
    elif args.mode == "timetravel":
        if not args.as_of:
            print("--as-of required for timetravel", file=sys.stderr)
            return 2
        rows = read_rows(
            spark, args.path, use_metadata=True, extra_options={"as.of.instant": args.as_of}
        )
    else:  # incremental / cdc
        if not args.begin:
            print("--begin required for incremental/cdc", file=sys.stderr)
            return 2
        options = {
            "hoodie.datasource.query.type": "incremental",
            "hoodie.datasource.read.begin.instanttime": args.begin,
        }
        if args.end:
            options["hoodie.datasource.read.end.instanttime"] = args.end
        if args.mode == "cdc":
            options["hoodie.datasource.query.incremental.format"] = "cdc"
            df_reader = spark.read.format("hudi")
            for k, v in options.items():
                df_reader = df_reader.option(k, v)
            df = df_reader.load(args.path)
            rows = [row.asDict(recursive=True) for row in df.collect()]
            rows.sort(key=lambda r: json.dumps(r, sort_keys=True, default=str))
        else:
            rows = read_rows(spark, args.path, use_metadata=True, extra_options=options)

    with open(args.out, "w") as f:
        json.dump(rows, f, default=str)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
