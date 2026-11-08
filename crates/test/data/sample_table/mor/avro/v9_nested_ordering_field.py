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

# Event-time ordering by a NESTED field, `meta.ts`, with Avro log blocks.
#
# Generated with Spark 3.5.5 and the Hudi 1.1.1 Spark bundle:
#   spark-submit --jars hudi-spark3.5-bundle_2.12-1.1.1.jar \
#     --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
#     v9_nested_ordering_field.py <output dir>
# The table lands in <output dir>/v9_nested_ordering_field and gold_data is
# `spark.read.format("hudi").load(<table>)` written as one parquet file beside it.
# The zip holds both directories with the `.crc` and `_SUCCESS` files removed.

import sys

from pyspark.sql import SparkSession

out = sys.argv[1]
path = f"{out}/v9_nested_ordering_field"
spark = SparkSession.builder.getOrCreate()
opts = {
    "hoodie.table.name": "v9_nested_ordering_field",
    "hoodie.datasource.write.table.type": "MERGE_ON_READ",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.precombine.field": "meta.ts",
    "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.NonpartitionedKeyGenerator",
    "hoodie.record.merge.mode": "EVENT_TIME_ORDERING",
    "hoodie.write.table.version": "9",
    "hoodie.metadata.enable": "false",
    "hoodie.compact.inline": "false",
}
# Each write is a SQL VALUES list so no Python rows go through the pickler.
writes = [
    # Base rows. No row has a null ordering value: Hudi 1.1.1 fails the write
    # on a null `meta` struct and fails its own read on a null `meta.ts`.
    """('a', 'x', named_struct('ts', 5L), false),
       ('b', 'x', named_struct('ts', 1L), false),
       ('d', 'x', named_struct('ts', 5L), false),
       ('e', 'x', named_struct('ts', 5L), false)""",
    # The issue's second write: `a` is below its base row and loses, `b` is
    # above and wins.
    """('a', 'y', named_struct('ts', 1L), false),
       ('b', 'y', named_struct('ts', 5L), false)""",
    # Deletes carry the row's ordering value into the delete block: `d`'s is
    # below its base row and the row survives, `e`'s is above and it goes.
    """('d', 'z', named_struct('ts', 1L), true),
       ('e', 'z', named_struct('ts', 9L), true)""",
]
for rows in writes:
    df = spark.sql(f"SELECT * FROM VALUES {rows} AS t(id, v, meta, _hoodie_is_deleted)")
    df.write.format("hudi").options(**opts).mode("append").save(path)

snapshot = spark.read.format("hudi").load(path)
snapshot.select("id", "v", "meta").orderBy("id").show(truncate=False)
snapshot.coalesce(1).write.parquet(f"{out}/gold_data")
