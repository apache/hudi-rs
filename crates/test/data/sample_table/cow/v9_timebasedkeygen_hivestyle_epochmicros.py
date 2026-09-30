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

# Generated with Apache Hudi 1.2.0 on Spark 3.5.7 (apache/spark:3.5.7, linux/arm64) using
# hudi-spark3.5-bundle_2.12, run with spark-submit. The DataFrame writer is used rather than
# Spark SQL because Hudi's SQL layer (SqlKeyGenerator.convertPartitionPathToSqlType) re-parses
# a timestamp partition path as a long and fails on 'yyyy-MM-dd'; Hudi 0.15.0 has no
# EPOCHMICROSECONDS at all and treats the micros as millis.
#
# Table version 9. The partition source column is a real TIMESTAMP, which Spark hands to the
# key generator as epoch microseconds, so EPOCHMICROSECONDS is the honest declaration of the
# column's encoding. A query engine never spells a timestamp literal that way.
#
# Partition paths: ts=2024-03-01

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType

NAME = "v9_timebasedkeygen_hivestyle_epochmicros"
spark = (SparkSession.builder.master("local[2]")
         .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
         .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
         .config("spark.ui.enabled", "false")
         .getOrCreate())

schema = StructType([
    StructField("id", IntegerType()), StructField("name", StringType()),
    StructField("region", StringType()), StructField("amount", DoubleType()),
    StructField("ts", TimestampType()),
])
rows = [
    (1, "Alice", "eu", 10.0, datetime(2024, 3, 1, 8, 0, 0)),
    (2, "Bob", "us", 20.0, datetime(2024, 3, 1, 18, 30, 0)),
    (3, "Carol", "eu", 30.0, datetime(2024, 3, 2, 9, 15, 0)),
    (4, "Dave", "us", 40.0, datetime(2024, 3, 3, 23, 59, 59)),
    (5, "Eve", "eu", 50.0, datetime(2024, 2, 29, 0, 0, 0)),
]
df = spark.createDataFrame(rows, schema)
(df.write.format("hudi")
   .option("hoodie.table.name", NAME)
   .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
   .option("hoodie.datasource.write.operation", "insert")
   .option("hoodie.datasource.write.recordkey.field", "id")
   .option("hoodie.datasource.write.precombine.field", "amount")
   .option("hoodie.datasource.write.partitionpath.field", "ts")
   .option("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.TimestampBasedKeyGenerator")
   .option("hoodie.datasource.write.hive_style_partitioning", "true")
   .option("hoodie.keygen.timebased.timestamp.type", "EPOCHMICROSECONDS")
   .option("hoodie.keygen.timebased.output.dateformat", "yyyy-MM-dd")
   .option("hoodie.keygen.timebased.timezone", "UTC")
   .option("hoodie.metadata.enable", "false")
   .mode("overwrite")
   .save(f"file:///tmp/{NAME}"))
spark.read.format("hudi").load(f"file:///tmp/{NAME}").select("_hoodie_partition_path", "id", "ts").orderBy("id").show(truncate=False)
