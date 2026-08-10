/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// Builds `table_duplicate_keys.zip`: the one fixture where merging a file group
// by record position and merging it by record key give different answers.
//
// A file group holding the same record key more than once is what separates
// them. Hudi's writer tags an incoming record with *every* base row the index
// matches, so one upsert of `k1` becomes two log records and one delete of `k2`
// becomes three. Keyed by record key those collapse to one entry each and only
// the first base row of each key is merged; keyed by position each base row is
// merged on its own.
//
// Run:
//   export OUT_ROOT=<empty dir>
//   export SPARK_HOME=<spark-3.5.3>
//   $SPARK_HOME/bin/spark-shell --master 'local[2]' \
//     --jars <hudi-spark3.5-bundle>.jar \
//     --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
//     --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
//     --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
//     -i table_duplicate_keys.scala
//   cd $OUT_ROOT && zip -r table_duplicate_keys.zip table_duplicate_keys gold_data gold_positions
//
// Generated with Spark 3.5.3 and Hudi 1.1.1 (writes table version 9).

import org.apache.spark.sql.SaveMode

val outRoot = sys.env("OUT_ROOT")
val tableName = "table_duplicate_keys"
val tablePath = s"$outRoot/$tableName"

val common = Map(
  "hoodie.table.name" -> tableName,
  "hoodie.datasource.write.table.type" -> "MERGE_ON_READ",
  "hoodie.datasource.write.recordkey.field" -> "id",
  "hoodie.datasource.write.precombine.field" -> "ts",
  "hoodie.datasource.write.partitionpath.field" -> "",
  "hoodie.datasource.write.keygenerator.class" -> "org.apache.hudi.keygen.NonpartitionedKeyGenerator",
  "hoodie.metadata.enable" -> "false",
  // Without this the log blocks carry no RECORD_POSITIONS header and a
  // position-based read falls back to merging by key, so the fixture would
  // stop separating the two strategies.
  "hoodie.write.record.positions" -> "true",
  "hoodie.insert.shuffle.parallelism" -> "1",
  "hoodie.upsert.shuffle.parallelism" -> "1",
  "hoodie.delete.shuffle.parallelism" -> "1",
  // One file group for the whole table, so every duplicate shares a base file.
  "hoodie.parquet.small.file.limit" -> "104857600",
  "hoodie.merge.allow.duplicate.on.inserts" -> "true",
  "hoodie.combine.before.insert" -> "false",
  "hoodie.datasource.write.insert.drop.duplicates" -> "false"
)

// c1 — INSERT without combining, so a key survives more than once in the one
// base file: k1 twice, k2 three times, k3 once (the singleton control).
Seq(
  ("k1", "k1-row-a", 100L),
  ("k1", "k1-row-b", 100L),
  ("k2", "k2-row-a", 100L),
  ("k2", "k2-row-b", 100L),
  ("k2", "k2-row-c", 100L),
  ("k3", "k3-row-a", 100L)
).toDF("id", "val", "ts")
  .write.format("hudi").options(common)
  .option("hoodie.datasource.write.operation", "insert")
  .mode(SaveMode.Overwrite).save(tablePath)

// c2 — UPSERT k1: one incoming row, written as one data-block record per
// matched base row.
Seq(("k1", "k1-updated", 200L)).toDF("id", "val", "ts")
  .write.format("hudi").options(common)
  .option("hoodie.datasource.write.operation", "upsert")
  .mode(SaveMode.Append).save(tablePath)

// c3 — DELETE k2: the same expansion in a delete block, three records deep.
Seq(("k2", "k2-row-a", 300L)).toDF("id", "val", "ts")
  .write.format("hudi").options(common)
  .option("hoodie.datasource.write.operation", "delete")
  .mode(SaveMode.Append).save(tablePath)

// Two snapshots of the same table, differing only in how the file group is
// merged. Both are Hudi's own output; the fixture exists because they disagree.
def dumpGold(dir: String, positions: Boolean): Unit = {
  val df = spark.read.format("hudi")
    .option("hoodie.merge.use.record.positions", positions.toString)
    .load(tablePath)
  df.select("id", "val", "ts").orderBy("id", "val").show(false)
  println(s"$dir rows = ${df.count()}")
  df.write.mode("overwrite").parquet(s"$outRoot/$dir")
}
dumpGold("gold_data", false)
dumpGold("gold_positions", true)

System.exit(0)
