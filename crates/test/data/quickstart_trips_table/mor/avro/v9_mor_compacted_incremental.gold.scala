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

// Regenerates the `gold_incremental/<window>` data inside this fixture: what
// Hudi's own reader returns for each window, so hudi-rs can be diffed against
// the reference implementation rather than against its own incumbent reader.
//
// Run against the unzipped fixture:
//
//   export FIXTURE_PATH=<unzipped>/v9_mor_compacted_incremental
//   export GOLD_ROOT=$FIXTURE_PATH/gold_incremental
//   $SPARK_HOME/bin/spark-shell --master 'local[2]' \
//     --jars <hudi-spark3.5-bundle>.jar \
//     --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
//     --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
//     --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
//     -i v9_mor_compacted_incremental.gold.scala
//
// Generated with Spark 3.5.3 and Hudi 1.2.0-SNAPSHOT.
//
// Hudi ranges these on COMPLETION time and includes both ends:
//   "New data written with completion_time >= START_COMMIT are fetched out"
//   "completion_time <= END_COMMIT are fetched out"
// hudi-rs ranges on REQUESTED time and excludes the start, so a window whose
// bounds fall between a commit's two timestamps disagrees. `only_b` is kept
// precisely because it does.
val tablePath = sys.env("FIXTURE_PATH")
val goldRoot = sys.env("GOLD_ROOT")

// (name, startCommit, endCommit) — commit times taken from the fixture timeline.
val windows = Seq(
  ("after_insert_through_c", "20260807223524868", "20260807223530767"),
  ("only_b", "20260807223528666", "20260807223529143"),
  ("through_compaction", "20260807223524868", "20260807223529586")
)

for ((name, start, end) <- windows) {
  val df = spark.read
    .format("hudi")
    .option("hoodie.datasource.query.type", "incremental")
    .option("hoodie.datasource.read.begin.instanttime", start)
    .option("hoodie.datasource.read.end.instanttime", end)
    .load(tablePath)

  val rows = df.orderBy("uuid").collect()
  println(s"GOLD_WINDOW\t$name\t$start\t$end\trows=${rows.length}")
  rows.foreach { r =>
    println(s"GOLD_ROW\t$name\t${r.getAs[String]("uuid")}\t${r.getAs[Double]("fare")}\t${r.getAs[String]("_hoodie_commit_time")}")
  }

  df.coalesce(1).write.mode("overwrite").parquet(s"$goldRoot/$name")
}

System.exit(0)
