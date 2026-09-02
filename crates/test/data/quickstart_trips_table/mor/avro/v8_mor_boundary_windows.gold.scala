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

// Regenerates gold_incremental/ for this fixture: what Hudi returns for four
// windows placed around the third commit (UPDATE b), whose requested and
// completion times differ. Same invocation as the v9 fixture's generator; see
// its header. Set SPARK_LOCAL_IP=127.0.0.1 if the host's address has changed
// since Spark was last run, or the driver fails to bind.
//
// Generated with Spark 3.5.3 and Hudi 1.2.0-SNAPSHOT.
val tablePath = sys.env("FIXTURE_PATH")
val goldRoot = sys.env("GOLD_ROOT")

// c3 = update b: requested 20260808010723246, completion 20260808010723734
val windows = Seq(
  // control: both bounds fall in the gaps between commits
  ("between_commits",        "20260808010722500", "20260808010724000"),
  // start exactly on c3's requested time
  ("start_on_requested",     "20260808010723246", "20260808010724000"),
  // start exactly on c3's completion time
  ("start_on_completion",    "20260808010723734", "20260808010725000"),
  // window spans c3's requested -> completion
  ("requested_to_completion","20260808010723246", "20260808010723734")
)

for ((name, start, end) <- windows) {
  val df = spark.read.format("hudi")
    .option("hoodie.datasource.query.type", "incremental")
    .option("hoodie.datasource.read.begin.instanttime", start)
    .option("hoodie.datasource.read.end.instanttime", end)
    .load(tablePath)
  val rows = df.orderBy("uuid").collect()
  println(s"GOLD\t$name\t[${rows.map(_.getAs[String]("uuid")).mkString(",")}]")
  df.coalesce(1).write.mode("overwrite").parquet(s"$goldRoot/$name")
}
System.exit(0)
