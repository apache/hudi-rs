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

-- Event-time ordering where a log update LOSES to the base row.
--
-- Generated with Spark 3.5.3 and Hudi 1.2.0-SNAPSHOT. gold_data is
-- `spark.read.format("hudi").load(<table>)` written as one parquet file beside
-- the table directory; gold_options comes from `gold_options.scala`.

CREATE TABLE table_event_time_stale (
    ts     BIGINT,
    uuid   STRING,
    rider  STRING,
    fare   DOUBLE
) USING HUDI
TBLPROPERTIES (
    type = 'mor',
    primaryKey = 'uuid',
    preCombineField = 'ts',
    'hoodie.write.table.version' = '9',
    'hoodie.record.merge.mode' = 'EVENT_TIME_ORDERING',
    'hoodie.metadata.enable' = 'false',
    'hoodie.parquet.small.file.limit' = '0',
    'hoodie.compact.inline' = 'false'
)
LOCATION 'FIXTURE_LOCATION';

INSERT INTO table_event_time_stale VALUES
    (100, 'a', 'rider-A', 10.0),
    (100, 'b', 'rider-B', 20.0),
    (100, 'c', 'rider-C', 30.0),
    (100, 'd', 'rider-D', 40.0);

-- Ordering value BELOW the base row: the base row must survive this update.
UPDATE table_event_time_stale SET fare = 99.0, rider = 'stale-A', ts = 50 WHERE uuid = 'a';

-- Ordering value ABOVE the base row: this update must win. Present so that
-- inverting the comparison fails the fixture rather than passing it.
UPDATE table_event_time_stale SET fare = 22.0, rider = 'fresh-B', ts = 200 WHERE uuid = 'b';

-- A delete record carries the ordering value of the source row it matched, so
-- MERGE INTO is what puts a delete BELOW the live row. This one is stale and
-- the row survives it.
MERGE INTO table_event_time_stale AS t
USING (SELECT 'c' AS uuid, 50L AS ts, 'zz' AS rider, 0.0 AS fare) AS s
ON t.uuid = s.uuid
WHEN MATCHED THEN DELETE;

-- The same shape above the live row: this delete applies.
MERGE INTO table_event_time_stale AS t
USING (SELECT 'd' AS uuid, 300L AS ts, 'zz' AS rider, 0.0 AS fare) AS s
ON t.uuid = s.uuid
WHEN MATCHED THEN DELETE;
