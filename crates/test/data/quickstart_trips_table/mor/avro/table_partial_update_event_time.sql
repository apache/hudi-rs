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

-- Two PARTIAL-update log blocks on one key, where the second one loses the
-- ordering. Generated with Spark 3.5.3 and Hudi 1.2.0-SNAPSHOT; gold_data is
-- `spark.read.format("hudi").load(<table>)` written beside the table directory.
--
-- MERGE INTO writes a partial block only when the table is MOR, the operation is
-- an upsert, the table version is at least 8, and the update touches a STRICT
-- SUBSET of the columns (MergeIntoHoodieTableCommand). Hence four columns and
-- two-column updates: touching all of them would silently fall back to a full
-- update and the fixture would prove nothing.
--
-- `hoodie.spark.sql.merge.into.partial.updates` defaults to true and is set here
-- so the fixture does not depend on that default holding.

CREATE TABLE table_partial_update_event_time (
    id  STRING,
    ts  BIGINT,
    a   STRING,
    b   STRING
) USING HUDI
TBLPROPERTIES (
    type = 'mor',
    primaryKey = 'id',
    preCombineField = 'ts',
    'hoodie.write.table.version' = '9',
    'hoodie.record.merge.mode' = 'EVENT_TIME_ORDERING',
    'hoodie.metadata.enable' = 'false',
    'hoodie.parquet.small.file.limit' = '0',
    'hoodie.compact.inline' = 'false'
)
LOCATION 'FIXTURE_LOCATION';

SET hoodie.merge.small.file.group.candidates.limit=0;
SET hoodie.spark.sql.merge.into.partial.updates=true;

INSERT INTO table_partial_update_event_time VALUES
    ('k1', 100, 'a-init', 'b-init'),
    ('k2', 100, 'a-init', 'b-init');

-- Partial update of (ts, a) only. k1 goes high, k2 goes low.
MERGE INTO table_partial_update_event_time t
USING (SELECT 'k1' AS id, 300L AS ts, 'a-high' AS a
       UNION ALL SELECT 'k2' AS id, 200L AS ts, 'a-low' AS a) s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET t.ts = s.ts, t.a = s.a;

-- Partial update of (ts, b) only. k1 now arrives BELOW its own previous log
-- record and must lose; k2 arrives above and must win. Each key therefore folds
-- in the opposite direction, and in both the column only the LOSING record
-- carries has to survive: k1 keeps b-low, k2 keeps a-low. A fold that keeps the
-- winner whole reads b-init / a-init from the base instead.
MERGE INTO table_partial_update_event_time t
USING (SELECT 'k1' AS id, 200L AS ts, 'b-low' AS b
       UNION ALL SELECT 'k2' AS id, 300L AS ts, 'b-high' AS b) s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET t.ts = s.ts, t.b = s.b;
