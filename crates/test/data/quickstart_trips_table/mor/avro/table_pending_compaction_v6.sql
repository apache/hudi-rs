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

-- Source for `table_pending_compaction_v6.zip`: delta commits written while a
-- compaction is scheduled and not yet executed, on table version 6.
-- Generated with Spark 3.5.5 and Hudi 1.1.1, with FIXTURE_LOCATION replaced.
--
-- `hoodie.compact.schedule.inline` with one delta commit per compaction makes
-- the second write schedule a compaction and leave it pending, since
-- `hoodie.compact.inline` is off. Version 6 names a log file on the base
-- instant of the file slice it appends to, and while the compaction is pending
-- that is the compaction's instant, so the last two updates land in log files
-- named on an instant that has not completed.
--
-- One step follows the SQL: read the table with Spark and write that to
-- `gold_data`.

CREATE TABLE table_pending_compaction_v6 (
    ts     BIGINT,
    uuid   STRING,
    rider  STRING,
    fare   DOUBLE
) USING HUDI
TBLPROPERTIES (
    type = 'mor',
    primaryKey = 'uuid',
    preCombineField = 'ts',
    'hoodie.write.table.version' = '6',
    'hoodie.metadata.enable' = 'true',
    'hoodie.parquet.small.file.limit' = '0',
    'hoodie.index.bloom.num_entries' = '100',
    'hoodie.compact.inline' = 'false',
    'hoodie.compact.schedule.inline' = 'true',
    'hoodie.compact.inline.max.delta.commits' = '1',
    'hoodie.clean.automatic' = 'false'
)
LOCATION 'FIXTURE_LOCATION';

INSERT INTO table_pending_compaction_v6 VALUES
    (100, 'a', 'rider-A', 10.0),
    (100, 'b', 'rider-B', 20.0),
    (100, 'c', 'rider-C', 30.0),
    (100, 'd', 'rider-D', 40.0);

-- A log file on the base file's instant; schedules the compaction.
UPDATE table_pending_compaction_v6 SET fare = 11.0, ts = 200 WHERE uuid = 'a';

-- Log files on the pending compaction's instant.
UPDATE table_pending_compaction_v6 SET fare = 22.0, ts = 300 WHERE uuid = 'b';
UPDATE table_pending_compaction_v6 SET fare = 33.0, ts = 400 WHERE uuid = 'c';
