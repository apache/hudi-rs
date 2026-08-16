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

-- Source for BOTH `table_uncommitted_log_v6.zip` and `table_uncommitted_log_v9.zip`:
-- a delta commit that wrote log blocks and never completed.
--
-- Run once per table version, substituting 6 and 9 for WRITE_TABLE_VERSION and
-- naming the table table_uncommitted_log_v6 / table_uncommitted_log_v9.
-- Generated with Spark 3.5.3 and Hudi 1.2.0-SNAPSHOT.
--
-- Two steps follow the SQL and cannot be expressed in it:
--
--   1. Delete the SECOND delta commit's completed timeline file, leaving its
--      `.inflight` and `.requested` and its log file in place. That is what a
--      writer killed mid-commit leaves behind. Layout v1 (version 6) keeps the
--      completed file at `.hoodie/<instant>.deltacommit`; layout v2 (version 9)
--      at `.hoodie/timeline/<requested>_<completion>.deltacommit`.
--
--   2. Read the table with Spark afterwards and write that to `gold_data`, so
--      the gold is Hudi's own answer to the doctored layout.
--
-- Why the orphan is the middle commit and not the last: an orphan at the end
-- sorts above the latest committed instant, so the future-block gate discards it
-- and the completed/inflight gate is never reached.
--
-- Why both table versions: on version 6 every log file carries the base instant
-- in its name, so the file cannot be attributed to a delta commit and only the
-- per-block check can exclude the orphaned blocks. From version 8 each log file
-- carries its own delta commit, so the whole file is dropped when the file slice
-- is built and the per-block check is redundant, which is the condition Java
-- applies in `BaseHoodieLogRecordReader`.

CREATE TABLE table_uncommitted_log_vN (
    ts     BIGINT,
    uuid   STRING,
    rider  STRING,
    fare   DOUBLE
) USING HUDI
TBLPROPERTIES (
    type = 'mor',
    primaryKey = 'uuid',
    preCombineField = 'ts',
    'hoodie.write.table.version' = 'WRITE_TABLE_VERSION',
    'hoodie.metadata.enable' = 'false',
    'hoodie.parquet.small.file.limit' = '0',
    'hoodie.compact.inline' = 'false'
)
LOCATION 'FIXTURE_LOCATION';

INSERT INTO table_uncommitted_log_vN VALUES
    (100, 'a', 'rider-A', 10.0),
    (100, 'b', 'rider-B', 20.0),
    (100, 'c', 'rider-C', 30.0),
    (100, 'd', 'rider-D', 40.0);

-- This commit is the one whose completed timeline file is deleted afterwards.
-- Its blocks must not reach the merge, so `b` keeps the base row.
UPDATE table_uncommitted_log_vN SET fare = 99.0, rider = 'ORPHANED-B', ts = 300 WHERE uuid = 'b';

-- Committed, and at a later instant, so the orphan above sits below the latest
-- committed instant rather than above it.
UPDATE table_uncommitted_log_vN SET fare = 22.0, rider = 'committed-A', ts = 200 WHERE uuid = 'a';
