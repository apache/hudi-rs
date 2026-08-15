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

-- A merge-on-read table whose base file is written by compaction, so it carries
-- records from several commits at once. An incremental read over a window that
-- admits only some of those commits must return only their rows: admitting the
-- file must not admit every record in it.
--
-- Timeline this builds:
--   c1  insert a, b, c, d          -> all four at c1
--   c2  update a                   -> log
--   c3  update b                   -> log
--   c4  COMPACTION                 -> base file holding a@c2 b@c3 c@c1 d@c1
--   c5  update c                   -> log
--   c6  update d                   -> log
--
-- An incremental read over (c2, c3] must return b alone. Without narrowing it
-- returns all four, because the compacted base file is inside the window.

SET hoodie.compact.inline=true;
SET hoodie.compact.inline.max.delta.commits=3;

CREATE TABLE v9_mor_compacted_incremental (
    ts     BIGINT,
    uuid   STRING,
    rider  STRING,
    fare   DOUBLE
) USING HUDI
TBLPROPERTIES (
    type = 'mor',
    primaryKey = 'uuid',
    preCombineField = 'ts',
    'hoodie.metadata.enable' = 'false',
    'hoodie.parquet.small.file.limit' = '0',
    'hoodie.compact.inline' = 'true',
    'hoodie.compact.inline.max.delta.commits' = '3'
)
LOCATION '<table location>';

INSERT INTO v9_mor_compacted_incremental VALUES
    (1000, 'a', 'rider-A', 10.0),
    (1000, 'b', 'rider-B', 20.0),
    (1000, 'c', 'rider-C', 30.0),
    (1000, 'd', 'rider-D', 40.0);

UPDATE v9_mor_compacted_incremental SET fare = 11.0, ts = 2000 WHERE uuid = 'a';

UPDATE v9_mor_compacted_incremental SET fare = 22.0, ts = 3000 WHERE uuid = 'b';

-- inline compaction fires on the third delta commit, above

UPDATE v9_mor_compacted_incremental SET fare = 33.0, ts = 5000 WHERE uuid = 'c';

UPDATE v9_mor_compacted_incremental SET fare = 44.0, ts = 6000 WHERE uuid = 'd';

SELECT _hoodie_commit_time, uuid, fare FROM v9_mor_compacted_incremental ORDER BY uuid;
