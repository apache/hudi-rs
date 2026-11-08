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

-- Written by Hudi 1.1.1 on Spark 3.5. Three commits on a partitioned table
-- with the cleaner running after each: an insert, then two updates of id 1, so
-- the cleaner deletes the oldest base file of id 1's file group while id 2's
-- file group keeps its only one.

CREATE TABLE v6_clean_keep_latest_commits (
    id INT,
    name STRING,
    isActive BOOLEAN,
    longField LONG,
    byteField BYTE
)
    USING HUDI
    location '/opt/data/external_tables/v6_clean_keep_latest_commits'
TBLPROPERTIES (
    type = 'cow',
    primaryKey = 'id',
    preCombineField = 'longField',
    'hoodie.write.table.version' = '6',
    'hoodie.metadata.enable' = 'false',
    'hoodie.clean.policy' = 'KEEP_LATEST_COMMITS',
    'hoodie.clean.commits.retained' = '1'
)
PARTITIONED BY (byteField);

INSERT INTO v6_clean_keep_latest_commits VALUES
    (1, 'Alice', true, 1000, 10),
    (2, 'Bob', false, 2000, 20);

UPDATE v6_clean_keep_latest_commits SET isActive = false, longField = 1001 WHERE id = 1;

UPDATE v6_clean_keep_latest_commits SET name = 'Alicia', longField = 1002 WHERE id = 1;
