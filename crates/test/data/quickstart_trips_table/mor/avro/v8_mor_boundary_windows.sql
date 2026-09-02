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


CREATE TABLE v8_mor_boundary_windows (
    ts     BIGINT,
    uuid   STRING,
    rider  STRING,
    fare   DOUBLE
) USING HUDI
TBLPROPERTIES (
    type = 'mor',
    primaryKey = 'uuid',
    preCombineField = 'ts',
    'hoodie.write.table.version' = '8',
    'hoodie.metadata.enable' = 'false',
    'hoodie.parquet.small.file.limit' = '0',
    'hoodie.compact.inline' = 'false'
)
LOCATION 'FIXTURE_LOCATION';

INSERT INTO v8_mor_boundary_windows VALUES
    (1000, 'a', 'rider-A', 10.0),
    (1000, 'b', 'rider-B', 20.0),
    (1000, 'c', 'rider-C', 30.0),
    (1000, 'd', 'rider-D', 40.0);

UPDATE v8_mor_boundary_windows SET fare = 11.0, ts = 2000 WHERE uuid = 'a';

UPDATE v8_mor_boundary_windows SET fare = 22.0, ts = 3000 WHERE uuid = 'b';

UPDATE v8_mor_boundary_windows SET fare = 33.0, ts = 4000 WHERE uuid = 'c';
