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

-- V8 MOR non-partitioned table with metadata table enabled.

CREATE TABLE v8_nonpartitioned (
    id INT,
    name STRING,
    isActive BOOLEAN,
    byteField BYTE,
    shortField SHORT,
    intField INT,
    longField LONG,
    floatField FLOAT,
    doubleField DOUBLE,
    decimalField DECIMAL(10,5),
    dateField DATE,
    timestampField TIMESTAMP,
    binaryField BINARY
) USING HUDI
TBLPROPERTIES (
    type = 'mor',
    primaryKey = 'id',
    preCombineField = 'longField',
    'hoodie.metadata.enable' = 'true',
    'hoodie.metadata.record.index.enable' = 'true',
    'hoodie.parquet.small.file.limit' = '0'
);

-- Initial insert: 5 records
INSERT INTO v8_nonpartitioned VALUES
    (1, 'Alice', true, 1, 300, 15000, 1234567890, 1.0, 3.14159, 12345.67890,
     CAST('2023-04-01' AS DATE), CAST('2023-04-01 12:01:00' AS TIMESTAMP), CAST('binary data' AS BINARY)),
    (2, 'Bob', false, 0, 100, 25000, 9876543210, 2.0, 2.71828, 67890.12345,
     CAST('2023-04-02' AS DATE), CAST('2023-04-02 13:02:00' AS TIMESTAMP), CAST('more binary data' AS BINARY)),
    (3, 'Carol', true, 1, 200, 35000, 1928374650, 3.0, 1.41421, 11111.22222,
     CAST('2023-04-03' AS DATE), CAST('2023-04-03 14:03:00' AS TIMESTAMP), CAST('even more binary data' AS BINARY)),
    (4, 'Diana', true, 1, 500, 45000, 987654321, 4.0, 2.468, 65432.12345,
     CAST('2023-04-04' AS DATE), CAST('2023-04-04 15:04:00' AS TIMESTAMP), CAST('new binary data' AS BINARY)),
    (5, 'Eve', false, 0, 150, 55000, 1122334455, 5.0, 1.732, 99999.11111,
     CAST('2023-04-05' AS DATE), CAST('2023-04-05 16:05:00' AS TIMESTAMP), CAST('eve binary data' AS BINARY));

-- Create secondary index on name field
CREATE INDEX name_idx ON v8_nonpartitioned (name);

-- Update record 1: change isActive and increment longField
UPDATE v8_nonpartitioned SET isActive = false, longField = 1234567891 WHERE id = 1;

-- Delete record 2
DELETE FROM v8_nonpartitioned WHERE id = 2;

-- Update record 3: change doubleField
UPDATE v8_nonpartitioned SET doubleField = 0.0, longField = 1928374651 WHERE id = 3;

-- Delete record 3 (after update)
DELETE FROM v8_nonpartitioned WHERE id = 3;

-- Update record 4: change name
UPDATE v8_nonpartitioned SET name = 'Diana Updated', longField = 987654322 WHERE id = 4;
