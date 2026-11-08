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

-- Generated with Apache Hudi 0.15.0 on Spark 3.5.3 (apache/spark:3.5.3, linux/arm64).
-- Table version 6. Partition paths: ts_str=2024-03-01

CREATE TABLE v6_timebasedkeygen_hivestyle_day (
    id INT, name STRING, region STRING, amount DOUBLE, ts_str STRING
) USING HUDI
LOCATION 'file:///tmp/v6_timebasedkeygen_hivestyle_day'
TBLPROPERTIES (
    type = 'cow',
    primaryKey = 'id',
    preCombineField = 'amount',
    'hoodie.metadata.enable' = 'false',
    'hoodie.datasource.write.hive_style_partitioning' = 'true',
    'hoodie.table.keygenerator.class' = 'org.apache.hudi.keygen.TimestampBasedKeyGenerator',
    'hoodie.keygen.timebased.timestamp.type' = 'DATE_STRING',
    'hoodie.keygen.timebased.input.dateformat' = "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
    'hoodie.keygen.timebased.output.dateformat' = 'yyyy-MM-dd',
    'hoodie.keygen.timebased.timezone' = 'UTC'
)
PARTITIONED BY (ts_str);

INSERT INTO v6_timebasedkeygen_hivestyle_day VALUES
  (1,'Alice','eu',10.0,'2024-03-01T08:00:00.000Z'),
  (2,'Bob','us',20.0,'2024-03-01T18:30:00.000Z'),
  (3,'Carol','eu',30.0,'2024-03-02T09:15:00.000Z'),
  (4,'Dave','us',40.0,'2024-03-03T23:59:59.000Z'),
  (5,'Eve','eu',50.0,'2024-02-29T00:00:00.000Z');
