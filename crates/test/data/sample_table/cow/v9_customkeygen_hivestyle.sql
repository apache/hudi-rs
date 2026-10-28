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

-- Generated with Apache Hudi 1.2.0 on Spark 4.1.0 (apache/spark:4.1.0, linux/arm64) using
-- hudi-spark4.1-bundle_2.13. Spark 4.1 is the newest Spark any Hudi release targets; Spark
-- 4.2 images exist but no Hudi bundle is published for them.
--
-- Table version 9. Partition paths: region=eu/ts_str=2024-03-01
--
-- hoodie.properties records hoodie.table.partition.fields=region:SIMPLE,ts_str:TIMESTAMP, so
-- the per-field key types survive without the writer-side configs. Contrast
-- v6_customkeygen_hivestyle, where they do not.

CREATE TABLE v9_customkeygen_hivestyle (
    id INT, name STRING, amount DOUBLE, region STRING, ts_str STRING
) USING HUDI
LOCATION 'file:///tmp/v9_customkeygen_hivestyle'
TBLPROPERTIES (
    type = 'cow',
    primaryKey = 'id',
    preCombineField = 'amount',
    'hoodie.metadata.enable' = 'false',
    'hoodie.datasource.write.hive_style_partitioning' = 'true',
    'hoodie.table.keygenerator.class' = 'org.apache.hudi.keygen.CustomKeyGenerator',
    'hoodie.datasource.write.partitionpath.field' = 'region:SIMPLE,ts_str:TIMESTAMP',
    'hoodie.keygen.timebased.timestamp.type' = 'DATE_STRING',
    'hoodie.keygen.timebased.input.dateformat' = "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
    'hoodie.keygen.timebased.output.dateformat' = 'yyyy-MM-dd',
    'hoodie.keygen.timebased.timezone' = 'UTC'
)
PARTITIONED BY (region, ts_str);
INSERT INTO v9_customkeygen_hivestyle VALUES
  (1,'Alice',10.0,'eu','2024-03-01T08:00:00.000Z'),
  (2,'Bob',20.0,'us','2024-03-01T18:30:00.000Z'),
  (3,'Carol',30.0,'eu','2024-03-02T09:15:00.000Z');
