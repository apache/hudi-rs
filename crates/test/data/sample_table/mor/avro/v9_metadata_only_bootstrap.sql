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

-- METADATA_ONLY bootstrap writes skeleton base files (meta fields only, at
-- instant 00000000000001) and leaves the data columns in the source dataset.
-- The fixture ships neither that dataset nor the bootstrap index under
-- .hoodie/.aux/.bootstrap, which names the source files by absolute path, and
-- hoodie.bootstrap.base.path in hoodie.properties was rewritten to the
-- placeholder source path below.
SET hoodie.metadata.enable = false;

CREATE TABLE bootstrap_source (id STRING, v BIGINT, region STRING)
USING PARQUET PARTITIONED BY (region) LOCATION '/opt/data/external_tables/v9_metadata_only_bootstrap_source';
INSERT INTO bootstrap_source VALUES ('a', 1, 'us'), ('b', 2, 'eu');

CALL run_bootstrap(
    table => 'v9_metadata_only_bootstrap',
    table_type => 'MERGE_ON_READ',
    bootstrap_path => '/opt/data/external_tables/v9_metadata_only_bootstrap_source',
    base_path => '/opt/data/external_tables/v9_metadata_only_bootstrap',
    rowKey_field => 'id',
    partition_path_field => 'region',
    selector_class => 'org.apache.hudi.client.bootstrap.selector.MetadataOnlyBootstrapModeSelector',
    bootstrap_overwrite => true,
    options => 'hoodie.datasource.write.hive_style_partitioning=true'
);

CREATE TABLE v9_metadata_only_bootstrap USING HUDI LOCATION '/opt/data/external_tables/v9_metadata_only_bootstrap';

-- Rewrites the region=us file group (a new base file on COW, a log file over
-- the skeleton on MOR); region=eu keeps its skeleton base file.
UPDATE v9_metadata_only_bootstrap SET v = 10 WHERE id = 'a';
