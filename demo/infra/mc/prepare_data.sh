#!/bin/sh
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

# create a bucket named `hudi-demo`
mc mb local/hudi-demo

# unzip the data
mkdir -p /tmp/tables/cow/
for zip in /opt/data/tables/cow/*.zip; do unzip -o "$zip" -d "/tmp/tables/cow/"; done
mkdir -p /tmp/tables/mor/
for zip in /opt/data/tables/mor/*.zip; do unzip -o "$zip" -d "/tmp/tables/mor/"; done

# copy the data to the bucket
mc cp -r /tmp/tables/cow/* local/hudi-demo/cow/
mc cp -r /tmp/tables/mor/* local/hudi-demo/mor/
