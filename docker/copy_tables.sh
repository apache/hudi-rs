#!/bin/bash
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

set -eux

echo ">>> Copying tables to Minio"

echo ">>> Creating Minio store"
mc config host add store http://minio:9000 $MINIO_ACCESS_KEY $MINIO_SECRET_KEY

echo ">>> Creating bucket"
mc mb store/$MINIO_TEST_BUCKET

echo ">>> Unzipping tables"
find /opt/data -name "*.zip" -exec unzip {} -d /tmp/data/ \;

echo ">>> Copying tables to Minio"
find /tmp/data -type d -mindepth 1 -maxdepth 1 -exec mc cp --recursive {} store/$MINIO_TEST_BUCKET \;

echo ">>> Listing uploaded tables"
mc ls store/$MINIO_TEST_BUCKET
