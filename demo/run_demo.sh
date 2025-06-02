#!/bin/bash
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

docker compose up --build -d

max_attempts=30
attempt=0

until [ "$(docker inspect -f '{{.State.Status}}' runner)" = "running" ] || [ $attempt -eq $max_attempts ]; do
  attempt=$(( $attempt + 1 ))
  echo "Waiting for container... (attempt $attempt of $max_attempts)"
  sleep 1
done

if [ $attempt -eq $max_attempts ]; then
  echo "Container failed to become ready in time"
  exit 1
fi

# Run the C++ demo app
docker compose exec -T runner /bin/bash -c "
  cd /opt/hudi-rs/cpp && \
  cargo build --release && \
  cd /opt/hudi-rs/demo/file-group-api/cpp && \
  mkdir build && cd build && \
  cmake .. && \
  make && \
  ./file_group_api_cpp
  "

# Run the Rust and Python demo apps
# Note: no need to activate venv since this is already in a container
docker compose exec -T runner /bin/bash -c "
  cd /opt/hudi-rs && \
  make setup develop && \
  cd /opt/hudi-rs/demo/sql-datafusion && ./run.sh &&\
  cd /opt/hudi-rs/demo/table-api-python && ./run.sh && \
  cd /opt/hudi-rs/demo/table-api-rust && ./run.sh
  "
