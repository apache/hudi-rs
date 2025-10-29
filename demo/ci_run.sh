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
# Enable BuildKit for faster, cache-friendly builds
export DOCKER_BUILDKIT=1
export COMPOSE_DOCKER_CLI_BUILD=1

# Ensure containers write files as the same uid/gid as the host to avoid permission issues
export HOST_UID=$(id -u)
export HOST_GID=$(id -g)

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

app_path=$1
if [ -z "$app_path" ]; then
  echo "Usage: $0 <path_to_app>"
  exit 1
fi

app_path_in_container="/opt/hudi-rs/demo/apps/$app_path"
if [ "$app_path" = "datafusion" ]; then
  docker compose exec -T runner /bin/bash -c "
    source /opt/.venv/bin/activate && \
    cd /opt/hudi-rs && make setup develop && \
    cd $app_path_in_container && \
    cargo run -- --no-build --no-tests
    "
elif [ "$app_path" = "hudi-table-api/rust" ]; then
  docker compose exec -T runner /bin/bash -c "
    source /opt/.venv/bin/activate && \
    cd /opt/hudi-rs && make setup develop && \
    cd $app_path_in_container && \
    cargo run -- --no-build --no-tests
    "
elif [ "$app_path" = "hudi-table-api/python" ]; then
  docker compose exec -T runner /bin/bash -c "
    source /opt/.venv/bin/activate && \
    cd /opt/hudi-rs && make setup develop && \
    cd $app_path_in_container && \
    python -m src.main
    "
elif [ "$app_path" = "hudi-file-group-api/cpp" ]; then
  docker compose exec -T runner /bin/bash -c "
    cd /opt/hudi-rs/cpp && cargo build --release && \
    cd $app_path_in_container && \
    mkdir build && cd build && \
    cmake .. && \
    make && \
    ./file_group_api_cpp
    "
else
  echo "Unknown app path: $app_path"
  exit 1
fi

# Always tear down the compose stack to release file locks and avoid cache save issues
docker compose down -v || echo 'Warning: Failed to tear down compose stack' >&2
