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

# Check for cached build flag (pre-compiled artifacts available in mounted volume)
USE_CACHED_BUILD=false
if [ "$2" = "--use-cached-build" ]; then
  USE_CACHED_BUILD=true
fi

# Start services with or without rebuild
if [ "$USE_CACHED_BUILD" = "true" ]; then
  # In CI, the runner image is pre-built and tagged as demo-runner:cached
  if docker images -q demo-runner:cached 2>/dev/null | grep -q .; then
    echo "Using pre-built runner image (demo-runner:cached)"
    docker tag demo-runner:cached demo-runner:latest
    docker compose up -d --no-build
  elif docker images -q demo-runner:latest 2>/dev/null | grep -q .; then
    echo "Using existing demo-runner:latest image"
    docker compose up -d --no-build
  else
    echo "WARNING: --use-cached-build specified but no pre-built image found"
    echo "Falling back to building runner image from scratch"
    docker compose up --build -d
  fi
else
  echo "Building runner image from scratch"
  docker compose up --build -d
fi

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

# Validate that Python bindings are available (either pre-built in cache or need to be built)
if [ "$USE_CACHED_BUILD" = "true" ]; then
  echo "Validating cached build has Python bindings..."
  if ! docker compose exec -T runner /bin/bash -c "source /opt/.venv/bin/activate && python -c 'import hudi' 2>/dev/null"; then
    echo "WARNING: Python bindings not found in cached build"
    echo "Build artifacts may not have been restored correctly. Rebuilding..."
    USE_CACHED_BUILD=false
  else
    echo "Cached build validation successful - Python bindings available"
  fi
fi

app_path=$1
if [ -z "$app_path" ]; then
  echo "Usage: $0 <path_to_app> [--use-cached-build]"
  exit 1
fi

app_path_in_container="/opt/hudi-rs/demo/apps/$app_path"
if [ "$app_path" = "datafusion" ]; then
  if [ "$USE_CACHED_BUILD" = "true" ]; then
    # Bindings already built in cached build artifacts (mounted volume)
    docker compose exec -T runner /bin/bash -c "
      source /opt/.venv/bin/activate && \
      cd $app_path_in_container && \
      cargo run -- --no-build --no-tests
      "
  else
    # Build bindings first
    docker compose exec -T runner /bin/bash -c "
      source /opt/.venv/bin/activate && \
      cd /opt/hudi-rs && make setup develop && \
      cd $app_path_in_container && \
      cargo run -- --no-build --no-tests
      "
  fi
elif [ "$app_path" = "hudi-table-api/rust" ]; then
  if [ "$USE_CACHED_BUILD" = "true" ]; then
    docker compose exec -T runner /bin/bash -c "
      source /opt/.venv/bin/activate && \
      cd $app_path_in_container && \
      cargo run -- --no-build --no-tests
      "
  else
    docker compose exec -T runner /bin/bash -c "
      source /opt/.venv/bin/activate && \
      cd /opt/hudi-rs && make setup develop && \
      cd $app_path_in_container && \
      cargo run -- --no-build --no-tests
      "
  fi
elif [ "$app_path" = "hudi-table-api/python" ]; then
  if [ "$USE_CACHED_BUILD" = "true" ]; then
    docker compose exec -T runner /bin/bash -c "
      source /opt/.venv/bin/activate && \
      cd $app_path_in_container && \
      python -m src.main
      "
  else
    docker compose exec -T runner /bin/bash -c "
      source /opt/.venv/bin/activate && \
      cd /opt/hudi-rs && make setup develop && \
      cd $app_path_in_container && \
      python -m src.main
      "
  fi
elif [ "$app_path" = "hudi-file-group-api/cpp" ]; then
  docker compose exec -T runner /bin/bash -c "
    cd /opt/hudi-rs/cpp && ../build-wrapper.sh cargo build --release && \
    cd $app_path_in_container && \
    mkdir -p build && cd build && \
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
