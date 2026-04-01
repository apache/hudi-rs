#!/usr/bin/env bash
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
# Bootstrap a GCP Debian 12 VM with system-level dependencies for TPC-H benchmarks.
# This script is repo-agnostic and intended to run as a GCP startup script.
#
# Prerequisites:
#   - VM created with --scopes=storage-read-only (or storage-full)
#   - Service account has roles/storage.objectViewer on the project
#
set -euo pipefail

SENTINEL="/var/lib/bootstrap-done"
[[ -f "$SENTINEL" ]] && exit 0

# System packages
sudo apt-get update
sudo apt-get install -y build-essential protobuf-compiler pkg-config git curl \
  openjdk-17-jdk-headless python3-pip sysstat tmux glances

# Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
. "$HOME/.cargo/env"

# PySpark
pip3 install --break-system-packages pyspark==3.5.8

# Spark GCS connector
SPARK_HOME=$(python3 -c "import pyspark; print(pyspark.__path__[0])")
mkdir -p "$SPARK_HOME/conf" "$SPARK_HOME/jars"
curl -L -o "$SPARK_HOME/jars/gcs-connector-hadoop3-latest.jar" \
  "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar"

# Spark local dirs for shuffle and event logs.
# If a local NVMe SSD is mounted at /mnt/nvme, symlink shuffle dir there for faster I/O.
if mountpoint -q /mnt/nvme 2>/dev/null; then
  mkdir -p /mnt/nvme/spark-local
  ln -sfn /mnt/nvme/spark-local /tmp/spark-local
else
  mkdir -p /tmp/spark-local
fi
mkdir -p /tmp/spark-events

# Persist SPARK_HOME for future sessions
echo "export SPARK_HOME=$SPARK_HOME" >> ~/.bashrc

sudo touch "$SENTINEL"
echo "Bootstrap complete."
