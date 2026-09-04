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
# Bootstrap an AWS EC2 Amazon Linux 2023 instance with system-level
# dependencies for TPC-H benchmarks. This script is repo-agnostic.
#
# Run it as the login user, not as EC2 user data: it installs into $HOME
# (rustup, pip --user, SPARK_HOME in .bashrc) and takes ownership of the
# instance-store mount, so a root-owned install from user data would leave the
# login user without a toolchain.
#
# Re-run it after a stop/start: the instance store is wiped, and the mount
# section below is deliberately outside the sentinel guard so it is restored.
#
# Prerequisites:
#   - Instance profile granting S3 access to the benchmark bucket
#     (object_store and the S3A connector both resolve instance credentials
#     automatically; no keys need to be configured)
#   - Outbound internet access, for the package, crate, PyPI and Maven
#     downloads here and for the Hudi Spark bundle that run.sh fetches
#
set -euo pipefail

if [[ $EUID -eq 0 ]]; then
  echo "Error: run this as the login user (e.g. ec2-user), not as root." >&2
  exit 1
fi

# Spark local dirs for shuffle and event logs. This section runs on every
# invocation (not sentinel-guarded): the instance store is wiped on every
# stop/start, so re-running the script restores the mount.
# If the instance has a local NVMe instance store, format and mount it at
# /mnt/nvme and put the shuffle dir there for faster I/O.
NVME_DEV=$(lsblk -dno NAME,MODEL | awk '/Instance Storage/ {print "/dev/" $1; exit}')
if [[ -n "${NVME_DEV:-}" ]]; then
  if ! sudo blkid "$NVME_DEV" >/dev/null 2>&1; then
    sudo mkfs -t xfs -q "$NVME_DEV"
  fi
  sudo mkdir -p /mnt/nvme
  mountpoint -q /mnt/nvme || sudo mount "$NVME_DEV" /mnt/nvme
  sudo chown "$(id -un):$(id -gn)" /mnt/nvme
  mkdir -p /mnt/nvme/spark-local
  ln -sfn /mnt/nvme/spark-local /tmp/spark-local
else
  # No instance store on this type; shuffle stays on the root volume. Clear any
  # symlink left by a run on an instance that had one, which would otherwise
  # point at an empty mount point.
  [[ -L /tmp/spark-local ]] && rm -f /tmp/spark-local
  mkdir -p /tmp/spark-local
fi
mkdir -p /tmp/spark-events

SENTINEL="/var/lib/bootstrap-done"
[[ -f "$SENTINEL" ]] && exit 0

# System packages
# clang-devel supplies the libclang that bindgen loads: hudi enables
# spill-rocksdb by default, and rocksdb generates its bindings at build time.
sudo dnf install -y gcc gcc-c++ clang-devel make git pkgconfig openssl-devel \
  protobuf-compiler protobuf-devel java-17-amazon-corretto-headless \
  python3-pip rsync tmux sysstat

# Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
. "$HOME/.cargo/env"

# arrow/DataFusion kernels rely on LLVM auto-vectorization, and rustc targets a
# conservative baseline for each architecture (SSE2 on x86-64, plain NEON on
# aarch64); target the local CPU so the wider vector units are actually used.
mkdir -p "$HOME/.cargo"
cat > "$HOME/.cargo/config.toml" <<'EOF'
[build]
rustflags = ["-C", "target-cpu=native"]
EOF

# PySpark
pip3 install --user pyspark==3.5.8

# S3A connector jars matching the Hadoop 3.3.4 client PySpark 3.5.x bundles
SPARK_HOME=$(python3 -c "import pyspark; print(pyspark.__path__[0])")
mkdir -p "$SPARK_HOME/conf" "$SPARK_HOME/jars"
curl -fL -o "$SPARK_HOME/jars/hadoop-aws-3.3.4.jar" \
  "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
curl -fL -o "$SPARK_HOME/jars/aws-java-sdk-bundle-1.12.262.jar" \
  "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"

# Persist SPARK_HOME for future sessions, plus the instance's region:
# object_store takes no region from the instance metadata and falls back to
# us-east-1, which misroutes reads of a bucket in any other region.
AWS_REGION=$(curl -fsS -H "X-aws-ec2-metadata-token: $(curl -fsS -X PUT \
  -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' \
  http://169.254.169.254/latest/api/token)" \
  http://169.254.169.254/latest/meta-data/placement/region)
{
  echo "export SPARK_HOME=$SPARK_HOME"
  echo "export AWS_REGION=$AWS_REGION"
} >> ~/.bashrc

sudo touch "$SENTINEL"
echo "Bootstrap complete."
