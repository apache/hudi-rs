#!/usr/bin/env bash
#
# Bootstrap a GCP Debian 12 VM for TPC-H benchmarks.
#
# Prerequisites:
#   - VM created with --scopes=storage-read-only (or storage-full)
#   - Service account has roles/storage.objectViewer on the project
#
# Usage:
#   gcloud compute scp bootstrap.sh <vm>:~ --zone=<zone>
#   gcloud compute ssh <vm> --zone=<zone> -- bash bootstrap.sh
#
set -euo pipefail

# System deps + Rust + Docker + Java + PySpark
sudo apt-get update
sudo apt-get install -y build-essential protobuf-compiler pkg-config git curl \
  openjdk-17-jdk-headless python3-pip
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
. "$HOME/.cargo/env"
pip3 install --break-system-packages pyspark==3.5.8

# Spark GCS connector
SPARK_HOME=$(python3 -c "import pyspark; print(pyspark.__path__[0])")
mkdir -p "$SPARK_HOME/conf" "$SPARK_HOME/jars"
curl -L -o "$SPARK_HOME/jars/gcs-connector-hadoop3-latest.jar" \
  "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar"

# Clone and build
cd ~
git clone https://github.com/apache/hudi-rs.git
cd hudi-rs
git fetch origin pull/548/head:df-optimize
git checkout df-optimize
cargo build -p tpch --release

# Persist SPARK_HOME for future sessions
echo "export SPARK_HOME=$SPARK_HOME" >> ~/.bashrc

echo ""
echo "Done. Run: cd ~/hudi-rs"
