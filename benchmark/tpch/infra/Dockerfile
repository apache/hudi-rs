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

# ---------- Stage 1: build the tpch binary ----------
FROM rust:1.88-bookworm AS builder

WORKDIR /build

# Copy the full workspace (Cargo.toml, crates, benchmark, etc.)
COPY Cargo.toml Cargo.lock ./
COPY crates crates
COPY benchmark/tpch benchmark/tpch
# Stub out workspace members that aren't needed for the tpch build
RUN mkdir -p cpp/src && echo "fn main() {}" > cpp/src/main.rs \
 && printf '[package]\nname = "hudi-cpp"\nversion = "0.0.0"\nedition = "2024"\n' > cpp/Cargo.toml \
 && mkdir -p python/src && touch python/src/lib.rs \
 && printf '[package]\nname = "hudi-python"\nversion = "0.0.0"\nedition = "2024"\n' > python/Cargo.toml

RUN cargo build -p tpch --release \
 && strip target/release/tpch

# ---------- Stage 2: runtime with Spark + Hudi + tpch ----------
FROM apache/spark:3.5.8

USER root

# Download Hudi Spark bundle jar
RUN cd /opt/spark/jars && \
  wget -q \
    https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.5-bundle_2.12/1.1.1/hudi-spark3.5-bundle_2.12-1.1.1.jar

# Bake in Spark and Hudi defaults, and logging config
COPY benchmark/tpch/infra/spark/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY benchmark/tpch/infra/spark/log4j2.properties /opt/spark/conf/log4j2.properties
COPY benchmark/tpch/infra/spark/hudi-defaults.conf /etc/hudi/conf/hudi-defaults.conf

# Install tpch binary
COPY --from=builder /build/target/release/tpch /usr/local/bin/tpch

USER spark

WORKDIR /opt/spark/work-dir
