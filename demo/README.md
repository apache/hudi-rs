<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# Demo

This directory contains demo applications showcasing Hudi-rs APIs across different programming languages. The demos run on Docker Compose and also serve as integration tests in the CI pipeline (see [`.github/workflows/ci.yml`](../.github/workflows/ci.yml)).

## Infrastructure

The demo infrastructure is defined in [`compose.yaml`](compose.yaml).

Test data copied from the [`crates/test`](../crates/test/data) are placed in Minio, providing COW and MOR tables at:
- `s3://hudi-demo/cow/`
- `s3://hudi-demo/mor/`

## Demo Apps

### [`datafusion`](apps/datafusion)
**Rust + SQL**: Demonstrates querying Hudi tables using Apache DataFusion with SQL syntax.

### [`hudi-table-api/rust`](apps/hudi-table-api/rust)
**Rust**: Shows native Rust API for reading Hudi table snapshots.

### [`hudi-table-api/python`](apps/hudi-table-api/python)
**Python**: Python bindings for Hudi tables with PyArrow integration.

### [`hudi-file-group-api/cpp`](apps/hudi-file-group-api/cpp)
**C++**: File group reading using C++ bindings and Arrow C ABI.
