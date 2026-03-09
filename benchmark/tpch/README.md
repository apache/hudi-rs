<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# TPC-H Benchmark

## Prerequisites

- Rust toolchain
- Docker (for Hudi table creation)

## Quick Start

```bash
# 1. Generate parquet data
make tpch-generate SF=1

# 2. Create Hudi COW tables from parquet (requires Docker)
make tpch-create-tables SF=1

# 3. Benchmark (results are automatically persisted)
make bench-tpch ENGINE=datafusion SF=1
make bench-tpch ENGINE=spark SF=1

# 4. Compare results
make tpch-compare ENGINES=datafusion,spark SF=1
```

## Options

| Variable  | Values                                                  | Default      |
|-----------|---------------------------------------------------------|--------------|
| `ENGINE`  | `datafusion`, `spark`                                   | `datafusion` |
| `SF`      | TPC-H scale factor                                      | `0.001`      |
| `QUERIES` | Comma-separated query numbers                           | all 22       |
| `MODE`    | `native`, `docker`                                      | `native`     |
| `ENGINES` | Comma-separated engine names (for `tpch-compare`)       |              |

## Examples

```bash
# Run only Q1, Q6, Q17
make bench-tpch QUERIES=1,6,17 SF=10

# Run inside Docker (same apache/spark:3.5.8 base image for fair comparison)
make bench-tpch ENGINE=datafusion SF=1 MODE=docker
make bench-tpch ENGINE=spark SF=1 MODE=docker
```
