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

## Local

### Prerequisites

- Rust toolchain
- Spark (for Hudi table creation and Spark SQL benchmarks)

### Run

```bash
# 1. Generate parquet data
make tpch-generate SF=1

# 2. Create Hudi COW tables from parquet
make tpch-create-tables SF=1

# 3. Run benchmarks
make bench-tpch ENGINE=datafusion SF=1
make bench-tpch ENGINE=spark SF=1

# 4. Compare results
make tpch-compare ENGINES=datafusion,spark SF=1
```

### Options

| Variable  | Values                                            | Default      |
|-----------|---------------------------------------------------|--------------|
| `ENGINE`  | `datafusion`, `spark`                             | `datafusion` |
| `SF`      | TPC-H scale factor                                | `0.001`      |
| `QUERIES` | Comma-separated query numbers                     | all 22       |
| `ENGINES` | Comma-separated engine names (for `tpch-compare`) |              |

### More examples

```bash
# Run only Q1, Q6, Q17
make bench-tpch QUERIES=1,6,17 SF=10

# Run against cloud-hosted data
make bench-tpch ENGINE=datafusion SF=100 HUDI_DIR=gs://bucket/sf100-hudi
```

## GCP VM

### One-time setup

Create a VM with the bootstrap script. It installs Rust, Java, PySpark,
and the GCS connector on first boot.

```bash
gcloud compute instances create bench-vm \
  --zone=us-central1-a \
  --machine-type=n2-standard-8 \
  --image-family=debian-12 \
  --image-project=debian-cloud \
  --scopes=storage-read-only \
  --metadata-from-file=startup-script=benchmark/tpch/infra/gcp/bootstrap.sh
```

### Sync code and build

From local, sync the repo and build the benchmark binary on the VM.
Re-run this whenever local code changes.

```bash
bash benchmark/tpch/infra/gcp/sync.sh bench-vm us-central1-a
```

### Run benchmarks on the VM

```bash
gcloud compute ssh bench-vm --zone=us-central1-a

# On the VM:
cd ~/hudi-rs

# Run against cloud-hosted data
benchmark/tpch/run.sh bench-datafusion --scale-factor 100 --hudi-dir gs://bucket/sf100-hudi
benchmark/tpch/run.sh bench-spark --scale-factor 100 --hudi-dir gs://bucket/sf100-hudi

# Or generate data locally on the VM and run
benchmark/tpch/run.sh generate --scale-factor 10
benchmark/tpch/run.sh create-tables --scale-factor 10
benchmark/tpch/run.sh bench-datafusion --scale-factor 10
benchmark/tpch/run.sh bench-spark --scale-factor 10

# Compare results
benchmark/tpch/run.sh compare --scale-factor 10 --engines datafusion,spark
```

### Stop/start the VM

```bash
gcloud compute instances stop bench-vm --zone=us-central1-a
gcloud compute instances start bench-vm --zone=us-central1-a
```

The bootstrap script only runs once (guarded by a sentinel file),
so restarting the VM is fast.
