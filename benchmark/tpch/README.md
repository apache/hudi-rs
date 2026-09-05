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

`create-tables` reuses tables that already exist and takes `--recreate` to
rebuild them, so step 2 is cheap to repeat. Before publishing timings, check
that the Hudi read path returns what the parquet it was built from returns:

```bash
benchmark/tpch/run.sh validate --scale-factor 1
```

It reports per query and exits non-zero if any of them differ.

### Options

| Variable  | Values                                            | Default      |
|-----------|---------------------------------------------------|--------------|
| `ENGINE`  | `datafusion`, `spark`                             | `datafusion` |
| `SF`      | TPC-H scale factor                                | `0.001`      |
| `QUERIES` | Comma-separated query numbers                     | all 22       |
| `ENGINES` | Comma-separated engine names (for `tpch-compare`) |              |

`run.sh` takes the same options plus `--recreate`, `--runs`, `--memory-limit`
and the `--hudi-dir` / `--parquet-dir` overrides; run it with no arguments for
the full list.

### More examples

```bash
# Run only Q1, Q6, Q17
make bench-tpch QUERIES=1,6,17 SF=10

# Run against cloud-hosted data
make bench-tpch ENGINE=datafusion SF=100 HUDI_DIR=gs://bucket/sf100-hudi
make bench-tpch ENGINE=datafusion SF=100 HUDI_DIR=s3://bucket/sf100-hudi
```

`bench-datafusion` benchmarks both formats when it finds data for both, writing
one result file per format. Pass `--format hudi` to run only the Hudi leg.

`compare --engines` pairs every engine with a single format. To chart one
engine's two formats against each other, name the result files instead:

```bash
benchmark/tpch/run.sh compare --scale-factor 100 \
  --runs datafusion_hudi_sf100,datafusion_parquet_sf100
```

Keep both sides of any such comparison on the same storage. Hudi tables on
object storage against parquet on a local disk measures the storage far more
than it measures the format, so keep the parquet alongside the tables and
point `--parquet-dir` at it.

Every data directory, `--hudi-dir` and `--parquet-dir` alike, takes either a
local path or a cloud URL, on every command that reads or writes one. So the
whole run can stay in object storage:

```bash
S3=s3://bucket/sf100
benchmark/tpch/run.sh generate --scale-factor 100 --parquet-dir $S3-parquet
benchmark/tpch/run.sh create-tables --scale-factor 100 \
  --parquet-dir $S3-parquet --hudi-dir $S3-hudi
```

Pointing `--parquet-dir` at the Hudi table itself is a further option: a
parquet scan of that directory reads the same files the Hudi tables are made
of, so file sizing and sort order are held constant and only the read path
differs. That is sound only for a table with a single commit, as here, since a
plain scan has no notion of file slices and would read superseded versions of
an updated table.

Credentials come from the environment for both engines: DataFusion reads the
`AWS_*` / `GOOGLE_*` / `AZURE_*` variables, and on a cloud VM with an attached
instance role or service account neither engine needs any variable set. For S3
outside `us-east-1`, set `AWS_REGION` (`object_store` defaults to `us-east-1`
when the region is neither configured nor derivable from the URL).

The two sections below cover running on a cloud VM, one per provider; they are
alternatives, so follow whichever matches your setup.

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

## AWS EC2

### One-time setup

Launch an Amazon Linux 2023 instance with an instance profile that grants
S3 access to the benchmark bucket and includes the
`AmazonSSMManagedInstanceCore` policy (for Session Manager access). The
instance needs outbound internet access for the package, crate, PyPI and
Maven downloads.

Get the repo onto the instance first. A fresh Amazon Linux 2023 image has no
`git` (bootstrap is what installs it), so either sync from a local checkout,
which needs nothing preinstalled on the instance:

```bash
bash benchmark/tpch/infra/aws/sync.sh i-0123456789abcdef0 us-west-2 ~/.ssh/key.pem
```

or install `git` on the instance and clone there:

```bash
sudo dnf install -y git && git clone <repo-url> ~/hudi-rs
```

Then run the bootstrap script as the login user (not as user data, which would
install the toolchain into root's home). It installs Rust, Java, PySpark, and
the S3A connector, and mounts a local NVMe instance store at `/mnt/nvme` when
the instance type has one (e.g. `r8gd.4xlarge` for SF100). The packages are
architecture-neutral, so Graviton and x86 instances both work.

```bash
cd ~/hudi-rs
bash benchmark/tpch/infra/aws/bootstrap.sh

exec bash -l                       # pick up the variables it appended
echo "$SPARK_HOME" "$AWS_REGION"   # both must be non-empty
```

The new shell matters: the one that ran bootstrap does not yet have those
variables, and an unset `AWS_REGION` sends DataFusion to `us-east-1` rather
than the bucket's region.

Both engines resolve instance-profile credentials automatically: DataFusion
through `object_store`, Spark through the S3A default credential chain, so no
keys need to be configured. The region is the one thing neither infers, which
is why bootstrap persists it from the instance metadata.

### Choose where the generated data lands

`generate` writes to `benchmark/tpch/data`, so size the root volume for the
scale factor (SF100 parquet is roughly 40 GB). Instance types ending in `d`
(`r8gd`, `r7gd`, `m7gd`) carry a local NVMe instance store, which bootstrap
mounts at `/mnt/nvme`; where one is present, keeping the generated parquet on
it is faster than EBS and leaves the root volume alone:

```bash
# optional, and only before generating
mountpoint -q /mnt/nvme && ln -sfn /mnt/nvme/tpch-data benchmark/tpch/data
```

Instance types without a local disk work unchanged; everything simply stays on
the root volume, so provision it accordingly.

### Run benchmarks on the instance

Same commands as on GCP, with `s3://` data URLs. `create-tables` writes the
Hudi tables straight to the bucket, so the instance only holds the generated
parquet. Run these under `tmux`: at SF100 they take hours, and a dropped SSM
session would otherwise kill them.

```bash
S3=s3://bucket/sf100-hudi

benchmark/tpch/run.sh generate --scale-factor 100
# reuses the tables if they are already there; --recreate rebuilds them
benchmark/tpch/run.sh create-tables --scale-factor 100 --hudi-dir $S3

benchmark/tpch/run.sh bench-datafusion --scale-factor 100 --hudi-dir $S3 \
  --output-dir benchmark/tpch/results
benchmark/tpch/run.sh bench-spark --scale-factor 100 --hudi-dir $S3 \
  --output-dir benchmark/tpch/results

benchmark/tpch/run.sh compare --scale-factor 100 --engines datafusion,spark
```

`--output-dir` is what makes the `bench-*` commands persist their results, and
`compare` reads them from there, so omitting it leaves nothing to compare once
the runs have finished. To check credentials, region and the S3 path before
committing hours to a full run, benchmark a couple of queries first with
`--queries 1,6`.

Alongside the results the `bench-*` commands write an environment report, which
`compare` prints under the chart so a copied result carries the hardware, build
and storage that produced it. `env-report` rewrites it for results already
collected, for when a run predates a change to the report; re-running a
benchmark leg to refresh it would overwrite that leg's results with whatever
subset of queries the rerun covered.

Re-run `sync.sh` from your local checkout whenever local code changes; it
rebuilds the binary on the instance.

### Stop/start the instance

Stopping wipes the NVMe instance store, taking the mount, any generated data on
it, and the shuffle directory with it. Re-run the bootstrap script after
starting again to restore them, then regenerate. The package installs are
sentinel-guarded and skipped, so that is fast. Tables already written to the
bucket are unaffected, and `create-tables` reuses them.
