# TPC-H Benchmark

Benchmark hudi-rs (DataFusion) against Spark+Hudi using TPC-H queries.

## Prerequisites

- Rust toolchain
- Docker (for Hudi table creation and fair engine comparison)

## Quick Start

```bash
# 1. Generate parquet data
make tpch-generate SF=1

# 2. Create Hudi COW tables from parquet (requires Docker)
make tpch-create-tables SF=1

# 3. Benchmark
make bench-tpch ENGINE=datafusion FORMAT=hudi SF=1
make bench-tpch ENGINE=spark FORMAT=hudi SF=1
make bench-tpch ENGINE=datafusion FORMAT=parquet SF=1
make bench-tpch ENGINE=spark FORMAT=parquet SF=1
```

Both engines run inside Docker (same `apache/spark:3.5.8` base image) for a fair
comparison. Set `MODE=native` to skip Docker.

### Options

| Variable | Values | Default |
|----------|--------|---------|
| `ENGINE` | `datafusion`, `spark` | `datafusion` |
| `FORMAT` | `hudi`, `parquet` | `hudi` |
| `SF` | TPC-H scale factor | `0.001` |
| `QUERIES` | Comma-separated query numbers | all 22 |
| `MODE` | `docker`, `native` | `docker` |
| `COMPARE` | `1` to persist results, or `engine1,engine2` to compare | |

### Examples

```bash
# Run only Q1, Q6, Q17
make bench-tpch QUERIES=1,6,17 SF=10

# Run natively (no Docker overhead)
make bench-tpch MODE=native SF=1

# Persist results then compare
make bench-tpch ENGINE=datafusion COMPARE=1 SF=10
make bench-tpch ENGINE=spark COMPARE=1 SF=10
make bench-tpch COMPARE=datafusion,spark SF=10
```

## Cloud Storage

`generate` and `bench` support cloud URLs (`s3://`, `gs://`, `az://`).
Credentials are read from environment variables (`AWS_*`, `GOOGLE_*`, `AZURE_*`).

```bash
cargo run -p tpch --release -- generate --scale-factor 1000 \
  --output-dir gs://my-bucket/tpch/sf1000-parquet

cargo run -p tpch --release -- bench \
  --hudi-dir gs://my-bucket/tpch/sf1000-hudi \
  --parquet-dir gs://my-bucket/tpch/sf1000-parquet
```

## Creating Hudi Tables on a Spark Cluster

For large scale factors or cloud data, use `render-ctas` to generate CTAS SQL
and submit it to any Spark 3.5 environment:

```bash
# Render SQL
cargo run -p tpch --release -- render-ctas \
  --parquet-base gs://my-bucket/tpch/sf1000-parquet \
  --hudi-base gs://my-bucket/tpch/sf1000-hudi \
  > create_hudi_tables.sql

# Submit to Spark
spark-sql \
  --jars hudi-spark3.5-bundle_2.12-1.1.1.jar \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
  --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
  -f create_hudi_tables.sql
```
