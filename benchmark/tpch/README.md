# TPC-H Benchmark

Generate TPC-H data, create Hudi COW tables, and run benchmarks.

## Prerequisites

- Rust toolchain
- Docker (for local Hudi table creation via Spark 3.5.8 + Hudi 1.1.1)

## Quick Start (Local)

From the repository root:

```bash
# Generate parquet tables at scale factor 0.001
make tpch-generate SF=0.001

# Create Hudi COW tables from the parquet data (requires Docker)
make tpch-create-tables SF=0.001

# Run benchmark against parquet tables
make tpch-bench-parquet SF=0.001

# Run benchmark against Hudi tables
make tpch-bench-hudi SF=0.001

# Run benchmark comparing Hudi vs Parquet (DataFusion)
make tpch-bench SF=0.001

# Run benchmark against Hudi tables using Spark SQL (requires Docker)
make tpch-bench-spark SF=0.001

# Clean up generated data
make tpch-clean
```

The `create-tables` command auto-detects host memory and allocates 75% to Spark.
For large scale factors (e.g., SF100), ensure the machine has sufficient memory
and disk.

## Using the Binary Directly

```bash
# Generate parquet tables
cargo run -p tpch --release -- generate --scale-factor 0.001 --output-dir benchmark/tpch/data/sf0.001-parquet

# Benchmark parquet
cargo run -p tpch --release -- bench --parquet-dir benchmark/tpch/data/sf0.001-parquet

# Benchmark specific queries with more iterations
cargo run -p tpch --release -- bench --parquet-dir benchmark/tpch/data/sf0.001-parquet --queries 1,3,6 --iterations 5
```

## Cloud Storage

Both `generate` and `bench` support cloud URLs directly. Cloud credentials are
picked up from environment variables (`AWS_*`, `GOOGLE_*`, `AZURE_*`).

```bash
# Generate parquet directly to cloud storage (streams, no local disk needed)
cargo run -p tpch --release -- generate --scale-factor 1000 --output-dir gs://my-bucket/tpch/sf1000-parquet

# Benchmark from cloud
cargo run -p tpch --release -- bench --parquet-dir gs://my-bucket/tpch/sf1000-parquet
```

## Creating Hudi Tables on a Spark Cluster

For large scale factors where a single Docker container is impractical, or when
data lives in cloud storage, use `render-sql` to generate the CTAS SQL and submit
it to any Spark 3.5 environment.

### Step 1: Generate parquet data

```bash
cargo run -p tpch --release -- generate \
  --scale-factor 1000 \
  --output-dir gs://my-bucket/tpch/sf1000-parquet
```

### Step 2: Render the CTAS SQL

```bash
benchmark/tpch/run.sh render-sql \
  --parquet-base gs://my-bucket/tpch/sf1000-parquet \
  --hudi-base gs://my-bucket/tpch/sf1000-hudi \
  > create_hudi_tables.sql
```

### Step 3: Submit to Spark

Run `spark-sql` with the following configuration:

```bash
spark-sql \
  --jars /path/to/hudi-spark3.5-bundle_2.12-1.1.1.jar \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
  --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
  -f create_hudi_tables.sql
```

The Hudi bundle jar is available from Maven Central:

```text
https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.5-bundle_2.12/1.1.1/hudi-spark3.5-bundle_2.12-1.1.1.jar
```

This works with any Spark deployment: managed services (Dataproc, EMR, HDInsight),
Kubernetes, or standalone clusters.

### Step 4: Benchmark

```bash
cargo run -p tpch --release -- bench \
  --hudi-dir gs://my-bucket/tpch/sf1000-hudi \
  --parquet-dir gs://my-bucket/tpch/sf1000-parquet
```
