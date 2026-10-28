#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
set -euo pipefail

if [[ ${1:-} == --help || $# -ne 1 ]]; then
  cat <<'HELP'
Usage: bash generate_lookup_fuzz_data.sh /absolute/local/output

Requires Spark 3.5 / Scala 2.12, its compatible Java installation, and Python 3.
Set SPARK_HOME or put spark-submit on PATH. Maven downloads the Hudi bundle
unless HUDI_BUNDLE_JAR points to an existing local bundle.
The output directory must not exist; existing data is never overwritten.

Environment settings (defaults):
  ROWS=1000000             Initial keys per table
  ROUNDS=6                Mutation rounds, including new keys each round
  INSERTS_PER_ROUND=10000  New keys per round
  UPDATE_PERCENT=20       Existing keys updated per round (hash-selected)
  DELETE_PERCENT=5        Existing keys deleted per round (disjoint from updates)
  SEED=42                 Reproducible workload seed
  TABLE_TYPES=cow,mor     cow, mor, or cow,mor
  DATA_PARTITIONS=16      Hudi partition paths
  RLI_SHARDS=32           Fixed global record-index shard count
  TASKS=16               Spark shuffle/output partitions
  PAYLOAD_BYTES=128       Approximate payload size per record
  QUERIES=100000          Lookup request count, with duplicate and missing keys
  BATCH_SIZE=256          Batch IDs in the lookup request files
  COMPACT_EVERY=2         MOR inline compaction interval; 0 disables
  CLUSTER_EVERY=0         Inline clustering interval; 0 disables
  MASTER=local[*]         Must be local for local filesystem output
  DRIVER_MEMORY=4g        Spark driver memory

Example:
  ROWS=10000000 ROUNDS=12 RLI_SHARDS=64 CLUSTER_EVERY=3 \
    bash generate_lookup_fuzz_data.sh /tmp/hudi-lookup-fuzz-seed42

Output:
  cow/, mor/              Actual Hudi tables, including .hoodie/metadata
  events/                Independent input history (Parquet, including tombstones)
  expected/              Final live records derived from input history (Parquet)
  queries/               JSON Lines parts: ordinal, batch_id, key, expected_present
  manifest.json          Parameters, paths, counts and captured completed timelines
  generator.py           Exact generator used for this run

Each query can be used alone or grouped by batch_id. Results are unordered;
deduplicate requested keys before comparing against expected/ by record_key.
Generation is offline and runs no Rust tests. Inline services exercise rewritten
files; this is not a concurrent-writer/inflight-commit fault injector. Positions
in the RLI depend on the writer; this script does not fabricate row positions.
HELP
  [[ ${1:-} == --help ]] && exit 0
  exit 2
fi

SPARK_SUBMIT=${SPARK_HOME:+$SPARK_HOME/bin/}spark-submit
command -v "$SPARK_SUBMIT" >/dev/null || { echo 'spark-submit not found' >&2; exit 1; }
OUTPUT=$1
[[ $OUTPUT == /* ]] || { echo 'Output must be an absolute local path' >&2; exit 2; }
[[ ! -e $OUTPUT ]] || { echo 'Output already exists; choose a new directory' >&2; exit 2; }
MASTER=${MASTER:-local[*]}
[[ $MASTER == local || $MASTER == local\[*\] ]] || { echo 'MASTER must be local' >&2; exit 2; }
export ROWS=${ROWS:-1000000} ROUNDS=${ROUNDS:-6} INSERTS_PER_ROUND=${INSERTS_PER_ROUND:-10000}
export UPDATE_PERCENT=${UPDATE_PERCENT:-20} DELETE_PERCENT=${DELETE_PERCENT:-5} SEED=${SEED:-42}
export TABLE_TYPES=${TABLE_TYPES:-cow,mor} DATA_PARTITIONS=${DATA_PARTITIONS:-16}
export RLI_SHARDS=${RLI_SHARDS:-32} TASKS=${TASKS:-16} PAYLOAD_BYTES=${PAYLOAD_BYTES:-128}
export QUERIES=${QUERIES:-100000} BATCH_SIZE=${BATCH_SIZE:-256}
export COMPACT_EVERY=${COMPACT_EVERY:-2} CLUSTER_EVERY=${CLUSTER_EVERY:-0}
bundle_args=(--packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.1.1)
if [[ -n ${HUDI_BUNDLE_JAR:-} ]]; then
  [[ -f $HUDI_BUNDLE_JAR ]] || { echo 'HUDI_BUNDLE_JAR does not exist' >&2; exit 2; }
  bundle_args=(--jars "$HUDI_BUNDLE_JAR")
fi
mkdir -p "$(dirname "$OUTPUT")"
mkdir "$OUTPUT"
cat > "$OUTPUT/generator.py" <<'PYTHON'
import json
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession, Window, functions as F

root = Path(sys.argv[1]).resolve()
names = ("ROWS ROUNDS INSERTS_PER_ROUND UPDATE_PERCENT DELETE_PERCENT SEED "
         "DATA_PARTITIONS RLI_SHARDS TASKS PAYLOAD_BYTES QUERIES BATCH_SIZE "
         "COMPACT_EVERY CLUSTER_EVERY").split()
cfg = {name: int(os.environ[name]) for name in names}
for name, value in cfg.items():
    if value < 0 or (name in {"ROWS", "DATA_PARTITIONS", "RLI_SHARDS", "TASKS", "BATCH_SIZE"} and value == 0):
        raise ValueError(f"Invalid {name}={value}")
if cfg["UPDATE_PERCENT"] + cfg["DELETE_PERCENT"] > 100:
    raise ValueError("UPDATE_PERCENT + DELETE_PERCENT must be <= 100")
types = os.environ["TABLE_TYPES"].split(",")
if not types or len(set(types)) != len(types) or any(t not in {"cow", "mor"} for t in types):
    raise ValueError("TABLE_TYPES must be cow, mor, or cow,mor")
spark = SparkSession.builder.appName("hudi-lookup-fuzz-data").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", cfg["TASKS"])
spark.conf.set("spark.sql.session.timeZone", "UTC")


def uri(path):
    return path.as_uri()


def key(column):
    # Include multibyte UTF-16 keys to exercise Java-compatible RLI hashing.
    return F.concat(F.when(F.pmod(column, F.lit(7)) == 0, F.lit("客户/😀/"))
                    .otherwise(F.lit("key/")), column.cast("string"))


def records(ids, version, deleted):
    return ids.select(
        "id", key(F.col("id")).alias("record_key"),
        F.concat(F.lit("p"), F.pmod(F.col("id"), F.lit(cfg["DATA_PARTITIONS"]))).alias("partition_path"),
        F.lit(version).cast("long").alias("version"),
        F.xxhash64("id", F.lit(version), F.lit(cfg["SEED"])).alias("value"),
        F.substring(F.repeat(F.sha2(F.concat_ws(":", F.col("id"), F.lit(version)), 256),
                             (cfg["PAYLOAD_BYTES"] + 63) // 64),
                    1, cfg["PAYLOAD_BYTES"]).alias("payload"),
        deleted.alias("_hoodie_is_deleted"),
    )


def ids(start, end):
    return spark.range(start, end, numPartitions=cfg["TASKS"])


def write_table(frame, table_type, first):
    options = {
        "hoodie.table.name": f"lookup_fuzz_{table_type}",
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE" if table_type == "cow" else "MERGE_ON_READ",
        "hoodie.datasource.write.operation": "bulk_insert" if first else "upsert",
        "hoodie.datasource.write.recordkey.field": "record_key",
        "hoodie.datasource.write.partitionpath.field": "partition_path",
        "hoodie.datasource.write.precombine.field": "version",
        "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.SimpleKeyGenerator",
        "hoodie.metadata.enable": "true",
        "hoodie.metadata.record.index.enable": "true",
        "hoodie.metadata.record.index.min.filegroup.count": str(cfg["RLI_SHARDS"]),
        "hoodie.metadata.record.index.max.filegroup.count": str(cfg["RLI_SHARDS"]),
        "hoodie.index.type": "RECORD_INDEX",
        "hoodie.populate.meta.fields": "true",
        "hoodie.parquet.max.file.size": str(64 * 1024 * 1024),
        "hoodie.clean.automatic": "false",
        "hoodie.compact.inline": str(table_type == "mor" and cfg["COMPACT_EVERY"] > 0).lower(),
        "hoodie.compact.inline.max.delta.commits": str(max(1, cfg["COMPACT_EVERY"])),
        "hoodie.clustering.inline": str(cfg["CLUSTER_EVERY"] > 0).lower(),
        "hoodie.clustering.inline.max.commits": str(max(1, cfg["CLUSTER_EVERY"])),
        "hoodie.clustering.async.enabled": "false",
        "hoodie.bulkinsert.shuffle.parallelism": str(cfg["TASKS"]),
        "hoodie.upsert.shuffle.parallelism": str(cfg["TASKS"]),
        "hoodie.insert.shuffle.parallelism": str(cfg["TASKS"]),
    }
    frame.write.format("hudi").options(**options).mode("append").save(uri(root / table_type))


try:
    total = cfg["ROWS"]
    for version in range(cfg["ROUNDS"] + 1):
        if version == 0:
            changes = records(ids(0, total), version, F.lit(False))
        else:
            bucket = F.pmod(F.xxhash64("id", F.lit(version), F.lit(cfg["SEED"])), F.lit(100))
            existing = ids(0, total).withColumn("bucket", bucket)
            existing = existing.filter(F.col("bucket") < cfg["UPDATE_PERCENT"] + cfg["DELETE_PERCENT"])
            changes = records(existing, version, F.col("bucket") >= cfg["UPDATE_PERCENT"])
            changes = changes.unionByName(records(ids(total, total + cfg["INSERTS_PER_ROUND"]), version, F.lit(False)))
            total += cfg["INSERTS_PER_ROUND"]
        # Materialize input once on disk, shared by the independent oracle and writers.
        event_path = root / "events" / f"round-{version:06d}"
        changes.write.mode("errorifexists").parquet(uri(event_path))
        changes = spark.read.parquet(uri(event_path))
        for table_type in types:
            write_table(changes, table_type, version == 0)
        print(f"Committed round {version}/{cfg['ROUNDS']}; key universe={total}", flush=True)

    history = spark.read.parquet(*[uri(p) for p in sorted((root / "events").iterdir()) if p.is_dir()])
    latest = history.withColumn("rank", F.row_number().over(Window.partitionBy("record_key").orderBy(F.desc("version"))))
    live = latest.filter((F.col("rank") == 1) & ~F.col("_hoodie_is_deleted")).drop("rank", "_hoodie_is_deleted")
    live.write.mode("errorifexists").parquet(uri(root / "expected"))
    expected = spark.read.parquet(uri(root / "expected"))
    # A hot key gives duplicates; the expanded key universe provides guaranteed misses.
    requests = ids(0, cfg["QUERIES"]).withColumnRenamed("id", "ordinal")
    candidate = F.when(F.pmod(F.col("ordinal"), F.lit(10)) == 0, F.lit(0)).otherwise(
        F.pmod(F.xxhash64("ordinal", F.lit(cfg["SEED"])), F.lit(total + max(1, total // 10))))
    requests = requests.select("ordinal", (F.col("ordinal") / cfg["BATCH_SIZE"]).cast("long").alias("batch_id"), key(candidate).alias("key"))
    presence = expected.select(F.col("record_key").alias("key"), F.lit(True).alias("expected_present"))
    requests.join(presence, "key", "left").fillna(False, ["expected_present"]).write.mode("errorifexists").json(uri(root / "queries"))
    timelines = {}
    for table_type in types:
        metadata = root / table_type / ".hoodie"
        if not (metadata / "metadata" / "record_index").is_dir():
            raise RuntimeError(f"RLI was not generated for {table_type}")
        timelines[table_type] = sorted(str(p.relative_to(metadata)) for p in metadata.rglob("*")
                                       if p.is_file() and p.suffix in {".commit", ".deltacommit", ".replacecommit"}
                                       and "metadata" not in p.relative_to(metadata).parts)
    manifest = {"parameters": cfg, "spark_version": spark.version, "table_types": types,
                "tables": {t: uri(root / t) for t in types}, "expected": uri(root / "expected"),
                "queries": uri(root / "queries"), "live_records": expected.count(),
                "key_universe": total, "completed_timeline_files": timelines,
                "oracle": "Latest input event per key, excluding tombstones; independent of Hudi reads"}
    (root / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"Dataset ready: {root / 'manifest.json'}", flush=True)
finally:
    spark.stop()
PYTHON
"$SPARK_SUBMIT" "${bundle_args[@]}" \
  --master "$MASTER" --driver-memory "${DRIVER_MEMORY:-4g}" \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
  "$OUTPUT/generator.py" "$OUTPUT"
