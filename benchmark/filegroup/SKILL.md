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

---
name: fg-bench-local
description: Run the hudi-rs file-group-reader performance benchmark locally — build the readers, generate single-FG MOR datasets, and run the containerized resource-tier matrix comparing the Rust hudi-rs reader against the Java gold readers (java-avro pure-JVM, java-spark columnar). Use when the user asks to "run the fg-bench benchmark", "benchmark the hudi-rs reader locally", "compare hudi-rs vs the Java reader", "run the FG reader perf matrix", "regenerate the bench datasets", or similar. Encodes the moved paths, the docker/sudo shim, and the hard-won gotchas (cp exec-bit, java-spark compaction.max.size budget key, cgroup-peak vs true RSS, JDK17 Spark flags) that otherwise silently break a run or make the numbers wrong.
---

# fg-bench local performance benchmark

Goal: measure the **hudi-rs MOR file-group reader** (`HoodieFileGroupReader`) in isolation under controlled CPU/memory tiers, and compare it head-to-head against the two **Java gold readers**. Three readers, one JSON schema, one report:

| ref | what | engine | in-memory format |
|---|---|---|---|
| `current` | hudi-rs reader (all A1–A6 fixes), `--streaming` | none (Rust) | Arrow columnar |
| `java` | Java `HoodieFileGroupReader` via `HoodieAvroReaderContext` | none (pure JVM) | Avro row (`IndexedRecord`) |
| `java-spark` | production columnar path via embedded `local[N]` Spark | in-process Spark driver | `InternalRow`/`ColumnarBatch` |

`baseline` (pre-fix Rust binary) is a 4th optional ref if a baseline binary is staged.

## Paths (verified 2026-06-11; the original `ws3`/`operations` paths are GONE)
- **Repo**: `/home/ubuntu/lin_root/hudi-rs-internal` — branch `lin/mor-on-0607` already contains the A6 work (`benchmark/filegroup/` = the `fg-bench` crate).
- **Bench tooling**: `/home/ubuntu/lin_root/0607-perf/scripts/` (`gen.sh`, `gen_fg_datasets.scala`, `run_quiet.sh`) and `.../scripts/container/` (`run_matrix.sh`, `run_cell.sh`, `make_report.py`, `cgroup_sampler.sh`, `java/`, `java-spark/`).
- **Datasets**: `/home/ubuntu/lin_root/fg-bench-data/<variant>-<scale>/`.
- **Spark dist**: `/home/ubuntu/spark-3.5.3-bin-hadoop3`. **Hudi bundle**: `/home/ubuntu/spark-jars/hudi-spark3.5-bundle_2.12-1.1.0.jar`.
- **hudi-internal** (gen + java build deps): `/home/ubuntu/lin_root/hudi-internal`.

## Gate G0 — Docker access (the #1 silent blocker)
The shell user is **not in the `docker` group** but has passwordless `sudo`. The matrix scripts call bare `docker`. Create a shim and put it first on PATH for every matrix invocation:
```bash
mkdir -p /tmp/dockershim
printf '#!/usr/bin/env bash\nexec sudo docker "$@"\n' > /tmp/dockershim/docker && chmod +x /tmp/dockershim/docker
export PATH=/tmp/dockershim:$PATH
```
The `cgroup_sampler.sh` reads `/sys/fs/cgroup` directly (no docker call) and uses the `@cidfile` path, so it works as the normal user.

## Step 1 — build the three readers
```bash
# hudi-rs (Rust) — ~60s, offline (cached arrow fork); rocksdb needs libclang (present)
cd /home/ubuntu/lin_root/hudi-rs-internal && CARGO_NET_OFFLINE=true cargo build -p fg-bench --release
# java-avro — offline build via harvested classpath (NOT a plain `mvn package`, which fails offline)
cd /home/ubuntu/lin_root/0607-perf/scripts/container/java && bash build.sh
# java-spark — compiles vs Spark dist + bundle, stages ~250 runtime jars for the image
cd /home/ubuntu/lin_root/0607-perf/scripts/container/java-spark && bash build.sh
```

## Step 2 — generate datasets (single-FG MOR v9, COMMIT_TIME_ORDERING)
`gen.sh`'s built-in paths are stale; pass env overrides:
```bash
cd /home/ubuntu/lin_root/0607-perf/scripts
export SPARK_SHELL_BIN=/home/ubuntu/spark-3.5.3-bin-hadoop3/bin/spark-shell
export HUDI_BUNDLE_JAR=/home/ubuntu/spark-jars/hudi-spark3.5-bundle_2.12-1.1.0.jar
export FGBENCH_DATA_ROOT=/home/ubuntu/lin_root/fg-bench-data
bash gen.sh <variant> <scale>     # variant: plain|churn|wide|logheavy|baseonly ; scale: smoke|full
```
Shapes: `plain` (600k base + 5 logs, light merge), `churn` (120k keys rewritten every log → heavy 6-way merge), `wide` (202 cols), `logheavy` (small base, 8 logs), `baseonly` (no logs). `smoke` ≈ tens of MB (CI-fast); `full` ≈ 1 GB base + 5×1 GB logs.

## Step 3 — run the matrix
```bash
cd /home/ubuntu/lin_root/0607-perf/scripts/container
export PATH=/tmp/dockershim:$PATH
bash run_matrix.sh \
  --datasets plain,churn --scales smoke --tiers normal,edge \
  --data-dir /home/ubuntu/lin_root/fg-bench-data \
  --iterations 3 --with-java --with-java-spark
```
- Tiers (docker `--cpus`/`--memory`, swap off → real OOM; cpuset 16-31): `unconstrained` 16c/64g, `normal` 4c/4g, `constrained` 1c/1g, `edge` 1c/512m.
- `--with-java` / `--with-java-spark` add the Java refs (each its own image). First run builds 3 images; reuse with `--skip-build`.
- Each cell is gated by `run_quiet.sh` (waits for a quiet host window, auto-reruns on contention) and a cgroup-v2 sampler. Per-iter 0 is a discarded warmup.
- **Shared merge budget** (so all readers get the same working memory): `run_cell.sh` passes one `--merge-max-size` to every reader = `MERGE_MAX_FRAC` (default 0.5) × container, or absolute `MERGE_MAX_SIZE=<bytes>` env. Spill threshold = **0.8 × budget**. To force a spill: `export MERGE_MAX_SIZE=20971520` (20 MiB) on a roomy tier.

## Step 4 — report
```bash
python3 make_report.py runs/<timestamp>     # writes runs/<ts>/report.md
column -t -s, runs/<timestamp>/index.csv
```
Report has: OOM-boundary map, A/B (current vs baseline), **Java gold readers vs hudi-rs** table, per-cell stage breakdown + contention. Outcomes: `OK` / `OOM` (rc137 or oom_kill) / `SLOW_TIMEOUT` (rc124) / `ERROR` / `OK_TAINTED` / `SKIP_NO_DATASET`.

## GOTCHAS — these silently break a run or the numbers (all hit on 2026-06-11)
1. **`run_matrix.sh --binary` drops the exec bit** (`cp` without `-p`) → staged `fg-bench` is non-executable → "no current binary". Workaround: pre-`chmod +x staging/fg-bench` and run WITHOUT `--binary`. *(unfixed bug in run_matrix.sh)*
2. **java-spark merge budget uses a DIFFERENT key.** The Spark file format (`HoodieFileGroupReaderBasedFileFormat:262`) overwrites `hoodie.memory.merge.max.size` with `getMaxMemoryPerCompaction(options)`, which reads **`hoodie.memory.compaction.max.size`**. `FgBenchSpark` sets BOTH; if you touch the budget plumbing, keep `compaction.max.size` or the budget is silently ignored (no spill).
3. **cgroup `memory.peak` ≠ memory.** Under spill it balloons with **page cache** from spill files. Judge memory by the JSON `max_rss_kb` (getrusage true RSS), not the report's cgroup-peak column. E.g. hudi-rs forced-spill: true RSS 167 MB while cgroup peak shows 1.6 GB.
4. **Spark on JDK17 needs module flags.** `entrypoint-java-spark.sh` injects the `--add-opens` set spark-submit normally adds; a plain `java -cp` without them dies on `sun.nio.ch.DirectBuffer`.
5. **Spark driver memory floor ~470 MB.** `UnifiedMemoryManager` refuses to start below it → `java-spark` `ERROR`s (not OOM) at the `edge` 512m tier (heap = 75% = 384 MB). Expected, not a harness bug.
6. **Java INFO logging not silenced** (reload4j/log4j2 config ignored) → noise in `container.log`. Cosmetic only — the result JSON (`--output-json`) is clean and separate.
7. **`gen.sh` errors `bad scale` for custom sizes** — only `smoke|full` allowed; edit `gen_fg_datasets.scala` `ScaleSpec` rows (and relax the `gen.sh` case) to add a size.

## What the numbers said (smoke baseline, for sanity-checking a rerun)
hudi-rs is ~3–9× faster and ~10× leaner than both Java readers; survives `edge` (512m) where java-avro OOMs and java-spark can't boot. Columnar (java-spark) beats Avro only on heavy-merge (`churn`: 3.4× vs 6.0× rs); near-parity on `plain` (Spark overhead dominates). Spilling bounds hudi-rs true RSS (167 MB) but costs 33× wall (B5 single-row IPC); the JVM readers spill but stay heap-heavy.
