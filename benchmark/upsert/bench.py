#!/usr/bin/env python3
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

"""Single-node upsert / delete benchmark for hudi-rs writes.

Every leg runs the same scenario against the same generated data: a bulk load,
K upsert commits, K delete commits, then a full read-back that is checked
against the expected end state. Each phase of each leg runs in its own
subprocess, so peak RSS is per phase and one leg's memory never shows up in
another's numbers.

Usage:
    bench.py --scenario uuid|tpcds-web-returns|nyc-taxi|rag-chunks|training-set
             [--smoke] [--legs hudi-cow,hudi-mor,...] [--extra-legs PATH]
             [--out results.json]
"""

import argparse
import datetime
import importlib.metadata
import importlib.util
import json
import os
import platform
import resource
import shutil
import signal
import statistics
import subprocess
import sys
import tempfile
import time
import traceback
import types
from pathlib import Path

import psutil
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from scenarios import (
    KEY_COL,
    KIND_COL,
    SCENARIOS,
    all_knobs,
    ensure_workload,
    key_strings,
    oracle_schema,
    plain_type,
    read_base,
    resolve_params,
    sorted_by_key,
    workload_paths,
)

HERE = Path(__file__).resolve().parent
RESULTS_SCHEMA_VERSION = 1

# Columns an engine adds to every row; they are not part of the logical table
# the oracle describes.
ENGINE_META_PREFIXES = ("_hoodie_",)

PHASES = ["load", "upsert", "delete", "readback"]

EXIT_FAILED = 1
EXIT_UNAVAILABLE = 3
EXIT_REFUSED = 4

# Free disk the headroom gate requires, as a multiple of the base dataset size.
DISK_FACTOR = 3


class LegUnavailable(Exception):
    """The leg's library, or a capability the scenario needs, is not available."""


class CheckFailed(Exception):
    """An engine-reported count or a read-back check disagrees with the plan."""


class HeadroomRefused(Exception):
    """The machine is not idle enough to time anything on it."""


def _version(dist):
    try:
        return importlib.metadata.version(dist)
    except importlib.metadata.PackageNotFoundError:
        return None


class Leg:
    """An engine leg: one way of writing and reading the scenario's table.

    Subclass it and register the subclass with ``register_leg``, in this file or
    in a module passed with ``--extra-legs``. Methods a scenario does not use
    (``marker``, ``incremental``, ``time_travel`` outside training-set) may be
    left as they are.
    """

    name = ""

    def config(self, plan):
        """Settings recorded in the results: package, version, write options."""
        return {}

    def load(self, root, table, plan):
        """Create the table under ``root``, write ``table``, and return
        ``{"marker": <version after the load>}``."""
        raise NotImplementedError

    def open(self, root, plan):
        raise NotImplementedError

    def upsert(self, batch):
        """Apply one upsert; return ``num_updated`` / ``num_inserted`` when known."""
        raise NotImplementedError

    def delete(self, keys):
        """Delete the rows whose key columns match the Arrow table ``keys``;
        return ``num_deleted`` when known."""
        raise NotImplementedError

    def read(self):
        """The full table as a ``pyarrow.Table``, or None when it is empty."""
        raise NotImplementedError

    def marker(self):
        raise LegUnavailable(f"{self.name} does not expose table versions")

    def incremental(self, prev, cur):
        """``(rows returned, changed rows, deleted keys)`` for the commits after
        marker ``prev`` up to ``cur``: the rows upserts wrote, and the keys
        deletes removed, or None for the deleted keys when the read does not
        return deletes (declare which with ``incremental_emits_deletes`` in
        ``config``)."""
        raise LegUnavailable(f"{self.name} has no incremental read")

    def time_travel(self, marker):
        raise LegUnavailable(f"{self.name} has no time travel read")

    def live_data_files(self, root):
        """Data files the current snapshot references, relative to ``root``."""
        raise NotImplementedError

    def classify(self, rel):
        """``"data"`` or ``"meta"`` for a file path relative to the table root."""
        return "data" if rel.suffix == ".parquet" else "meta"

    def crash_cause(self, plan, phase, in_flight):
        """A one-line explanation when the phase process dies on a signal, if
        the leg knows one; ``in_flight`` names the commit that was running."""
        return None


LEGS = {}


def register_leg(name, factory):
    """Make a leg available to ``--legs``; ``factory()`` returns a ``Leg``."""
    if name in LEGS:
        raise ValueError(f"leg {name!r} is already registered")
    LEGS[name] = factory


def load_extra_legs(paths):
    """Import each module and call its ``register(api)``; ``api`` carries
    ``Leg``, ``LegUnavailable`` and ``register_leg``."""
    api = types.SimpleNamespace(
        Leg=Leg, LegUnavailable=LegUnavailable, register_leg=register_leg
    )
    for path in paths:
        path = Path(path).resolve()
        spec = importlib.util.spec_from_file_location(f"extra_legs_{path.stem}", path)
        if spec is None:
            raise SystemExit(f"--extra-legs {path}: not a Python module")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        if not hasattr(module, "register"):
            raise SystemExit(f"--extra-legs {path}: defines no register(api)")
        module.register(api)


class HudiLeg(Leg):
    def __init__(self, name, table_type, record_index):
        self.name = name
        self.table_type = table_type
        self.record_index = record_index

    def config(self, plan):
        return {
            "package": "hudi",
            "version": _version("hudi"),
            "table_type": self.table_type,
            "record_index": self.record_index,
            "record_key": plan["key_cols"],
            "ordering_fields": plan["ordering"],
            "target_file_size": "library default",
            "upsert": "HudiTable.upsert(arrow_table)",
            "delete": "HudiTable.delete_keys(keys)",
            "read": "HudiTable.read()",
            "incremental_read": (
                "HudiTable.read(HudiReadOptions().with_query_type(Incremental)"
                ".with_start_timestamp(<instant of the commit just written>)); on table "
                "version 9 the start bound is a completion time, so the commit's own "
                "requested instant selects exactly that commit"
            ),
            "incremental_emits_deletes": False,
            "time_travel": "HudiTable.read(HudiReadOptions().with_as_of_timestamp(<load instant>))",
        }

    def _hudi(self):
        try:
            import hudi
        except ImportError as e:
            raise LegUnavailable(
                f"hudi write API not installed: cannot import hudi ({e}); install a hudi "
                "build with HudiTable.create into the benchmark venv"
            ) from e
        if not hasattr(hudi.HudiTable, "create"):
            raise LegUnavailable(
                f"hudi write API not installed: hudi {_version('hudi')} has no "
                "HudiTable.create; install a hudi build with the write API into the "
                "benchmark venv"
            )
        return hudi

    def load(self, root, base, plan):
        hudi = self._hudi()
        t = hudi.HudiTable.create(
            str(root),
            record_key=plan["key_cols"],
            ordering_fields=plan["ordering"],
            table_type=self.table_type,
            record_index_enabled=self.record_index,
        )
        r = t.append(base)
        return {"marker": r.instant_time}

    def open(self, root, plan):
        self.plan = plan
        self.t = self._hudi().HudiTable(str(root))
        self.last_instant = None

    def upsert(self, batch):
        r = self.t.upsert(batch)
        self.last_instant = r.instant_time
        return {
            "num_updated": r.num_updates,
            "num_inserted": r.num_inserts,
            "instant": r.instant_time,
        }

    def record_keys(self, keys):
        """Record key strings as the writer stores them: the value for one key
        field, ``f1:v1,f2:v2`` in key order for several (the Java
        ComplexKeyGenerator form)."""
        cols = self.plan["key_cols"]
        values = [keys.column(c).to_pylist() for c in cols]
        if len(cols) == 1:
            return [str(v) for v in values[0]]
        return [",".join(f"{c}:{v}" for c, v in zip(cols, row)) for row in zip(*values)]

    def delete(self, keys):
        r = self.t.delete_keys(self.record_keys(keys))
        self.last_instant = r.instant_time
        return {"num_deleted": r.num_deletes, "instant": r.instant_time}

    def _read(self, opts=None):
        batches = self.t.read(opts)
        if not batches:
            return None
        return pa.Table.from_batches(batches)

    def read(self):
        return self._read()

    def marker(self):
        return self.last_instant

    def incremental(self, prev, cur):
        hudi = self._hudi()
        opts = (
            hudi.HudiReadOptions()
            .with_query_type(hudi.HudiQueryType.Incremental)
            .with_start_timestamp(cur)
        )
        t = self._read(opts)
        return (0, None, None) if t is None else (t.num_rows, t, None)

    def time_travel(self, first):
        return self._read(self._hudi().HudiReadOptions().with_as_of_timestamp(first))

    def live_data_files(self, root):
        live = set()
        for fs in self._hudi().HudiTable(str(root)).get_file_slices():
            names = ([fs.base_file_name] if fs.base_file_name else []) + list(
                fs.log_file_names
            )
            live.update(Path(fs.partition_path) / n for n in names)
        return live

    def classify(self, rel):
        # .hoodie holds the timeline and the metadata table, which is where the
        # record-level index lives, so index maintenance shows up as meta bytes.
        if rel.parts[0] == ".hoodie":
            return "meta"
        if rel.suffix == ".parquet" or ".log." in rel.name:
            return "data"
        return "meta"


register_leg("hudi-cow", lambda: HudiLeg("hudi-cow", "cow", True))
register_leg("hudi-mor", lambda: HudiLeg("hudi-mor", "mor", True))
register_leg("hudi-cow-noindex", lambda: HudiLeg("hudi-cow-noindex", "cow", False))


# ---------------------------------------------------------------------------
# Measurement helpers
# ---------------------------------------------------------------------------


def snapshot(root):
    out = {}
    if not root.exists():
        return out
    for dirpath, _, files in os.walk(root):
        for f in files:
            p = Path(dirpath) / f
            try:
                out[p.relative_to(root)] = p.stat().st_size
            except FileNotFoundError:
                pass
    return out


def diff(before, after, classify):
    """Bytes and files a commit added, split into data and metadata."""
    res = {
        "data_bytes_written": 0,
        "meta_bytes_written": 0,
        "data_files_created": 0,
        "meta_files_created": 0,
        "data_files_removed": 0,
        "meta_files_removed": 0,
        "files_modified_in_place": 0,
    }
    for rel, size in after.items():
        kind = classify(rel)
        if rel not in before:
            res[f"{kind}_bytes_written"] += size
            res[f"{kind}_files_created"] += 1
        elif size != before[rel]:
            res[f"{kind}_bytes_written"] += max(0, size - before[rel])
            res["files_modified_in_place"] += 1
    for rel in before:
        if rel not in after:
            res[f"{classify(rel)}_files_removed"] += 1
    res["bytes_written"] = res["data_bytes_written"] + res["meta_bytes_written"]
    res["table_bytes_after"] = sum(after.values())
    res["table_data_files_after"] = sum(1 for rel in after if classify(rel) == "data")
    return res


def reclaim(leg, root, after):
    """Delete data files the table's current snapshot no longer references.

    Old versions are never read by a later commit or by the read-back, so this
    changes no measured operation; it keeps a 10M-row copy-on-write run from
    needing a full table copy of disk per commit.
    """
    live = leg.live_data_files(root)
    stale = [rel for rel in after if leg.classify(rel) == "data" and rel not in live]
    if stale and not live:
        raise RuntimeError("live file listing came back empty; refusing to reclaim")
    freed = 0
    for rel in stale:
        freed += after[rel]
        (root / rel).unlink()
    return {"reclaimed_files": len(stale), "reclaimed_bytes": freed}


def headroom(work_dir, base_bytes, limits):
    avail = psutil.virtual_memory().available
    r = {
        "load1": os.getloadavg()[0],
        "mem_available_bytes": avail,
        "disk_free_bytes": shutil.disk_usage(work_dir).free,
    }
    v = {}
    if r["load1"] > limits.max_load1:
        v["load1"] = f"load1 {r['load1']:.2f} > {limits.max_load1}"
    if avail < limits.min_mem_gib * 2**30:
        v["memory"] = (
            f"available memory {avail / 2**30:.1f} GiB < {limits.min_mem_gib} GiB"
        )
    if r["disk_free_bytes"] < DISK_FACTOR * base_bytes:
        v["disk"] = (
            f"free disk {r['disk_free_bytes'] / 2**30:.1f} GiB < {DISK_FACTOR} x base "
            f"dataset ({base_bytes / 2**30:.2f} GiB)"
        )
    r["violations"] = v
    return r


def gate(work_dir, base_bytes, limits, what):
    """Wait for the machine to be idle, then return the readings.

    load1 is a one-minute moving average that the benchmark itself raises, so
    when it is the only violation the gate polls for up to --settle-timeout,
    letting the previous commit's own load decay instead of counting it against
    the next; under --force, where a load violation only warns, it does not
    wait. Memory and disk shortfalls do not settle, so they are judged at
    once. A disk shortfall refuses even under --force: a copy-on-write commit
    can rewrite the whole table, and running out of disk mid-commit hurts more
    than the run.
    """
    start = time.monotonic()
    while True:
        r = headroom(work_dir, base_bytes, limits)
        v = r["violations"]
        settling = list(v) == ["load1"] and not limits.force
        if not settling or time.monotonic() - start >= limits.settle_timeout:
            break
        time.sleep(2)
    r["waited_s"] = round(time.monotonic() - start, 1)
    if v:
        msg = f"headroom gate before {what}: " + "; ".join(v.values())
        if "disk" in v or not limits.force:
            raise HeadroomRefused(msg)
        print(f"WARNING (--force): {msg}", file=sys.stderr, flush=True)
        r["headroom_violated"] = True
    return r


def peak_rss_bytes():
    r = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return r if sys.platform == "darwin" else r * 1024


def current_rss_bytes():
    return psutil.Process().memory_info().rss


def machine_info(work_dir):
    info = {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "python": platform.python_version(),
        "cpu_count": os.cpu_count(),
    }
    if sys.platform == "darwin":
        try:
            info["cpu"] = subprocess.run(
                ["sysctl", "-n", "machdep.cpu.brand_string"],
                capture_output=True,
                text=True,
                check=True,
            ).stdout.strip()
        except (OSError, subprocess.CalledProcessError):
            pass
    else:
        try:
            for line in Path("/proc/cpuinfo").read_text().splitlines():
                if line.startswith("model name"):
                    info["cpu"] = line.split(":", 1)[1].strip()
                    break
        except OSError:
            pass
    info["memory_bytes"] = psutil.virtual_memory().total
    info["work_dir_free_bytes"] = shutil.disk_usage(work_dir).free
    try:
        info["git_commit"] = subprocess.run(
            ["git", "-C", str(HERE), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        pass
    info["packages"] = {
        d: _version(d) for d in ("pyarrow", "numpy", "psutil", "duckdb", "hudi")
    }
    return info


# ---------------------------------------------------------------------------
# Phase (child process)
# ---------------------------------------------------------------------------


class PhaseWriter:
    """Persists the phase record after every commit, so a killed phase still
    leaves the commits it finished on disk."""

    def __init__(self, path):
        self.path = Path(path)
        self.rec = {}

    def flush(self):
        self.rec["peak_rss_bytes"] = peak_rss_bytes()
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.rec, indent=2, default=str))
        tmp.replace(self.path)


def _expect(name, got, want):
    if got is not None and got != want:
        raise CheckFailed(f"{name}: engine reported {got}, plan has {want}")


def _key_set(table, key_cols):
    if table is None or table.num_rows == 0:
        return set()
    return set(key_strings(table, key_cols).to_pylist())


def measure_reads(
    leg, plan, prev, cur, first, kind, arg, base, emits_deletes, gate_before=None
):
    """The two reads a training-data consumer makes after a commit: what changed,
    and the table as it was when first loaded. Neither is in the commit time;
    ``gate_before(what)`` is the headroom gate, run before each timed read."""
    keys = plan["key_cols"]
    room = gate_before(f"incremental read after {kind}") if gate_before else None
    t0 = time.perf_counter()
    rows, changed, deleted = leg.incremental(prev, cur)
    inc = {"wall_s": time.perf_counter() - t0, "rows": rows}
    if room is not None:
        inc["headroom"] = room
    n_changed = 0 if changed is None else changed.num_rows
    n_deleted = 0 if deleted is None else deleted.num_rows
    want = _key_set(arg, keys)
    if kind == "upsert":
        expected = f"exactly the {arg.num_rows} upserted rows, values included"
        got = changed if changed is not None else arg.schema.empty_table()
        # compare_tables expects the expected side sorted by key, as the
        # oracle files are; the commit's input is in generation order.
        same = compare_tables(got, sorted_by_key(arg, keys), keys)
        if not same["passed"]:
            inc["mismatches"] = same["mismatches"]
            expected += ": " + json.dumps(same["mismatches"], default=str)[:1500]
        ok = same["passed"] and n_deleted == 0
    elif emits_deletes:
        expected = f"no changed rows and exactly the {arg.num_rows} deleted keys"
        ok = n_changed == 0 and n_deleted == arg.num_rows
        ok = ok and _key_set(deleted, keys) == want
    else:
        expected = "no rows: this leg's incremental read does not return deletes"
        ok = n_changed == 0 and n_deleted == 0
    inc.update(changed_rows=n_changed, deleted_rows=n_deleted, expected=expected)
    if not ok:
        raise CheckFailed(
            f"incremental read after {kind} returned {n_changed} changed and "
            f"{n_deleted} deleted rows; expected {expected}"
        )
    room = gate_before("time travel read") if gate_before else None
    t0 = time.perf_counter()
    tt = leg.time_travel(first)
    wall = time.perf_counter() - t0
    if tt is None:
        tt = base.schema.empty_table()
    res = compare_tables(tt, base, keys)
    out = {"incremental": inc}
    out["time_travel"] = {
        "wall_s": wall,
        "rows": tt.num_rows,
        "matches_load": res["passed"],
    }
    if room is not None:
        out["time_travel"]["headroom"] = room
    if not res["passed"]:
        raise CheckFailed(
            "time travel to the load does not return the loaded table: "
            + json.dumps(res["mismatches"], default=str)[:1500]
        )
    return out


def run_commits(leg, root, wd, plan, kind, w, a):
    leg.open(root, plan)
    if plan["incremental_reads"]:
        base = pq.read_table(Path(a.work_dir) / plan["oracle_base"])
        emits_deletes = leg.config(plan).get("incremental_emits_deletes")
        if emits_deletes is None:
            raise CheckFailed(
                f"{leg.name} config does not declare incremental_emits_deletes"
            )
    w.rec["rss_before_bytes"] = current_rss_bytes()
    w.rec["commits"] = []
    for c in range(plan["commits"]):
        arg = pq.read_table(wd / f"{kind}-{c:02d}.parquet")
        prev = leg.marker() if plan["incremental_reads"] else None
        before = snapshot(root)
        room = gate(root, a.base_bytes, a, f"{kind} commit {c}")
        # Lets the driver name the commit if the process dies on a signal.
        w.rec["in_flight"] = {"commit": c, "input_rows": arg.num_rows}
        w.flush()
        t0 = time.perf_counter()
        info = leg.upsert(arg) if kind == "upsert" else leg.delete(arg)
        wall = time.perf_counter() - t0
        after = snapshot(root)
        rec = {
            "commit": c,
            "wall_s": wall,
            "input_rows": arg.num_rows,
            "headroom": room,
            **diff(before, after, leg.classify),
            "engine": info,
        }
        w.rec.pop("in_flight", None)
        w.rec["commits"].append(rec)
        w.flush()
        exp = plan["expected"][kind][c]
        if kind == "upsert":
            _expect(f"upsert {c} updated rows", info.get("num_updated"), exp["updated"])
            _expect(
                f"upsert {c} inserted rows", info.get("num_inserted"), exp["inserted"]
            )
        else:
            _expect(f"delete {c} deleted rows", info.get("num_deleted"), exp["deleted"])
        if plan["incremental_reads"]:
            rec["reads"] = measure_reads(
                leg,
                plan,
                prev,
                leg.marker(),
                a.first_marker,
                kind,
                arg,
                base,
                emits_deletes,
                lambda what: gate(root, a.base_bytes, a, f"{what} (commit {c})"),
            )
            w.flush()
        if a.reclaim:
            rec.update(reclaim(leg, root, after))
            w.flush()


def check_readback(table, wd, plan):
    verify = pq.read_table(wd / "verify.parquet")
    cols = plan["check_cols"]
    got = table.select(plan["key_cols"] + cols)
    bad = type_mismatches(got.schema, verify.schema, cols)
    if bad:
        return {"passed": False, "type_mismatches": bad}
    keys = key_strings(got, plan["key_cols"])
    got = pa.table(
        {KEY_COL: keys}
        | {c: pc.cast(got.column(c), verify.schema.field(c).type) for c in cols}
    )
    rows = got.num_rows
    distinct = pc.count_distinct(keys).as_py()
    hits = got.filter(pc.is_in(keys, value_set=verify.column(KEY_COL)))
    found = {}
    for row in zip(*(hits.column(c).to_pylist() for c in [KEY_COL] + cols)):
        found.setdefault(row[0], []).append(tuple(row[1:]))

    failures = {"updated": 0, "inserted": 0, "deleted": 0, "untouched": 0}
    examples = []
    for row in zip(*(verify.column(c).to_pylist() for c in [KEY_COL, KIND_COL] + cols)):
        key, kind, want = row[0], row[1], tuple(row[2:])
        seen = found.get(key, [])
        ok = not seen if kind == "deleted" else seen == [want]
        if not ok:
            failures[kind] += 1
            if len(examples) < 5:
                examples.append(
                    {
                        "key": key,
                        "kind": kind,
                        "want": "absent" if kind == "deleted" else str(want)[:200],
                        "got": str(seen)[:200],
                    }
                )

    checks = {
        "rows": rows,
        "expected_rows": plan["expected_rows"],
        "distinct_keys": distinct,
        "row_count_ok": rows == plan["expected_rows"],
        "keys_unique_ok": distinct == rows,
        "sampled": plan["verify_counts"],
        "sample_failures": failures,
        "failure_examples": examples,
    }
    checks["passed"] = (
        checks["row_count_ok"]
        and checks["keys_unique_ok"]
        and not any(failures.values())
    )
    return checks


def _first_diffs(keys, got, want, limit):
    out = []
    step = 65_536
    for start in range(0, len(want), step):
        g, w = got.slice(start, step), want.slice(start, step)
        if g.equals(w):
            continue
        for i, (x, y) in enumerate(zip(g.to_pylist(), w.to_pylist())):
            if x != y or type(x) is not type(y):
                d = {"key": keys[start + i].as_py()}
                if isinstance(x, list) and isinstance(y, list):
                    j = next(
                        (j for j, (u, v) in enumerate(zip(x, y)) if u != v),
                        min(len(x), len(y)),
                    )
                    d |= {
                        "element": j,
                        "expected_len": len(y),
                        "got_len": len(x),
                        "expected": repr(y[j]) if j < len(y) else "<end>",
                        "got": repr(x[j]) if j < len(x) else "<end>",
                    }
                else:
                    d |= {"expected": repr(y)[:300], "got": repr(x)[:300]}
                out.append(d)
                if len(out) >= limit:
                    return out
    return out


def type_mismatches(got_schema, exp_schema, cols):
    """Columns whose read-back type differs from the expected one after the only
    normalizations allowed: the plain, large and view forms of strings and
    binaries are one type, and list item names are ignored."""
    out = []
    for c in cols:
        g, e = got_schema.field(c).type, exp_schema.field(c).type
        if plain_type(g) != plain_type(e):
            out.append({"column": c, "expected_type": str(e), "got_type": str(g)})
    return out


def compare_to_oracle(table, work_dir, plan):
    """Compare the leg's whole table with the oracle written by the workload
    replay: same rows, same keys, same types, every column value equal, floats
    and embeddings included, with no tolerance."""
    exp = pq.read_table(Path(work_dir) / plan["oracle"])
    return {"path": plan["oracle"]} | compare_tables(table, exp, plan["key_cols"])


def compare_tables(table, exp, key_cols):
    exp = exp.cast(oracle_schema(exp.schema)).combine_chunks()
    res = {
        "rows_expected": exp.num_rows,
        "rows_read": table.num_rows,
        "passed": False,
        "mismatches": [],
    }
    dropped = [c for c in table.column_names if c.startswith(ENGINE_META_PREFIXES)]
    res["dropped_engine_columns"] = dropped
    got = table.drop_columns(dropped)
    missing = [c for c in exp.column_names if c not in got.column_names]
    extra = [c for c in got.column_names if c not in exp.column_names]
    if missing or extra:
        res["mismatches"].append({"columns_missing": missing, "columns_extra": extra})
        return res
    bad = type_mismatches(got.schema, exp.schema, exp.column_names)
    if bad:
        res["mismatches"] += bad
        return res
    got = got.select(exp.column_names).cast(exp.schema)
    got = sorted_by_key(got, key_cols).combine_chunks()
    if got.equals(exp):
        res["passed"] = True
        return res
    gk, ek = key_strings(got, key_cols), key_strings(exp, key_cols)
    if got.num_rows != exp.num_rows or not gk.equals(ek):
        missing_keys = ek.filter(pc.invert(pc.is_in(ek, value_set=gk)))
        extra_keys = gk.filter(pc.invert(pc.is_in(gk, value_set=ek)))
        res["mismatches"].append(
            {
                "keys_missing": len(missing_keys),
                "keys_missing_examples": missing_keys.slice(0, 5).to_pylist(),
                "keys_extra": len(extra_keys),
                "keys_extra_examples": extra_keys.slice(0, 5).to_pylist(),
                "duplicate_keys": got.num_rows - pc.count_distinct(gk).as_py(),
            }
        )
        return res
    for c in exp.column_names:
        if not got.column(c).equals(exp.column(c)):
            res["mismatches"].append(
                {"column": c, "rows": _first_diffs(ek, got.column(c), exp.column(c), 5)}
            )
    return res


def phase_main(argv):
    p = argparse.ArgumentParser(prog="bench.py _phase")
    p.add_argument("--leg", required=True)
    p.add_argument("--phase", required=True, choices=PHASES)
    p.add_argument("--root", required=True)
    p.add_argument("--base-dir", required=True)
    p.add_argument("--workload-dir", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--reclaim", action="store_true")
    p.add_argument("--first-marker", default=None)
    p.add_argument("--extra-legs", action="append", default=[])
    p.add_argument("--work-dir", required=True)
    add_headroom_args(p)
    p.add_argument("--base-bytes", type=int, required=True)
    a = p.parse_args(argv)
    load_extra_legs(a.extra_legs)
    if a.first_marker is not None:
        a.first_marker = json.loads(a.first_marker)

    leg = LEGS[a.leg]()
    root = Path(a.root)
    wd = Path(a.workload_dir)
    plan = json.loads((wd / "plan.json").read_text())
    w = PhaseWriter(a.out)
    try:
        if a.phase == "load":
            base = read_base(Path(a.base_dir))
            w.rec["rss_before_bytes"] = current_rss_bytes()
            before = snapshot(root)
            w.rec["headroom"] = gate(root, a.base_bytes, a, "load")
            t0 = time.perf_counter()
            info = leg.load(root, base, plan)
            w.rec["wall_s"] = time.perf_counter() - t0
            w.rec.update(diff(before, snapshot(root), leg.classify))
            w.rec["engine"] = info
        elif a.phase in ("upsert", "delete"):
            run_commits(leg, root, wd, plan, a.phase, w, a)
        else:
            leg.open(root, plan)
            w.rec["rss_before_bytes"] = current_rss_bytes()
            w.rec["headroom"] = gate(root, a.base_bytes, a, "read-back")
            t0 = time.perf_counter()
            table = leg.read()
            w.rec["wall_s"] = time.perf_counter() - t0
            if table is None:
                raise CheckFailed("read-back returned no batches")
            checks = check_readback(table, wd, plan)
            t0 = time.perf_counter()
            checks["oracle"] = compare_to_oracle(table, a.work_dir, plan)
            checks["oracle"]["wall_s"] = time.perf_counter() - t0
            checks["sampled_passed"] = checks["passed"]
            checks["passed"] = checks["sampled_passed"] and checks["oracle"]["passed"]
            w.rec["checks"] = checks
            if not checks["passed"]:
                w.flush()
                raise CheckFailed(
                    "read-back does not match the expected end state "
                    f"(sampled checks {'passed' if checks['sampled_passed'] else 'failed'}, "
                    f"oracle {'passed' if checks['oracle']['passed'] else 'failed'}: "
                    f"{json.dumps(checks['oracle']['mismatches'], default=str)[:1500]})"
                )
        w.flush()
        return 0
    except HeadroomRefused as e:
        w.rec["refused"] = str(e)
        w.flush()
        return EXIT_REFUSED
    except LegUnavailable as e:
        w.rec["unavailable"] = str(e)
        w.flush()
        return EXIT_UNAVAILABLE
    except Exception as e:
        w.rec["error"] = f"{type(e).__name__}: {e}"[:2000]
        w.rec["traceback"] = traceback.format_exc()
        w.flush()
        return EXIT_FAILED


# ---------------------------------------------------------------------------
# Driver (parent process)
# ---------------------------------------------------------------------------


def all_readings(leg):
    out = []
    for rep in leg.get("repetitions", []):
        if rep.get("headroom_before"):
            out.append(rep["headroom_before"])
        for ph in rep.get("phases", {}).values():
            if ph.get("headroom"):
                out.append(ph["headroom"])
            for c in ph.get("commits", []):
                if c.get("headroom"):
                    out.append(c["headroom"])
                out += [
                    r["headroom"]
                    for r in c.get("reads", {}).values()
                    if r.get("headroom")
                ]
    return out


def summarize_rep(phases):
    """One repetition's numbers; the leg summary aggregates these."""
    ph = phases
    s = {}
    if "load" in ph and "wall_s" in ph["load"]:
        s["load_s"] = ph["load"]["wall_s"]
        s["load_bytes"] = ph["load"]["bytes_written"]
        s["data_files_after_load"] = ph["load"]["table_data_files_after"]
    for kind in ("upsert", "delete"):
        commits = ph.get(kind, {}).get("commits", [])
        if not commits:
            continue
        walls = [c["wall_s"] for c in commits]
        s[f"{kind}_commits"] = len(commits)
        s[f"{kind}_mean_s"] = statistics.fmean(walls)
        s[f"{kind}_median_s"] = statistics.median(walls)
        s[f"{kind}_bytes_total"] = sum(c["bytes_written"] for c in commits)
        s[f"{kind}_bytes_per_commit"] = s[f"{kind}_bytes_total"] / len(commits)
        s[f"{kind}_meta_bytes_total"] = sum(c["meta_bytes_written"] for c in commits)
        s[f"{kind}_data_files_created_per_commit"] = statistics.fmean(
            c["data_files_created"] for c in commits
        )
    reads = [
        c["reads"]
        for kind in ("upsert", "delete")
        for c in ph.get(kind, {}).get("commits", [])
        if c.get("reads")
    ]
    for name in ("incremental", "time_travel"):
        r = [x[name] for x in reads if name in x]
        if r:
            s[f"{name}_mean_s"] = statistics.fmean(x["wall_s"] for x in r)
            s[f"{name}_rows_mean"] = statistics.fmean(x["rows"] for x in r)
    if "readback" in ph and "wall_s" in ph["readback"]:
        s["readback_s"] = ph["readback"]["wall_s"]
    rss = {k: v.get("peak_rss_bytes") for k, v in ph.items() if v.get("peak_rss_bytes")}
    if rss:
        s["peak_rss_bytes_by_phase"] = rss
    checks = ph.get("readback", {}).get("checks", {})
    s["correct"] = bool(checks.get("passed"))
    s["sampled_correct"] = checks.get("sampled_passed", checks.get("passed"))
    s["oracle_correct"] = checks.get("oracle", {}).get("passed")
    return s


# Timings aggregated across repetitions; upsert and delete use each
# repetition's mean over its commits as the sample.
TIMINGS = (
    "load_s",
    "upsert_mean_s",
    "delete_mean_s",
    "readback_s",
    "incremental_mean_s",
    "time_travel_mean_s",
)

# Fields a commit writes that do not depend on timing, so every repetition of a
# leg should reproduce them. File counts must match exactly; byte counts may
# drift by WRITE_BYTES_TOLERANCE, because instant times, commit metadata and file
# names embedded in the files differ from run to run.
WRITE_BYTES_TOLERANCE = 0.01
WRITE_FIELDS = (
    "data_bytes_written",
    "meta_bytes_written",
    "data_files_created",
    "meta_files_created",
)


def stats(values):
    """mean, median, min, max and coefficient of variation (sample standard
    deviation over mean; None for a single value)."""
    v = [x for x in values if x is not None]
    if not v:
        return None
    mean = statistics.fmean(v)
    cv = statistics.stdev(v) / mean if len(v) > 1 and mean else None
    return {
        "n": len(v),
        "mean": mean,
        "median": statistics.median(v),
        "min": min(v),
        "max": max(v),
        "cv": cv,
    }


def write_signature(phases):
    """What each step wrote, keyed by step, for comparing repetitions."""
    sig = {}
    if "load" in phases and "wall_s" in phases["load"]:
        sig["load"] = [phases["load"][f] for f in WRITE_FIELDS]
    for kind in ("upsert", "delete"):
        for c in phases.get(kind, {}).get("commits", []):
            sig[f"{kind} commit {c['commit']}"] = [c[f] for f in WRITE_FIELDS]
    return sig


def _differs(field, x, y):
    if field.endswith("_files_created"):
        return x != y
    return abs(x - y) > WRITE_BYTES_TOLERANCE * max(x, y)


def write_mismatches(reps):
    """Every step whose file counts differ from the first repetition's, or
    whose bytes written differ by more than WRITE_BYTES_TOLERANCE of the larger
    value; smaller byte differences are not recorded."""
    if len(reps) < 2:
        return []
    first = write_signature(reps[0]["phases"])
    out = []
    for rep in reps[1:]:
        sig = write_signature(rep["phases"])
        for step in sorted(set(first) | set(sig)):
            a, b = first.get(step), sig.get(step)
            if a is None or b is None:
                out.append(
                    {
                        "rep": rep["rep"],
                        "step": step,
                        "missing_in": 1 if a is None else rep["rep"],
                    }
                )
                continue
            for f, x, y in zip(WRITE_FIELDS, a, b):
                if _differs(f, x, y):
                    out.append(
                        {
                            "rep": rep["rep"],
                            "step": step,
                            "field": f,
                            "rep1": x,
                            "this": y,
                        }
                    )
    return out


def summarize_leg(leg):
    reps = leg.get("repetitions", [])
    done = [r for r in reps if r.get("status") == "ok"]
    s = {"repetitions": len(done), "stats": {}}
    for m in TIMINGS:
        st = stats(r["summary"].get(m) for r in done)
        if st:
            s["stats"][m] = st
    if done:
        first = done[0]["summary"]
        for k, v in first.items():
            if (
                k.endswith(("_bytes", "_bytes_total", "_bytes_per_commit"))
                or "files" in k
            ):
                s[k] = v
        for k in ("incremental_rows_mean", "time_travel_rows_mean"):
            if k in first:
                s[k] = first[k]
        rss = {}
        for r in done:
            for k, v in r["summary"].get("peak_rss_bytes_by_phase", {}).items():
                rss[k] = max(rss.get(k, 0), v)
        s["peak_rss_bytes_by_phase"] = rss
        s["peak_rss_bytes"] = max(rss.values()) if rss else None
        mism = write_mismatches(done)
        s["writes_consistent"] = not mism
        s["write_mismatches"] = mism[:20]
    all_ok = bool(reps) and len(done) == len(reps)
    s["correct"] = all_ok and all(r["summary"]["correct"] for r in done)
    for k in ("sampled_correct", "oracle_correct"):
        vals = [r["summary"].get(k) for r in reps if r.get("summary")]
        s[k] = None if not vals or None in vals else all(vals)
    readings = all_readings(leg)
    if readings:
        s["headroom"] = {
            "readings": len(readings),
            "max_load1": max(r["load1"] for r in readings),
            "min_mem_available_bytes": min(r["mem_available_bytes"] for r in readings),
            "min_disk_free_bytes": min(r["disk_free_bytes"] for r in readings),
            "violated": any(r.get("headroom_violated") for r in readings),
        }
    return s


def run_slug(bd, wd):
    """Names one (scenario, scale, workload); tables and logs live under it so
    runs at different scales or knobs never share a directory."""
    return f"{bd.parent.name}-{wd.name}"


class RunLock:
    """One run per slug at a time: a second run would delete the first one's
    tables and overwrite its phase logs."""

    def __init__(self, work_dir, slug):
        self.path = Path(work_dir) / "locks" / f"{slug}.lock"

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            holder = self.path.read_text().strip()
            if holder.isdigit() and not psutil.pid_exists(int(holder)):
                # The process that held it is gone; the lock is stale.
                self.path.unlink()
                return self.__enter__()
            raise SystemExit(
                f"another run (pid {holder or '?'}) holds {self.path}; "
                "wait for it or remove the lock if it is stale"
            ) from None
        with os.fdopen(fd, "w") as f:
            f.write(str(os.getpid()))
        return self

    def __exit__(self, *exc):
        self.path.unlink(missing_ok=True)


def run_leg(name, a, plan, bd, wd, log_dir):
    """Runs the leg --repeat times, one repetition after another; each rebuilds
    the table from the base data and replays the whole workload."""
    leg = LEGS[name]()
    root = Path(a.work_dir) / "tables" / run_slug(bd, wd) / name
    rec = {
        "status": "ok",
        "config": leg.config(plan),
        "table_dir": str(root.relative_to(a.work_dir)),
        "reclaim": a.reclaim,
        "repeat": a.repeat,
        "repetitions": [],
    }
    for r in range(1, a.repeat + 1):
        rep = run_rep(name, leg, r, a, plan, bd, wd, root, log_dir)
        rec["repetitions"].append(rep)
        if rep["status"] != "ok":
            rec["status"] = rep["status"]
            rec["error"] = f"repetition {r}: {rep.get('error')}"
            for k in ("signal", "unavailable_in_phase"):
                if k in rep:
                    rec[k] = rep[k]
            break
    rec["summary"] = summarize_leg(rec)
    if rec["status"] == "ok" and not rec["summary"]["correct"]:
        rec["status"] = "failed"
    if not a.keep_tables:
        shutil.rmtree(root, ignore_errors=True)
        rec["table_dir_removed"] = True
    return rec


def run_rep(name, leg, r, a, plan, bd, wd, root, log_dir):
    rep = {"rep": r, "status": "ok", "phases": {}}
    tag = f"[{name} {r}/{a.repeat}]"
    try:
        rep["headroom_before"] = gate(
            a.work_dir, a.base_bytes, a, f"leg {name} repetition {r}"
        )
    except HeadroomRefused as e:
        rep["status"] = "refused"
        rep["error"] = str(e)
        rep["headroom_before"] = headroom(a.work_dir, a.base_bytes, a)
        return rep
    h = rep["headroom_before"]
    print(
        f"{tag} headroom: load1 {h['load1']:.2f}, available memory "
        f"{h['mem_available_bytes'] / 2**30:.1f} GiB, "
        f"free disk {h['disk_free_bytes'] / 2**30:.1f} GiB",
        flush=True,
    )
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True)
    start = time.monotonic()
    first_marker = None
    for phase in PHASES:
        out = log_dir / f"{name}-r{r}-{phase}.json"
        out.unlink(missing_ok=True)
        log = log_dir / f"{name}-r{r}-{phase}.log"
        cmd = [
            sys.executable,
            str(Path(__file__).resolve()),
            "_phase",
            "--leg", name,
            "--phase", phase,
            "--root", str(root),
            "--base-dir", str(bd),
            "--workload-dir", str(wd),
            "--out", str(out),
            "--max-load1", str(a.max_load1),
            "--min-mem-gib", str(a.min_mem_gib),
            "--settle-timeout", str(a.settle_timeout),
            "--base-bytes", str(a.base_bytes),
            "--work-dir", str(a.work_dir),
            *(x for path in a.extra_legs for x in ("--extra-legs", str(path))),
        ]  # fmt: skip
        if a.reclaim:
            cmd.append("--reclaim")
        if a.force:
            cmd.append("--force")
        if first_marker is not None:
            cmd += ["--first-marker", json.dumps(first_marker)]
        timeout = None
        if a.leg_timeout:
            timeout = a.leg_timeout - (time.monotonic() - start)
            if timeout <= 0:
                rep["status"] = "timeout"
                rep["error"] = f"budget of {a.leg_timeout}s spent before phase {phase}"
                break
        print(f"{tag} {phase} ...", flush=True)
        with open(log, "w") as lf:
            try:
                rc = subprocess.run(
                    cmd, stdout=lf, stderr=subprocess.STDOUT, timeout=timeout, cwd=HERE
                ).returncode
            except subprocess.TimeoutExpired:
                rc = None
        result = json.loads(out.read_text()) if out.exists() else {}
        # The traceback stays in the phase file under the work dir; results
        # files are meant to be shareable and carry no local paths.
        result.pop("traceback", None)
        rep["phases"][phase] = result
        if phase == "load":
            first_marker = result.get("engine", {}).get("marker")
        if rc is None:
            rep["status"] = "timeout"
            rep["error"] = (
                f"leg exceeded --leg-timeout {a.leg_timeout}s during phase {phase}; killed"
            )
            break
        if rc == EXIT_REFUSED:
            rep["status"] = "refused"
            rep["error"] = result.get("refused")
            break
        if rc == EXIT_UNAVAILABLE:
            rep["status"] = "unavailable"
            rep["error"] = result.get("unavailable")
            rep["unavailable_in_phase"] = phase
            break
        if rc != 0:
            rep["status"] = "failed"
            how = f"exited {rc}"
            if rc < 0:
                rep["signal"] = signal.Signals(-rc).name
                how = f"was killed by {rep['signal']} (no Python traceback)"
                cause = leg.crash_cause(plan, phase, result.get("in_flight"))
                if cause:
                    how += f": {cause}"
            rep["error"] = result.get("error") or (
                f"phase {phase} {how}; see {log.relative_to(a.work_dir)} in the work dir"
            )
            break
    rep["elapsed_s"] = time.monotonic() - start
    rep["summary"] = summarize_rep(rep["phases"])
    if rep["status"] == "ok" and not rep["summary"]["correct"]:
        rep["status"] = "failed"
        rep["error"] = "read-back checks failed"
    return rep


def add_headroom_args(p):
    p.add_argument(
        "--max-load1",
        type=float,
        default=2.0,
        help="refuse to time anything while the 1-minute load average is above this",
    )
    p.add_argument(
        "--min-mem-gib",
        type=float,
        default=16.0,
        help="refuse to time anything with less available memory than this",
    )
    p.add_argument(
        "--settle-timeout",
        type=float,
        default=None,
        help="seconds the gate waits for the machine to become idle before refusing "
        "(default 180; 0 with --smoke)",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="downgrade a headroom refusal to a warning; the results are tagged "
        "headroom_violated and summarize.py will not render them by default",
    )


def apply_smoke_defaults(a):
    """A smoke run checks the setup rather than timing anything, so a busy
    machine only warns and the gate does not wait for load to settle."""
    if a.smoke:
        a.force = True
    if a.settle_timeout is None:
        a.settle_timeout = 0.0 if a.smoke else 180.0


def fmt_s(v):
    return "-" if v is None else f"{v:.2f}s"


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "_phase":
        return phase_main(sys.argv[2:])

    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--scenario", default="uuid", choices=sorted(SCENARIOS))
    p.add_argument(
        "--smoke",
        action="store_true",
        help="use the scenario's smoke scale for every knob not given explicitly; "
        "smoke runs are not timed results, so a load or memory shortfall at the "
        "headroom gate is a warning, as with --force",
    )
    for knob, typ in sorted(all_knobs().items()):
        p.add_argument(
            "--" + knob.replace("_", "-"),
            type=typ,
            default=None,
            help="scenario knob; the README lists which scenario reads it and its default",
        )
    p.add_argument(
        "--verify-sample",
        type=int,
        default=1_000,
        help="keys checked per category (updated, inserted, deleted, untouched)",
    )
    p.add_argument(
        "--legs",
        default="hudi-cow,hudi-mor,hudi-cow-noindex",
        help="comma-separated registered legs",
    )
    p.add_argument(
        "--extra-legs",
        action="append",
        default=[],
        metavar="PATH",
        help="load additional leg modules from a path; each defines register(api) "
        "(repeatable)",
    )
    p.add_argument(
        "--out",
        default=None,
        help="results file (default: <work-dir>/smoke-<scenario>.json with --smoke, "
        "results.json otherwise)",
    )
    p.add_argument(
        "--work-dir",
        default=os.environ.get(
            "UPSERT_BENCH_WORK_DIR",
            str(Path(tempfile.gettempdir()) / "hudi-upsert-bench"),
        ),
    )
    p.add_argument("--seed", type=int, default=42)
    p.add_argument(
        "--repeat",
        type=int,
        default=3,
        choices=range(1, 6),
        metavar="N",
        help="run each leg N times (1 to 5), one repetition after another, each "
        "from a fresh table; timings are aggregated across repetitions (default 3)",
    )
    p.add_argument(
        "--leg-timeout",
        type=float,
        default=0,
        help="wall-clock budget per leg repetition in seconds; 0 disables",
    )
    p.add_argument(
        "--keep-tables",
        action="store_true",
        help="keep each leg's table after the run (removed by default to save disk)",
    )
    p.add_argument(
        "--reclaim",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="after each commit, outside the timed region, delete data files the "
        "current snapshot no longer references (default: on; always off for "
        "scenarios that read old versions)",
    )
    p.add_argument("--label", default=None, help="free-form note stored in the results")
    add_headroom_args(p)
    a = p.parse_args()

    legs = [x.strip() for x in a.legs.split(",") if x.strip()]
    a.extra_legs = [str(Path(x).resolve()) for x in a.extra_legs]
    load_extra_legs(a.extra_legs)
    unknown = [x for x in legs if x not in LEGS]
    if unknown:
        p.error(f"unknown legs {unknown}; choose from {sorted(LEGS)}")

    sc = SCENARIOS[a.scenario]
    params = resolve_params(sc, a, a.smoke)
    if sc.incremental_reads:
        # Time travel and incremental reads need the superseded files.
        a.reclaim = False

    apply_smoke_defaults(a)
    work = Path(a.work_dir).resolve()
    work.mkdir(parents=True, exist_ok=True)
    a.work_dir = work
    out = (
        Path(a.out)
        if a.out
        else (work / f"smoke-{sc.name}.json" if a.smoke else Path("results.json"))
    )
    bd, wd = workload_paths(sc, work, params, a.seed, a.verify_sample)
    slug = run_slug(bd, wd)
    with RunLock(work, slug):
        results = run(a, sc, params, legs, work, slug, out)
    print(f"wrote {out}")
    failed = [
        n
        for n, r in results["legs"].items()
        if r["status"] in ("failed", "timeout", "refused")
    ]
    return 1 if failed else 0


def run(a, sc, params, legs, work, slug, out):
    started = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")
    t0 = time.perf_counter()
    # Workloads at different knobs share a base table; building it is guarded
    # by its own lock.
    bd, _ = workload_paths(sc, work, params, a.seed, a.verify_sample)
    with RunLock(work, f"base-{bd.parent.name}"):
        bd, wd = ensure_workload(sc, work, params, a.seed, a.verify_sample)
    prep_s = time.perf_counter() - t0
    plan = json.loads((wd / "plan.json").read_text())
    a.base_bytes = sum(f.stat().st_size for f in bd.glob("part-*.parquet"))
    log_dir = work / "logs" / slug
    log_dir.mkdir(parents=True, exist_ok=True)

    results = {
        "schema_version": RESULTS_SCHEMA_VERSION,
        "harness": "benchmark/upsert/bench.py",
        "scenario": sc.name,
        "started_at": started,
        "label": a.label,
        "params": {
            "scenario": sc.name,
            "smoke": a.smoke,
            **params,
            "verify_sample": a.verify_sample,
            "seed": a.seed,
            "leg_timeout_s": a.leg_timeout,
            "repeat": a.repeat,
            "reclaim": a.reclaim,
            "extra_legs": [Path(x).name for x in a.extra_legs],
            "headroom_limits": {
                "max_load1": a.max_load1,
                "min_mem_available_gib": a.min_mem_gib,
                "min_disk_free": f"{DISK_FACTOR} x base dataset ({a.base_bytes} bytes)",
                "settle_timeout_s": a.settle_timeout,
                "force": a.force,
            },
        },
        "plan": plan,
        "data_prep_s": prep_s,
        "machine": machine_info(work),
        "legs": {},
    }
    out.parent.mkdir(parents=True, exist_ok=True)
    print(f"{sc.name}: {plan['scale']}, {plan['base_rows']:,} base rows", flush=True)
    for name in legs:
        run_one(name, a, plan, bd, wd, log_dir, results, out)
    return results


def run_one(name, a, plan, bd, wd, log_dir, results, out):
    rec = run_leg(name, a, plan, bd, wd, log_dir)
    results["legs"][name] = rec
    results["headroom_violated"] = any(
        r.get("headroom_violated")
        for leg in results["legs"].values()
        for r in all_readings(leg)
    )
    out.write_text(json.dumps(results, indent=2, default=str))
    s = rec["summary"]

    def med(m):
        st = s["stats"].get(m)
        return fmt_s(st["median"] if st else None)

    if rec["status"] == "unavailable":
        print(f"[{name}] unavailable: {rec['error']}", flush=True)
        return
    print(
        f"[{name}] {rec['status']} over {s['repetitions']}/{a.repeat} repetitions "
        f"(medians): load {med('load_s')}, upsert {med('upsert_mean_s')}, "
        f"delete {med('delete_mean_s')}, readback {med('readback_s')}, "
        f"correct={s['correct']} (sampled={s.get('sampled_correct')}, "
        f"oracle={s.get('oracle_correct')})"
        + (f"\n  error: {rec['error']}" if rec.get("error") else ""),
        flush=True,
    )


if __name__ == "__main__":
    sys.exit(main())
