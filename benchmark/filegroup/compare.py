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

"""Compare two or more fg-bench JSON reports.

Prints a table of the per-iteration metrics with a percent delta of every file
versus the FIRST file (the baseline). Stdlib only.

Only fields fg-bench actually emits are compared. Per-stage timings are not
among them: they live in HoodieReadStats, which is not on the public reader
surface, so no report carries them.

Usage:
    compare.py baseline.json candidate.json [more.json ...]
"""

import json
import sys


# (json_key, display_label) — medianed over the measured (non-warmup)
# iterations, same as wall. Every key here is a field of IterationReport; a
# report written by an older build that lacks one reads as 0 rather than
# raising, so a comparison across builds still runs.
MEDIAN_FIELDS = [
    ("wall_ms", "wall(ms)"),
    ("user_ms", "user(ms)"),
    ("sys_ms", "sys(ms)"),
    ("spill_peak_bytes", "spill(b)"),
]

# Peak rather than median: RSS and spill are high-water marks, so the largest
# value across iterations is the one a memory bound has to hold against.
PEAK_FIELDS = [
    ("max_rss_kb", "rss(kb)"),
]


def _median(values):
    s = sorted(values)
    n = len(s)
    if n == 0:
        return 0
    if n % 2 == 1:
        return s[n // 2]
    return (s[n // 2 - 1] + s[n // 2]) / 2


def load(path):
    with open(path) as f:
        report = json.load(f)
    measured = [it for it in report["iterations"] if not it.get("warmup")]
    if not measured:  # only a warmup iteration exists
        measured = report["iterations"]

    row = {
        "file": path,
        "rows": measured[0]["rows"] if measured else 0,
        "contended": report.get("contended", False),
        "spilled": any(it.get("spilled", False) for it in measured),
    }
    for key, _ in MEDIAN_FIELDS:
        row[key] = _median([it.get(key, 0) for it in measured])
    for key, _ in PEAK_FIELDS:
        row[key] = max((it.get(key, 0) for it in measured), default=0)
    return row


def pct(base, val):
    if base == 0:
        return "  n/a" if val == 0 else "  +inf"
    return f"{(val - base) / base * 100:+6.1f}%"


def main(argv):
    if len(argv) < 3:
        print(__doc__)
        return 1
    rows = [load(p) for p in argv[1:]]
    base = rows[0]

    metrics = MEDIAN_FIELDS + PEAK_FIELDS

    info_label = "rows/spill/cont"
    name_w = max(len("metric"), len(info_label), max(len(m[1]) for m in metrics))
    col_w = 22

    header = "metric".ljust(name_w)
    for i, r in enumerate(rows):
        tag = "baseline" if i == 0 else f"file{i}"
        header += "  " + f"{tag}".ljust(col_w)
    print(header)

    legend = " " * name_w
    for i, r in enumerate(rows):
        legend += "  " + r["file"][-col_w:].ljust(col_w)
    print(legend)
    print("-" * len(header))

    for key, label in metrics:
        line = label.ljust(name_w)
        b = base[key]
        for i, r in enumerate(rows):
            v = r[key]
            cell = f"{v}" if i == 0 else f"{v} ({pct(b, v)})"
            line += "  " + cell.ljust(col_w)
        print(line)

    print("-" * len(header))
    info = info_label.ljust(name_w)
    for r in rows:
        info += "  " + f"{r['rows']} / {r['spilled']} / {r['contended']}".ljust(col_w)
    print(info)

    if any(r["contended"] for r in rows):
        print("\nWARNING: at least one report was flagged CONTENDED — numbers unreliable.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
