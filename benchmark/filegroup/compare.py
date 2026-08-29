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

Prints a table of median wall, per-stage timing breakdown, and peak RSS, with a
percent delta of every file versus the FIRST file (the baseline). Stdlib only.

Usage:
    compare.py baseline.json candidate.json [more.json ...]
"""

import json
import sys


# (json_key, display_label) — stage timings are medianed over measured
# (non-warmup) iterations, same as wall.
STAGE_FIELDS = [
    ("base_read_ms", "base_read"),
    ("log_block_read_ms", "log_read"),
    ("log_block_decode_ms", "log_decode"),
    ("merge_insert_ms", "merge_ins"),
    ("final_merge_ms", "final_mrg"),
    ("output_build_ms", "out_build"),
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
        "wall_ms": _median([it["wall_ms"] for it in measured]),
        "max_rss_kb": max((it["max_rss_kb"] for it in measured), default=0),
        "rows": measured[0]["rows"] if measured else 0,
        "contended": report.get("contended", False),
    }
    for key, _ in STAGE_FIELDS:
        row[key] = _median([it["read_stats"][key] for it in measured])
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

    metrics = [("wall_ms", "wall(ms)"), ("max_rss_kb", "rss(kb)")] + [
        (k, lbl) for k, lbl in STAGE_FIELDS
    ]

    name_w = max(len("metric"), max(len(m[1]) for m in metrics))
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
    info = "rows/contended".ljust(name_w)
    for r in rows:
        info += "  " + f"{r['rows']} / {r['contended']}".ljust(col_w)
    print(info)

    if any(r["contended"] for r in rows):
        print("\nWARNING: at least one report was flagged CONTENDED — numbers unreliable.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
