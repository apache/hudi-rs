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

"""Turn one or more bench.py results files into a table. Stdlib only.

A file whose run was forced past the headroom gate (headroom_violated) is
refused unless --allow-loaded is passed, so loaded-machine timings do not end up
in a table by accident.

Every leg of every file becomes one row, labelled with the scale it ran at, so
results from different scales can sit in one table without being mistaken for
each other. Timings show the median (or, with --stat mean, the mean) across
repetitions with the min to max range, and a note flags any timing whose
coefficient of variation across repetitions is above 10 percent.

Usage:
    summarize.py results.json [more.json ...] [--markdown] [--stat median|mean]
                 [--allow-loaded]
"""

import argparse
import json
import sys

MIB = 1024 * 1024

COLUMNS = [
    "Scenario",
    "Scale",
    "Leg",
    "Status",
    "Reps",
    "Load",
    "Upsert / commit",
    "Delete / commit",
    "Read-back",
    "Incremental read (rows)",
    "Time travel",
    "Upsert MiB/commit",
    "Delete MiB/commit",
    "Data files/upsert",
    "Peak RSS load (MiB)",
    "Peak RSS write (MiB)",
    "Peak RSS read (MiB)",
    "Correct",
]
TEXT_COLUMNS = 4
CV_NOTE = 0.10
TIMING_COLUMNS = {
    "load_s": "load",
    "upsert_mean_s": "upsert per commit",
    "delete_mean_s": "delete per commit",
    "readback_s": "read-back",
    "incremental_mean_s": "incremental read",
    "time_travel_mean_s": "time travel",
}


def _s(v):
    return "-" if v is None else f"{v:.2f} s"


def timing(st, stat):
    """The chosen statistic with the range across repetitions, e.g.
    ``1.23 s (1.20 to 1.31)``; a single repetition shows no range."""
    if not st:
        return "-"
    if st["n"] == 1:
        return _s(st[stat])
    return f"{st[stat]:.2f} s ({st['min']:.2f} to {st['max']:.2f})"


def _mib(v):
    return "-" if v is None else f"{v / MIB:,.0f}"


def _correct(s):
    if s.get("correct"):
        return "pass"
    sampled = "pass" if s.get("sampled_correct") else "FAIL"
    oracle = "pass" if s.get("oracle_correct") else "FAIL"
    return f"FAIL (sampled {sampled}, oracle {oracle})"


def readings(leg):
    """Every headroom reading the gate took for a leg; the per-reading
    headroom_violated flag is the only one the harness records."""
    out = []
    for rep in leg.get("repetitions", []):
        if rep.get("headroom_before"):
            out.append(rep["headroom_before"])
        for ph in rep.get("phases", {}).values():
            if ph.get("headroom"):
                out.append(ph["headroom"])
            out += [c["headroom"] for c in ph.get("commits", []) if c.get("headroom")]
    return out


def violated(leg):
    return any(r.get("headroom_violated") for r in readings(leg))


def leg_row(name, leg, scenario, scale, stat):
    s = leg.get("summary", {})
    st = s.get("stats", {})
    by_phase = s.get("peak_rss_bytes_by_phase", {})
    write_rss = [by_phase[p] for p in ("upsert", "delete") if p in by_phase]
    files = s.get("upsert_data_files_created_per_commit")
    ok = leg.get("status") == "ok"
    inc = timing(st.get("incremental_mean_s"), stat)
    if inc != "-" and "incremental_rows_mean" in s:
        inc += f" ({s['incremental_rows_mean']:,.0f})"
    ran = any("readback" in r.get("phases", {}) for r in leg.get("repetitions", []))
    return [
        scenario,
        scale,
        name,
        leg.get("status", "?"),
        f"{s.get('repetitions', 0)}/{leg.get('repeat', '?')}",
        timing(st.get("load_s"), stat),
        timing(st.get("upsert_mean_s"), stat),
        timing(st.get("delete_mean_s"), stat),
        timing(st.get("readback_s"), stat),
        inc,
        timing(st.get("time_travel_mean_s"), stat),
        _mib(s.get("upsert_bytes_per_commit")),
        _mib(s.get("delete_bytes_per_commit")),
        "-" if files is None else f"{files:g}",
        _mib(by_phase.get("load")) if ok else "-",
        _mib(max(write_rss)) if write_rss and ok else "-",
        _mib(by_phase.get("readback")) if ok else "-",
        _correct(s) if ok or ran else "-",
    ]


def leg_notes(scenario, name, leg):
    s = leg.get("summary", {})
    out = []
    noisy = [
        f"{label} {st['cv']:.0%}"
        for key, label in TIMING_COLUMNS.items()
        if (st := s.get("stats", {}).get(key)) and st.get("cv") and st["cv"] > CV_NOTE
    ]
    if noisy:
        out.append(
            f"{scenario} {name}: coefficient of variation across repetitions above "
            f"{CV_NOTE:.0%} for " + ", ".join(noisy)
        )
    if s.get("writes_consistent") is False:
        m = s["write_mismatches"][0]
        out.append(
            f"{scenario} {name}: bytes or files written differ between repetitions "
            f"({len(s['write_mismatches'])} differences; first: repetition {m['rep']} "
            f"{m['step']} {m.get('field', '')} {m.get('rep1')} vs {m.get('this')})"
        )
    return out


def render(rows, markdown):
    if markdown:
        out = [
            "| " + " | ".join(COLUMNS) + " |",
            "|"
            + "|".join(
                "---" if i < TEXT_COLUMNS else "---:" for i in range(len(COLUMNS))
            )
            + "|",
        ]
        out += ["| " + " | ".join(r) + " |" for r in rows]
        return "\n".join(out)
    widths = [max(len(str(x)) for x in col) for col in zip(COLUMNS, *rows)]

    def line(r):
        return "  ".join(str(x).ljust(w) for x, w in zip(r, widths))

    return "\n".join(
        [line(COLUMNS), line(["-" * w for w in widths])] + [line(r) for r in rows]
    )


def machine_line(path, res):
    m = res.get("machine", {})
    plan = res.get("plan", {})
    pk = m.get("packages", {})
    versions = ", ".join(
        f"{k} {v}" for k, v in pk.items() if v and k in ("pyarrow", "hudi")
    )
    mem = m.get("memory_bytes")
    commits = plan.get("commits", res.get("params", {}).get("commits"))
    return (
        f"{path}: {res['scenario']} at {res['plan']['scale']}, {commits} upsert + {commits} "
        f"delete commits; {m.get('cpu', m.get('machine', '?'))}, "
        f"{m.get('cpu_count', '?')} cpus, {(mem or 0) / 1024**3:.0f} GiB, "
        f"{m.get('platform', '?')}; {versions}"
    )


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("files", nargs="+")
    ap.add_argument("--markdown", action="store_true")
    ap.add_argument(
        "--stat",
        choices=("median", "mean"),
        default="median",
        help="statistic shown for timings across repetitions (default median)",
    )
    ap.add_argument(
        "--allow-loaded",
        action="store_true",
        help="render files whose runs were forced past the headroom gate",
    )
    a = ap.parse_args()

    rows, notes, errors = [], [], []
    for path in a.files:
        with open(path) as f:
            res = json.load(f)
        if not a.allow_loaded and any(
            violated(leg) for leg in res.get("legs", {}).values()
        ):
            print(
                f"{path}: run was forced past the headroom gate (the machine was not "
                "idle); its timings are not comparable. Pass --allow-loaded to render "
                "it anyway.",
                file=sys.stderr,
            )
            return 2
        scenario, scale = res["scenario"], res["plan"]["scale"]
        notes.append(machine_line(path, res))
        if res.get("label"):
            notes.append(f"{path}: {res['label']}")
        for name, leg in res.get("legs", {}).items():
            rows.append(leg_row(name, leg, scenario, scale, a.stat))
            notes += leg_notes(scenario, name, leg)
            h = leg.get("summary", {}).get("headroom")
            if h:
                mem = h.get("min_mem_available_bytes")
                notes.append(
                    f"{scenario} {name} headroom over {h['readings']} readings: "
                    f"max load1 {h['max_load1']:.2f}, min available memory "
                    f"{'-' if mem is None else f'{mem / 1024**3:.1f} GiB'}, min free disk "
                    f"{h['min_disk_free_bytes'] / 1024**3:.1f} GiB"
                    + (" (HEADROOM VIOLATED)" if violated(leg) else "")
                )
            if leg.get("error"):
                errors.append(f"{scenario} {name} ({leg['status']}): {leg['error']}")

    print(render(rows, a.markdown))
    print()
    print(
        ("- " if a.markdown else "")
        + f"Timings: {a.stat} across repetitions, with min to max in parentheses; "
        "upsert and delete use each repetition's mean over its commits."
    )
    bullet = "- " if a.markdown else ""
    for line in notes:
        print(bullet + line)
    if errors:
        print()
        for line in errors:
            print(bullet + line)
    return 0


if __name__ == "__main__":
    sys.exit(main())
