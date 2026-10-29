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

"""Per-commit byte accounting, the reclaim guard, the headroom gate and the
run lock: the parts that decide what a number means or whether it is taken."""

import argparse
import os
import time
from pathlib import Path

import bench
import pytest


def classify(rel):
    return "meta" if rel.parts[0] == "_log" else "data"


def test_diff_counts_new_grown_and_removed_files():
    before = {Path("a.parquet"): 100, Path("b.parquet"): 50, Path("_log/0.json"): 10}
    after = {
        Path("b.parquet"): 80,
        Path("c.parquet"): 300,
        Path("_log/0.json"): 10,
        Path("_log/1.json"): 20,
    }
    d = bench.diff(before, after, classify)
    assert d["data_bytes_written"] == 300 + 30
    assert d["meta_bytes_written"] == 20
    assert d["bytes_written"] == 350
    assert (d["data_files_created"], d["meta_files_created"]) == (1, 1)
    assert d["data_files_removed"] == 1
    assert d["files_modified_in_place"] == 1
    assert d["table_data_files_after"] == 2
    assert d["table_bytes_after"] == 410


def test_diff_never_counts_a_shrunk_file_as_written():
    d = bench.diff({Path("a"): 100}, {Path("a"): 40}, classify)
    assert d["bytes_written"] == 0


class FakeLeg:
    def __init__(self, live):
        self.live = live

    def live_data_files(self, root):
        return self.live

    classify = staticmethod(classify)


def touch(root, rel, size):
    p = root / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(b"x" * size)


def test_reclaim_deletes_only_unreferenced_data_files(tmp_path):
    for rel, size in (("old.parquet", 5), ("live.parquet", 7), ("_log/0.json", 3)):
        touch(tmp_path, rel, size)
    r = bench.reclaim(
        FakeLeg({Path("live.parquet")}), tmp_path, bench.snapshot(tmp_path)
    )
    assert r == {"reclaimed_files": 1, "reclaimed_bytes": 5}
    assert sorted(p.name for p in tmp_path.rglob("*") if p.is_file()) == [
        "0.json",
        "live.parquet",
    ]


def test_reclaim_refuses_when_the_live_listing_is_empty(tmp_path):
    touch(tmp_path, "a.parquet", 5)
    with pytest.raises(RuntimeError, match="empty"):
        bench.reclaim(FakeLeg(set()), tmp_path, bench.snapshot(tmp_path))
    assert (tmp_path / "a.parquet").exists()


def limits(**kw):
    base = {
        "max_load1": 2.0,
        "min_mem_gib": 16.0,
        "settle_timeout": 30.0,
        "force": False,
    }
    return argparse.Namespace(**(base | kw))


def fake_headroom(monkeypatch, *violation_sets):
    seq = list(violation_sets)

    def headroom(work_dir, base_bytes, lim):
        v = seq.pop(0) if len(seq) > 1 else seq[0]
        return {
            "load1": 9.0,
            "mem_available_bytes": 1,
            "disk_free_bytes": 1,
            "violations": dict(v),
        }

    monkeypatch.setattr(bench, "headroom", headroom)
    monkeypatch.setattr(bench.time, "sleep", lambda s: None)


def test_gate_does_not_wait_on_memory_shortfall(monkeypatch):
    fake_headroom(monkeypatch, {"memory": "low", "load1": "busy"})
    start = time.monotonic()
    with pytest.raises(bench.HeadroomRefused, match="low"):
        bench.gate(".", 1, limits(), "x")
    assert time.monotonic() - start < 5


def test_gate_waits_for_load_to_settle(monkeypatch):
    fake_headroom(monkeypatch, {"load1": "busy"}, {"load1": "busy"}, {})
    r = bench.gate(".", 1, limits(), "x")
    assert not r["violations"]
    assert "headroom_violated" not in r


def test_force_downgrades_load_but_not_disk(monkeypatch):
    fake_headroom(monkeypatch, {"load1": "busy"})
    r = bench.gate(".", 1, limits(force=True, settle_timeout=0), "x")
    assert r["headroom_violated"]
    fake_headroom(monkeypatch, {"disk": "full"})
    with pytest.raises(bench.HeadroomRefused, match="full"):
        bench.gate(".", 1, limits(force=True, settle_timeout=0), "x")


def test_run_lock_refuses_a_second_run_and_releases(tmp_path):
    with bench.RunLock(tmp_path, "s"):
        with pytest.raises(SystemExit, match="holds"):
            bench.RunLock(tmp_path, "s").__enter__()
    assert not (tmp_path / "locks" / "s.lock").exists()


def test_run_lock_takes_over_a_stale_lock(tmp_path):
    lock = tmp_path / "locks" / "s.lock"
    lock.parent.mkdir()
    pid = 2**22 + 12345
    while bench.psutil.pid_exists(pid):
        pid += 1
    lock.write_text(str(pid))
    with bench.RunLock(tmp_path, "s"):
        assert lock.read_text() == str(os.getpid())
