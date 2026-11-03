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
import subprocess
import sys
import threading
import time
import types
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


class FakeCase:
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
        FakeCase({Path("live.parquet")}), tmp_path, bench.snapshot(tmp_path)
    )
    assert r == {"reclaimed_files": 1, "reclaimed_bytes": 5}
    assert sorted(p.name for p in tmp_path.rglob("*") if p.is_file()) == [
        "0.json",
        "live.parquet",
    ]


def test_reclaim_refuses_when_the_live_listing_is_empty(tmp_path):
    touch(tmp_path, "a.parquet", 5)
    with pytest.raises(RuntimeError, match="empty"):
        bench.reclaim(FakeCase(set()), tmp_path, bench.snapshot(tmp_path))
    assert (tmp_path / "a.parquet").exists()


def limits(**kw):
    base = {
        "max_load": 2.0,
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
            "other_load": 9.0,
            "load1": 9.0,
            "mem_available_bytes": 1,
            "disk_free_bytes": 1,
            "violations": dict(v),
        }

    monkeypatch.setattr(bench, "headroom", headroom)
    monkeypatch.setattr(bench.time, "sleep", lambda s: None)


def test_gate_does_not_wait_on_memory_shortfall(monkeypatch):
    fake_headroom(monkeypatch, {"memory": "low", "load": "busy"})
    start = time.monotonic()
    with pytest.raises(bench.HeadroomRefused, match="low"):
        bench.gate(".", 1, limits(), "x")
    assert time.monotonic() - start < 5


def test_gate_waits_for_load_to_settle(monkeypatch):
    fake_headroom(monkeypatch, {"load": "busy"}, {"load": "busy"}, {})
    r = bench.gate(".", 1, limits(), "x")
    assert not r["violations"]
    assert "headroom_violated" not in r


def test_force_downgrades_load_but_not_disk(monkeypatch):
    fake_headroom(monkeypatch, {"load": "busy"})
    r = bench.gate(".", 1, limits(force=True, settle_timeout=0), "x")
    assert r["headroom_violated"]
    fake_headroom(monkeypatch, {"disk": "full"})
    with pytest.raises(bench.HeadroomRefused, match="full"):
        bench.gate(".", 1, limits(force=True, settle_timeout=0), "x")


def test_forced_gate_does_not_wait_for_load_to_settle(monkeypatch):
    calls = []

    def headroom(work_dir, base_bytes, lim):
        calls.append(1)
        return {
            "other_load": 9.0,
            "load1": 9.0,
            "mem_available_bytes": 1,
            "disk_free_bytes": 1,
            "violations": {"load": "busy"},
        }

    monkeypatch.setattr(bench, "headroom", headroom)
    monkeypatch.setattr(bench.time, "sleep", lambda s: pytest.fail("gate slept"))
    r = bench.gate(".", 1, limits(force=True, settle_timeout=180), "x")
    assert r["headroom_violated"] and len(calls) == 1


@pytest.mark.parametrize(
    "smoke, given, force, settle",
    [
        (True, None, True, 0.0),
        (True, 30.0, True, 30.0),
        (False, None, False, 180.0),
        (False, 5.0, False, 5.0),
    ],
)
def test_smoke_defaults(smoke, given, force, settle):
    a = argparse.Namespace(smoke=smoke, settle_timeout=given, force=False)
    bench.apply_smoke_defaults(a)
    assert (a.force, a.settle_timeout) == (force, settle)


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


def test_busy_cores_excludes_the_benchmarks_own_cpu():
    # 10 s apart: the system was busy for 50 core-seconds, 30 of them this process's.
    assert bench.busy_cores((0.0, 100.0, 5.0), (10.0, 150.0, 35.0)) == 2.0
    # Own CPU time is sampled separately from system time, so it can come out
    # slightly ahead; the reading never goes negative.
    assert bench.busy_cores((0.0, 100.0, 5.0), (10.0, 110.0, 16.0)) == 0.0


def test_headroom_judges_other_process_load_not_load1(monkeypatch, tmp_path):
    windows = []

    def fake_other_load(window):
        windows.append(window)
        return load, window

    monkeypatch.setattr(bench.os, "getloadavg", lambda: (50.0, 50.0, 50.0))
    monkeypatch.setattr(bench, "other_load", fake_other_load)
    load = 1.0
    r = bench.headroom(tmp_path, 1, limits(min_mem_gib=0))
    assert "load" not in r["violations"] and r["load1"] == 50.0
    load = 3.0
    assert "load" in bench.headroom(tmp_path, 1, limits(min_mem_gib=0))["violations"]
    # A forced run only warns on load, so it does not wait for a full window.
    bench.headroom(tmp_path, 1, limits(min_mem_gib=0, force=True))
    assert windows == [bench.LOAD_WINDOW_S, bench.LOAD_WINDOW_S, 1.0]


def test_other_load_reads_the_newest_sample_a_full_window_old(monkeypatch):
    load = bench.OtherLoad.__new__(bench.OtherLoad)
    load.lock = threading.Lock()
    # One sample a second; the system ran 3 cores busy, 1 of them this process.
    load.samples = [(float(t), 3.0 * t, 1.0 * t) for t in range(0, 11)]
    monkeypatch.setattr(
        bench.OtherLoad, "sample", staticmethod(lambda: (10.5, 31.5, 10.5))
    )
    assert load.read(10.0) == (2.0, 10.5)
    assert load.read(1.0) == (2.0, 1.5)


def test_own_cpu_counts_a_reaped_child():
    before = bench.own_cpu_s()
    subprocess.run(
        [
            sys.executable,
            "-c",
            "import time\nt=time.process_time()\nwhile time.process_time()-t<0.3: pass",
        ],
        check=True,
    )
    assert bench.own_cpu_s() - before >= 0.25


class _Child:
    def __init__(self, *outcomes):
        self.outcomes = list(outcomes)

    def cpu_times(self):
        o = self.outcomes.pop(0)
        if isinstance(o, Exception):
            raise o
        return types.SimpleNamespace(user=o, system=0.0)


def fake_tree(monkeypatch, child, reaped):
    monkeypatch.setattr(
        bench.psutil,
        "Process",
        lambda: types.SimpleNamespace(children=lambda recursive: [child]),
    )
    monkeypatch.setattr(bench, "reaped_cpu_s", lambda: reaped.pop(0))
    monkeypatch.setattr(bench.time, "sleep", lambda s: None)


def test_own_cpu_walks_again_when_a_child_is_reaped_mid_walk(monkeypatch):
    # The child's 5 s show up in neither sum on the first walk.
    child = _Child(5.0, 7.0)
    fake_tree(monkeypatch, child, [10.0, 15.0, 15.0, 15.0])
    me = bench.resource.getrusage(bench.resource.RUSAGE_SELF)
    got = bench.own_cpu_s() - (me.ru_utime + me.ru_stime)
    assert got == pytest.approx(22.0, abs=0.5)


def test_own_cpu_walks_again_past_an_unreaped_child(monkeypatch):
    child = _Child(bench.psutil.ZombieProcess(1), 7.0)
    fake_tree(monkeypatch, child, [10.0, 15.0, 15.0])
    me = bench.resource.getrusage(bench.resource.RUSAGE_SELF)
    assert bench.own_cpu_s() - (me.ru_utime + me.ru_stime) == pytest.approx(
        22.0, abs=0.5
    )


def test_own_cpu_gives_up_rather_than_undercount(monkeypatch):
    child = _Child(*[bench.psutil.ZombieProcess(1)] * 3)
    fake_tree(monkeypatch, child, [10.0] * 6)
    assert bench.own_cpu_s(attempts=3) is None


def test_sampler_skips_a_failed_sample(monkeypatch):
    load = bench.OtherLoad.__new__(bench.OtherLoad)
    load.lock = threading.Lock()
    load.samples = [(1.0, 1.0, 1.0)]

    def boom():
        raise OSError("transient")

    for sample in (boom, lambda: None):
        monkeypatch.setattr(bench.OtherLoad, "sample", staticmethod(sample))
        load._record()
    assert load.samples == [(1.0, 1.0, 1.0)]
