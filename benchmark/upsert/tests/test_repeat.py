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

"""Aggregation across --repeat repetitions, and the results shape a run
produces, end to end through a plain-parquet leg loaded with --extra-legs."""

import json
import statistics
import subprocess
import sys
import textwrap
from pathlib import Path

import bench
import pytest

HERE = Path(__file__).resolve().parent.parent


def test_stats_odd_count():
    st = bench.stats([3.0, 1.0, 2.0])
    assert (st["n"], st["mean"], st["median"], st["min"], st["max"]) == (
        3,
        2.0,
        2.0,
        1.0,
        3.0,
    )
    assert st["cv"] == pytest.approx(statistics.stdev([1, 2, 3]) / 2.0)


def test_stats_even_count_median_is_the_midpoint():
    st = bench.stats([4.0, 1.0, 2.0, 3.0])
    assert st["median"] == 2.5
    assert st["mean"] == 2.5


def test_stats_single_value_has_no_cv_and_skips_none():
    st = bench.stats([None, 5.0])
    assert (st["n"], st["median"], st["cv"]) == (1, 5.0, None)
    assert bench.stats([None]) is None


def test_stats_cv_is_zero_for_identical_values():
    assert bench.stats([2.0, 2.0])["cv"] == 0.0


def commit(c, data_bytes, data_files=1):
    return {
        "commit": c,
        "data_bytes_written": data_bytes,
        "meta_bytes_written": 10,
        "data_files_created": data_files,
        "meta_files_created": 1,
    }


def rep(r, upsert_bytes, upsert_files=1):
    return {
        "rep": r,
        "phases": {
            "load": {
                "wall_s": 1.0,
                "data_bytes_written": 100,
                "meta_bytes_written": 5,
                "data_files_created": 1,
                "meta_files_created": 1,
            },
            "upsert": {
                "commits": [commit(0, upsert_bytes, upsert_files), commit(1, 7)]
            },
        },
    }


def test_byte_drift_within_one_percent_is_not_recorded():
    # 1,000 vs 1,010 differs by 0.99% of the larger value; 1,011 would be 1.09%.
    assert bench.write_mismatches([rep(1, 1_000), rep(2, 1_010), rep(3, 995)]) == []


def test_byte_difference_above_one_percent_is_flagged():
    assert bench.write_mismatches([rep(1, 1_000), rep(2, 1_011), rep(3, 1_000)]) == [
        {
            "rep": 2,
            "step": "upsert commit 0",
            "field": "data_bytes_written",
            "rep1": 1_000,
            "this": 1_011,
        }
    ]


def test_any_file_count_difference_is_flagged():
    assert bench.write_mismatches([rep(1, 1_000), rep(2, 1_000, upsert_files=2)]) == [
        {
            "rep": 2,
            "step": "upsert commit 0",
            "field": "data_files_created",
            "rep1": 1,
            "this": 2,
        }
    ]


def test_write_mismatches_needs_two_repetitions():
    assert bench.write_mismatches([rep(1, 50)]) == []


PARQUET_LEG = '''
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pathlib import Path


def register(api):
    class ParquetLeg(api.Leg):
        """Copy-on-write over one parquet file per version."""

        name = "parquet"

        def _write(self, table):
            self.version = getattr(self, "version", -1) + 1
            pq.write_table(table, self.root / f"v{self.version:03d}.parquet")

        def _latest(self):
            return sorted(self.root.glob("v*.parquet"))[-1]

        def load(self, root, table, plan):
            self.root = Path(root)
            self._write(table)
            return {"marker": self.version}

        def open(self, root, plan):
            self.root, self.plan = Path(root), plan
            self.version = int(self._latest().stem[1:])

        def _keep(self, cur, keys):
            k = self.plan["key_cols"][0]
            return cur.filter(pc.invert(pc.is_in(cur[k], value_set=keys[k])))

        def upsert(self, batch):
            cur = pq.read_table(self._latest())
            self._write(pa.concat_tables([self._keep(cur, batch), batch.cast(cur.schema)]))
            return {}

        def delete(self, keys):
            self._write(self._keep(pq.read_table(self._latest()), keys))
            return {}

        def read(self):
            return pq.read_table(self._latest())

        def live_data_files(self, root):
            return {self._latest().relative_to(root)}

    api.register_leg("parquet", ParquetLeg)
'''


def run_bench(tmp_path, repeat):
    tmp_path.mkdir(parents=True, exist_ok=True)
    plugin = tmp_path / "parquet_leg.py"
    plugin.write_text(textwrap.dedent(PARQUET_LEG))
    out = tmp_path / f"r{repeat}.json"
    cmd = [
        sys.executable,
        str(HERE / "bench.py"),
        "--scenario", "uuid",
        "--rows", "2000",
        "--commits", "2",
        "--upsert-batch", "100",
        "--insert-batch", "10",
        "--delete-batch", "20",
        "--verify-sample", "20",
        "--legs", "parquet",
        "--extra-legs", str(plugin),
        "--repeat", str(repeat),
        "--force",
        "--settle-timeout", "0",
        "--work-dir", str(tmp_path / "work"),
        "--out", str(out),
    ]  # fmt: skip
    p = subprocess.run(cmd, capture_output=True, text=True, cwd=HERE)
    assert p.returncode == 0, p.stdout + p.stderr
    return json.loads(out.read_text())


def keys(d):
    return set(d) if isinstance(d, dict) else set()


@pytest.fixture(scope="module")
def runs(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("repeat")
    return run_bench(tmp / "one", 1), run_bench(tmp / "two", 2)


def test_repeat_one_and_two_produce_the_same_shape(runs):
    one, two = runs
    assert keys(one) == keys(two)
    l1, l2 = one["legs"]["parquet"], two["legs"]["parquet"]
    assert keys(l1) == keys(l2)
    assert keys(l1["summary"]) == keys(l2["summary"])
    assert keys(l1["summary"]["stats"]) == keys(l2["summary"]["stats"])
    assert keys(l1["repetitions"][0]) == keys(l2["repetitions"][1])
    assert (len(l1["repetitions"]), len(l2["repetitions"])) == (1, 2)


def test_every_repetition_is_checked_and_aggregated(runs):
    _, two = runs
    leg = two["legs"]["parquet"]
    assert leg["status"] == "ok"
    for r in leg["repetitions"]:
        checks = r["phases"]["readback"]["checks"]
        assert checks["sampled_passed"] and checks["oracle"]["passed"]
    s = leg["summary"]
    assert s["correct"] and s["repetitions"] == 2
    assert s["stats"]["upsert_mean_s"]["n"] == 2
    upsert_means = [r["summary"]["upsert_mean_s"] for r in leg["repetitions"]]
    assert s["stats"]["upsert_mean_s"]["mean"] == pytest.approx(
        statistics.fmean(upsert_means)
    )
    assert s["writes_consistent"], s["write_mismatches"]
    assert two["params"]["repeat"] == 2
