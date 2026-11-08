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

"""Key strings, the sampled read-back check, and knob resolution."""

import argparse

import bench
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from scenarios import KEY_COL, KIND_COL, SCENARIOS, key_strings, resolve_params


def test_key_strings_single_column_is_the_value_as_string():
    t = pa.table({"id": pa.array([3, 1], pa.int64())})
    assert key_strings(t, ["id"]).to_pylist() == ["3", "1"]


def test_key_strings_composite_joins_in_key_order():
    t = pa.table({"a": pa.array([1, 2], pa.int64()), "b": ["x", "y"]})
    assert key_strings(t, ["a", "b"]).to_pylist() == ["1|x", "2|y"]
    assert key_strings(t, ["b", "a"]).to_pylist() == ["x|1", "y|2"]


def test_key_strings_accepts_view_types():
    t = pa.table({"id": pa.array(["k"], pa.string_view())})
    assert key_strings(t, ["id"]).to_pylist() == ["k"]


PLAN = {
    "key_cols": ["id"],
    "check_cols": ["fare"],
    "expected_rows": 3,
    "verify_counts": {"updated": 1, "inserted": 1, "deleted": 1, "untouched": 1},
}


@pytest.fixture
def wd(tmp_path):
    verify = pa.table(
        {
            KEY_COL: ["a", "b", "gone", "c"],
            "fare": pa.array([1.5, 2.5, None, 3.5]),
            KIND_COL: ["updated", "inserted", "deleted", "untouched"],
        }
    )
    pq.write_table(verify, tmp_path / "verify.parquet")
    return tmp_path


def table(ids, fares):
    return pa.table({"id": ids, "fare": pa.array(fares, pa.float64())})


def test_sampled_check_passes_on_expected_state(wd):
    c = bench.check_readback(table(["c", "a", "b"], [3.5, 1.5, 2.5]), wd, PLAN)
    assert c["passed"]
    assert c["sample_failures"] == {
        "updated": 0,
        "inserted": 0,
        "deleted": 0,
        "untouched": 0,
    }


def test_sampled_check_flags_stale_update(wd):
    c = bench.check_readback(table(["a", "b", "c"], [9.0, 2.5, 3.5]), wd, PLAN)
    assert not c["passed"]
    assert c["sample_failures"]["updated"] == 1
    assert c["failure_examples"][0]["key"] == "a"


def test_sampled_check_flags_resurrected_delete(wd):
    c = bench.check_readback(table(["a", "b", "gone"], [1.5, 2.5, 0.0]), wd, PLAN)
    assert not c["passed"]
    assert c["sample_failures"]["deleted"] == 1
    assert c["sample_failures"]["untouched"] == 1


def test_sampled_check_flags_duplicate_key(wd):
    c = bench.check_readback(
        table(["a", "b", "c", "a"], [1.5, 2.5, 3.5, 1.5]), wd, PLAN
    )
    assert not c["passed"]
    assert not c["keys_unique_ok"]
    assert not c["row_count_ok"]


def ns(**kw):
    return argparse.Namespace(**kw)


def test_resolve_params_defaults_smoke_and_overrides():
    sc = SCENARIOS["uuid"]
    unset = {k: None for k in sc.knobs}
    assert resolve_params(sc, ns(**unset), smoke=False)["rows"] == 10_000_000
    assert resolve_params(sc, ns(**unset), smoke=True)["rows"] == 100_000
    p = resolve_params(sc, ns(**(unset | {"rows": 5})), smoke=True)
    assert p["rows"] == 5
    assert p["commits"] == sc.knobs["commits"][1]


def test_resolve_params_ignores_other_scenarios_knobs():
    sc = SCENARIOS["rag-chunks"]
    p = resolve_params(sc, ns(rows=7, docs=None), smoke=True)
    assert "rows" not in p
    assert p["docs"] == 1_000
