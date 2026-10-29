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

"""Leg registration, modules loaded with --extra-legs, and the Hudi leg's
capability checks, which decide unavailable-versus-failed before any write."""

import textwrap
from pathlib import Path

import bench
import pyarrow as pa
import pytest

HUDI_LEGS = {"hudi-cow", "hudi-mor", "hudi-cow-noindex"}


@pytest.fixture
def registry(monkeypatch):
    monkeypatch.setattr(bench, "LEGS", dict(bench.LEGS))
    return bench.LEGS


def test_built_in_legs_are_the_hudi_legs():
    assert set(bench.LEGS) == HUDI_LEGS
    assert all(isinstance(f(), bench.Leg) for f in bench.LEGS.values())


def test_register_leg_rejects_a_duplicate_name(registry):
    with pytest.raises(ValueError, match="already registered"):
        bench.register_leg("hudi-cow", lambda: None)


def write_module(tmp_path, body):
    path = tmp_path / "more_legs.py"
    path.write_text(textwrap.dedent(body))
    return path


def test_extra_legs_module_registers_through_the_api(tmp_path, registry):
    path = write_module(
        tmp_path,
        """
        def register(api):
            class Probe(api.Leg):
                name = "probe"

                def load(self, root, table, plan):
                    raise api.LegUnavailable("probe only")

            api.register_leg("probe", Probe)
        """,
    )
    bench.load_extra_legs([path])
    leg = registry["probe"]()
    assert isinstance(leg, bench.Leg)
    with pytest.raises(bench.LegUnavailable, match="probe only"):
        leg.load(None, None, None)
    assert leg.crash_cause({}, "upsert", None) is None


def test_extra_legs_module_without_register_is_refused(tmp_path, registry):
    path = write_module(tmp_path, "X = 1\n")
    with pytest.raises(SystemExit, match="register"):
        bench.load_extra_legs([path])


class FakeHudi:
    def __init__(self, error):
        self.error = error
        test = self

        class HudiTable:
            @staticmethod
            def create(path, record_key):
                test.created = (path, record_key)
                return HudiTable()

            def append(self, table):
                if test.error:
                    raise test.error

        self.HudiTable = HudiTable


COMPOSITE = {"key_cols": ["a", "b"], "ordering": None}


def test_hudi_composite_key_probe_rejection_is_unavailable(tmp_path):
    leg = bench.HudiLeg("hudi-cow", "cow", True)
    with pytest.raises(bench.LegUnavailable, match=r"apache/hudi-rs#871"):
        leg._check_capabilities(FakeHudi(ValueError("no")), tmp_path / "t", COMPOSITE)


def test_hudi_composite_key_probe_success_writes_a_two_field_key_and_cleans_up(
    tmp_path,
):
    leg = bench.HudiLeg("hudi-cow", "cow", True)
    fake = FakeHudi(None)
    leg._check_capabilities(fake, tmp_path / "t", COMPOSITE)
    path, record_key = fake.created
    assert path.endswith("-key-probe") and record_key == ["k1", "k2"]
    assert not Path(path).exists()


def test_hudi_single_key_needs_no_probe(tmp_path):
    bench.HudiLeg("hudi-cow", "cow", True)._check_capabilities(
        None, tmp_path / "t", {"key_cols": ["id"]}
    )


def test_hudi_record_keys_use_the_complex_key_form_for_composite_keys():
    leg = bench.HudiLeg("hudi-cow", "cow", True)
    keys = pa.table({"a": pa.array([1, 2], pa.int64()), "b": ["x", "y"]})
    leg.plan = COMPOSITE
    assert leg.record_keys(keys) == ["a:1,b:x", "a:2,b:y"]
    leg.plan = {"key_cols": ["a"]}
    assert leg.record_keys(keys) == ["1", "2"]
