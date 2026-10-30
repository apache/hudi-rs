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

"""Case registration, modules loaded with --extra-cases, and the Hudi case's
record key form."""

import textwrap

import bench
import pyarrow as pa
import pytest

HUDI_CASES = {"hudi-cow", "hudi-mor", "hudi-cow-noindex"}


@pytest.fixture
def registry(monkeypatch):
    monkeypatch.setattr(bench, "CASES", dict(bench.CASES))
    return bench.CASES


def test_built_in_cases_are_the_hudi_cases():
    assert set(bench.CASES) == HUDI_CASES
    assert all(isinstance(f(), bench.Case) for f in bench.CASES.values())


def test_register_case_rejects_a_duplicate_name(registry):
    with pytest.raises(ValueError, match="already registered"):
        bench.register_case("hudi-cow", lambda: None)


def write_module(tmp_path, body):
    path = tmp_path / "more_cases.py"
    path.write_text(textwrap.dedent(body))
    return path


def test_extra_cases_module_registers_through_the_api(tmp_path, registry):
    path = write_module(
        tmp_path,
        """
        def register(api):
            class Probe(api.Case):
                name = "probe"

                def load(self, root, table, plan):
                    raise api.CaseUnavailable("probe only")

            api.register_case("probe", Probe)
        """,
    )
    bench.load_extra_cases([path])
    case = registry["probe"]()
    assert isinstance(case, bench.Case)
    with pytest.raises(bench.CaseUnavailable, match="probe only"):
        case.load(None, None, None)
    assert case.crash_cause({}, "upsert", None) is None


def test_extra_cases_module_without_register_is_refused(tmp_path, registry):
    path = write_module(tmp_path, "X = 1\n")
    with pytest.raises(SystemExit, match="register"):
        bench.load_extra_cases([path])


COMPOSITE = {"key_cols": ["a", "b"], "ordering": None}


def test_hudi_record_keys_use_the_complex_key_form_for_composite_keys():
    case = bench.HudiCase("hudi-cow", "cow", True)
    keys = pa.table({"a": pa.array([1, 2], pa.int64()), "b": ["x", "y"]})
    case.plan = COMPOSITE
    assert case.record_keys(keys) == ["a:1,b:x", "a:2,b:y"]
    case.plan = {"key_cols": ["a"]}
    assert case.record_keys(keys) == ["1", "2"]
