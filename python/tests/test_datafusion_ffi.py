#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

"""Reads through the FFI table provider, against the real DataFusion Python package.

This is the one path where a version skew between the `datafusion-ffi` this
crate is built against and the `datafusion` wheel installed alongside it shows
up, and it shows up as an ABI mismatch rather than a compile error, so nothing
else in the suite would catch it.
"""

import pytest

datafusion = pytest.importorskip("datafusion")

from hudi import HudiTable  # noqa: E402
from hudi._internal import HudiDataFusionDataSource, get_test_table_path  # noqa: E402


@pytest.fixture
def v6_table() -> str:
    return get_test_table_path("v6_simplekeygen_nonhivestyle", "cow")


def test_datafusion_ffi_reads_through_the_table_provider(v6_table):
    ctx = datafusion.SessionContext()
    ctx.register_table("hudi_t", HudiDataFusionDataSource(v6_table))

    rows = []
    for batch in ctx.sql(
        'SELECT id, name, "isActive" FROM hudi_t ORDER BY id'
    ).collect():
        columns = batch.to_pydict()
        for i in range(batch.num_rows):
            rows.append((columns["id"][i], columns["name"][i], columns["isActive"][i]))

    assert rows == [
        (1, "Alice", False),
        (2, "Bob", False),
        (3, "Carol", True),
        (4, "Diana", True),
    ]


def test_datafusion_ffi_pushes_the_filter_into_the_provider(v6_table):
    ctx = datafusion.SessionContext()
    ctx.register_table("hudi_t", HudiDataFusionDataSource(v6_table))

    batch = ctx.sql("SELECT count(*) AS n FROM hudi_t WHERE id % 2 = 0").collect()[0]
    assert batch.to_pydict()["n"] == [2]


def test_datafusion_ffi_plan_goes_through_the_ffi_boundary(v6_table):
    ctx = datafusion.SessionContext()
    ctx.register_table("hudi_t", HudiDataFusionDataSource(v6_table))

    plans = []
    for batch in ctx.sql("EXPLAIN SELECT id FROM hudi_t").collect():
        columns = batch.to_pydict()
        for i in range(batch.num_rows):
            if columns["plan_type"][i] == "physical_plan":
                plans.append(columns["plan"][i])

    assert any("FFI_ExecutionPlan" in plan for plan in plans), plans


def test_datafusion_ffi_reads_a_lance_table():
    # Lance's I/O scheduler spawns onto the current tokio runtime. The provider's
    # stream is polled from datafusion-python's runtime, which the binding's copy
    # of tokio cannot see, so without a handle on the FFI provider this read
    # aborted the interpreter. A local Parquet table never asks for a runtime,
    # which is why the tests above did not catch it.
    lance_table = get_test_table_path("v9_lance_nonpartitioned", "cow")
    expected = sorted(
        (row["id"], row["name"])
        for batch in HudiTable(lance_table).read()
        for row in batch.to_pylist()
    )
    assert expected, "fixture should hold rows"

    ctx = datafusion.SessionContext()
    ctx.register_table("lance_t", HudiDataFusionDataSource(lance_table))
    got = sorted(
        (row["id"], row["name"])
        for batch in ctx.sql("SELECT id, name FROM lance_t").collect()
        for row in batch.to_pylist()
    )
    assert got == expected
