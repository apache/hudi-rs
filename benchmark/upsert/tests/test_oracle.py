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

"""The full-table oracle comparison decides whether a leg is correct.

It has to accept every representation a correct leg legitimately returns (rows
in any order, engine meta columns, view/large types, renamed list items) and
reject every wrong end state, naming the key and both values.
"""

from decimal import Decimal

import bench
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest
from scenarios import oracle_schema

PLAN = {"key_cols": ["id"], "oracle": "oracle/expected.parquet"}


@pytest.fixture
def oracle(tmp_path):
    t = pa.table(
        {
            "id": pa.array([f"k{i:03d}" for i in range(200)]),
            "fare": pa.array([i * 1.25 for i in range(200)], pa.float64()),
            "status": pa.array(["a", "b"] * 100),
            "emb": pa.array(
                [[float(i), float(i) / 3] for i in range(200)], pa.list_(pa.float32())
            ),
        }
    )
    t = t.cast(oracle_schema(t.schema))
    (tmp_path / "oracle").mkdir()
    pq.write_table(t, tmp_path / "oracle" / "expected.parquet")
    return tmp_path, pq.read_table(tmp_path / "oracle" / "expected.parquet")


def compare(work, table):
    return bench.compare_to_oracle(table, work, PLAN)


def with_column(t, name, values):
    i = t.schema.get_field_index(name)
    return t.set_column(i, t.schema.field(i), pa.array(values, t.schema.field(i).type))


def test_identical_table_passes(oracle):
    work, exp = oracle
    r = compare(work, exp)
    assert r["passed"]
    assert r["rows_expected"] == r["rows_read"] == 200


def test_shuffled_rows_pass(oracle):
    work, exp = oracle
    assert compare(work, exp.take(pa.array(list(range(199, -1, -1)))))["passed"]


def test_engine_meta_column_is_dropped(oracle):
    work, exp = oracle
    t = exp.append_column("_hoodie_commit_time", pa.array(["x"] * exp.num_rows))
    r = compare(work, t)
    assert r["passed"]
    assert r["dropped_engine_columns"] == ["_hoodie_commit_time"]


def test_large_and_renamed_types_pass(oracle):
    work, exp = oracle
    t = exp.cast(
        pa.schema(
            [
                pa.field("id", pa.large_string()),
                pa.field("fare", pa.float64()),
                pa.field("status", pa.string_view()),
                pa.field("emb", pa.list_(pa.field("element", pa.float32()))),
            ]
        )
    )
    assert compare(work, t)["passed"]


def test_changed_float_is_reported_with_both_values(oracle):
    work, exp = oracle
    fares = exp.column("fare").to_pylist()
    fares[7] += 0.01
    r = compare(work, with_column(exp, "fare", fares))
    assert not r["passed"]
    assert r["mismatches"] == [
        {
            "column": "fare",
            "rows": [{"key": "k007", "expected": "8.75", "got": repr(8.75 + 0.01)}],
        }
    ]


def test_changed_list_element_names_the_element(oracle):
    work, exp = oracle
    emb = exp.column("emb").to_pylist()
    emb[3][1] = 99.0
    r = compare(work, with_column(exp, "emb", emb))
    assert not r["passed"]
    row = r["mismatches"][0]["rows"][0]
    assert r["mismatches"][0]["column"] == "emb"
    assert (row["key"], row["element"], row["got"]) == ("k003", 1, "99.0")


def test_changed_string_field_is_reported(oracle):
    work, exp = oracle
    status = exp.column("status").to_pylist()
    status[10] = "z"
    r = compare(work, with_column(exp, "status", status))
    assert r["mismatches"][0]["rows"] == [
        {"key": "k010", "expected": "'a'", "got": "'z'"}
    ]


def test_dropped_row_is_a_missing_key(oracle):
    work, exp = oracle
    r = compare(work, exp.slice(1))
    assert not r["passed"]
    assert r["mismatches"][0]["keys_missing"] == 1
    assert r["mismatches"][0]["keys_missing_examples"] == ["k000"]


def test_duplicate_row_is_reported(oracle):
    work, exp = oracle
    r = compare(work, pa.concat_tables([exp, exp.slice(5, 1)]))
    assert not r["passed"]
    assert r["mismatches"][0]["duplicate_keys"] == 1


def test_extra_key_is_reported(oracle):
    work, exp = oracle
    extra = exp.slice(0, 1).set_column(
        0, exp.schema.field(0), pa.array(["zzz"], exp.schema.field(0).type)
    )
    r = compare(work, pa.concat_tables([exp.slice(1), extra]))
    assert r["mismatches"][0]["keys_extra_examples"] == ["zzz"]


def test_missing_or_extra_column_fails(oracle):
    work, exp = oracle
    assert compare(work, exp.drop_columns(["status"]))["mismatches"] == [
        {"columns_missing": ["status"], "columns_extra": []}
    ]
    t = exp.append_column("other", pa.array([1] * exp.num_rows))
    assert not compare(work, t)["passed"]


def test_null_where_value_expected_fails(oracle):
    work, exp = oracle
    fares = exp.column("fare").to_pylist()
    fares[0] = None
    assert not compare(work, with_column(exp, "fare", fares))["passed"]


def test_first_diffs_spans_slices_and_honors_limit():
    n = 70_000
    keys = pa.array([str(i) for i in range(n)])
    want = pa.array(range(n), pa.int64())
    got = pc.add(
        want, pa.array([1 if i in (3, 65_540, 69_999) else 0 for i in range(n)])
    )
    diffs = bench._first_diffs(keys, got, want, limit=2)
    assert [d["key"] for d in diffs] == ["3", "65540"]


@pytest.fixture
def typed_oracle(tmp_path):
    t = pa.table(
        {
            "id": ["a", "b"],
            "f": pa.array([1.5, 2.5], pa.float64()),
            "i": pa.array([1, 2], pa.int64()),
            "d": pa.array([Decimal("1.25"), Decimal("2.50")], pa.decimal128(7, 2)),
            "t": pa.array([1_000_000, 2_000_000], pa.timestamp("us")),
        }
    )
    (tmp_path / "oracle").mkdir()
    pq.write_table(
        t.cast(oracle_schema(t.schema)), tmp_path / "oracle" / "expected.parquet"
    )
    return tmp_path, pq.read_table(tmp_path / "oracle" / "expected.parquet")


@pytest.mark.parametrize(
    "column, read_as",
    [
        ("f", pa.string()),
        ("i", pa.int32()),
        ("d", pa.float64()),
        ("t", pa.timestamp("ms")),
        ("t", pa.timestamp("us", tz="UTC")),
    ],
)
def test_read_back_type_must_match_exactly(typed_oracle, column, read_as):
    work, exp = typed_oracle
    i = exp.schema.get_field_index(column)
    t = exp.set_column(i, column, exp.column(column).cast(read_as))
    r = compare(work, t)
    assert not r["passed"]
    assert r["mismatches"] == [
        {
            "column": column,
            "expected_type": str(exp.schema.field(column).type),
            "got_type": str(read_as),
        }
    ]


def test_sampled_check_also_requires_exact_types(tmp_path):
    verify = pa.table(
        {"__key": ["a"], "fare": pa.array([1.5]), "__kind": ["untouched"]}
    )
    pq.write_table(verify, tmp_path / "verify.parquet")
    plan = {"key_cols": ["id"], "check_cols": ["fare"], "expected_rows": 1}
    got = pa.table({"id": ["a"], "fare": pa.array([1.5], pa.float32())})
    c = bench.check_readback(got, tmp_path, plan)
    assert not c["passed"]
    assert c["type_mismatches"][0]["got_type"] == "float"


def test_oracle_strings_use_64_bit_offsets():
    # A 10M-row text column passes 2 GiB, past what 32-bit string offsets index
    # once the oracle is sorted or its chunks combined.
    s = oracle_schema(
        pa.schema(
            [
                pa.field("a", pa.string()),
                pa.field("b", pa.binary_view()),
                pa.field("c", pa.list_(pa.string())),
            ]
        )
    )
    assert s.field("a").type == pa.large_string()
    assert s.field("b").type == pa.large_binary()
    assert s.field("c").type == pa.list_(pa.large_string())
