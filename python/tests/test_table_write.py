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

"""Tests for the HudiTable write API."""

import datetime
import math
from decimal import Decimal

import pyarrow as pa
import pytest

from hudi import (
    HudiCompactionResult,
    HudiCoreError,
    HudiQueryType,
    HudiReadOptions,
    HudiTable,
    HudiWriteResult,
)

SCHEMA = pa.schema(
    [
        ("id", pa.string()),
        ("city", pa.string()),
        ("ts", pa.int64()),
        ("fare", pa.float64()),
    ]
)


def trips(*rows):
    ids, cities, ts, fares = zip(*rows)
    return pa.table({"id": ids, "city": cities, "ts": ts, "fare": fares}, schema=SCHEMA)


def snapshot(table):
    """Map record key to (city, ts, fare) for the current snapshot."""
    read = pa.Table.from_batches(table.read())
    return {
        r["id"]: (r["city"], r["ts"], r["fare"])
        for r in read.select(["id", "city", "ts", "fare"]).to_pylist()
    }


def new_table(tmp_path, table_type="cow"):
    table = HudiTable.create(
        str(tmp_path / "trips"),
        record_key="id",
        partition_by="city",
        ordering_fields="ts",
        table_type=table_type,
    )
    table.append(
        trips(("t1", "sf", 1, 30.0), ("t2", "sf", 1, 45.0), ("t3", "nyc", 1, 20.0))
    )
    return table


class StreamOnly:
    """Exposes only the Arrow PyCapsule stream interface, like polars or pandas."""

    def __init__(self, table):
        self._table = table

    def __arrow_c_stream__(self, requested_schema=None):
        return self._table.__arrow_c_stream__(requested_schema)


class ArrayOnly:
    """Exposes only the Arrow PyCapsule array interface for one record batch."""

    def __init__(self, batch):
        self._batch = batch

    def __arrow_c_array__(self, requested_schema=None):
        return self._batch.__arrow_c_array__(requested_schema)


@pytest.mark.parametrize(
    "table_type, expected_type",
    [
        ("cow", "COPY_ON_WRITE"),
        ("Merge_On_Read", "MERGE_ON_READ"),
    ],
)
def test_create_table_types(tmp_path, table_type, expected_type):
    base = tmp_path / "trips"
    table = HudiTable.create(str(base), record_key=["id"], table_type=table_type)
    assert table.table_type == expected_type
    assert table.is_mor == (expected_type == "MERGE_ON_READ")
    assert table.table_name == "trips"
    assert (base / ".hoodie" / "hoodie.properties").is_file()
    assert (base / ".hoodie" / "metadata" / "record_index").is_dir()


def test_create_with_explicit_options(tmp_path):
    base = tmp_path / "plain"
    table = HudiTable.create(
        str(base) + "/",
        record_key="id",
        partition_by=["city"],
        table_name="renamed",
        table_version=8,
        metadata_table_enabled=False,
        hive_style_partitioning=False,
        hudi_options={"hoodie.table.timeline.timezone": "UTC"},
    )
    assert table.table_name == "renamed"
    assert table.hudi_options()["hoodie.table.version"] == "8"
    assert not (base / ".hoodie" / "metadata").exists()
    table.append(trips(("t1", "sf", 1, 30.0)))
    assert (base / "sf").is_dir()


def test_create_rejects_bad_table_type(tmp_path):
    with pytest.raises(ValueError, match="invalid table_type 'parquet'"):
        HudiTable.create(str(tmp_path / "t"), record_key="id", table_type="parquet")
    assert not (tmp_path / "t").exists()


def test_create_on_existing_table_raises(tmp_path):
    HudiTable.create(str(tmp_path / "t"), record_key="id")
    with pytest.raises(HudiCoreError, match="already exists"):
        HudiTable.create(str(tmp_path / "t"), record_key="id")


@pytest.mark.parametrize(
    "flags, error",
    [
        (
            dict(metadata_table_enabled=False, record_index_enabled=True),
            "record_index requires the metadata",
        ),
        (
            dict(metadata_table_enabled=False, column_stats_enabled=True),
            "column_stats requires the metadata",
        ),
        # The explicit partition_stats choice is applied after column_stats,
        # so it is reported rather than silently turned off.
        (
            dict(column_stats_enabled=False, partition_stats_enabled=True),
            "partition_stats requires",
        ),
    ],
)
def test_create_rejects_inconsistent_index_options(tmp_path, flags, error):
    with pytest.raises(HudiCoreError, match=error):
        HudiTable.create(str(tmp_path / "t"), record_key="id", **flags)
    assert not (tmp_path / "t").exists()


def test_create_index_options(tmp_path):
    base = tmp_path / "t"
    table = HudiTable.create(
        str(base),
        record_key="id",
        partition_by="city",
        record_index_enabled=False,
        column_stats_enabled=True,
        partition_stats_enabled=False,
    )
    partitions = table.hudi_options()["hoodie.table.metadata.partitions"].split(",")
    assert "column_stats" in partitions
    assert "partition_stats" not in partitions
    assert "record_index" not in partitions
    # Without the record index, upserts tag records through the simple index.
    table.append(trips(("t1", "sf", 1, 30.0)))
    assert table.upsert(trips(("t1", "sf", 2, 31.0))).num_updates == 1
    assert snapshot(table) == {"t1": ("sf", 2, 31.0)}


def test_create_with_field_lists_and_storage_options(tmp_path):
    table = HudiTable.create(
        str(tmp_path / "t"),
        record_key=["id"],
        partition_by=["city", "ts"],
        ordering_fields=["ts"],
        storage_options={"allow_http": "true"},
    )
    options = table.hudi_options()
    assert options["hoodie.table.recordkey.fields"] == "id"
    assert options["hoodie.table.partition.fields"] == "city,ts"
    assert options["hoodie.table.ordering.fields"] == "ts"
    assert table.storage_options()["allow_http"] == "true"
    table.append(trips(("t1", "sf", 1, 30.0)))
    assert (tmp_path / "t" / "city=sf" / "ts=1").is_dir()


@pytest.mark.parametrize("table_type", ["cow", "mor"])
def test_append_then_read(tmp_path, table_type):
    table = HudiTable.create(
        str(tmp_path / "trips"),
        record_key="id",
        partition_by="city",
        table_type=table_type,
    )
    res = table.append(trips(("t1", "sf", 1, 30.0), ("t2", "nyc", 1, 45.0)))
    assert isinstance(res, HudiWriteResult)
    assert res.requested_time
    assert (res.num_writes, res.num_inserts, res.num_updates, res.num_deletes) == (
        2,
        2,
        0,
        0,
    )
    assert "num_inserts=2" in repr(res)
    assert f"requested_time='{res.requested_time}'" in repr(res)
    assert f"completion_time='{res.completion_time}'" in repr(res)
    with pytest.raises(AttributeError):
        res.num_inserts = 0
    assert snapshot(table) == {"t1": ("sf", 1, 30.0), "t2": ("nyc", 1, 45.0)}
    # A fresh handle sees the same committed data.
    assert snapshot(HudiTable(str(tmp_path / "trips"))) == snapshot(table)


@pytest.mark.parametrize("table_type", ["cow", "mor"])
def test_upsert_updates_and_inserts(tmp_path, table_type):
    table = new_table(tmp_path, table_type)
    res = table.upsert(trips(("t2", "sf", 2, 50.0), ("t4", "nyc", 2, 15.0)))
    assert (res.num_updates, res.num_inserts, res.num_deletes) == (1, 1, 0)
    assert snapshot(table) == {
        "t1": ("sf", 1, 30.0),
        "t2": ("sf", 2, 50.0),
        "t3": ("nyc", 1, 20.0),
        "t4": ("nyc", 2, 15.0),
    }


def test_upsert_keeps_higher_ordering_value(tmp_path):
    table = new_table(tmp_path)
    table.upsert(trips(("t1", "sf", 0, 99.0)))
    assert snapshot(table)["t1"] == ("sf", 1, 30.0)


def test_upsert_update_columns(tmp_path):
    table = new_table(tmp_path)
    res = table.upsert(trips(("t1", "sf", 5, 31.0)), update_columns=["fare"])
    assert res.num_updates == 1
    assert snapshot(table)["t1"] == ("sf", 1, 31.0)


def test_completion_time_bounds_incremental_reads(tmp_path):
    table = HudiTable.create(str(tmp_path / "t"), record_key="id", ordering_fields="ts")
    base = table.append(trips(("t1", "sf", 1, 30.0), ("t2", "sf", 1, 45.0)))
    assert base.completion_time is not None
    assert base.completion_time > base.requested_time
    upsert = table.upsert(trips(("t2", "sf", 2, 50.0), ("t3", "nyc", 2, 15.0)))
    assert upsert.completion_time > base.completion_time
    no_op = table.delete_keys(["missing"])
    assert (no_op.requested_time, no_op.completion_time) == (None, None)
    assert "completion_time=None" in repr(no_op)

    def changed_after(bound):
        batches = table.read(
            HudiReadOptions()
            .with_query_type(HudiQueryType.Incremental)
            .with_start_timestamp(bound)
        )
        return sorted(
            id_ for batch in batches for id_ in batch.column("id").to_pylist()
        )

    # The bound is compared against completion times: the requested time of
    # the base commit admits the base commit itself.
    assert changed_after(base.completion_time) == ["t2", "t3"]
    assert changed_after(base.requested_time) == ["t1", "t2", "t3"]
    assert changed_after(upsert.completion_time) == []


@pytest.mark.parametrize("table_type", ["cow", "mor"])
def test_update_with_dict(tmp_path, table_type):
    table = new_table(tmp_path, table_type)
    # An int value lands in the float64 column.
    res = table.update(predicate="city = 'nyc'", updates={"fare": 99})
    assert res.num_updates == 1
    assert snapshot(table)["t3"] == ("nyc", 1, 99.0)
    assert snapshot(table)["t1"] == ("sf", 1, 30.0)


@pytest.mark.parametrize("table_type", ["cow", "mor"])
def test_update_with_batch(tmp_path, table_type):
    table = new_table(tmp_path, table_type)
    set_batch = pa.record_batch(
        {"fare": pa.array([7.5]), "ts": pa.array([3], pa.int64())}
    )
    table.update("id = 't1'", set_batch)
    assert snapshot(table)["t1"] == ("sf", 3, 7.5)
    table.update("id = 't2'", pa.table({"fare": [8.5]}))
    assert snapshot(table)["t2"] == ("sf", 1, 8.5)
    table.update("id = 't3'", [pa.record_batch({"fare": [9.5]})])
    assert snapshot(table)["t3"] == ("nyc", 1, 9.5)


def test_update_set_batches_must_hold_one_row(tmp_path):
    table = new_table(tmp_path)
    with pytest.raises(ValueError, match="got no batches"):
        table.update("id = 't1'", [])
    two = [pa.record_batch({"fare": [1.0]}), pa.record_batch({"fare": [2.0]})]
    with pytest.raises(HudiCoreError, match="single-row"):
        table.update("id = 't1'", two)
    assert snapshot(table)["t1"] == ("sf", 1, 30.0)


def test_update_dict_null_and_nan(tmp_path):
    table = new_table(tmp_path)
    table.update("id = 't1'", {"fare": None})
    assert snapshot(table)["t1"] == ("sf", 1, None)
    table.update("id = 't2'", {"fare": float("nan")})
    assert math.isnan(snapshot(table)["t2"][2])


def test_update_no_match_creates_no_commit(tmp_path):
    table = new_table(tmp_path)
    commits = len(table.get_timeline().get_completed_commits())
    res = table.update("city = 'la'", {"fare": 1.0})
    assert (res.requested_time, res.num_updates) == (None, 0)
    assert "requested_time=None" in repr(res)
    assert len(table.get_timeline().get_completed_commits()) == commits


def test_update_rejects_bad_set(tmp_path):
    table = new_table(tmp_path)
    with pytest.raises(
        ValueError, match="update column 'nope' is not in the table schema"
    ):
        table.update("city = 'nyc'", {"nope": 1})
    with pytest.raises(
        ValueError, match="cannot convert update value for column 'fare'"
    ):
        table.update("city = 'nyc'", {"fare": "not a number"})
    with pytest.raises(ValueError, match="at least one column"):
        table.update("city = 'nyc'", {})


def test_update_dict_values_must_fit_column_type(tmp_path):
    table = HudiTable.create(str(tmp_path / "t"), record_key="id")
    table.append(
        pa.table(
            {
                "id": ["t1"],
                "n": pa.array([1], pa.int64()),
                "x": pa.array([1.0], pa.float64()),
                "f": pa.array([1.0], pa.float32()),
                "amount": pa.array([Decimal("1.00")], pa.decimal128(10, 2)),
            }
        )
    )

    def row():
        return (
            pa.Table.from_batches(table.read())
            .select(["n", "x", "amount"])
            .to_pylist()[0]
        )

    table.update("id = 't1'", {"x": 3, "amount": 2})
    table.update("id = 't1'", {"n": 4.0})
    # float32 rounding is accepted; only finite to inf is not.
    table.update("id = 't1'", {"f": 0.1})
    assert row() == {"n": 4, "x": 3.0, "amount": Decimal("2.00")}

    for set_, column in [
        ({"n": 1.5}, "n"),
        ({"amount": 1.234}, "amount"),
        ({"amount": Decimal("1.234")}, "amount"),
        ({"n": "7"}, "n"),
        ({"f": 1e300}, "f"),
        ({"x": True}, "x"),
    ]:
        with pytest.raises(ValueError, match=f"column '{column}'"):
            table.update("id = 't1'", set_)
    assert row() == {"n": 4, "x": 3.0, "amount": Decimal("2.00")}


def test_update_dict_nested_and_numpy_values_must_fit(tmp_path):
    np = pytest.importorskip("numpy")
    table = HudiTable.create(str(tmp_path / "t"), record_key="id")
    table.append(
        pa.table(
            {
                "id": ["t1"],
                "n": pa.array([1], pa.int64()),
                "st": pa.array([{"k": 1}], pa.struct([("k", pa.int64())])),
                "xs": pa.array([[1]], pa.list_(pa.int64())),
            }
        )
    )
    table.update("id = 't1'", {"st": {"k": 3}, "xs": [np.int64(4), 5.0]})

    for set_, column in [
        ({"st": {"k": 2.5}}, "st"),
        ({"xs": [np.float64(7.9)]}, "xs"),
        ({"n": np.True_}, "n"),
        ({"xs": [np.True_]}, "xs"),
    ]:
        with pytest.raises(ValueError, match=f"column '{column}'"):
            table.update("id = 't1'", set_)
    row = pa.Table.from_batches(table.read()).select(["n", "st", "xs"]).to_pylist()
    assert row == [{"n": 1, "st": {"k": 3}, "xs": [4, 5]}]


def _np():
    return pytest.importorskip("numpy")


def _pd():
    return pytest.importorskip("pandas")


_AWARE = datetime.datetime(
    2024, 1, 3, 2, tzinfo=datetime.timezone(datetime.timedelta(hours=2))
)


@pytest.mark.parametrize(
    "make_set, error",
    [
        # Accepted.
        (lambda: {"d": datetime.date(2024, 2, 2)}, None),
        (lambda: {"ms": datetime.datetime(2024, 1, 2, 0, 0, 0, 5000)}, None),
        (lambda: {"utc": datetime.datetime(2024, 1, 2)}, None),
        (lambda: {"utc": _AWARE}, None),
        (lambda: {"n": pa.scalar(5)}, None),
        (lambda: {"st": {"k": pa.scalar(3), "inner": {"j": pa.scalar(2)}}}, None),
        (
            lambda: {
                "tl": [pa.scalar(datetime.datetime(2024, 1, 1), pa.timestamp("ms"))]
            },
            None,
        ),
        (lambda: {"st": {"k": 3}}, None),
        # Temporal columns take only dates and datetimes, at any depth.
        (lambda: {"d": datetime.datetime(2024, 3, 3, 12)}, "column 'd'"),
        (lambda: {"d": 5}, "column 'd'"),
        (lambda: {"utc": 1.7e9}, "column 'utc'"),
        (lambda: {"ms": Decimal(5)}, "column 'ms'"),
        (lambda: {"ms": _np().datetime64("2024-01-01")}, "column 'ms'"),
        (
            lambda: {"tl": _np().array(["2024-01-01"], dtype="datetime64[s]")},
            "column 'tl'",
        ),
        # Sub-unit truncation, including pandas nanoseconds.
        (lambda: {"ms": datetime.datetime(2024, 1, 2, 0, 0, 0, 123456)}, "column 'ms'"),
        (
            lambda: {"utc": _pd().Timestamp("2024-01-01 00:00:00.000000001")},
            "column 'utc'",
        ),
        # A tz-aware datetime into a timestamp without a time zone.
        (lambda: {"ms": _AWARE}, "column 'ms'"),
        # Unknown struct keys, at any depth.
        (lambda: {"st": {"k": 1, "x": 2}}, "column 'st'.*'x'"),
        (lambda: {"st": {"inner": {"j": 1, "y": 2}}}, "column 'st'.*'y'"),
        # pyarrow scalars are unwrapped before the check.
        (lambda: {"n": pa.scalar(1.5)}, "column 'n'"),
        (lambda: {"st": {"k": pa.scalar(2.5)}}, "column 'st'"),
        (lambda: {"tl": [pa.scalar(5)]}, "column 'tl'"),
        # A value pyarrow stores but cannot read back.
        (lambda: {"ms": 10**15}, "read back .*column 'ms'"),
        (lambda: {1: 2}, "must be str"),
    ],
)
def test_update_dict_value_checks(tmp_path, make_set, error):
    set_ = make_set()
    table = HudiTable.create(str(tmp_path / "t"), record_key="id")
    inner = pa.struct([("j", pa.int64())])
    table.append(
        pa.table(
            {
                "id": ["t1"],
                "n": pa.array([1], pa.int64()),
                "d": pa.array([datetime.date(2024, 1, 1)], pa.date32()),
                "ms": pa.array([datetime.datetime(2024, 1, 1)], pa.timestamp("ms")),
                "utc": pa.array(
                    [datetime.datetime(2024, 1, 1)], pa.timestamp("us", "UTC")
                ),
                "tl": pa.array(
                    [[datetime.datetime(2024, 1, 1)]], pa.list_(pa.timestamp("ms"))
                ),
                "st": pa.array(
                    [{"k": 1, "inner": {"j": 1}}],
                    pa.struct([("k", pa.int64()), ("inner", inner)]),
                ),
            }
        )
    )
    if error is None:
        assert table.update("id = 't1'", set_).num_updates == 1
        return
    before = pa.Table.from_batches(table.read()).drop_columns(
        ["_hoodie_commit_time", "_hoodie_commit_seqno"]
    )
    with pytest.raises(ValueError, match=error):
        table.update("id = 't1'", set_)
    after = pa.Table.from_batches(table.read()).drop_columns(
        ["_hoodie_commit_time", "_hoodie_commit_seqno"]
    )
    assert after.equals(before)


@pytest.mark.parametrize("table_type", ["cow", "mor"])
def test_delete_by_key_predicate(tmp_path, table_type):
    table = new_table(tmp_path, table_type)
    res = table.delete("id IN ('t1', 't3')")
    assert res.num_deletes == 2
    assert set(snapshot(table)) == {"t2"}


def test_delete_by_non_key_predicate(tmp_path):
    table = new_table(tmp_path)
    res = table.delete(predicate="fare > 40")
    assert res.num_deletes == 1
    assert set(snapshot(table)) == {"t1", "t3"}
    res = table.delete("fare > 1000")
    assert (res.requested_time, res.num_deletes) == (None, 0)


@pytest.mark.parametrize("table_type", ["cow", "mor"])
def test_delete_keys(tmp_path, table_type):
    table = new_table(tmp_path, table_type)
    res = table.delete_keys(["t1", ("t3", "city=nyc")])
    assert res.num_deletes == 2
    assert set(snapshot(table)) == {"t2"}
    res = table.delete_keys(["missing"])
    assert (res.requested_time, res.num_deletes) == (None, 0)
    res = table.delete_keys(k for k in ("t2",))
    assert res.num_deletes == 1
    assert snapshot(table) == {}


def test_delete_keys_rejects_bad_input(tmp_path):
    table = new_table(tmp_path)
    with pytest.raises(TypeError, match="not a single str"):
        table.delete_keys("t1")
    for bad in ([1], [("t1",)], [("t1", "city=sf", "x")]):
        with pytest.raises(TypeError, match="each key must be a str"):
            table.delete_keys(bad)
    assert len(snapshot(table)) == 3


@pytest.mark.parametrize("table_type", ["cow", "mor"])
def test_overwrite(tmp_path, table_type):
    table = new_table(tmp_path, table_type)
    res = table.overwrite(trips(("t9", "sf", 9, 10.0)))
    assert res.requested_time
    assert snapshot(table) == {"t9": ("sf", 9, 10.0)}


@pytest.mark.parametrize("table_type", ["cow", "mor"])
def test_dynamic_partition_overwrite(tmp_path, table_type):
    table = new_table(tmp_path, table_type)
    res = table.dynamic_partition_overwrite(trips(("t8", "sf", 9, 11.0)))
    assert res.requested_time
    assert snapshot(table) == {"t8": ("sf", 9, 11.0), "t3": ("nyc", 1, 20.0)}


@pytest.mark.parametrize(
    "make_data",
    [
        pytest.param(lambda t: t, id="table"),
        pytest.param(lambda t: t.to_batches()[0], id="record_batch"),
        pytest.param(lambda t: t.to_reader(), id="record_batch_reader"),
        pytest.param(lambda t: t.to_batches(), id="list"),
        pytest.param(StreamOnly, id="arrow_c_stream"),
        pytest.param(
            lambda t: ArrayOnly(t.combine_chunks().to_batches()[0]), id="arrow_c_array"
        ),
        pytest.param(
            lambda t: [ArrayOnly(b) for b in t.to_batches()], id="list_of_arrow_c_array"
        ),
        pytest.param(lambda t: polars().from_arrow(t), id="polars"),
        pytest.param(
            lambda t: t.to_pandas(types_mapper=pandas().ArrowDtype), id="pandas_arrow"
        ),
        pytest.param(lambda t: t.to_pandas(), id="pandas_numpy"),
    ],
)
def test_data_input_types(tmp_path, make_data):
    table = HudiTable.create(str(tmp_path / "t"), record_key="id")
    data = trips(("t1", "sf", 1, 30.0), ("t2", "nyc", 1, 45.0))
    table.append(make_data(data))
    res = table.upsert(make_data(trips(("t2", "nyc", 2, 46.0))))
    assert res.num_updates == 1
    assert snapshot(table) == {"t1": ("sf", 1, 30.0), "t2": ("nyc", 2, 46.0)}


def polars():
    return pytest.importorskip("polars")


def pandas():
    return pytest.importorskip("pandas")


def test_nested_view_large_and_dictionary_columns_are_cast(tmp_path):
    table = HudiTable.create(str(tmp_path / "t"), record_key="id")
    addr = pa.field(
        "addr",
        pa.struct([("city", pa.large_string()), ("zip", pa.int64())]),
        metadata={b"k": b"v"},
    )
    schema = pa.schema(
        [
            ("id", pa.string_view()),
            addr,
            ("kind", pa.dictionary(pa.int8(), pa.large_string())),
        ]
    )

    def rows(*values):
        ids, cities, kinds = zip(*values)
        return pa.table(
            {
                "id": ids,
                "addr": [{"city": c, "zip": 94000} for c in cities],
                "kind": list(kinds),
            },
            schema=schema,
        )

    table.append(rows(("t1", "sf", "x"), ("t2", "nyc", "y")))
    res = table.upsert(rows(("t2", "la", "x")))
    assert res.num_updates == 1
    read = pa.Table.from_batches(table.read()).sort_by("id")
    assert [a["city"] for a in read.column("addr").to_pylist()] == ["sf", "la"]
    assert read.column("kind").to_pylist() == ["x", "x"]


def polars_frame(ids, tags, cities):
    return polars().DataFrame(
        {"id": ids, "tags": tags, "addr": [{"city": c} for c in cities]}
    )


def test_polars_nested_columns_append(tmp_path):
    table = HudiTable.create(str(tmp_path / "t"), record_key="id")
    table.append(polars_frame(["t1", "t2"], [["a", "b"], ["c"]], ["sf", "nyc"]))
    written = table.get_schema()
    assert written.field("tags").type.value_type == pa.string()
    assert written.field("addr").type.field("city").type == pa.string()
    read = pa.Table.from_batches(table.read()).sort_by("id")
    assert read.column("tags").to_pylist() == [["a", "b"], ["c"]]


def test_polars_nested_columns_upsert(tmp_path):
    table = HudiTable.create(str(tmp_path / "t"), record_key="id")
    table.append(polars_frame(["t1", "t2"], [["a", "b"], ["c"]], ["sf", "nyc"]))
    res = table.upsert(polars_frame(["t2"], [["d"]], ["la"]))
    assert res.num_updates == 1
    read = pa.Table.from_batches(table.read()).sort_by("id")
    assert read.column("tags").to_pylist() == [["a", "b"], ["d"]]
    assert [a["city"] for a in read.column("addr").to_pylist()] == ["sf", "la"]


@pytest.mark.parametrize("source", ["pandas", "polars"])
def test_nanosecond_timestamps_are_checked(tmp_path, source):
    # 2024-01-02 03:04:05 plus `nanos`, as a naive nanosecond timestamp.
    def frame(nanos):
        epoch_ns = 1_704_164_645 * 10**9 + nanos
        if source == "pandas":
            pd = pandas()
            ts = pd.Series(pd.to_datetime([epoch_ns], unit="ns")).astype(
                "datetime64[ns]"
            )
            return pd.DataFrame({"id": ["t1"], "ts": ts})
        pl = polars()
        ts = pl.Series([epoch_ns]).cast(pl.Datetime("ns"))
        return pl.DataFrame({"id": ["t1"], "ts": ts})

    assert pa.table(frame(1)).schema.field("ts").type == pa.timestamp("ns")
    table = HudiTable.create(str(tmp_path / "t"), record_key="id")
    table.append(frame(123_456_000))
    read = pa.Table.from_batches(table.read())
    assert read.column("ts").to_pylist() == [
        datetime.datetime(2024, 1, 2, 3, 4, 5, 123456)
    ]
    with pytest.raises(
        ValueError, match="column 'ts' has timestamps with sub-microsecond"
    ):
        table.upsert(frame(1))


def test_unreferenced_nanosecond_values_are_ignored(tmp_path):
    ns = pa.timestamp("ns")
    sliced = pa.table(
        {"id": ["a", "b"], "ts": pa.array([[1], [1_000]], pa.list_(ns))}
    ).slice(1)
    unused = pa.DictionaryArray.from_arrays(
        pa.array([1], pa.int8()), pa.array([1, 1_000], ns)
    )
    table = HudiTable.create(str(tmp_path / "t"), record_key="id")
    table.append(sliced.append_column("d", unused))
    read = pa.Table.from_batches(table.read()).select(["id", "ts", "d"]).to_pylist()
    one_us = datetime.datetime(1970, 1, 1, 0, 0, 0, 1)
    assert read == [{"id": "b", "ts": [one_us], "d": one_us}]


def test_pandas_category_and_index_columns(tmp_path):
    pd = pandas()
    table = HudiTable.create(str(tmp_path / "t"), record_key="id")
    frame = pd.DataFrame(
        {
            "id": ["t2", "t1", "t3"],
            "kind": pd.Categorical(["x", "y", "x"]),
            "fare": [2.0, 1.0, 3.0],
        }
    )
    # A sorted frame keeps a non-default index, which pandas exports as an
    # unnamed `__index_level_0__` column.
    table.append(frame.sort_values("id"))
    assert "__index_level_0__" not in table.get_schema().names
    res = table.upsert(
        pd.DataFrame({"id": ["t1"], "kind": pd.Categorical(["z"]), "fare": [9.0]})
    )
    assert res.num_updates == 1
    read = pa.Table.from_batches(table.read()).sort_by("id")
    assert read.column("kind").to_pylist() == ["z", "x", "x"]
    assert read.column("fare").to_pylist() == [9.0, 2.0, 3.0]


def test_index_like_column_without_pandas_metadata_is_kept(tmp_path):
    table = HudiTable.create(str(tmp_path / "t"), record_key="id")
    table.append(pa.table({"id": ["t1"], "__index_level_0__": [7]}))
    assert "__index_level_0__" in table.get_schema().names


def test_pandas_named_index_is_kept(tmp_path):
    pd = pandas()
    table = HudiTable.create(str(tmp_path / "t"), record_key="id")
    frame = pd.DataFrame({"id": ["t1", "t2"], "fare": [1.0, 2.0]}).set_index("id")
    table.append(frame)
    res = table.upsert(pd.DataFrame({"fare": [5.0]}, index=pd.Index(["t2"], name="id")))
    assert res.num_updates == 1
    read = pa.Table.from_batches(table.read()).sort_by("id")
    assert read.column("fare").to_pylist() == [1.0, 5.0]


def test_write_errors_leave_the_table_unchanged(tmp_path):
    table = new_table(tmp_path)
    timeline = tmp_path / "trips" / ".hoodie" / "timeline"
    instants = sorted(p.name for p in timeline.iterdir())
    no_key = pa.table({"city": ["sf"], "ts": [2], "fare": [1.0]})
    with pytest.raises(HudiCoreError, match="schema does not match"):
        table.upsert(no_key)
    with pytest.raises(HudiCoreError, match="nope"):
        table.upsert(trips(("t1", "sf", 2, 1.0)), update_columns=["nope"])
    with pytest.raises(HudiCoreError, match="'no_such_column' not found"):
        table.delete("no_such_column = 1")
    assert sorted(p.name for p in timeline.iterdir()) == instants
    assert snapshot(table)["t1"] == ("sf", 1, 30.0)


def test_binary_large_list_and_dictionary_columns_are_cast(tmp_path):
    table = HudiTable.create(str(tmp_path / "t"), record_key="id")
    schema = pa.schema(
        [
            ("id", pa.string()),
            ("blob", pa.large_binary()),
            ("view", pa.binary_view()),
            ("xs", pa.large_list(pa.int64())),
            ("code", pa.dictionary(pa.int8(), pa.int64())),
        ]
    )

    def rows(id_, blob, xs, code):
        return pa.table(
            {"id": [id_], "blob": [blob], "view": [blob], "xs": [xs], "code": [code]},
            schema=schema,
        )

    table.append(rows("t1", b"a", [1, 2], 7))
    written = table.get_schema()
    assert written.field("blob").type == pa.binary()
    assert written.field("view").type == pa.binary()
    assert pa.types.is_list(written.field("xs").type)
    assert written.field("xs").type.value_type == pa.int64()
    assert written.field("code").type == pa.int64()
    assert table.upsert(rows("t1", b"b", [3], 8)).num_updates == 1
    read = pa.Table.from_batches(table.read()).select(["blob", "view", "xs", "code"])
    assert read.to_pylist() == [{"blob": b"b", "view": b"b", "xs": [3], "code": 8}]


def test_update_dict_struct_value_by_position(tmp_path):
    table = HudiTable.create(str(tmp_path / "t"), record_key="id")
    st = pa.struct([("k", pa.int64()), ("v", pa.string())])
    table.append(pa.table({"id": ["t1"], "st": pa.array([{"k": 1, "v": "a"}], st)}))
    table.update("id = 't1'", {"st": (2, "b")})
    with pytest.raises(ValueError, match="column 'st'"):
        table.update("id = 't1'", {"st": (2.5, "c")})
    read = pa.Table.from_batches(table.read()).select(["st"]).to_pylist()
    assert read == [{"st": {"k": 2, "v": "b"}}]


def test_unsupported_data_type_raises(tmp_path):
    table = HudiTable.create(str(tmp_path / "t"), record_key="id")
    with pytest.raises(TypeError, match="unsupported data type 'dict'"):
        table.append({"id": ["t1"]})
    with pytest.raises(TypeError, match=r"data\[0\] is not a pyarrow.RecordBatch"):
        table.append([{"id": "t1"}])


def test_compact_folds_logs_and_keeps_rows(tmp_path):
    table = new_table(tmp_path, "mor")
    table.upsert(trips(("t1", "sf", 2, 31.0), ("t4", "nyc", 2, 10.0)))
    before = snapshot(table)
    result = table.compact()
    assert isinstance(result, HudiCompactionResult)
    assert result.requested_time is not None and result.completion_time is not None
    assert (result.num_file_groups, result.num_log_files) == (1, 1)
    assert result.num_writes == 2
    assert snapshot(table) == before
    nothing = table.compact()
    assert nothing.requested_time is None and nothing.num_file_groups == 0
    assert table.await_compaction() is None


def test_compaction_in_the_background(tmp_path):
    table = HudiTable.create(
        str(tmp_path / "trips"),
        record_key="id",
        partition_by="city",
        ordering_fields="ts",
        table_type="mor",
        hudi_options={
            "hoodie.datasource.compaction.async.enable": "true",
            "hoodie.compact.inline.max.delta.commits": "2",
        },
    )
    table.append(trips(("t1", "sf", 1, 30.0)))
    table.upsert(trips(("t1", "sf", 2, 31.0)))
    result = table.await_compaction()
    assert result is not None and result.num_log_files == 1
    assert snapshot(table) == {"t1": ("sf", 2, 31.0)}


def test_compact_rejects_copy_on_write(tmp_path):
    with pytest.raises(HudiCoreError, match="MERGE_ON_READ"):
        new_table(tmp_path).compact()
