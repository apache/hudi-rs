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

"""Table read tests using v8_trips_8i3u1d (MOR, partitioned by city).

SQL: 8 inserts, UPDATE rider-A fare=0, DELETE rider-F,
UPDATE rider-J fare=0, DELETE rider-J, UPDATE rider-G fare=0.
Final state: 6 rows (riders A, C, D, E, G, I).
"""

import pyarrow as pa

from hudi import HudiReadOptions, HudiTable


def test_read_table_has_correct_schema(v8_trips_table):
    table = HudiTable(v8_trips_table)

    assert table.get_schema_with_meta_fields().names == [
        "_hoodie_commit_time",
        "_hoodie_commit_seqno",
        "_hoodie_record_key",
        "_hoodie_partition_path",
        "_hoodie_file_name",
        "ts",
        "uuid",
        "rider",
        "driver",
        "fare",
        "city",
    ]


def test_read_table_has_correct_partition_schema(v8_trips_table):
    table = HudiTable(v8_trips_table)
    assert table.get_partition_schema().names == ["city"]


def test_read_table_returns_correct_file_slices(v8_trips_table):
    table = HudiTable(v8_trips_table)

    file_slices = table.get_file_slices()
    assert len(file_slices) == 3
    assert all(f.base_file_byte_size > 0 for f in file_slices)
    partition_prefixes = sorted(
        f.base_file_relative_path().split("/")[0] for f in file_slices
    )
    assert partition_prefixes == [
        "city=chennai",
        "city=san_francisco",
        "city=sao_paulo",
    ]


def test_read_table_can_read_from_batches(v8_trips_table):
    table = HudiTable(v8_trips_table)

    file_slices = table.get_file_slices()
    file_slice_paths = [f.base_file_relative_path() for f in file_slices]
    batch = table.create_file_group_reader_with_options().read_file_slice_from_paths(
        file_slice_paths[0], []
    )
    t = pa.Table.from_batches([batch])
    assert t.num_rows > 0
    assert t.num_columns == 11

    all_slices = table.get_file_slices()
    assert len(all_slices) == 3


def test_read_table_returns_correct_data(v8_trips_table):
    table = HudiTable(v8_trips_table)

    batches = table.read()
    t = (
        pa.Table.from_batches(batches)
        .select(["ts", "uuid", "rider", "fare"])
        .sort_by("ts")
    )
    rows = t.to_pylist()

    # 6 surviving rows (8 inserts - 2 deletes: rider-F, rider-J)
    assert len(rows) == 6

    rider_fares = {r["rider"]: r["fare"] for r in rows}
    assert rider_fares == {
        "rider-D": 33.9,
        "rider-C": 27.7,
        "rider-A": 0.0,  # updated fare=0
        "rider-I": 41.06,
        "rider-E": 93.5,
        "rider-G": 0.0,  # updated fare=0
    }

    assert "rider-F" not in rider_fares
    assert "rider-J" not in rider_fares

    uuid_riders = {r["uuid"]: r["rider"] for r in rows}
    assert uuid_riders["334e26e9-8355-45cc-97c6-c31daf0df330"] == "rider-A"
    assert uuid_riders["9909a8b1-2d15-4d3d-8ec9-efc48c536a00"] == "rider-D"
    assert uuid_riders["7a84095f-737f-40bc-b62f-6b69664712d2"] == "rider-G"


def test_read_table_for_partition(v8_trips_table):
    table = HudiTable(v8_trips_table)

    batches = table.read(HudiReadOptions(filters=[("city", "=", "san_francisco")]))
    t = (
        pa.Table.from_batches(batches)
        .select(["ts", "uuid", "rider", "fare"])
        .sort_by("ts")
    )
    rows = t.to_pylist()

    assert len(rows) == 4
    rider_fares = {r["rider"]: r["fare"] for r in rows}
    assert rider_fares == {
        "rider-D": 33.9,
        "rider-C": 27.7,
        "rider-A": 0.0,
        "rider-E": 93.5,
    }


def test_table_apis_as_of_timestamp(v8_trips_table):
    table = HudiTable(v8_trips_table)

    timeline = table.get_timeline()
    all_commits = timeline.get_completed_commits()
    first_commit = all_commits[0].timestamp

    options_at_first = HudiReadOptions().with_as_of_timestamp(first_commit)
    all_slices = table.get_file_slices(options_at_first)
    assert len(all_slices) == 3
    partition_prefixes = sorted(
        f.base_file_relative_path().split("/")[0] for f in all_slices
    )
    assert partition_prefixes == [
        "city=chennai",
        "city=san_francisco",
        "city=sao_paulo",
    ]

    # get_completed_commits() returns compaction commits for MOR tables.
    # The sole compaction commit predates the final deltacommit (rider-G update).
    batches = table.read(options_at_first)
    t = (
        pa.Table.from_batches(batches)
        .select(["ts", "uuid", "rider", "fare"])
        .sort_by("ts")
    )
    rows = t.to_pylist()
    assert len(rows) == 6

    rider_fares = {r["rider"]: r["fare"] for r in rows}
    assert rider_fares["rider-A"] == 0.0  # updated before compaction
    assert rider_fares["rider-D"] == 33.9
    assert rider_fares["rider-C"] == 27.7
    assert rider_fares["rider-G"] == 43.40  # not yet updated at compaction time


def test_convert_filters_valid(v8_trips_table):
    table = HudiTable(v8_trips_table)

    filters = [
        ("city", "=", "san_francisco"),
        ("city", ">", "san_francisco"),
        ("city", "<", "san_francisco"),
        ("city", "<=", "san_francisco"),
        ("city", ">=", "san_francisco"),
    ]

    expected = [1, 1, 1, 2, 2]

    for f, exp in zip(filters, expected):
        file_slices = table.get_file_slices(HudiReadOptions(filters=[f]))
        assert len(file_slices) == exp, (
            f"Filter {f} expected {exp} slices, got {len(file_slices)}"
        )


def test_read_snapshot_filters_apply_as_row_predicate(v8_trips_table):
    """Filters on a non-partition column drop rows during reading.

    'rider' is a non-partition column. Filtering on it does not prune any files
    (since stats may not exclude the file), but rows must still be filtered.
    """
    table = HudiTable(v8_trips_table)
    options = HudiReadOptions(filters=[("rider", "=", "rider-A")])
    batches = table.read(options)
    t = pa.Table.from_batches(batches)
    assert t.num_rows == 1
    assert t.column("rider").to_pylist() == ["rider-A"]


def test_read_snapshot_stream_filters_apply_as_row_predicate(v8_trips_table):
    table = HudiTable(v8_trips_table)
    options = HudiReadOptions(filters=[("rider", "=", "rider-A")])
    batches = list(table.read_stream(options))
    t = pa.Table.from_batches(batches)
    assert t.num_rows == 1
    assert t.column("rider").to_pylist() == ["rider-A"]
