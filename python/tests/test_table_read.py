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

import pyarrow as pa

from hudi import HudiTable


def test_read_table_has_correct_schema(get_sample_table):
    table_path = get_sample_table
    table = HudiTable(table_path)

    assert table.get_schema().names == [
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


def test_read_table_has_correct_partition_schema(get_sample_table):
    table_path = get_sample_table
    table = HudiTable(table_path)
    assert table.get_partition_schema().names == ["city"]


def test_read_table_returns_correct_file_slices(get_sample_table):
    table_path = get_sample_table
    table = HudiTable(table_path)

    file_slices = table.get_file_slices()
    assert len(file_slices) == 5
    assert set(f.commit_time for f in file_slices) == {
        "20240402123035233",
        "20240402144910683",
    }
    assert all(f.num_records == 1 for f in file_slices)
    assert all(f.size_bytes > 0 for f in file_slices)
    file_slice_paths = [f.base_file_relative_path() for f in file_slices]
    assert set(file_slice_paths) == {
        "chennai/68d3c349-f621-4cd8-9e8b-c6dd8eb20d08-0_4-12-0_20240402123035233.parquet",
        "san_francisco/d9082ffd-2eb1-4394-aefc-deb4a61ecc57-0_1-9-0_20240402123035233.parquet",
        "san_francisco/780b8586-3ad0-48ef-a6a1-d2217845ce4a-0_0-8-0_20240402123035233.parquet",
        "san_francisco/5a226868-2934-4f84-a16f-55124630c68d-0_0-7-24_20240402144910683.parquet",
        "sao_paulo/ee915c68-d7f8-44f6-9759-e691add290d8-0_3-11-0_20240402123035233.parquet",
    }


def test_read_table_can_read_from_batches(get_sample_table):
    table_path = get_sample_table
    table = HudiTable(table_path)

    file_slices = table.get_file_slices()
    file_slice_paths = [f.base_file_relative_path() for f in file_slices]
    batch = table.create_file_group_reader().read_file_slice_by_base_file_path(
        file_slice_paths[0]
    )
    t = pa.Table.from_batches([batch])
    assert t.num_rows == 1
    assert t.num_columns == 11

    file_slices_gen = iter(table.get_file_slices_splits(2))
    assert len(next(file_slices_gen)) == 3
    assert len(next(file_slices_gen)) == 2


def test_read_table_returns_correct_data(get_sample_table):
    table_path = get_sample_table
    table = HudiTable(table_path)

    batches = table.read_snapshot()
    t = pa.Table.from_batches(batches).select([0, 5, 6, 9]).sort_by("ts")
    assert t.to_pylist() == [
        {
            "_hoodie_commit_time": "20240402144910683",
            "ts": 1695046462179,
            "uuid": "9909a8b1-2d15-4d3d-8ec9-efc48c536a00",
            "fare": 339.0,
        },
        {
            "_hoodie_commit_time": "20240402123035233",
            "ts": 1695091554788,
            "uuid": "e96c4396-3fad-413a-a942-4cb36106d721",
            "fare": 27.7,
        },
        {
            "_hoodie_commit_time": "20240402123035233",
            "ts": 1695115999911,
            "uuid": "c8abbe79-8d89-47ea-b4ce-4d224bae5bfa",
            "fare": 17.85,
        },
        {
            "_hoodie_commit_time": "20240402123035233",
            "ts": 1695159649087,
            "uuid": "334e26e9-8355-45cc-97c6-c31daf0df330",
            "fare": 19.1,
        },
        {
            "_hoodie_commit_time": "20240402123035233",
            "ts": 1695516137016,
            "uuid": "e3cf430c-889d-4015-bc98-59bdce1e530c",
            "fare": 34.15,
        },
    ]


def test_read_table_for_partition(get_sample_table):
    table_path = get_sample_table
    table = HudiTable(table_path)

    batches = table.read_snapshot([("city", "=", "san_francisco")])
    t = pa.Table.from_batches(batches).select([0, 5, 6, 9]).sort_by("ts")
    assert t.to_pylist() == [
        {
            "_hoodie_commit_time": "20240402144910683",
            "ts": 1695046462179,
            "uuid": "9909a8b1-2d15-4d3d-8ec9-efc48c536a00",
            "fare": 339.0,
        },
        {
            "_hoodie_commit_time": "20240402123035233",
            "ts": 1695091554788,
            "uuid": "e96c4396-3fad-413a-a942-4cb36106d721",
            "fare": 27.7,
        },
        {
            "_hoodie_commit_time": "20240402123035233",
            "ts": 1695159649087,
            "uuid": "334e26e9-8355-45cc-97c6-c31daf0df330",
            "fare": 19.1,
        },
    ]


def test_read_table_as_of_timestamp(get_sample_table):
    table_path = get_sample_table
    table = HudiTable(
        table_path, options={"hoodie.read.as.of.timestamp": "20240402123035233"}
    )

    batches = table.read_snapshot()
    t = pa.Table.from_batches(batches).select([0, 5, 6, 9]).sort_by("ts")
    assert t.to_pylist() == [
        {
            "_hoodie_commit_time": "20240402123035233",
            "ts": 1695046462179,
            "uuid": "9909a8b1-2d15-4d3d-8ec9-efc48c536a00",
            "fare": 33.9,
        },
        {
            "_hoodie_commit_time": "20240402123035233",
            "ts": 1695091554788,
            "uuid": "e96c4396-3fad-413a-a942-4cb36106d721",
            "fare": 27.7,
        },
        {
            "_hoodie_commit_time": "20240402123035233",
            "ts": 1695115999911,
            "uuid": "c8abbe79-8d89-47ea-b4ce-4d224bae5bfa",
            "fare": 17.85,
        },
        {
            "_hoodie_commit_time": "20240402123035233",
            "ts": 1695159649087,
            "uuid": "334e26e9-8355-45cc-97c6-c31daf0df330",
            "fare": 19.1,
        },
        {
            "_hoodie_commit_time": "20240402123035233",
            "ts": 1695516137016,
            "uuid": "e3cf430c-889d-4015-bc98-59bdce1e530c",
            "fare": 34.15,
        },
    ]
