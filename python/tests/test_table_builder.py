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
import pytest

from hudi import HudiTableBuilder


@pytest.fixture
def base_uri():
    return "test://base/uri"


@pytest.fixture
def builder(base_uri):
    return HudiTableBuilder(base_uri)


def test_initialization(builder, base_uri):
    assert builder.base_uri == base_uri
    assert builder.hudi_options == {}
    assert builder.storage_options == {}
    assert builder.options == {}


def test_from_base_uri(base_uri):
    builder = HudiTableBuilder.from_base_uri(base_uri)
    assert builder.base_uri == base_uri


@pytest.mark.parametrize(
    "method,attr",
    [
        ("with_hudi_option", "hudi_options"),
        ("with_storage_option", "storage_options"),
        ("with_option", "options"),
    ],
)
def test_with_single_option(builder, method, attr):
    getattr(builder, method)("key1", "value1")
    assert getattr(builder, attr) == {"key1": "value1"}


@pytest.mark.parametrize(
    "method,attr",
    [
        ("with_hudi_options", "hudi_options"),
        ("with_storage_options", "storage_options"),
        ("with_options", "options"),
    ],
)
def test_with_multiple_options(builder, method, attr):
    options = {"key1": "value1", "key2": "value2"}
    getattr(builder, method)(options)
    assert getattr(builder, attr) == options


def test_read_table_returns_correct_data(get_sample_table):
    table_path = get_sample_table
    table = HudiTableBuilder.from_base_uri(table_path).build()

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


@pytest.mark.parametrize(
    "hudi_options,storage_options,options",
    [
        (
            {"hoodie.read.file_group.start_timestamp": "resolved value"},
            {"hoodie.read.file_group.start_timestamp": "not taking"},
            {"hoodie.read.file_group.start_timestamp": "lower precedence"},
        ),
        (
            {},
            {"hoodie.read.file_group.start_timestamp": "not taking"},
            {"hoodie.read.file_group.start_timestamp": "resolved value"},
        ),
    ],
)
def test_setting_table_options(
    get_sample_table, hudi_options, storage_options, options
):
    table_path = get_sample_table
    table = (
        HudiTableBuilder.from_base_uri(table_path)
        .with_hudi_options(hudi_options)
        .with_storage_options(storage_options)
        .with_options(options)
        .build()
    )

    assert (
        table.hudi_options().get("hoodie.read.file_group.start_timestamp")
        == "resolved value"
    )
