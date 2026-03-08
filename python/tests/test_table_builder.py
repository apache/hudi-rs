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


def test_read_table_returns_correct_data(v8_trips_table):
    table = HudiTableBuilder.from_base_uri(v8_trips_table).build()

    batches = table.read_snapshot()
    t = pa.Table.from_batches(batches).select(["rider", "fare"]).sort_by("rider")
    rows = t.to_pylist()

    # 6 surviving rows from SQL: 8 inserts - 2 deletes (rider-F, rider-J)
    assert len(rows) == 6
    rider_fares = {r["rider"]: r["fare"] for r in rows}
    assert rider_fares == {
        "rider-A": 0.0,
        "rider-C": 27.7,
        "rider-D": 33.9,
        "rider-E": 93.5,
        "rider-G": 0.0,
        "rider-I": 41.06,
    }


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
def test_setting_table_options(v8_trips_table, hudi_options, storage_options, options):
    table = (
        HudiTableBuilder.from_base_uri(v8_trips_table)
        .with_hudi_options(hudi_options)
        .with_storage_options(storage_options)
        .with_options(options)
        .build()
    )

    assert (
        table.hudi_options().get("hoodie.read.file_group.start_timestamp")
        == "resolved value"
    )
