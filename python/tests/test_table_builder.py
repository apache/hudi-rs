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

from hudi import build_hudi_table

PYARROW_LE_8_0_0 = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) < (
    8,
    0,
    0,
)
pytestmark = pytest.mark.skipif(
    PYARROW_LE_8_0_0, reason="hudi only supported if pyarrow >= 8.0.0"
)

def test_read_table_returns_correct_data(get_sample_table):
    table_path = get_sample_table
    table = build_hudi_table(table_path)

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

@pytest.mark.parametrize("hudi_options,storage_options,options", [
({"hoodie.read.as.of.timestamp": "20240402123035233"}, None, None),
(None, None, {"hoodie.read.as.of.timestamp": "20240402123035233"})])
def test_read_table_as_of_timestamp(get_sample_table, hudi_options, storage_options, options):
    table_path = get_sample_table
    table = build_hudi_table(base_uri=table_path,
                             hudi_options=hudi_options,
                             storage_options=storage_options,
                             options=options)

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
