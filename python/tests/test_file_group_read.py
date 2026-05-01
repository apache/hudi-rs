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

"""File group reader tests using v8_trips_8i3u1d (MOR, partitioned by city).

SQL: 8 inserts, 3 updates (A, J, G fare=0), 2 deletes (F, J).
Final: san_francisco(4 rows), sao_paulo(1), chennai(1).
"""

import pyarrow as pa

from hudi import HudiFileGroupReader, HudiTable


def test_file_group_api_read_file_slice(v8_trips_table):
    table = HudiTable(v8_trips_table)
    file_group_reader = HudiFileGroupReader(v8_trips_table)

    file_slices = table.get_file_slices()
    sf_slice = [
        f for f in file_slices if "san_francisco" in f.base_file_relative_path()
    ][0]

    batch = file_group_reader.read_file_slice(sf_slice)

    t = pa.Table.from_batches([batch]).select(["rider", "fare"]).sort_by("rider")
    rows = t.to_pylist()
    assert len(rows) == 4
    rider_fares = {r["rider"]: r["fare"] for r in rows}
    assert rider_fares == {
        "rider-A": 0.0,
        "rider-C": 27.7,
        "rider-D": 33.9,
        "rider-E": 93.5,
    }


def test_file_group_api_read_file_slice_from_paths(v8_trips_table):
    table = HudiTable(v8_trips_table)
    file_group_reader = HudiFileGroupReader(v8_trips_table)

    file_slices = table.get_file_slices()
    sf_slice = [
        f for f in file_slices if "san_francisco" in f.base_file_relative_path()
    ][0]
    sf_path = sf_slice.base_file_relative_path()

    batch = file_group_reader.read_file_slice_from_paths(sf_path, [])

    assert batch.num_rows == 4
    assert batch.num_columns > 0
