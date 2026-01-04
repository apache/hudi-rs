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

from hudi import HudiFileGroupReader, HudiTable


def test_file_group_api_read_file_slice(get_sample_table):
    table_path = get_sample_table
    table = HudiTable(table_path)
    file_group_reader = table.create_file_group_reader()

    # Get file slices from table and find the one for san_francisco
    file_slices = table.get_file_slices()
    sf_slices = [
        f
        for f in file_slices
        if f.partition_path == "san_francisco"
        and f.creation_instant_time == "20240402123035233"
    ]
    assert len(sf_slices) >= 1

    # Pick the file slice with the expected file id
    file_slice = [
        f
        for f in sf_slices
        if "780b8586-3ad0-48ef-a6a1-d2217845ce4a" in f.base_file_name
    ][0]

    batch = file_group_reader.read_file_slice(file_slice)

    t = pa.Table.from_batches(batch).select([0, 5, 6, 9]).sort_by("ts")
    assert t.to_pylist() == [
        {
            "_hoodie_commit_time": "20240402123035233",
            "ts": 1695159649087,
            "uuid": "334e26e9-8355-45cc-97c6-c31daf0df330",
            "fare": 19.1,
        },
    ]


def test_file_group_api_read_file_slice_with_file_group_reader(get_sample_table):
    """Test read_file_slice using HudiFileGroupReader created from table path."""
    table_path = get_sample_table
    table = HudiTable(table_path)
    file_group_reader = HudiFileGroupReader(table_path)

    # Get file slices from table and find the one for san_francisco
    file_slices = table.get_file_slices()
    sf_slices = [
        f
        for f in file_slices
        if f.partition_path == "san_francisco"
        and f.creation_instant_time == "20240402123035233"
    ]
    assert len(sf_slices) >= 1

    # Pick the file slice with the expected file id
    file_slice = [
        f
        for f in sf_slices
        if "780b8586-3ad0-48ef-a6a1-d2217845ce4a" in f.base_file_name
    ][0]

    # Read using read_file_slice
    batch = file_group_reader.read_file_slice(file_slice)

    # Verify it returns data
    t = pa.Table.from_batches(batch)
    assert t.num_rows == 1
    assert t.num_columns > 0

    # Verify the data matches expected values
    t = t.select([0, 5, 6, 9]).sort_by("ts")
    assert t.to_pylist() == [
        {
            "_hoodie_commit_time": "20240402123035233",
            "ts": 1695159649087,
            "uuid": "334e26e9-8355-45cc-97c6-c31daf0df330",
            "fare": 19.1,
        },
    ]
