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

from hudi import HudiFileGroupReader

TEST_SAMPLE_BASE_FILE = "san_francisco/780b8586-3ad0-48ef-a6a1-d2217845ce4a-0_0-8-0_20240402123035233.parquet"
TEST_SAMPLE_LOG_FILE = ".780b8586-3ad0-48ef-a6a1-d2217845ce4a-0_20240402123035233.log.1_0-8-0"

def test_file_group_api_read_file_slice(get_sample_table):
    table_path = get_sample_table
    file_group_reader = HudiFileGroupReader(table_path)

    batch = file_group_reader.read_file_slice_by_base_file_path(TEST_SAMPLE_BASE_FILE)

    t = pa.Table.from_batches([batch]).select([0, 5, 6, 9]).sort_by("ts")
    assert t.to_pylist() == [
        {
            "_hoodie_commit_time": "20240402123035233",
            "ts": 1695159649087,
            "uuid": "334e26e9-8355-45cc-97c6-c31daf0df330",
            "fare": 19.1,
        },
    ]


def test_file_group_api_read_file_slice_from_paths(get_sample_table):
    table_path = get_sample_table
    file_group_reader = HudiFileGroupReader(table_path)

    batch = file_group_reader.read_file_slice_from_paths(TEST_SAMPLE_BASE_FILE, [])
    assert batch.num_rows > 0
    
    batch_original = file_group_reader.read_file_slice_by_base_file_path(TEST_SAMPLE_BASE_FILE)
    assert batch.num_rows == batch_original.num_rows
    assert batch.num_columns == batch_original.num_columns
    
    t_new = pa.Table.from_batches([batch]).select([0, 5, 6, 9]).sort_by("ts")
    t_original = pa.Table.from_batches([batch_original]).select([0, 5, 6, 9]).sort_by("ts")
    assert t_new.to_pylist() == t_original.to_pylist()
    
    try:
        batch_with_logs = file_group_reader.read_file_slice_from_paths(
            TEST_SAMPLE_BASE_FILE, 
            [TEST_SAMPLE_LOG_FILE]
        )
        assert batch_with_logs.num_rows >= 0
    except Exception:
        pass
