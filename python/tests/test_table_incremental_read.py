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


def test_table_incremental_read_returns_correct_data(get_sample_table):
    table_path = get_sample_table
    table = HudiTable(table_path)

    batches = table.read_incremental_records("20240402123035233", "20240402144910683")
    t = pa.Table.from_batches(batches).select([0, 3, 4, 5, 6, 9]).sort_by("ts")
    assert t.to_pylist() == [
        {
            "_hoodie_commit_time": "20240402144910683",
            "_hoodie_partition_path": "san_francisco",
            "_hoodie_file_name": "5a226868-2934-4f84-a16f-55124630c68d-0_0-7-24_20240402144910683.parquet",
            "ts": 1695046462179,
            "uuid": "9909a8b1-2d15-4d3d-8ec9-efc48c536a00",
            "fare": 339.0,
        },
    ]
