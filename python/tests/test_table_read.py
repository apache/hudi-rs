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

from hudi import HudiTable

PYARROW_LE_8_0_0 = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) < (8, 0, 0)
pytestmark = pytest.mark.skipif(PYARROW_LE_8_0_0, reason="hudi only supported if pyarrow >= 8.0.0")


def test_sample_table(get_sample_table):
    table_path = get_sample_table
    table = HudiTable(table_path, {})

    assert table.schema().names == ['_hoodie_commit_time', '_hoodie_commit_seqno', '_hoodie_record_key',
                                    '_hoodie_partition_path', '_hoodie_file_name', 'ts', 'uuid', 'rider', 'driver',
                                    'fare', 'city']

    file_slices = table.get_latest_file_slices()
    assert len(file_slices) == 5
    assert set(f.commit_time for f in file_slices) == {'20240402123035233', '20240402144910683'}
    assert all(f.num_records == 1 for f in file_slices)
    file_slice_paths = [f.base_file_path for f in file_slices]
    assert set(file_slice_paths) == {'chennai/68d3c349-f621-4cd8-9e8b-c6dd8eb20d08-0_4-12-0_20240402123035233.parquet',
                                'san_francisco/d9082ffd-2eb1-4394-aefc-deb4a61ecc57-0_1-9-0_20240402123035233.parquet',
                                'san_francisco/780b8586-3ad0-48ef-a6a1-d2217845ce4a-0_0-8-0_20240402123035233.parquet',
                                'san_francisco/5a226868-2934-4f84-a16f-55124630c68d-0_0-7-24_20240402144910683.parquet',
                                'sao_paulo/ee915c68-d7f8-44f6-9759-e691add290d8-0_3-11-0_20240402123035233.parquet'}

    batches = table.read_file_slice(file_slice_paths[0])
    t = pa.Table.from_batches(batches)
    assert t.num_rows == 1
    assert t.num_columns == 11

    file_slices_gen = table.split_latest_file_slices(2)
    assert len(next(file_slices_gen)) == 3
    assert len(next(file_slices_gen)) == 2
