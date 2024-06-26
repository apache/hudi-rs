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

    assert table.schema().names == ['_hoodie_commit_time', '_hoodie_commit_seqno', '_hoodie_record_key', '_hoodie_partition_path', '_hoodie_file_name', 'ts', 'uuid', 'rider', 'driver', 'fare', 'city']

    file_slices = table.get_latest_file_slices()
    assert len(file_slices) == 5
    assert set(f.commit_time for f in file_slices) == {'20240402123035233', '20240402144910683'}
    print([f.num_records for f in file_slices])
    file_slice_paths = [f.base_file_path for f in file_slices]
    print(file_slice_paths)
    batches = table.read_file_slice(file_slice_paths[0])
    t = pa.Table.from_batches(batches)
    print(t.num_rows, t.num_columns)

    file_slices_gen = table.split_latest_file_slices(2)
    assert len(next(file_slices_gen)) == 3
    assert len(next(file_slices_gen)) == 2
