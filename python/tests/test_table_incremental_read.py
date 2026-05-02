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

"""Incremental read tests using v9_txns_simple_nometa (COW, partitioned by region).

SQL sequence: INSERT 8 rows, then UPDATE TXN-001 txn_type='reversal', etc.
The incremental read from the insert commit to the update commit should
return the updated TXN-001 row.
"""

import pyarrow as pa

from hudi import HudiQueryType, HudiReadOptions, HudiTable
from hudi._internal import get_test_table_path


def _commits(table: HudiTable):
    return table.get_timeline().get_completed_commits()


def test_table_incremental_read_returns_correct_data():
    table = HudiTable(get_test_table_path("v9_txns_simple_nometa", "cow"))
    commits = _commits(table)
    assert len(commits) >= 2

    options = (
        HudiReadOptions()
        .with_query_type(HudiQueryType.Incremental)
        .with_start_timestamp(commits[0].timestamp)
        .with_end_timestamp(commits[1].timestamp)
    )
    batches = table.read(options)
    t = pa.Table.from_batches(batches)

    assert t.num_rows == 1
    assert t.column("txn_id").to_pylist() == ["TXN-001"]
    assert t.column("txn_type").to_pylist() == ["reversal"]


def test_incremental_read_with_partition_filter():
    table = HudiTable(get_test_table_path("v9_txns_simple_nometa", "cow"))
    commits = _commits(table)
    insert_ts, update_ts = commits[0].timestamp, commits[1].timestamp

    # TXN-001 lives in region 'us'; filtering to 'us' keeps it.
    options_us = (
        HudiReadOptions(filters=[("region", "=", "us")])
        .with_query_type(HudiQueryType.Incremental)
        .with_start_timestamp(insert_ts)
        .with_end_timestamp(update_ts)
    )
    batches = table.read(options_us)
    t = pa.Table.from_batches(batches)
    assert t.num_rows == 1
    assert t.column("txn_id").to_pylist() == ["TXN-001"]

    # Filtering to a non-matching region prunes the change away.
    options_eu = (
        HudiReadOptions(filters=[("region", "=", "eu")])
        .with_query_type(HudiQueryType.Incremental)
        .with_start_timestamp(insert_ts)
        .with_end_timestamp(update_ts)
    )
    batches = table.read(options_eu)
    assert sum(b.num_rows for b in batches) == 0


def test_get_incremental_file_slices_with_partition_filter():
    table = HudiTable(get_test_table_path("v9_txns_simple_nometa", "cow"))
    commits = _commits(table)
    insert_ts, update_ts = commits[0].timestamp, commits[1].timestamp

    slices_us = table.get_file_slices(
        HudiReadOptions(filters=[("region", "=", "us")])
        .with_query_type(HudiQueryType.Incremental)
        .with_start_timestamp(insert_ts)
        .with_end_timestamp(update_ts)
    )
    assert len(slices_us) == 1
    assert "region=us" in slices_us[0].base_file_relative_path()

    slices_eu = table.get_file_slices(
        HudiReadOptions(filters=[("region", "=", "eu")])
        .with_query_type(HudiQueryType.Incremental)
        .with_start_timestamp(insert_ts)
        .with_end_timestamp(update_ts)
    )
    assert len(slices_eu) == 0
