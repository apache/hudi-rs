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

from hudi import HudiTable
from hudi._internal import get_test_table_path


def test_table_incremental_read_returns_correct_data():
    table_path = get_test_table_path("v9_txns_simple_nometa", "cow")
    table = HudiTable(table_path)

    timeline = table.get_timeline()
    commits = timeline.get_completed_commits()
    assert len(commits) >= 2

    insert_ts = commits[0].timestamp
    update_ts = commits[1].timestamp

    batches = table.read_incremental_records(insert_ts, update_ts)
    t = pa.Table.from_batches(batches)

    assert t.num_rows == 1
    assert t.column("txn_id").to_pylist() == ["TXN-001"]
    assert t.column("txn_type").to_pylist() == ["reversal"]


def test_incremental_read_with_partition_filter():
    table_path = get_test_table_path("v9_txns_simple_nometa", "cow")
    table = HudiTable(table_path)
    commits = table.get_timeline().get_completed_commits()
    insert_ts = commits[0].timestamp
    update_ts = commits[1].timestamp

    # TXN-001 lives in region 'us'; filtering to 'us' keeps it.
    batches = table.read_incremental_records(
        insert_ts, update_ts, filters=[("region", "=", "us")]
    )
    t = pa.Table.from_batches(batches)
    assert t.num_rows == 1
    assert t.column("txn_id").to_pylist() == ["TXN-001"]

    # Filtering to a non-matching region prunes the change away.
    batches = table.read_incremental_records(
        insert_ts, update_ts, filters=[("region", "=", "eu")]
    )
    assert sum(b.num_rows for b in batches) == 0


def test_get_file_slices_between_with_partition_filter():
    table_path = get_test_table_path("v9_txns_simple_nometa", "cow")
    table = HudiTable(table_path)
    commits = table.get_timeline().get_completed_commits()
    insert_ts = commits[0].timestamp
    update_ts = commits[1].timestamp

    slices_us = table.get_file_slices_between(
        insert_ts, update_ts, filters=[("region", "=", "us")]
    )
    assert len(slices_us) == 1
    assert "region=us" in slices_us[0].base_file_relative_path()

    slices_eu = table.get_file_slices_between(
        insert_ts, update_ts, filters=[("region", "=", "eu")]
    )
    assert len(slices_eu) == 0


def test_get_file_slices_splits_between_with_partition_filter():
    table_path = get_test_table_path("v9_txns_simple_nometa", "cow")
    table = HudiTable(table_path)
    commits = table.get_timeline().get_completed_commits()
    insert_ts = commits[0].timestamp
    update_ts = commits[1].timestamp

    splits = table.get_file_slices_splits_between(
        2, insert_ts, update_ts, filters=[("region", "=", "us")]
    )
    flattened = [s for split in splits for s in split]
    assert len(flattened) == 1
    assert "region=us" in flattened[0].base_file_relative_path()
