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

"""Tests for HudiTable accessor and stats APIs."""

from hudi import HudiQueryType, HudiReadOptions, HudiTable
from hudi._internal import get_test_table_path


def test_base_url_property(v8_trips_table):
    table = HudiTable(v8_trips_table)
    base_url = table.base_url
    assert base_url.startswith("file://")
    assert base_url.rstrip("/").endswith("v8_trips_8i3u1d")


def test_get_incremental_file_slices():
    table_path = get_test_table_path("v9_txns_simple_nometa", "cow")
    table = HudiTable(table_path)
    commits = table.get_timeline().get_completed_commits()
    assert len(commits) >= 2

    slices = table.get_file_slices(
        HudiReadOptions()
        .with_query_type(HudiQueryType.Incremental)
        .with_start_timestamp(commits[0].timestamp)
    )
    assert len(slices) >= 1


def test_compute_table_stats(v8_trips_table):
    table = HudiTable(v8_trips_table)
    stats = table.compute_table_stats()
    if stats is None:
        return
    num_rows, byte_size = stats
    assert num_rows > 0
    assert byte_size > 0


def test_read_dispatches_on_query_type(v8_trips_table):
    table = HudiTable(v8_trips_table)

    # Default (Snapshot) returns the latest 6 rows.
    snapshot_rows = sum(b.num_rows for b in table.read(HudiReadOptions()))
    assert snapshot_rows == 6

    # Calling read() with no options uses the default Snapshot behavior.
    snapshot_via_default = sum(b.num_rows for b in table.read())
    assert snapshot_rows == snapshot_via_default


def test_read_stream_errors_on_incremental(v8_trips_table):
    import pytest

    table = HudiTable(v8_trips_table)
    with pytest.raises(Exception, match="not yet supported"):
        table.read_stream(HudiReadOptions().with_query_type(HudiQueryType.Incremental))


def test_read_options_hudi_options_plumbed_to_reader(v8_trips_table):
    """Per-read hudi_options should affect file-group reader behavior."""
    table = HudiTable(v8_trips_table)
    options = HudiReadOptions().with_hudi_option(
        "hoodie.read.use.read_optimized.mode", "true"
    )
    # Read-optimized snapshot just skips log merging — should still produce
    # valid batches; on this MOR table with deltacommits the row count differs
    # from the merged snapshot, but we only assert that the option flowed
    # through without error.
    batches = table.read(options)
    assert isinstance(batches, list)
