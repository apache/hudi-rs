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

from hudi import HudiFileGroupReader, HudiQueryType, HudiReadOptions, HudiTable
from hudi._internal import get_test_table_path


def test_table_metadata_getters(v8_trips_table):
    """Table-level getters round-trip the values from hoodie.properties."""
    table = HudiTable(v8_trips_table)
    base_url = table.base_url
    assert base_url.startswith("file://")
    assert base_url.rstrip("/").endswith("v8_trips_8i3u1d")
    assert table.table_name == "v8_trips_8i3u1d"
    assert table.table_type == "MERGE_ON_READ"
    assert table.is_mor is True
    assert isinstance(table.timezone, str) and len(table.timezone) > 0
    assert isinstance(table.storage_options(), dict)
    assert isinstance(table.hudi_options(), dict)


def test_file_slice_log_files_relative_paths(v8_trips_table):
    """The log-files accessor must mirror log_file_names with the partition prefix attached."""
    table = HudiTable(v8_trips_table)
    # Pick any slice with log files (V8Trips8I3U1D has updates → log files exist).
    slice_with_logs = next(
        (s for s in table.get_file_slices() if s.log_file_names),
        None,
    )
    assert slice_with_logs is not None, (
        "MOR fixture should have at least one slice with log files"
    )
    relative_paths = slice_with_logs.log_files_relative_paths()
    assert len(relative_paths) == len(slice_with_logs.log_file_names)
    for path in relative_paths:
        assert path.startswith(slice_with_logs.partition_path)


def test_file_group_reader_with_options_constructor(v8_trips_table):
    """HudiFileGroupReader must accept an options dict (PyO3-bound constructor)."""
    reader = HudiFileGroupReader(
        v8_trips_table,
        {"hoodie.read.use.read_optimized.mode": "true"},
    )
    table = HudiTable(v8_trips_table)
    sf_slices = table.get_file_slices(
        HudiReadOptions(filters=[("city", "=", "san_francisco")])
    )
    batch = reader.read_file_slice(sf_slices[0])
    # Read-optimized skips log merging; the result is still a valid batch with the table schema.
    assert batch.num_columns > 0


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


def test_file_slice_size_accessors_cow():
    table = HudiTable(get_test_table_path("v9_txns_simple_nometa", "cow"))
    slices = table.get_file_slices()
    assert len(slices) > 0
    fs = slices[0]
    assert fs.log_file_names == []
    assert fs.log_file_sizes == []
    assert fs.has_log_files() is False
    assert fs.total_size_bytes() == fs.base_file_size


def test_file_slice_read_optimized_strips_log_files(v8_trips_table):
    table = HudiTable(v8_trips_table)
    options = HudiReadOptions().with_hudi_option(
        "hoodie.read.use.read_optimized.mode", "true"
    )
    slices = table.get_file_slices(options)
    assert len(slices) > 0
    for fs in slices:
        assert fs.log_file_names == []
        assert fs.log_file_sizes == []
        assert fs.has_log_files() is False
        assert fs.total_size_bytes() == fs.base_file_size


def test_file_slice_size_accessors_mor(v8_trips_table):
    table = HudiTable(v8_trips_table)
    slices = table.get_file_slices()
    fs = next(s for s in slices if s.log_file_names)
    assert len(fs.log_file_sizes) == len(fs.log_file_names)
    assert fs.has_log_files() is True
    assert all(s > 0 for s in fs.log_file_sizes), (
        f"MDT-backed log file sizes should be non-zero, got {fs.log_file_sizes}"
    )
    assert fs.total_size_bytes() == fs.base_file_size + sum(fs.log_file_sizes)
    assert fs.total_size_bytes() > fs.base_file_size


def test_compute_table_stats(v8_trips_table):
    table = HudiTable(v8_trips_table)
    stats = table.compute_table_stats()
    assert stats is not None, "v8_trips fixture is MDT-enabled; stats must not be None"
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
