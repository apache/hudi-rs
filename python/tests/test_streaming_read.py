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

"""Streaming read tests using v8_trips_8i3u1d (MOR, partitioned by city)."""

import pyarrow as pa

from hudi import (
    HudiFileGroupReader,
    HudiReadOptions,
    HudiRecordBatchStream,
    HudiTable,
)


def test_read_options_default():
    options = HudiReadOptions()
    assert options.filters == []
    assert options.projection is None
    assert options.hudi_options == {}


def test_read_options_with_values():
    options = (
        HudiReadOptions(filters=[("city", "=", "san_francisco")])
        .with_projection(["uuid", "rider"])
        .with_batch_size(2048)
        .with_as_of_timestamp("20240101000000000")
    )
    assert options.filters == [("city", "=", "san_francisco")]
    assert options.projection == ["uuid", "rider"]
    # Builders insert into hudi_options under the matching HudiReadConfig keys.
    assert options.hudi_options["hoodie.read.stream.batch_size"] == "2048"
    assert options.hudi_options["hoodie.read.as.of.timestamp"] == "20240101000000000"
    # Typed accessors round-trip.
    assert options.batch_size() == 2048
    assert options.as_of_timestamp() == "20240101000000000"
    assert "HudiReadOptions" in repr(options)


def test_read_snapshot_stream_yields_all_rows(v8_trips_table):
    table = HudiTable(v8_trips_table)
    stream = table.read_stream()
    assert isinstance(stream, HudiRecordBatchStream)

    batches = list(stream)
    assert len(batches) > 0
    t = pa.Table.from_batches(batches)
    assert t.num_rows == 6


def test_read_snapshot_stream_with_partition_filter(v8_trips_table):
    table = HudiTable(v8_trips_table)
    options = HudiReadOptions(filters=[("city", "=", "san_francisco")])
    batches = list(table.read_stream(options))
    t = pa.Table.from_batches(batches)
    assert t.num_rows == 4


def test_read_snapshot_stream_is_single_use(v8_trips_table):
    table = HudiTable(v8_trips_table)
    stream = table.read_stream()
    first = list(stream)
    assert len(first) > 0
    # Re-iterating an exhausted stream yields nothing
    assert list(stream) == []


def test_read_file_slice_stream_file_group_reader(v8_trips_table):
    table = HudiTable(v8_trips_table)
    file_slices = table.get_file_slices(
        HudiReadOptions(filters=[("city", "=", "san_francisco")])
    )
    reader = HudiFileGroupReader(v8_trips_table)

    batches = list(reader.read_file_slice_stream(file_slices[0]))
    t = pa.Table.from_batches(batches)
    assert t.num_rows == 4


def test_read_file_slice_from_paths_stream(v8_trips_table):
    table = HudiTable(v8_trips_table)
    file_slices = table.get_file_slices(
        HudiReadOptions(filters=[("city", "=", "san_francisco")])
    )
    reader = HudiFileGroupReader(v8_trips_table)
    base_path = file_slices[0].base_file_relative_path()

    batches = list(reader.read_file_slice_from_paths_stream(base_path, []))
    t = pa.Table.from_batches(batches)
    assert t.num_rows == 4


def test_read_snapshot_stream_with_batch_size(v8_trips_table):
    table = HudiTable(v8_trips_table)
    options = HudiReadOptions().with_batch_size(1)
    batches = list(table.read_stream(options))
    t = pa.Table.from_batches(batches)
    assert t.num_rows == 6
