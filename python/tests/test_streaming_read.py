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
import pytest

from hudi import (
    HudiFileGroupReader,
    HudiQueryType,
    HudiReadOptions,
    HudiRecordBatchStream,
    HudiTable,
)


def test_read_options_default():
    options = HudiReadOptions()
    assert options.filters == []
    assert options.projection is None
    assert options.hudi_options == {}


def test_read_options_typed_accessors_round_trip():
    """Every builder that writes into hudi_options must round-trip via its typed accessor."""
    options = (
        HudiReadOptions(filters=[("city", "=", "san_francisco")])
        .with_projection(["uuid", "rider"])
        .with_query_type(HudiQueryType.Incremental)
        .with_batch_size(2048)
        .with_as_of_timestamp("20240101000000000")
        .with_start_timestamp("20240101000000000")
        .with_end_timestamp("20240301000000000")
    )
    assert options.filters == [("city", "=", "san_francisco")]
    assert options.projection == ["uuid", "rider"]
    # Builders insert into hudi_options under the matching HudiReadConfig keys.
    assert options.hudi_options["hoodie.read.stream.batch_size"] == "2048"
    assert options.hudi_options["hoodie.read.as.of.timestamp"] == "20240101000000000"
    assert options.hudi_options["hoodie.read.start.timestamp"] == "20240101000000000"
    assert options.hudi_options["hoodie.read.end.timestamp"] == "20240301000000000"
    # Typed accessors round-trip.
    assert options.query_type() == HudiQueryType.Incremental
    assert options.batch_size() == 2048
    assert options.as_of_timestamp() == "20240101000000000"
    assert options.start_timestamp() == "20240101000000000"
    assert options.end_timestamp() == "20240301000000000"
    assert "HudiReadOptions" in repr(options)


def test_read_options_query_type_bad_value_raises():
    """A bogus value left in hudi_options under the query-type key surfaces as an error."""
    bogus = HudiReadOptions(hudi_options={"hoodie.read.query.type": "bogus"})
    with pytest.raises(Exception):
        bogus.query_type()


def test_read_options_batch_size_bad_value_raises():
    """Bad batch_size values: non-integers stuck in the bag surface at the accessor,
    and `with_batch_size(0)` is rejected at the builder so callers catch the misuse
    synchronously rather than at read time."""
    bogus = HudiReadOptions(
        hudi_options={"hoodie.read.stream.batch_size": "not_a_number"}
    )
    with pytest.raises(Exception):
        bogus.batch_size()

    with pytest.raises(Exception, match="must be > 0"):
        HudiReadOptions().with_batch_size(0)


def test_read_options_with_filters_validates_at_build_time():
    """`with_filters` parses + cardinality-validates upfront, so a bad operator or
    empty IN value list raises here rather than at read time."""
    with pytest.raises(Exception):
        HudiReadOptions().with_filters([("col", "BAD_OP", "x")])

    with pytest.raises(Exception, match="at least one value"):
        HudiReadOptions().with_filters([("col", "IN", "")])


def test_read_options_bulk_setters():
    """The plural builders (with_filters, with_hudi_options) chain identically to the singular ones."""
    options = (
        HudiReadOptions()
        .with_filters([("city", "=", "x"), ("rider", "=", "rider-A")])
        .with_hudi_options({"k1": "v1", "k2": "v2"})
    )
    assert options.filters == [("city", "=", "x"), ("rider", "=", "rider-A")]
    assert options.hudi_options["k1"] == "v1"
    assert options.hudi_options["k2"] == "v2"


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
