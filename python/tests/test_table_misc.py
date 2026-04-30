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

from itertools import chain

from hudi import HudiTable
from hudi._internal import get_test_table_path


def test_base_url_property(v8_trips_table):
    table = HudiTable(v8_trips_table)
    base_url = table.base_url
    assert base_url.startswith("file://")
    assert base_url.rstrip("/").endswith("v8_trips_8i3u1d")


def test_get_file_slices_splits_between():
    table_path = get_test_table_path("v9_txns_simple_nometa", "cow")
    table = HudiTable(table_path)
    commits = table.get_timeline().get_completed_commits()
    assert len(commits) >= 2

    splits = table.get_file_slices_splits_between(2, commits[0].timestamp)
    flattened = list(chain.from_iterable(splits))
    assert len(flattened) >= 1


def test_compute_table_stats(v8_trips_table):
    table = HudiTable(v8_trips_table)
    stats = table.compute_table_stats()
    if stats is None:
        return
    num_rows, byte_size = stats
    assert num_rows > 0
    assert byte_size > 0
