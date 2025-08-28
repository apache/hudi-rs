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

from hudi import HudiQueryType


def test_query_type_variants_are_distinct_and_comparable():
    assert HudiQueryType.Snapshot == HudiQueryType.Snapshot
    assert HudiQueryType.ReadOptimized == HudiQueryType.ReadOptimized
    assert HudiQueryType.Incremental == HudiQueryType.Incremental
    assert HudiQueryType.Snapshot != HudiQueryType.ReadOptimized
    assert HudiQueryType.Snapshot != HudiQueryType.Incremental
    assert HudiQueryType.ReadOptimized != HudiQueryType.Incremental


def test_query_type_str_returns_canonical_hudi_value():
    assert str(HudiQueryType.Snapshot) == "snapshot"
    assert str(HudiQueryType.ReadOptimized) == "read_optimized"
    assert str(HudiQueryType.Incremental) == "incremental"


def test_query_type_repr_includes_variant_name():
    assert repr(HudiQueryType.Snapshot) == "HudiQueryType.Snapshot"
    assert repr(HudiQueryType.ReadOptimized) == "HudiQueryType.ReadOptimized"
    assert repr(HudiQueryType.Incremental) == "HudiQueryType.Incremental"
