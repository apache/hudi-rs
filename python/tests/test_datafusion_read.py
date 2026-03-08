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

"""DataFusion read tests for v9 txns tables.

Verification logic is shared with Rust tests via hudi_test::v9_verification.
"""

import pytest

from hudi._internal import verify_v9_txns_table

# ============================================================================
# COW tests
# ============================================================================


@pytest.mark.parametrize(
    "table_name",
    [
        "v9_txns_simple_nometa",
        "v9_txns_simple_meta",
        "v9_txns_complex_nometa",
        "v9_txns_complex_meta",
    ],
)
def test_v9_cow_partitioned(table_name):
    verify_v9_txns_table(table_name, cow=True, partitioned=True)


@pytest.mark.parametrize(
    "table_name",
    [
        "v9_txns_nonpart_nometa",
        "v9_txns_nonpart_meta",
    ],
)
def test_v9_cow_nonpart(table_name):
    verify_v9_txns_table(table_name, cow=True, partitioned=False)


# ============================================================================
# MOR tests (read-optimized mode, after compaction + clustering)
# ============================================================================


@pytest.mark.parametrize(
    "table_name",
    [
        "v9_txns_simple_nometa",
        "v9_txns_simple_meta",
        "v9_txns_complex_nometa",
        "v9_txns_complex_meta",
    ],
)
def test_v9_mor_partitioned(table_name):
    verify_v9_txns_table(table_name, cow=False, partitioned=True)


@pytest.mark.parametrize(
    "table_name",
    [
        "v9_txns_nonpart_nometa",
        "v9_txns_nonpart_meta",
    ],
)
def test_v9_mor_nonpart(table_name):
    verify_v9_txns_table(table_name, cow=False, partitioned=False)
