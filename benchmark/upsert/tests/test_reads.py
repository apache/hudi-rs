# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""The training-set reads: an incremental read must return exactly what the
commit changed, and time travel to the load must return the loaded values."""

import bench
import pyarrow as pa
import pytest

PLAN = {"key_cols": ["id"]}
BASE = pa.table({"id": ["a", "b", "c"], "updated_at": pa.array([1, 2, 3], pa.int64())})


class FakeLeg:
    def __init__(self, changed=None, deleted=None, travel=BASE):
        self.changed, self.deleted, self.travel = changed, deleted, travel

    def incremental(self, prev, cur):
        rows = sum(t.num_rows for t in (self.changed, self.deleted) if t is not None)
        return rows, self.changed, self.deleted

    def time_travel(self, marker):
        return self.travel


def reads(leg, kind, arg, emits_deletes=False):
    return bench.measure_reads(leg, PLAN, 0, 1, 0, kind, arg, BASE, emits_deletes)


UPSERT = pa.table({"id": ["a", "z"], "updated_at": pa.array([10, 11], pa.int64())})
DELETE = pa.table({"id": ["b"]})


def test_upsert_returning_exactly_its_keys_passes():
    out = reads(FakeLeg(changed=UPSERT), "upsert", UPSERT)
    assert out["incremental"]["changed_rows"] == 2
    assert out["time_travel"]["matches_load"]


@pytest.mark.parametrize(
    "changed",
    [
        UPSERT.slice(0, 1),
        pa.concat_tables([UPSERT, BASE.slice(2, 1).cast(UPSERT.schema)]),
        pa.concat_tables([UPSERT, UPSERT.slice(0, 1)]),
        None,
    ],
    ids=["missing key", "extra key", "duplicate row", "nothing"],
)
def test_upsert_returning_anything_else_fails(changed):
    with pytest.raises(bench.CheckFailed, match="after upsert"):
        reads(FakeLeg(changed=changed), "upsert", UPSERT)


def test_delete_for_a_leg_without_delete_rows_must_return_nothing():
    reads(FakeLeg(), "delete", DELETE)
    with pytest.raises(bench.CheckFailed, match="does not return deletes"):
        reads(FakeLeg(changed=BASE.slice(0, 1)), "delete", DELETE)


def test_delete_for_a_leg_with_delete_rows_must_return_exactly_the_keys():
    reads(FakeLeg(deleted=DELETE), "delete", DELETE, emits_deletes=True)
    with pytest.raises(bench.CheckFailed, match="deleted keys"):
        reads(FakeLeg(), "delete", DELETE, emits_deletes=True)
    with pytest.raises(bench.CheckFailed, match="deleted keys"):
        reads(
            FakeLeg(deleted=BASE.select(["id"])), "delete", DELETE, emits_deletes=True
        )


def test_time_travel_must_carry_the_loaded_values_not_just_the_count():
    changed = BASE.set_column(1, "updated_at", pa.array([1, 20, 3], pa.int64()))
    with pytest.raises(bench.CheckFailed, match="time travel") as e:
        reads(FakeLeg(changed=UPSERT, travel=changed), "upsert", UPSERT)
    assert '"key": "b"' in str(e.value) and '"got": "20"' in str(e.value)


def test_time_travel_returning_nothing_fails():
    with pytest.raises(bench.CheckFailed, match="keys_missing"):
        reads(FakeLeg(changed=UPSERT, travel=None), "upsert", UPSERT)
