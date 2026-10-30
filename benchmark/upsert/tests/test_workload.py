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

"""The workload replay is the source of truth every case is judged against, so
it must be deterministic and must agree with applying the commit files to the
base table independently."""

import json

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from scenarios import (
    SCENARIOS,
    ensure_workload,
    key_strings,
    oracle_schema,
    read_base,
    sorted_by_key,
)

UUID = {
    "rows": 3_000,
    "commits": 2,
    "upsert_batch": 200,
    "insert_batch": 20,
    "delete_batch": 30,
}
RAG = {
    "docs": 40,
    "chunks_per_doc": 3,
    "commits": 2,
    "changed_docs": 5,
    "removed_docs": 2,
}


def build(work, name, params):
    sc = SCENARIOS[name]
    bd, wd = ensure_workload(sc, work, params, 7, 25)
    plan = json.loads((wd / "plan.json").read_text())
    return bd, wd, plan, pq.read_table(work / plan["oracle"])


def apply_files(bd, wd, plan):
    """Replays the commit files without the builder's in-memory state."""
    keys = plan["key_cols"]
    t = read_base(bd)
    for c in range(plan["commits"]):
        up = pq.read_table(wd / f"upsert-{c:02d}.parquet")
        drop = pc.is_in(key_strings(t, keys), value_set=key_strings(up, keys))
        t = pa.concat_tables([t.filter(pc.invert(drop)), up.cast(t.schema)])
    for c in range(plan["commits"]):
        dk = pq.read_table(wd / f"delete-{c:02d}.parquet")
        drop = pc.is_in(key_strings(t, keys), value_set=key_strings(dk, keys))
        t = t.filter(pc.invert(drop))
    t = sorted_by_key(t, keys)
    return t.cast(oracle_schema(t.schema))


def test_uuid_oracle_is_deterministic_and_matches_the_commit_files(tmp_path):
    bd, wd, plan, exp = build(tmp_path / "a", "uuid", UUID)
    _, _, _, again = build(tmp_path / "b", "uuid", UUID)
    assert exp.equals(again)
    assert exp.num_rows == plan["expected_rows"] == 3_000 + 2 * 20 - 2 * 30
    assert pc.count_distinct(exp.column("id")).as_py() == exp.num_rows
    assert exp.equals(apply_files(bd, wd, plan))
    assert plan["expected"]["upsert"] == [
        {"rows": 220, "updated": 200, "inserted": 20},
        {"rows": 220, "updated": 200, "inserted": 20},
    ]


def test_rag_oracle_keeps_embeddings_and_drops_removed_docs(tmp_path):
    bd, wd, plan, exp = build(tmp_path, "rag-chunks", RAG)
    assert exp.equals(apply_files(bd, wd, plan))
    assert exp.num_rows == (40 - 2 * 2) * 3
    assert exp.schema.field("embedding").type.value_type == pa.float32()


TRAINING = {
    "rows": 500,
    "commits": 2,
    "upsert_batch": 40,
    "insert_batch": 0,
    "delete_batch": 10,
}


def test_incremental_scenario_writes_the_loaded_table_as_its_own_oracle(tmp_path):
    bd, _, plan, _ = build(tmp_path, "training-set", TRAINING)
    base = pq.read_table(tmp_path / plan["oracle_base"])
    loaded = sorted_by_key(read_base(bd), plan["key_cols"])
    assert base.equals(loaded.cast(oracle_schema(loaded.schema)))


def test_scenarios_without_incremental_reads_have_no_base_oracle(tmp_path):
    _, _, plan, _ = build(tmp_path, "uuid", UUID)
    assert plan["oracle_base"] is None


def test_cached_workload_without_oracle_is_rebuilt(tmp_path):
    _, _, plan, _ = build(tmp_path, "uuid", UUID)
    (tmp_path / plan["oracle"]).unlink()
    _, _, plan2, exp = build(tmp_path, "uuid", UUID)
    assert plan2["oracle"] == plan["oracle"]
    assert exp.num_rows == plan2["expected_rows"]
