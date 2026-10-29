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

"""Scenario data: the base table, every commit's input, and the expected end state.

Every scenario is generated (or downloaded) once into the work dir, so all legs
apply byte-identical inputs. The workload builder replays each commit against an
in-memory copy of the table to know the exact end state, then samples keys from
it for the read-back checks.
"""

import hashlib
import json
import shutil
import urllib.request
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

GEN_CHUNK_ROWS = 1_000_000
TS_EPOCH = 1_700_000_000_000
KEY_SEP = "|"
KEY_COL = "__key"
KIND_COL = "__kind"

# SHA-256 of the file as published; a different download is refused rather
# than benchmarked.
NYC_TAXI_SHA256 = "797d6f008e67c3e3371b7f957ab97dbc2b38a9fb0ac083cb5eae4b0297b32372"
NYC_TAXI_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet"
)

CITIES = [
    "amsterdam", "austin", "bangalore", "berlin", "boston", "chicago", "dublin",
    "lisbon", "london", "madrid", "mumbai", "new_york", "paris", "san_francisco",
    "sao_paulo", "seattle", "singapore", "sydney", "tokyo", "toronto",
]  # fmt: skip
STATUSES = ["active", "pending", "completed", "cancelled"]
LABELS = ["accepted", "needs_review", "corrected", "flagged"]

UUID_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False),
        pa.field("ts", pa.int64()),
        pa.field("city", pa.string()),
        pa.field("fare", pa.float64()),
        pa.field("status", pa.string()),
        pa.field("note", pa.string()),
    ]
)

EMBEDDING_DIM = 384
RAG_SCHEMA = pa.schema(
    [
        pa.field("chunk_id", pa.string(), nullable=False),
        pa.field("doc_id", pa.string()),
        pa.field("pos", pa.int32()),
        pa.field("text", pa.string()),
        pa.field("embedding", pa.list_(pa.float32())),
        pa.field("updated_at", pa.int64()),
    ]
)

TRAINING_SCHEMA = pa.schema(
    [
        pa.field("example_id", pa.string(), nullable=False),
        pa.field("messages", pa.string()),
        pa.field("label", pa.string()),
        pa.field("source_chunk_id", pa.string()),
        pa.field("updated_at", pa.int64()),
    ]
)


# ---------------------------------------------------------------------------
# Vectorized string helpers
# ---------------------------------------------------------------------------

_HEX = np.frombuffer(b"0123456789abcdef", dtype=np.uint8)
_ALPHA = np.frombuffer(b"abcdefghijklmnopqrstuvwxyz", dtype=np.uint8)


def fixed_width_strings(mat):
    n, width = mat.shape
    data = pa.py_buffer(np.ascontiguousarray(mat).tobytes())
    offsets = pa.py_buffer((np.arange(n + 1, dtype=np.int64) * width).tobytes())
    return pa.LargeStringArray.from_buffers(n, offsets, data).cast(pa.string())


def uuid4_strings(rng, n):
    """n random version-4 UUID strings, built without per-row Python objects."""
    b = rng.integers(0, 256, size=(n, 16), dtype=np.uint8)
    b[:, 6] = (b[:, 6] & 0x0F) | 0x40
    b[:, 8] = (b[:, 8] & 0x3F) | 0x80
    hexc = np.empty((n, 32), dtype=np.uint8)
    hexc[:, 0::2] = _HEX[b >> 4]
    hexc[:, 1::2] = _HEX[b & 0x0F]
    out = np.full((n, 36), ord("-"), dtype=np.uint8)
    out[:, 0:8] = hexc[:, 0:8]
    out[:, 9:13] = hexc[:, 8:12]
    out[:, 14:18] = hexc[:, 12:16]
    out[:, 19:23] = hexc[:, 16:20]
    out[:, 24:36] = hexc[:, 20:32]
    return fixed_width_strings(out)


def make_vocab(seed):
    """512 seven-letter pseudo-words, each followed by a space (8 bytes)."""
    rng = np.random.default_rng([seed, 2])
    words = _ALPHA[rng.integers(0, len(_ALPHA), size=(512, 7))]
    return np.concatenate([words, np.full((512, 1), ord(" "), dtype=np.uint8)], axis=1)


def words(rng, vocab, n, nwords):
    return fixed_width_strings(
        vocab[rng.integers(0, len(vocab), size=(n, nwords))].reshape(n, nwords * 8)
    )


def padded_ids(prefix, values, width):
    """prefix + zero-padded integers, e.g. doc-00000042."""
    values = np.asarray(values)
    if values.size == 0:
        return pa.array([], pa.string())
    digits = np.char.zfill(values.astype(str), width)
    return pa.array(np.char.add(prefix, digits), pa.string())


def key_strings(table, key_cols):
    """One string per row identifying its record key; the same function is used
    to plan the expected state and to check the read-back."""
    cols = [pc.cast(table.column(k), pa.string()) for k in key_cols]
    if len(cols) == 1:
        return pc.cast(cols[0], pa.string())
    return pc.binary_join_element_wise(*cols, KEY_SEP)


# ---------------------------------------------------------------------------
# Scenario definitions
# ---------------------------------------------------------------------------


class Scenario:
    name = ""
    key_cols = []
    ordering = None
    check_cols = []
    incremental_reads = False
    # (knob, default, smoke default)
    knobs = {}

    def base_params(self, p):
        """Parameters that determine the base table (the rest shape commits)."""
        raise NotImplementedError

    def scale_label(self, p):
        raise NotImplementedError

    def scale_slug(self, p):
        return "-".join(f"{k}{v}" for k, v in sorted(self.base_params(p).items()))

    def write_base(self, d, p, seed, work_dir):
        raise NotImplementedError

    def upsert(self, c, state, rng, p, ctx):
        raise NotImplementedError

    def delete(self, c, state, rng, p, ctx):
        raise NotImplementedError


class UuidScenario(Scenario):
    name = "uuid"
    key_cols = ["id"]
    ordering = "ts"
    check_cols = ["ts", "fare", "status"]
    knobs = {
        "rows": (10_000_000, 100_000),
        "commits": (5, 2),
        "upsert_batch": (10_000, 10_000),
        "insert_batch": (1_000, 1_000),
        "delete_batch": (1_000, 1_000),
    }

    def base_params(self, p):
        return {"rows": p["rows"]}

    def scale_label(self, p):
        return f"{p['rows']:,} rows"

    def rows(self, rng, vocab, ids, ts_start):
        n = len(ids)
        return pa.Table.from_arrays(
            [
                ids,
                pa.array(np.arange(ts_start, ts_start + n, dtype=np.int64)),
                pa.array(CITIES).take(pa.array(rng.integers(0, len(CITIES), size=n))),
                pa.array(np.round(rng.uniform(2.5, 250.0, size=n), 2)),
                pa.array(STATUSES).take(
                    pa.array(rng.integers(0, len(STATUSES), size=n))
                ),
                words(rng, vocab, n, 8),
            ],
            schema=UUID_SCHEMA,
        )

    def write_base(self, d, p, seed, work_dir):
        rng = np.random.default_rng([seed, 0])
        vocab = make_vocab(seed)
        written = part = 0
        while written < p["rows"]:
            m = min(GEN_CHUNK_ROWS, p["rows"] - written)
            t = self.rows(rng, vocab, uuid4_strings(rng, m), TS_EPOCH + written)
            pq.write_table(t, d / f"part-{part:05d}.parquet")
            written += m
            part += 1

    def upsert(self, c, state, rng, p, ctx):
        u, i = p["upsert_batch"], p["insert_batch"]
        idx = rng.choice(state.num_rows, size=u, replace=False)
        ids = pa.concat_arrays(
            [
                state.column("id").take(pa.array(idx)).combine_chunks(),
                uuid4_strings(rng, i),
            ]
        )
        ts = TS_EPOCH + p["rows"] + c * (u + i)
        batch = self.rows(rng, make_vocab(ctx["seed"]), ids, ts)
        return batch.take(pa.array(rng.permutation(len(batch))))

    def delete(self, c, state, rng, p, ctx):
        idx = rng.choice(state.num_rows, size=p["delete_batch"], replace=False)
        return state.select(self.key_cols).take(pa.array(idx))


class TpcdsWebReturnsScenario(Scenario):
    name = "tpcds-web-returns"
    key_cols = ["wr_order_number", "wr_item_sk"]
    ordering = None
    check_cols = ["wr_return_quantity", "wr_return_amt"]
    knobs = {
        "scale_factor": (10.0, 1.0),
        "commits": (5, 2),
        "matched_fraction": (0.01, 0.01),
        "not_matched_fraction": (0.10, 0.10),
        "delete_fraction": (0.05, 0.05),
    }

    def base_params(self, p):
        return {"scale_factor": p["scale_factor"]}

    def scale_label(self, p):
        return f"sf {p['scale_factor']:g}"

    def write_base(self, d, p, seed, work_dir):
        import duckdb

        con = duckdb.connect()
        con.execute("INSTALL tpcds")
        con.execute("LOAD tpcds")
        con.execute(f"CALL dsdgen(sf={float(p['scale_factor'])})")
        t = con.sql("SELECT * FROM web_returns").to_arrow_table()
        for k in self.key_cols:
            if t.column(k).null_count:
                raise SystemExit(f"web_returns.{k} has nulls")
        fields = [
            pa.field(f.name, f.type, nullable=f.name not in self.key_cols)
            for f in t.schema
        ]
        t = t.cast(pa.schema(fields))
        for part, start in enumerate(range(0, t.num_rows, GEN_CHUNK_ROWS)):
            pq.write_table(
                t.slice(start, GEN_CHUNK_ROWS), d / f"part-{part:05d}.parquet"
            )

    def upsert(self, c, state, rng, p, ctx):
        n = state.num_rows
        matched = state.take(
            pa.array(rng.choice(n, size=int(n * p["matched_fraction"]), replace=False))
        )
        # Every matched row changes a value, so a writer that skips rows whose
        # values did not change still updates all of them.
        q = pc.add(pc.fill_null(matched.column("wr_return_quantity"), 0), 1)
        matched = matched.set_column(
            matched.schema.get_field_index("wr_return_quantity"),
            "wr_return_quantity",
            q,
        )
        new = state.take(
            pa.array(
                rng.choice(n, size=int(n * p["not_matched_fraction"]), replace=False)
            )
        )
        # Shifting past the largest live order number makes every sampled row a
        # new key while keeping its other values realistic.
        offset = pc.max(state.column("wr_order_number")).as_py() + 1
        new = new.set_column(
            new.schema.get_field_index("wr_order_number"),
            new.schema.field("wr_order_number"),
            pc.add(new.column("wr_order_number"), offset),
        )
        out = pa.concat_tables([matched, new]).drop_columns([KEY_COL])
        return out.take(pa.array(rng.permutation(out.num_rows)))

    def delete(self, c, state, rng, p, ctx):
        n = state.num_rows
        idx = rng.choice(n, size=int(n * p["delete_fraction"]), replace=False)
        return state.select(self.key_cols).take(pa.array(idx))


class NycTaxiScenario(Scenario):
    name = "nyc-taxi"
    key_cols = ["trip_id"]
    ordering = "tpep_pickup_datetime"
    check_cols = ["fare_amount", "tip_amount"]
    knobs = {
        "rows": (0, 100_000),
        "commits": (5, 2),
        "upsert_fraction": (0.01, 0.01),
        "delete_fraction": (0.001, 0.001),
    }

    def base_params(self, p):
        return {"rows": p["rows"]}

    def scale_label(self, p):
        return "all rows" if not p["rows"] else f"{p['rows']:,} rows"

    def write_base(self, d, p, seed, work_dir):
        # The download sits in its own directory and is only ever read as
        # parquet data.
        dl = Path(work_dir) / "download" / "nyc-taxi"
        dl.mkdir(parents=True, exist_ok=True)
        src = dl / NYC_TAXI_URL.rsplit("/", 1)[1]
        if not src.exists():
            tmp = src.with_suffix(".part")
            with (
                urllib.request.urlopen(NYC_TAXI_URL, timeout=120) as r,
                open(tmp, "wb") as f,
            ):
                shutil.copyfileobj(r, f)
            tmp.replace(src)
        digest = hashlib.sha256(src.read_bytes()).hexdigest()
        if digest != NYC_TAXI_SHA256:
            raise SystemExit(
                f"{src}: SHA-256 {digest} does not match the expected "
                f"{NYC_TAXI_SHA256}; delete the file to download it again, or check "
                f"whether {NYC_TAXI_URL} changed"
            )
        t = pq.read_table(src)
        if p["rows"]:
            t = t.slice(0, p["rows"])
        trip_id = padded_ids("", np.arange(t.num_rows), 9)
        t = t.add_column(0, pa.field("trip_id", pa.string(), nullable=False), trip_id)
        pq.write_table(t, d / "part-00000.parquet")

    def upsert(self, c, state, rng, p, ctx):
        n = state.num_rows
        rows = state.take(
            pa.array(rng.choice(n, size=int(n * p["upsert_fraction"]), replace=False))
        ).drop_columns([KEY_COL])
        m = rows.num_rows
        for col, lo, hi in (("fare_amount", 3.0, 120.0), ("tip_amount", 0.0, 30.0)):
            i = rows.schema.get_field_index(col)
            vals = pa.array(np.round(rng.uniform(lo, hi, size=m), 2)).cast(
                rows.schema.field(col).type
            )
            rows = rows.set_column(i, rows.schema.field(col), vals)
        return rows

    def delete(self, c, state, rng, p, ctx):
        n = state.num_rows
        idx = rng.choice(n, size=max(1, int(n * p["delete_fraction"])), replace=False)
        return state.select(self.key_cols).take(pa.array(idx))


class RagChunksScenario(Scenario):
    name = "rag-chunks"
    key_cols = ["chunk_id"]
    ordering = "updated_at"
    check_cols = ["updated_at", "embedding"]
    knobs = {
        "docs": (100_000, 1_000),
        "chunks_per_doc": (10, 10),
        "commits": (5, 2),
        "changed_docs": (1_000, 100),
        "removed_docs": (100, 10),
    }

    def base_params(self, p):
        return {"docs": p["docs"], "chunks_per_doc": p["chunks_per_doc"]}

    def scale_label(self, p):
        return f"{p['docs']:,} docs x {p['chunks_per_doc']}"

    def chunks(self, rng, vocab, docs, cpd, ts_start):
        docs = np.asarray(docs)
        n = len(docs) * cpd
        doc_idx = np.repeat(docs, cpd)
        pos = np.tile(np.arange(cpd, dtype=np.int32), len(docs))
        doc_id = padded_ids("doc-", doc_idx, 8)
        chunk_id = pc.binary_join_element_wise(doc_id, pa.array(pos.astype(str)), "#")
        emb = rng.random((n, EMBEDDING_DIM), dtype=np.float32).reshape(-1)
        offsets = pa.array(np.arange(n + 1, dtype=np.int32) * EMBEDDING_DIM)
        return pa.Table.from_arrays(
            [
                chunk_id,
                doc_id,
                pa.array(pos),
                words(rng, vocab, n, 64),
                pa.ListArray.from_arrays(offsets, pa.array(emb)),
                pa.array(np.arange(ts_start, ts_start + n, dtype=np.int64)),
            ],
            schema=RAG_SCHEMA,
        )

    def write_base(self, d, p, seed, work_dir):
        rng = np.random.default_rng([seed, 0])
        vocab = make_vocab(seed)
        cpd = p["chunks_per_doc"]
        per_part = max(1, 100_000 // cpd)
        for part, start in enumerate(range(0, p["docs"], per_part)):
            docs = np.arange(start, min(p["docs"], start + per_part))
            t = self.chunks(rng, vocab, docs, cpd, TS_EPOCH + start * cpd)
            pq.write_table(t, d / f"part-{part:05d}.parquet")

    def _live_docs(self, state):
        ids = pc.unique(state.column("doc_id"))
        return pc.utf8_slice_codeunits(ids, 4).cast(pa.int64()).to_numpy()

    def upsert(self, c, state, rng, p, ctx):
        live = self._live_docs(state)
        docs = np.sort(rng.choice(live, size=p["changed_docs"], replace=False))
        cpd = p["chunks_per_doc"]
        ts = TS_EPOCH + p["docs"] * cpd + c * p["changed_docs"] * cpd
        return self.chunks(rng, make_vocab(ctx["seed"]), docs, cpd, ts)

    def delete(self, c, state, rng, p, ctx):
        live = self._live_docs(state)
        docs = rng.choice(live, size=p["removed_docs"], replace=False)
        doc_ids = padded_ids("doc-", docs, 8)
        return state.filter(pc.is_in(state.column("doc_id"), value_set=doc_ids)).select(
            self.key_cols
        )


class TrainingSetScenario(Scenario):
    name = "training-set"
    key_cols = ["example_id"]
    ordering = "updated_at"
    check_cols = ["updated_at", "label", "messages"]
    incremental_reads = True
    knobs = {
        "rows": (1_000_000, 10_000),
        "commits": (5, 2),
        "upsert_batch": (10_000, 500),
        "insert_batch": (0, 0),
        "delete_batch": (1_000, 100),
    }

    def base_params(self, p):
        return {"rows": p["rows"]}

    def scale_label(self, p):
        return f"{p['rows']:,} rows"

    def examples(self, rng, vocab, ids, ts_start):
        n = len(ids)
        messages = pc.binary_join_element_wise(
            pa.scalar('[{"role":"user","content":"'),
            words(rng, vocab, n, 24),
            pa.scalar('"},{"role":"assistant","content":"'),
            words(rng, vocab, n, 48),
            pa.scalar('"}]'),
            "",
        )
        src = pc.binary_join_element_wise(
            padded_ids("doc-", rng.integers(0, 100_000, size=n), 8),
            pa.array(rng.integers(0, 10, size=n).astype(str)),
            "#",
        )
        return pa.Table.from_arrays(
            [
                ids,
                messages,
                pa.array(LABELS).take(pa.array(rng.integers(0, len(LABELS), size=n))),
                src,
                pa.array(np.arange(ts_start, ts_start + n, dtype=np.int64)),
            ],
            schema=TRAINING_SCHEMA,
        )

    def write_base(self, d, p, seed, work_dir):
        rng = np.random.default_rng([seed, 0])
        vocab = make_vocab(seed)
        for part, start in enumerate(range(0, p["rows"], GEN_CHUNK_ROWS)):
            stop = min(p["rows"], start + GEN_CHUNK_ROWS)
            ids = padded_ids("ex-", np.arange(start, stop), 9)
            t = self.examples(rng, vocab, ids, TS_EPOCH + start)
            pq.write_table(t, d / f"part-{part:05d}.parquet")

    def upsert(self, c, state, rng, p, ctx):
        u, i = p["upsert_batch"], p["insert_batch"]
        idx = rng.choice(state.num_rows, size=u, replace=False)
        first_new = p["rows"] + c * i
        ids = pa.concat_arrays(
            [
                state.column("example_id").take(pa.array(idx)).combine_chunks(),
                padded_ids("ex-", np.arange(first_new, first_new + i), 9),
            ]
        )
        ts = TS_EPOCH + p["rows"] + c * (u + i)
        return self.examples(rng, make_vocab(ctx["seed"]), ids, ts)

    def delete(self, c, state, rng, p, ctx):
        idx = rng.choice(state.num_rows, size=p["delete_batch"], replace=False)
        return state.select(self.key_cols).take(pa.array(idx))


SCENARIOS = {
    s.name: s
    for s in (
        UuidScenario(),
        TpcdsWebReturnsScenario(),
        NycTaxiScenario(),
        RagChunksScenario(),
        TrainingSetScenario(),
    )
}


def all_knobs():
    out = {}
    for s in SCENARIOS.values():
        out.update({k: type(v[0]) for k, v in s.knobs.items()})
    return out


def resolve_params(sc, args, smoke):
    p = {}
    for k, (default, smoke_default) in sc.knobs.items():
        v = getattr(args, k, None)
        p[k] = v if v is not None else (smoke_default if smoke else default)
    return p


def _digest(obj):
    return hashlib.sha1(json.dumps(obj, sort_keys=True).encode()).hexdigest()[:10]


# ---------------------------------------------------------------------------
# Workload builder
# ---------------------------------------------------------------------------


def workload_paths(sc, work_dir, p, seed, verify_sample):
    """The base and workload directories for these knobs, without creating
    anything, so a caller can lock them before they are built."""
    bd = Path(work_dir) / "data" / f"{sc.name}-{_digest([sc.base_params(p), seed])}"
    return bd / "base", bd / f"workload-{_digest([p, seed, verify_sample])}"


def ensure_base(sc, work_dir, p, seed):
    bp = sc.base_params(p)
    d = workload_paths(sc, work_dir, p, seed, 0)[0]
    marker = d / "_SUCCESS"
    if marker.exists():
        return d
    if d.exists():
        shutil.rmtree(d)
    d.mkdir(parents=True)
    sc.write_base(d, p, seed, work_dir)
    marker.write_text(json.dumps({"scenario": sc.name, **bp, "seed": seed}))
    return d


def read_base(d):
    files = sorted(Path(d).glob("part-*.parquet"))
    return pa.concat_tables([pq.read_table(f) for f in files]).combine_chunks()


def _with_key(sc, t):
    return t.append_column(KEY_COL, key_strings(t, sc.key_cols))


def plain_type(t):
    """The type a leg's output is compared in: view and large variants become
    their plain form, and list item fields are named and nullable as pyarrow's
    default."""
    if pa.types.is_string_view(t) or pa.types.is_large_string(t):
        return pa.string()
    if pa.types.is_binary_view(t) or pa.types.is_large_binary(t):
        return pa.binary()
    if pa.types.is_list(t) or pa.types.is_large_list(t) or pa.types.is_list_view(t):
        return pa.list_(plain_type(t.value_type))
    return t


def oracle_schema(schema):
    # Every field is nullable: legs differ in the nullability they report, and
    # whether a value is null is still compared row by row.
    return pa.schema([pa.field(f.name, plain_type(f.type)) for f in schema])


def sorted_by_key(table, key_cols):
    return table.sort_by([(k, "ascending") for k in key_cols])


def oracle_dir(sc, work_dir, p, wd):
    return Path(work_dir) / "oracle" / f"{sc.name}-{sc.scale_slug(p)}-{wd.name}"


def write_oracle(sc, state, path):
    t = sorted_by_key(state.drop_columns([KEY_COL]), sc.key_cols)
    pq.write_table(t.cast(oracle_schema(t.schema)), path)


def ensure_workload(sc, work_dir, p, seed, verify_sample):
    """Write each commit's input, the read-back expectations and plan.json.

    Commits replay against an in-memory copy of the table, so the expected end
    state is exact rather than estimated.
    """
    bd = ensure_base(sc, work_dir, p, seed)
    wd = workload_paths(sc, work_dir, p, seed, verify_sample)[1]
    marker = wd / "_SUCCESS"
    od = oracle_dir(sc, work_dir, p, wd)
    oracles = ["expected.parquet"] + (["base.parquet"] if sc.incremental_reads else [])
    if marker.exists() and all((od / f).exists() for f in oracles):
        return bd, wd
    for d in (wd, od):
        if d.exists():
            shutil.rmtree(d)
    wd.mkdir(parents=True)
    od.mkdir(parents=True)

    rng = np.random.default_rng([seed, 1])
    ctx = {"seed": seed}
    state = _with_key(sc, read_base(bd))
    base_rows = state.num_rows
    if pc.count_distinct(state.column(KEY_COL)).as_py() != base_rows:
        raise SystemExit(f"{sc.name}: base record keys are not unique")
    base_keys = state.column(KEY_COL).combine_chunks()
    if sc.incremental_reads:
        # Time travel to the load must return exactly this table.
        write_oracle(sc, state, od / "base.parquet")

    expected = {"upsert": [], "delete": []}
    updated_existing, inserted, deleted = set(), set(), set()
    for c in range(p["commits"]):
        up = sc.upsert(c, state, rng, p, ctx)
        up = up.cast(pa.schema([state.schema.field(f.name) for f in up.schema]))
        upk = key_strings(up, sc.key_cols)
        if pc.count_distinct(upk).as_py() != up.num_rows:
            raise SystemExit(f"{sc.name}: upsert {c} has duplicate keys")
        exists = pc.is_in(upk, value_set=state.column(KEY_COL))
        n_upd = pc.sum(exists).as_py() or 0
        expected["upsert"].append(
            {"rows": up.num_rows, "updated": n_upd, "inserted": up.num_rows - n_upd}
        )
        for k, e in zip(upk.to_pylist(), exists.to_pylist()):
            (updated_existing if e and k not in inserted else inserted).add(k)
        pq.write_table(up, wd / f"upsert-{c:02d}.parquet")
        state = state.filter(pc.invert(pc.is_in(state.column(KEY_COL), value_set=upk)))
        state = pa.concat_tables([state, up.append_column(KEY_COL, upk)])
    for c in range(p["commits"]):
        dk = sc.delete(c, state, rng, p, ctx)
        dkk = key_strings(dk, sc.key_cols)
        hit = pc.sum(pc.is_in(dkk, value_set=state.column(KEY_COL))).as_py() or 0
        if hit != dk.num_rows or pc.count_distinct(dkk).as_py() != dk.num_rows:
            raise SystemExit(f"{sc.name}: delete {c} keys are not distinct live keys")
        expected["delete"].append({"deleted": dk.num_rows})
        pq.write_table(dk, wd / f"delete-{c:02d}.parquet")
        deleted.update(dkk.to_pylist())
        state = state.filter(pc.invert(pc.is_in(state.column(KEY_COL), value_set=dkk)))
    write_oracle(sc, state, od / "expected.parquet")

    def pick(keys):
        keys = sorted(keys)
        if len(keys) <= verify_sample:
            return keys
        return [
            keys[j]
            for j in sorted(rng.choice(len(keys), size=verify_sample, replace=False))
        ]

    groups = {
        "updated": pick(k for k in updated_existing if k not in deleted),
        "inserted": pick(k for k in inserted if k not in deleted),
        "deleted": pick(deleted),
    }
    touched = updated_existing | inserted | deleted
    untouched = []
    for j in rng.permutation(len(base_keys)):
        if len(untouched) >= verify_sample:
            break
        k = base_keys[int(j)].as_py()
        if k not in touched:
            untouched.append(k)
    groups["untouched"] = sorted(untouched)

    present = [k for kind in ("updated", "inserted", "untouched") for k in groups[kind]]
    exp = state.filter(pc.is_in(state.column(KEY_COL), value_set=pa.array(present)))
    exp = exp.select([KEY_COL] + sc.check_cols)
    kind_of = {k: kind for kind, ks in groups.items() for k in ks}
    exp = exp.append_column(
        KIND_COL, pa.array([kind_of[k] for k in exp.column(KEY_COL).to_pylist()])
    )
    gone = pa.table(
        {KEY_COL: pa.array(groups["deleted"], pa.string())}
        | {
            c: pa.nulls(len(groups["deleted"]), exp.schema.field(c).type)
            for c in sc.check_cols
        }
        | {KIND_COL: pa.array(["deleted"] * len(groups["deleted"]), pa.string())}
    )
    verify = pa.concat_tables([exp, gone.select(exp.column_names)])
    pq.write_table(verify, wd / "verify.parquet")

    plan = {
        "scenario": sc.name,
        "params": p,
        "seed": seed,
        "scale": sc.scale_label(p),
        "key_cols": sc.key_cols,
        "ordering": sc.ordering,
        "check_cols": sc.check_cols,
        "incremental_reads": sc.incremental_reads,
        "commits": p["commits"],
        "base_rows": base_rows,
        "expected": expected,
        "expected_rows": state.num_rows,
        "verify_counts": {k: len(v) for k, v in groups.items()},
        "oracle": str((od / "expected.parquet").relative_to(work_dir)),
        "oracle_base": (
            str((od / "base.parquet").relative_to(work_dir))
            if sc.incremental_reads
            else None
        ),
    }
    (wd / "plan.json").write_text(json.dumps(plan, indent=2))
    marker.write_text("ok")
    return bd, wd
