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

"""How summarize.py renders a case row."""

import pytest
import summarize


def case(status, repetitions):
    stat = {"n": repetitions, "median": 0.5, "min": 0.4, "max": 0.6, "cv": 0.1}
    return {
        "status": status,
        "repeat": 3,
        "repetitions": [{"phases": {"readback": {}}}] * repetitions,
        "summary": {
            "repetitions": repetitions,
            "stats": {
                k: stat
                for k in ("load_s", "upsert_mean_s", "delete_mean_s", "readback_s")
            },
            "upsert_bytes_per_commit": 8 * summarize.MIB,
            "delete_bytes_per_commit": 4 * summarize.MIB,
            "upsert_data_files_created_per_commit": 2.0,
            "correct": status == "ok",
        },
    }


def test_a_case_that_finished_shows_its_timings_and_bytes():
    row = summarize.case_row("hudi-mor", case("ok", 3), "uuid", "1,000 rows", "median")
    assert row[3:14] == [
        "ok",
        "3/3",
        "0.50 s (0.40 to 0.60)",
        "0.50 s (0.40 to 0.60)",
        "0.50 s (0.40 to 0.60)",
        "0.50 s (0.40 to 0.60)",
        "-",
        "-",
        "8",
        "4",
        "2",
    ]


@pytest.mark.parametrize("status", ["refused", "failed", "timeout"])
def test_a_case_that_did_not_finish_shows_no_timings_or_bytes(status):
    row = summarize.case_row(
        "hudi-mor", case(status, 2), "uuid", "1,000 rows", "median"
    )
    assert row[3:4] == [status]
    assert row[5:14] == ["-"] * 9


@pytest.mark.parametrize("status, noted", [("ok", True), ("failed", False)])
def test_only_a_finished_case_gets_a_variation_note(status, noted):
    c = case(status, 2)
    c["summary"]["stats"]["upsert_mean_s"]["cv"] = 0.5
    notes = summarize.case_notes("uuid", "hudi-mor", c)
    assert any("upsert" in n for n in notes) == noted


@pytest.mark.parametrize(
    "extra, shown",
    [({"max_other_load": 1.25}, "max other-process load 1.25 cores, "), ({}, "")],
)
def test_headroom_note_shows_other_load_when_recorded(extra, shown):
    c = case("ok", 3)
    c["summary"]["headroom"] = {
        "readings": 4,
        "max_load1": 3.5,
        "min_mem_available_bytes": 20 * 1024**3,
        "min_disk_free_bytes": 100 * 1024**3,
        **extra,
    }
    assert summarize.headroom_note("uuid", "hudi-mor", c) == (
        f"uuid hudi-mor headroom over 4 readings: {shown}max load1 3.50, "
        "min available memory 20.0 GiB, min free disk 100.0 GiB"
    )
    assert summarize.headroom_note("uuid", "hudi-mor", case("ok", 3)) is None
