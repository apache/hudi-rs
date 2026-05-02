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


def test_internal_config_keys_returns_both_enums():
    """The Rust pyfunction returns entries for both config enums."""
    from hudi._internal import _config_keys

    keys = _config_keys()
    assert set(keys.keys()) == {"HudiTableConfig", "HudiReadConfig"}

    table_entries = keys["HudiTableConfig"]
    read_entries = keys["HudiReadConfig"]

    assert all(
        isinstance(name, str) and isinstance(key, str) for name, key in table_entries
    )
    assert all(key.startswith("hoodie.") for _, key in table_entries)
    assert all(key.startswith("hoodie.") for _, key in read_entries)

    table_dict = dict(table_entries)
    read_dict = dict(read_entries)
    assert table_dict["TABLE_NAME"] == "hoodie.table.name"
    assert table_dict["BASE_FILE_FORMAT"] == "hoodie.table.base.file.format"
    assert read_dict["INPUT_PARTITIONS"] == "hoodie.read.input.partitions"
    assert read_dict["START_TIMESTAMP"] == "hoodie.read.start.timestamp"


def test_config_enums_are_real_python_enums():
    """The exported enums are real Python `Enum` classes built from Rust at import."""
    from enum import Enum

    from hudi import HudiReadConfig, HudiTableConfig

    assert issubclass(HudiTableConfig, Enum)
    assert issubclass(HudiReadConfig, Enum)
    assert isinstance(HudiTableConfig.TABLE_NAME, HudiTableConfig)

    # str mixin
    assert HudiTableConfig.TABLE_NAME.value == "hoodie.table.name"
    assert HudiTableConfig.TABLE_NAME == "hoodie.table.name"
    assert HudiReadConfig.INPUT_PARTITIONS == "hoodie.read.input.partitions"

    # Lookups, hashing, iteration
    assert HudiTableConfig["TABLE_NAME"] is HudiTableConfig.TABLE_NAME
    assert HudiTableConfig("hoodie.table.name") is HudiTableConfig.TABLE_NAME
    assert hash(HudiTableConfig.TABLE_NAME) == hash(HudiTableConfig.TABLE_NAME)
    assert {HudiTableConfig.TABLE_NAME: 1}[HudiTableConfig.TABLE_NAME] == 1
    assert HudiTableConfig.TABLE_NAME in list(HudiTableConfig)

    # Population non-empty (defends against silent regression)
    assert len(HudiTableConfig) >= 20
    assert len(HudiReadConfig) >= 5
