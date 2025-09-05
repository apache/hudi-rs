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

import pyarrow as pa
import pytest

from hudi import HudiReadConfig, HudiTableBuilder, HudiTableConfig


@pytest.fixture
def base_uri():
    return "test://base/uri"


@pytest.fixture
def builder(base_uri):
    return HudiTableBuilder(base_uri)


def test_initialization(builder, base_uri):
    assert builder.base_uri == base_uri
    assert builder.hudi_options == {}
    assert builder.storage_options == {}
    assert builder.options == {}


def test_from_base_uri(base_uri):
    builder = HudiTableBuilder.from_base_uri(base_uri)
    assert builder.base_uri == base_uri


@pytest.mark.parametrize(
    "method,attr",
    [
        ("with_hudi_option", "hudi_options"),
        ("with_storage_option", "storage_options"),
        ("with_option", "options"),
    ],
)
def test_with_single_option(builder, method, attr):
    getattr(builder, method)("key1", "value1")
    assert getattr(builder, attr) == {"key1": "value1"}


@pytest.mark.parametrize(
    "method,attr",
    [
        ("with_hudi_options", "hudi_options"),
        ("with_storage_options", "storage_options"),
        ("with_options", "options"),
    ],
)
def test_with_multiple_options(builder, method, attr):
    options = {"key1": "value1", "key2": "value2"}
    getattr(builder, method)(options)
    assert getattr(builder, attr) == options


def test_read_table_returns_correct_data(get_sample_table):
    table_path = get_sample_table
    table = HudiTableBuilder.from_base_uri(table_path).build()

    batches = table.read_snapshot()
    t = pa.Table.from_batches(batches).select([0, 5, 6, 9]).sort_by("ts")
    assert t.to_pylist() == [
        {
            "_hoodie_commit_time": "20240402144910683",
            "ts": 1695046462179,
            "uuid": "9909a8b1-2d15-4d3d-8ec9-efc48c536a00",
            "fare": 339.0,
        },
        {
            "_hoodie_commit_time": "20240402123035233",
            "ts": 1695091554788,
            "uuid": "e96c4396-3fad-413a-a942-4cb36106d721",
            "fare": 27.7,
        },
        {
            "_hoodie_commit_time": "20240402123035233",
            "ts": 1695115999911,
            "uuid": "c8abbe79-8d89-47ea-b4ce-4d224bae5bfa",
            "fare": 17.85,
        },
        {
            "_hoodie_commit_time": "20240402123035233",
            "ts": 1695159649087,
            "uuid": "334e26e9-8355-45cc-97c6-c31daf0df330",
            "fare": 19.1,
        },
        {
            "_hoodie_commit_time": "20240402123035233",
            "ts": 1695516137016,
            "uuid": "e3cf430c-889d-4015-bc98-59bdce1e530c",
            "fare": 34.15,
        },
    ]


@pytest.mark.parametrize(
    "hudi_options,storage_options,options",
    [
        (
            {"hoodie.read.file_group.start_timestamp": "resolved value"},
            {"hoodie.read.file_group.start_timestamp": "not taking"},
            {"hoodie.read.file_group.start_timestamp": "lower precedence"},
        ),
        (
            {},
            {"hoodie.read.file_group.start_timestamp": "not taking"},
            {"hoodie.read.file_group.start_timestamp": "resolved value"},
        ),
    ],
)
def test_setting_table_options(
    get_sample_table, hudi_options, storage_options, options
):
    table_path = get_sample_table
    table = (
        HudiTableBuilder.from_base_uri(table_path)
        .with_hudi_options(hudi_options)
        .with_storage_options(storage_options)
        .with_options(options)
        .build()
    )

    assert (
        table.hudi_options().get("hoodie.read.file_group.start_timestamp")
        == "resolved value"
    )


def test_with_hudi_option_enum(builder):
    """Test that HudiTableConfig and HudiReadConfig enums work with with_hudi_option."""
    builder.with_hudi_option(HudiTableConfig.TABLE_NAME, "test_table")
    assert builder.hudi_options["hoodie.table.name"] == "test_table"

    builder.with_hudi_option(HudiReadConfig.INPUT_PARTITIONS, "5")
    assert builder.hudi_options["hoodie.read.input.partitions"] == "5"


def test_with_option_enum(builder):
    """Test that HudiTableConfig and HudiReadConfig enums work with with_option."""
    builder.with_option(HudiTableConfig.BASE_FILE_FORMAT, "parquet")
    assert builder.options["hoodie.table.base.file.format"] == "parquet"

    builder.with_option(HudiReadConfig.LISTING_PARALLELISM, "10")
    assert builder.options["hoodie.read.listing.parallelism"] == "10"


def test_enum_values_match_expected_strings():
    """Test that enum values match the expected configuration key strings."""
    assert HudiTableConfig.TABLE_NAME.value == "hoodie.table.name"
    assert HudiTableConfig.TABLE_TYPE.value == "hoodie.table.type"
    assert HudiTableConfig.BASE_FILE_FORMAT.value == "hoodie.table.base.file.format"

    assert HudiReadConfig.INPUT_PARTITIONS.value == "hoodie.read.input.partitions"
    assert HudiReadConfig.LISTING_PARALLELISM.value == "hoodie.read.listing.parallelism"
    assert (
        HudiReadConfig.USE_READ_OPTIMIZED_MODE.value
        == "hoodie.read.use.read_optimized.mode"
    )
    assert (
        HudiReadConfig.FILE_GROUP_START_TIMESTAMP.value
        == "hoodie.read.file_group.start_timestamp"
    )
    assert (
        HudiReadConfig.FILE_GROUP_END_TIMESTAMP.value
        == "hoodie.read.file_group.end_timestamp"
    )


def test_auto_generated_enum_completeness():
    """Test that all expected enum variants are auto-generated."""
    expected_table_config_variants = [
        "BASE_FILE_FORMAT",
        "BASE_PATH",
        "CHECKSUM",
        "CREATE_SCHEMA",
        "DATABASE_NAME",
        "DROPS_PARTITION_FIELDS",
        "IS_HIVE_STYLE_PARTITIONING",
        "IS_PARTITION_PATH_URLENCODED",
        "KEY_GENERATOR_CLASS",
        "PARTITION_FIELDS",
        "PRECOMBINE_FIELD",
        "POPULATES_META_FIELDS",
        "RECORD_KEY_FIELDS",
        "RECORD_MERGE_STRATEGY",
        "TABLE_NAME",
        "TABLE_TYPE",
        "TABLE_VERSION",
        "TIMELINE_LAYOUT_VERSION",
        "TIMELINE_TIMEZONE",
    ]

    for variant in expected_table_config_variants:
        assert hasattr(HudiTableConfig, variant), f"HudiTableConfig missing {variant}"
        enum_instance = getattr(HudiTableConfig, variant)
        assert hasattr(enum_instance, "value"), (
            f"{variant} instance missing 'value' property"
        )
        assert isinstance(enum_instance.value, str), f"{variant}.value is not a string"

    expected_read_config_variants = [
        "FILE_GROUP_START_TIMESTAMP",
        "FILE_GROUP_END_TIMESTAMP",
        "INPUT_PARTITIONS",
        "LISTING_PARALLELISM",
        "USE_READ_OPTIMIZED_MODE",
    ]

    for variant in expected_read_config_variants:
        assert hasattr(HudiReadConfig, variant), f"HudiReadConfig missing {variant}"
        enum_instance = getattr(HudiReadConfig, variant)
        assert hasattr(enum_instance, "value"), (
            f"{variant} instance missing 'value' property"
        )
        assert isinstance(enum_instance.value, str), f"{variant}.value is not a string"


def test_auto_generated_enum_methods():
    """Test that auto-generated enum methods work correctly."""
    table_variants = HudiTableConfig.all_variants()
    assert isinstance(table_variants, list), "all_variants() should return a list"
    assert len(table_variants) == 19, "HudiTableConfig should have 19 variants"

    read_variants = HudiReadConfig.all_variants()
    assert isinstance(read_variants, list), "all_variants() should return a list"
    assert len(read_variants) == 5, "HudiReadConfig should have 5 variants"

    table_instance = HudiTableConfig.TABLE_NAME
    assert str(table_instance) == "hoodie.table.name"
    assert "HudiTableConfig" in repr(table_instance)
    assert "hoodie.table.name" in repr(table_instance)

    read_instance = HudiReadConfig.INPUT_PARTITIONS
    assert str(read_instance) == "hoodie.read.input.partitions"
    assert "HudiReadConfig" in repr(read_instance)
    assert "hoodie.read.input.partitions" in repr(read_instance)


def test_auto_generated_enum_equality():
    """Test that auto-generated enum instances support equality."""
    table1 = HudiTableConfig.TABLE_NAME
    table2 = HudiTableConfig.TABLE_NAME
    assert table1 == table2

    table_name = HudiTableConfig.TABLE_NAME
    table_type = HudiTableConfig.TABLE_TYPE
    assert table_name != table_type

    table_config = HudiTableConfig.TABLE_NAME
    read_config = HudiReadConfig.INPUT_PARTITIONS
    assert table_config != read_config


def test_no_manual_duplication_regression():
    """Test that we haven't accidentally kept manual enum definitions."""
    import inspect

    # Get all class attributes of HudiTableConfig - filter out methods and properties
    table_attrs = []
    for attr in dir(HudiTableConfig):
        if attr.startswith("_") or attr in [
            "all_variants"
        ]:  # Skip special methods and class methods
            continue
        attr_value = getattr(HudiTableConfig, attr)
        # Skip methods, classmethods, staticmethods, and properties
        if (
            inspect.ismethod(attr_value)
            or inspect.isfunction(attr_value)
            or isinstance(attr_value, (classmethod, staticmethod, property))
            or inspect.isdatadescriptor(attr_value)
            or hasattr(attr_value, "__self__")
        ):  # Skip bound methods
            continue
        table_attrs.append(attr)

    # Get all class attributes of HudiReadConfig - filter out methods and properties
    read_attrs = []
    for attr in dir(HudiReadConfig):
        if attr.startswith("_") or attr in [
            "all_variants"
        ]:  # Skip special methods and class methods
            continue
        attr_value = getattr(HudiReadConfig, attr)
        # Skip methods, classmethods, staticmethods, and properties
        if (
            inspect.ismethod(attr_value)
            or inspect.isfunction(attr_value)
            or isinstance(attr_value, (classmethod, staticmethod, property))
            or inspect.isdatadescriptor(attr_value)
            or hasattr(attr_value, "__self__")
        ):  # Skip bound methods
            continue
        read_attrs.append(attr)

    # All attributes should be auto-generated enum instances
    for attr in table_attrs:
        instance = getattr(HudiTableConfig, attr)
        assert hasattr(instance, "value"), (
            f"Table config {attr} should have 'value' property"
        )
        assert isinstance(instance.value, str), (
            f"Table config {attr}.value should be a string"
        )
        assert instance.value.startswith("hoodie."), (
            f"Table config {attr}.value should start with 'hoodie.'"
        )

    for attr in read_attrs:
        instance = getattr(HudiReadConfig, attr)
        assert hasattr(instance, "value"), (
            f"Read config {attr} should have 'value' property"
        )
        assert isinstance(instance.value, str), (
            f"Read config {attr}.value should be a string"
        )
        assert instance.value.startswith("hoodie."), (
            f"Read config {attr}.value should start with 'hoodie.'"
        )

    assert len(table_attrs) == 19, (
        f"Expected 19 table config variants, got {len(table_attrs)}: {table_attrs}"
    )
    assert len(read_attrs) == 5, (
        f"Expected 5 read config variants, got {len(read_attrs)}: {read_attrs}"
    )


def test_mixed_string_and_enum_usage(builder):
    """Test that strings and enums can be used together."""
    builder.with_hudi_option("custom.string.key", "string_value")
    builder.with_hudi_option(HudiTableConfig.TABLE_NAME, "enum_table")

    assert builder.hudi_options["custom.string.key"] == "string_value"
    assert builder.hudi_options["hoodie.table.name"] == "enum_table"


def test_backward_compatibility(builder):
    """Test that existing string-based API still works."""
    builder.with_hudi_option("hoodie.table.name", "string_table")
    builder.with_option("hoodie.read.input.partitions", "8")

    assert builder.hudi_options["hoodie.table.name"] == "string_table"
    assert builder.options["hoodie.read.input.partitions"] == "8"
