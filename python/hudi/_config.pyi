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
from enum import Enum

class HudiTableConfig(str, Enum):
    """Configurations for Hudi tables, most of them are persisted in `hoodie.properties`."""

    BASE_FILE_FORMAT = "hoodie.table.base.file.format"
    BASE_PATH = "hoodie.base.path"
    CHECKSUM = "hoodie.table.checksum"
    CREATE_SCHEMA = "hoodie.table.create.schema"
    DATABASE_NAME = "hoodie.database.name"
    DROPS_PARTITION_FIELDS = "hoodie.datasource.write.drop.partition.columns"
    IS_HIVE_STYLE_PARTITIONING = "hoodie.datasource.write.hive_style_partitioning"
    IS_PARTITION_PATH_URLENCODED = "hoodie.datasource.write.partitionpath.urlencode"
    KEY_GENERATOR_CLASS = "hoodie.table.keygenerator.class"
    KEY_GENERATOR_TYPE = "hoodie.table.keygenerator.type"
    PARTITION_FIELDS = "hoodie.table.partition.fields"
    PRECOMBINE_FIELD = "hoodie.table.precombine.field"
    POPULATES_META_FIELDS = "hoodie.populate.meta.fields"
    RECORD_KEY_FIELDS = "hoodie.table.recordkey.fields"
    RECORD_MERGE_STRATEGY = "hoodie.table.record.merge.strategy"
    TABLE_NAME = "hoodie.table.name"
    TABLE_TYPE = "hoodie.table.type"
    TABLE_VERSION = "hoodie.table.version"
    TIMELINE_LAYOUT_VERSION = "hoodie.timeline.layout.version"
    TIMELINE_TIMEZONE = "hoodie.table.timeline.timezone"
    ARCHIVE_LOG_FOLDER = "hoodie.archivelog.folder"
    TIMELINE_PATH = "hoodie.timeline.path"
    TIMELINE_HISTORY_PATH = "hoodie.timeline.history.path"
    METADATA_TABLE_ENABLED = "hoodie.metadata.enable"
    METADATA_TABLE_PARTITIONS = "hoodie.table.metadata.partitions"

class HudiReadConfig(str, Enum):
    """Configurations for reading Hudi tables."""

    FILE_GROUP_START_TIMESTAMP = "hoodie.read.file_group.start_timestamp"
    FILE_GROUP_END_TIMESTAMP = "hoodie.read.file_group.end_timestamp"
    INPUT_PARTITIONS = "hoodie.read.input.partitions"
    LISTING_PARALLELISM = "hoodie.read.listing.parallelism"
    USE_READ_OPTIMIZED_MODE = "hoodie.read.use.read_optimized.mode"
    STREAM_BATCH_SIZE = "hoodie.read.stream.batch_size"
