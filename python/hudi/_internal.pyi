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
from dataclasses import dataclass
from typing import ClassVar, Dict, List, Optional, Tuple

import pyarrow  # type: ignore[import-untyped]

__version__: str

def _config_keys() -> Dict[str, List[Tuple[str, str]]]: ...

class HudiQueryType:
    """Selects the read semantic on :class:`HudiReadOptions`."""

    Snapshot: ClassVar["HudiQueryType"]
    Incremental: ClassVar["HudiQueryType"]

    @property
    def name(self) -> str: ...
    @property
    def value(self) -> str: ...

@dataclass(init=False)
class HudiReadOptions:
    """
    Options for all Hudi read APIs (snapshot, time-travel, incremental, eager and streaming).

    Mirrors the Rust ``ReadOptions`` struct shape: only three stored attributes —
    ``filters``, ``projection``, ``hudi_options``. All other knobs (query type,
    timestamps, batch size) are set via the chainable ``with_*`` builders, which
    insert into ``hudi_options`` under the matching ``HudiReadConfig`` key.
    Typed accessors (``query_type()``, ``as_of_timestamp()``, etc.) read back
    from the bag.

    Attributes:
        filters (List[Tuple[str, str, str]]): Column filters as ``(field, op, value)`` tuples.
        projection (Optional[List[str]]): Column names to read. If None, all columns are read.
        hudi_options (Dict[str, str]): Per-read Hudi configs (``hoodie.*``).
    """

    filters: List[Tuple[str, str, str]]
    projection: Optional[List[str]]
    hudi_options: Dict[str, str]

    def __init__(
        self,
        filters: Optional[List[Tuple[str, str, str]]] = None,
        projection: Optional[List[str]] = None,
        hudi_options: Optional[Dict[str, str]] = None,
    ): ...

    # Builders return a new HudiReadOptions for chaining.
    def with_query_type(self, query_type: HudiQueryType) -> "HudiReadOptions": ...
    def with_as_of_timestamp(self, timestamp: str) -> "HudiReadOptions": ...
    def with_start_timestamp(self, timestamp: str) -> "HudiReadOptions": ...
    def with_end_timestamp(self, timestamp: str) -> "HudiReadOptions": ...
    def with_batch_size(self, size: int) -> "HudiReadOptions":
        """Target rows per batch for streaming reads. Raises ``HudiCoreError`` if ``size == 0``."""
        ...
    def with_filters(self, filters: List[Tuple[str, str, str]]) -> "HudiReadOptions":
        """Set column filters as ``(field, op, value)`` tuples. Parses and cardinality-validates immediately; an unrecognized operator or empty ``IN`` / ``NOT IN`` value list raises ``HudiCoreError``."""
        ...
    def with_projection(self, columns: List[str]) -> "HudiReadOptions": ...
    def with_hudi_option(self, key: str, value: str) -> "HudiReadOptions": ...
    def with_hudi_options(self, opts: Dict[str, str]) -> "HudiReadOptions": ...

    # Typed accessors read from hudi_options.
    def query_type(self) -> HudiQueryType: ...
    def as_of_timestamp(self) -> Optional[str]: ...
    def start_timestamp(self) -> Optional[str]: ...
    def end_timestamp(self) -> Optional[str]: ...
    def batch_size(self) -> Optional[int]:
        """Target batch size for streaming reads, or ``None`` if unset. Raises ``HudiCoreError`` if the stored value is malformed."""
        ...

@dataclass(init=False)
class HudiRecordBatchStream:
    """
    A stream of record batches yielded one at a time.

    Iterating consumes the stream. The stream is single-use.
    """

    def __iter__(self) -> "HudiRecordBatchStream": ...
    def __next__(self) -> "pyarrow.RecordBatch": ...

@dataclass(init=False)
class HudiFileGroupReader:
    """
    The reader that handles all read operations against a file group.

    Attributes:
        base_uri (str): The base URI of the file group's residing table.
        options (Optional[Dict[str, str]]): Additional options for the reader.
    """
    def __init__(self, base_uri: str, options: Optional[Dict[str, str]] = None):
        """
        Initializes the HudiFileGroupReader.

        Parameters:
            base_uri (str): The base URI of the Hudi table.
            options (Optional[Dict[str, str]]): Additional configuration options (optional).
        """
        ...
    def read_file_slice(
        self,
        file_slice: HudiFileSlice,
        options: Optional[HudiReadOptions] = None,
    ) -> "pyarrow.RecordBatch":
        """
        Read the data from the given file slice (base + log files merged).
        """
        ...

    def read_file_slice_from_paths(
        self,
        base_file_path: str,
        log_file_paths: List[str],
        options: Optional[HudiReadOptions] = None,
    ) -> "pyarrow.RecordBatch":
        """
        Read a file slice from a base file path and a list of log file paths.
        """
        ...

    def read_file_slice_stream(
        self,
        file_slice: HudiFileSlice,
        options: Optional[HudiReadOptions] = None,
    ) -> HudiRecordBatchStream:
        """
        Reads a file slice as a stream of record batches.

        For COW tables or read-optimized mode, this yields batches as they are read
        without loading all data into memory. For MOR tables with log files, this
        currently falls back to a single merged batch.
        """
        ...

    def read_file_slice_from_paths_stream(
        self,
        base_file_path: str,
        log_file_paths: List[str],
        options: Optional[HudiReadOptions] = None,
    ) -> HudiRecordBatchStream:
        """
        Reads a file slice from explicit paths as a stream of record batches.
        """
        ...

    @property
    def is_metadata_table(self) -> bool:
        """
        Whether this reader targets a metadata table (base path ends with ``.hoodie/metadata``).
        """
        ...

@dataclass(init=False)
class HudiFileSlice:
    """
    Represents a file slice in a Hudi table. A file slice includes information about the base file,
    the partition it belongs to, and associated metadata.

    Attributes:
        file_id (str): The id of the file group this file slice belongs to.
        partition_path (str): The path of the partition containing this file slice.
        creation_instant_time (str): The creation instant time of this file slice.
        base_file_name (str): The name of the base file.
        base_file_size (int): The on-disk size of the base file in bytes.
        base_file_byte_size (int): The in-memory size of the base file in bytes.
        log_file_names (List[str]): The names of the ordered log files.
        num_records (int): The number of records in the file slice.
    """

    file_id: str
    partition_path: str
    creation_instant_time: str
    base_file_name: str
    base_file_size: int
    base_file_byte_size: int
    log_file_names: List[str]
    num_records: int

    def base_file_relative_path(self) -> str:
        """
        Returns the relative path of the base file for this file slice.

        Returns:
            str: The relative path of the base file.
        """
        ...
    def log_files_relative_paths(self) -> List[str]:
        """
        Returns the relative paths of the log files for this file slice.

        Returns:
            List[str]: A list of relative paths of the log files.
        """
        ...

@dataclass(init=False)
class HudiInstant:
    @property
    def timestamp(self) -> str: ...
    @property
    def action(self) -> str: ...
    @property
    def state(self) -> str: ...
    @property
    def epoch_mills(self) -> int: ...

@dataclass(init=False)
class HudiTable:
    """
    Represents a Hudi table and provides methods to interact with it.

    Attributes:
        base_uri (str): The base URI of the Hudi table.
        options (Optional[Dict[str, str]]): Additional options for table operations.
    """

    def __init__(
        self,
        base_uri: str,
        options: Optional[Dict[str, str]] = None,
    ):
        """
        Initializes the HudiTable.

        Parameters:
            base_uri (str): The base URI of the Hudi table.
            options (Optional[Dict[str, str]]): Additional configuration options (optional).
        """
        ...
    def hudi_options(self) -> Dict[str, str]:
        """
        Get hudi options for table.

        Returns:
            Dict[str, str]: A dictionary of hudi options.
        """
        ...
    def storage_options(self) -> Dict[str, str]:
        """
        Get storage options set for table instance.

        Returns:
            Dict[str, str]: A dictionary of storage options.
        """
        ...
    @property
    def table_name(self) -> str:
        """
        Get table name.

        Returns:
            str: The name of the table.
        """
        ...
    @property
    def table_type(self) -> str:
        """
        Get table type.

        Returns:
            str: The type of the table.
        """
        ...
    @property
    def is_mor(self) -> bool:
        """
        Get whether the table is an MOR table.

        Returns:
            bool: True if the table is a MOR table, False otherwise.
        """
        ...
    @property
    def timezone(self) -> str:
        """
        Get timezone.

        Returns:
            str: The timezone of the table.
        """
        ...
    def get_schema_in_avro_str(self) -> str:
        """
        Returns the Avro schema of the Hudi table, without meta fields.

        Returns:
            str: The Avro schema of the table.
        """
        ...
    def get_schema_in_avro_str_with_meta_fields(self) -> str:
        """
        Returns the Avro schema of the Hudi table, with meta fields prepended.

        Returns:
            str: The Avro schema of the table.
        """
        ...
    def get_schema(self) -> "pyarrow.Schema":
        """
        Returns the schema of the Hudi table, without meta fields.

        Returns:
            pyarrow.Schema: The schema of the table.
        """
        ...
    def get_schema_with_meta_fields(self) -> "pyarrow.Schema":
        """
        Returns the schema of the Hudi table, with meta fields prepended.

        Returns:
            pyarrow.Schema: The schema of the table.
        """
        ...
    def get_partition_schema(self) -> "pyarrow.Schema":
        """
        Returns the partition schema of the Hudi table.

        Returns:
            pyarrow.Schema: The schema used for partitioning the table.
        """
        ...
    def get_timeline(self) -> HudiTimeline:
        """
        Returns the timeline of the Hudi table.
        """
        ...
    @property
    def base_url(self) -> str:
        """
        Get the base URL of the Hudi table.
        """
        ...
    def get_file_slices(
        self, options: Optional[HudiReadOptions] = None
    ) -> List[HudiFileSlice]:
        """
        Get the file slices the read targets, dispatching on ``options.query_type``.

        - Snapshot: slices visible at ``options.as_of_timestamp`` (defaults to the
          latest commit). ``options.filters`` drive partition + file-level stats
          pruning.
        - Incremental: slices changed in ``(options.start_timestamp,
          options.end_timestamp]``. ``options.filters`` drive partition pruning only.
        """
        ...
    def create_file_group_reader_with_options(
        self,
        read_options: Optional[HudiReadOptions] = None,
        extra_hudi_overrides: Optional[Dict[str, str]] = None,
        extra_storage_overrides: Optional[Dict[str, str]] = None,
    ) -> HudiFileGroupReader:
        """
        Create a :class:`HudiFileGroupReader` using the table's Hudi configs.

        Two override channels keep Hudi configs and storage credentials cleanly
        separated — a ``hoodie.*`` key can't be misclassified as storage, and a
        stray storage option can't be silently picked up as a Hudi config.

        **Hudi configs** (last-writer-wins):

        1. Table-level Hudi configs.
        2. ``read_options.hudi_options`` when ``read_options`` is provided, **excluding**
           the four keys the ``Table`` layer interprets directly (``query_type``,
           ``as_of_timestamp``, ``start_timestamp``, ``end_timestamp``).
        3. ``extra_hudi_overrides`` — caller-supplied resolved Hudi configs; always win.

        **Storage options** (last-writer-wins):

        1. Table-level storage options (cloud credentials, endpoints, etc).
        2. ``extra_storage_overrides`` — caller-supplied per-path storage overrides.
        """
        ...
    def read(
        self, options: Optional[HudiReadOptions] = None
    ) -> List["pyarrow.RecordBatch"]:
        """
        Read records, dispatching on ``options.query_type``.
        """
        ...
    def compute_table_stats(self) -> Optional[Tuple[int, int]]:
        """
        Compute estimated table-level statistics from the metadata table.

        Returns:
            Optional[Tuple[int, int]]: ``(estimated_num_rows, estimated_total_byte_size)``,
            or ``None`` if the metadata table is not enabled or statistics cannot be computed.
        """
        ...
    def read_stream(
        self, options: Optional[HudiReadOptions] = None
    ) -> HudiRecordBatchStream:
        """
        Streaming read; dispatches on ``options.query_type``. Incremental streaming
        is not yet supported and raises ``HudiCoreError``.
        """
        ...

@dataclass(init=False)
class HudiTimeline:
    def get_completed_commits(self, desc: bool = False) -> List[HudiInstant]: ...
    def get_completed_deltacommits(self, desc: bool = False) -> List[HudiInstant]: ...
    def get_completed_replacecommits(self, desc: bool = False) -> List[HudiInstant]: ...
    def get_completed_clustering_commits(
        self, desc: bool = False
    ) -> List[HudiInstant]: ...
    def get_instant_metadata_in_json(self, instant: HudiInstant) -> str: ...
    def get_latest_commit_timestamp(self) -> str: ...
    def get_latest_avro_schema(self) -> str: ...
    def get_latest_schema(self) -> "pyarrow.Schema": ...

def build_hudi_table(
    base_uri: str,
    hudi_options: Optional[Dict[str, str]] = None,
    storage_options: Optional[Dict[str, str]] = None,
    options: Optional[Dict[str, str]] = None,
) -> HudiTable:
    """
    Builds hudi table from base_uri and options.

    Parameters:
        base_uri (str): location of a hudi table.
        hudi_options (Optional[Dict[str, str]]): hudi options.
        storage_options (Optional[Dict[str, str]]): storage_options.
        options (Optional[Dict[str, str]]): hudi or storage options.

    Returns:
        HudiTable: An instance of hudi table.
    """
    ...

@dataclass(init=False)
class HudiDataFusionDataSource:
    def __init__(
        self,
        base_uri: str,
        options: Optional[List[Tuple[str, str]]] = None,
    ):
        """
        Initializes the HudiDataFusionDataSource.

        Parameters:
            base_uri (str): The base URI of the Hudi table.
            options (Optional[List[Tuple[str, str]]]): Additional configuration options (optional).
        """
        ...
