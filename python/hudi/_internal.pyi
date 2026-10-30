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
from typing import Any, ClassVar, Dict, List, Optional, Protocol, Tuple, Union

import pyarrow  # type: ignore[import-untyped]

__version__: str

class _ArrowStreamExportable(Protocol):
    def __arrow_c_stream__(self, requested_schema: Any = None) -> Any: ...

WriteData = Union[
    "pyarrow.Table",
    "pyarrow.RecordBatch",
    "pyarrow.RecordBatchReader",
    List["pyarrow.RecordBatch"],
    _ArrowStreamExportable,
]
"""Tabular input accepted by the :class:`HudiTable` write methods."""

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
        without loading all data into memory. For MOR slices with log files, file
        group reader version 2 (the default) yields merged chunks incrementally;
        version 1 falls back to a single merged batch.
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
        log_file_sizes (List[int]): On-disk sizes of the log files, parallel to ``log_file_names``.
        num_records (int): The number of records in the file slice.
    """

    file_id: str
    partition_path: str
    creation_instant_time: str
    base_file_name: str
    base_file_size: int
    base_file_byte_size: int
    log_file_names: List[str]
    log_file_sizes: List[int]
    num_records: int

    def total_size_bytes(self) -> int:
        """Total on-disk size (base + log files) in bytes.

        Use for I/O cost estimation and split sizing. For estimated in-memory
        size and record count for query planning, use
        :meth:`HudiTable.compute_table_stats` instead.
        """
        ...
    def has_log_files(self) -> bool:
        """True if this slice has at least one log file."""
        ...
    def base_file_column_stats(
        self,
    ) -> Optional[
        Dict[str, Tuple[Optional["pyarrow.Array"], Optional["pyarrow.Array"]]]
    ]:
        """Column statistics (min, max) from the base file Parquet footer.

        Returns a dict mapping column names to ``(min_array, max_array)``
        tuples, where each is a single-element PyArrow array or ``None``.
        Only populated when data-column filters trigger footer-based
        pruning on COW tables or MOR read-optimized mode.
        """
        ...
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

    A ``HudiTable`` must not be shared across threads while a write is running:
    each write method takes exclusive access to the instance, so a concurrent
    call on it raises ``RuntimeError``. Use one instance per thread.

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
          latest commit). ``options.filters`` drive partition pruning; file-level
          stats pruning applies only for COW tables or MOR read-optimized mode.
          When footer stats are loaded for pruning, exact ``num_records``,
          ``byte_size``, and column stats are attached to the returned slices.
        - Incremental: slices changed in ``(options.start_timestamp,
          options.end_timestamp]``. ``options.filters`` drive partition pruning only.
        - When ``UseReadOptimizedMode`` is enabled (via options or table config),
          log files are stripped from all returned slices.
        """
        ...
    def create_file_group_reader_with_options(
        self,
        read_options: Optional[HudiReadOptions] = None,
        extra_storage_overrides: Optional[Dict[str, str]] = None,
    ) -> HudiFileGroupReader:
        """
        Create a :class:`HudiFileGroupReader` using the table's Hudi configs.

        ``read_options.hudi_options`` override table-level Hudi configs
        (last-writer-wins). ``extra_storage_overrides`` override table-level
        storage options (cloud credentials, endpoints, etc).

        Timestamps are resolved automatically (e.g. ``AsOfTimestamp`` →
        ``EndTimestamp``), so callers can pass the same options used for
        :meth:`get_file_slices`.
        """
        ...
    def read(
        self, options: Optional[HudiReadOptions] = None
    ) -> List["pyarrow.RecordBatch"]:
        """
        Read records, dispatching on ``options.query_type``.
        """
        ...
    def compute_table_stats(
        self,
        options: Optional[HudiReadOptions] = None,
    ) -> Optional[Tuple[int, int]]:
        """Compute estimated (rows, bytes) for snapshot queries.

        Returns snapshot stats derived from the metadata table (MDT).
        Returns ``None`` when the MDT is not enabled, statistics cannot
        be computed, or the query type is incremental.
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
    @staticmethod
    def create(
        base_uri: str,
        *,
        record_key: Union[str, List[str]],
        partition_by: Optional[Union[str, List[str]]] = None,
        ordering_fields: Optional[Union[str, List[str]]] = None,
        table_type: str = "cow",
        table_name: Optional[str] = None,
        table_version: Optional[int] = None,
        metadata_table_enabled: Optional[bool] = None,
        record_index_enabled: Optional[bool] = None,
        column_stats_enabled: Optional[bool] = None,
        partition_stats_enabled: Optional[bool] = None,
        hive_style_partitioning: Optional[bool] = None,
        hudi_options: Optional[Dict[str, str]] = None,
        storage_options: Optional[Dict[str, str]] = None,
    ) -> "HudiTable":
        """
        Create a new Hudi table at ``base_uri`` and open it for reads and writes.

        Arguments left as ``None`` keep the native defaults: table version 9,
        metadata table on with the record index, column stats and partition
        stats, and hive-style partition paths. Turning ``metadata_table_enabled`` off also
        turns off the indexes that live in it.

        Parameters:
            base_uri (str): Location of the new table.
            record_key (Union[str, List[str]]): Record key field(s).
            partition_by (Optional[Union[str, List[str]]]): Partition field(s); unpartitioned if omitted.
            ordering_fields (Optional[Union[str, List[str]]]): Field(s) that pick the winning
                record when a key is written more than once; commit order if omitted.
            table_type (str): ``"cow"``/``"COPY_ON_WRITE"`` or ``"mor"``/``"MERGE_ON_READ"``,
                case-insensitive.
            table_name (Optional[str]): Defaults to the last path segment of ``base_uri``.
            table_version (Optional[int]): Table version to write, 8 or 9.
            metadata_table_enabled (Optional[bool]): Metadata table.
            record_index_enabled (Optional[bool]): Record-level index in the metadata table.
            column_stats_enabled (Optional[bool]): Column stats index in the metadata table.
            partition_stats_enabled (Optional[bool]): Partition stats index in the metadata table.
            hive_style_partitioning (Optional[bool]): Use ``field=value`` partition paths.
            hudi_options (Optional[Dict[str, str]]): Extra ``hoodie.*`` table configs, written into
                ``hoodie.properties``. Read options belong on the reader, not here.
            storage_options (Optional[Dict[str, str]]): Storage options such as cloud credentials.

        Returns:
            HudiTable: The new table.

        Raises:
            ValueError: If ``table_type`` is not recognized.
            HudiCoreError: If a table already exists at ``base_uri`` or the
                options are inconsistent.
        """
        ...
    def append(self, data: WriteData) -> "HudiWriteResult":
        """
        Insert ``data`` as one commit without looking up existing keys.

        Parameters:
            data (WriteData): A pyarrow ``Table``, ``RecordBatch``, ``RecordBatchReader``,
                a list of ``RecordBatch``, or any object implementing ``__arrow_c_stream__``
                (for example a polars or pandas >= 2.2 DataFrame).

        Returns:
            HudiWriteResult: ``num_writes`` and ``num_inserts`` are the rows written.
        """
        ...
    def upsert(
        self,
        data: WriteData,
        update_columns: Optional[List[str]] = None,
    ) -> "HudiWriteResult":
        """
        Update rows whose record key exists and insert the rest.

        Parameters:
            data (WriteData): Rows to upsert; see :meth:`append` for accepted types.
            update_columns (Optional[List[str]]): When set, matched rows only take these
                columns from ``data`` and keep their other values. Copy-on-write tables only.

        Returns:
            HudiWriteResult: The commit and its update/insert counts.
        """
        ...
    def update(
        self,
        predicate: str,
        updates: Union[
            Dict[str, Any],
            "pyarrow.RecordBatch",
            "pyarrow.Table",
            _ArrowStreamExportable,
        ],
    ) -> "HudiWriteResult":
        """
        Set columns on the rows matching ``predicate``.

        Parameters:
            predicate (str): A single binary predicate such as ``"city = 'nyc'"``.
            updates (Union[Dict[str, Any], pyarrow.RecordBatch, pyarrow.Table]): Column values to
                set, either as a dict or as a one-row batch carrying only those columns.
                Dict values must fit their column's type exactly, except that a float32
                column rounds to the nearest float32; a bool fits only a bool column. Date
                columns take ``datetime.date`` and timestamp columns take
                ``datetime.datetime``; a tz-aware datetime needs a timestamp column with a
                time zone. Struct fields missing from a dict value are set to null.

        Returns:
            HudiWriteResult: No commit is created when nothing matches.

        Raises:
            ValueError: For dict input, if a key is not a str column of the table schema
                or a value does not fit its column; for batch input, if it yields no record
                batches (such as an empty ``pyarrow.Table``).
            HudiCoreError: For batch input that does not match the table schema or does not
                hold exactly one row.
        """
        ...
    def delete(self, predicate: str) -> "HudiWriteResult":
        """
        Delete the rows matching ``predicate``.

        Parameters:
            predicate (str): A single binary predicate such as ``"id IN ('t1', 't2')"``.
                Predicates on the record key use the index; others scan the table.

        Returns:
            HudiWriteResult: No commit is created when nothing matches.
        """
        ...
    def delete_keys(self, keys: List[Union[str, Tuple[str, str]]]) -> "HudiWriteResult":
        """
        Delete records by record key.

        Parameters:
            keys (List[Union[str, Tuple[str, str]]]): Record keys, or
                ``(record_key, partition_path)`` tuples. ``partition_path`` is the
                table-relative path as stored, e.g. ``city=nyc`` with hive-style
                partitioning. A bare key is looked up in every partition.

        Returns:
            HudiWriteResult: No commit is created when no key exists.
        """
        ...
    def overwrite(self, data: WriteData) -> "HudiWriteResult":
        """
        Replace all rows in the table with ``data``.

        Parameters:
            data (WriteData): Replacement rows; see :meth:`append` for accepted types.

        Returns:
            HudiWriteResult: The replace commit.
        """
        ...
    def dynamic_partition_overwrite(self, data: WriteData) -> "HudiWriteResult":
        """
        Replace only the partitions present in ``data`` in a partitioned table.

        Parameters:
            data (WriteData): Replacement rows; see :meth:`append` for accepted types.

        Returns:
            HudiWriteResult: The replace commit.
        """
        ...

@dataclass(init=False)
class HudiWriteResult:
    """
    Outcome of a write on :class:`HudiTable`.

    Attributes:
        instant_time (Optional[str]): Instant time of the completed commit, or ``None``
            when no commit was made.
        completion_time (Optional[str]): When the commit completed, or ``None`` when no
            commit was made or the table's timeline records no completion times (table
            version 6). When set, incremental reads compare their start bound against
            completion times, so ``with_start_timestamp(completion_time)`` returns the
            commits after this one while passing ``instant_time`` would include this commit.
        num_writes (int): Rows written to data files.
        num_updates (int): Input rows that replaced existing records.
        num_inserts (int): Input rows that were new records.
        num_deletes (int): Records deleted.
    """

    instant_time: Optional[str]
    completion_time: Optional[str]
    num_writes: int
    num_updates: int
    num_inserts: int
    num_deletes: int

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
