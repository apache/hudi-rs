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
from typing import Dict, List, Optional, Tuple

import pyarrow  # type: ignore

__version__: str

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
    def read_file_slice_by_base_file_path(
        self, relative_path: str
    ) -> "pyarrow.RecordBatch":
        """
        Reads the data from the base file at the given relative path.

        Parameters:
            relative_path (str): The relative path to the base file.

        Returns:
            pyarrow.RecordBatch: A record batch read from the base file.
        """
        ...
    def read_file_slice(self, file_slice: HudiFileSlice) -> "pyarrow.RecordBatch":
        """
        Reads the data from the given file slice.

        Parameters:
            file_slice (HudiFileSlice): The file slice to read from.

        Returns:
            pyarrow.RecordBatch: A record batch read from the file slice.
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
    def get_schema(self) -> "pyarrow.Schema":
        """
        Returns the schema of the Hudi table.

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
    def get_file_slices_splits(
        self, n: int, filters: Optional[List[Tuple[str, str, str]]]
    ) -> List[List[HudiFileSlice]]:
        """
        Retrieves all file slices in the Hudi table in 'n' splits, optionally filtered by given filters.

        Parameters:
            n (int): The number of parts to split the file slices into.
            filters (Optional[List[Tuple[str, str, str]]]): Optional filters for selecting file slices.

        Returns:
            List[List[HudiFileSlice]]: A list of file slice groups, each group being a list of HudiFileSlice objects.
        """
        ...
    def get_file_slices_splits_as_of(
        self, n: int, timestamp: str, filters: Optional[List[Tuple[str, str, str]]]
    ) -> List[List[HudiFileSlice]]:
        """
        Retrieves all file slices in the Hudi table as of a timestamp in 'n' splits, optionally filtered by given filters.
        """
        ...
    def get_file_slices(
        self, filters: Optional[List[Tuple[str, str, str]]]
    ) -> List[HudiFileSlice]:
        """
        Retrieves all file slices in the Hudi table, optionally filtered by the provided filters.

        Parameters:
            filters (Optional[List[Tuple[str, str, str]]]): Optional filters for selecting file slices.

        Returns:
            List[HudiFileSlice]: A list of file slices matching the filters.
        """
        ...
    def get_file_slices_as_of(
        self, timestamp: str, filters: Optional[List[Tuple[str, str, str]]]
    ) -> List[HudiFileSlice]:
        """
        Retrieves all file slices in the Hudi table as of a timestamp, optionally filtered by the provided filters.
        """
        ...
    def get_file_slices_between(
        self,
        start_timestamp: Optional[str],
        end_timestamp: Optional[str],
    ) -> List[HudiFileSlice]:
        """
        Retrieves all changed file slices in the Hudi table between the given timestamps.
        """
        ...
    def create_file_group_reader_with_options(
        self, options: Optional[Dict[str, str]] = None
    ) -> HudiFileGroupReader:
        """
        Creates a HudiFileGroupReader for reading records from file groups in the Hudi table.

        Returns:
            HudiFileGroupReader: A reader object for reading file groups.
        """
        ...
    def read_snapshot(
        self, filters: Optional[List[Tuple[str, str, str]]]
    ) -> List["pyarrow.RecordBatch"]:
        """
        Reads the latest snapshot of the Hudi table, optionally filtered by the provided filters.

        Parameters:
            filters (Optional[List[Tuple[str, str, str]]]): Optional filters for selecting file slices.

        Returns:
            List[pyarrow.RecordBatch]: A list of record batches from the snapshot of the table.
        """
        ...
    def read_snapshot_as_of(
        self, timestamp: str, filters: Optional[List[Tuple[str, str, str]]]
    ) -> List["pyarrow.RecordBatch"]:
        """
        Reads the snapshot of the Hudi table as of a timestamp, optionally filtered by the provided filters.
        """
        ...
    def read_incremental_records(
        self, start_timestamp: str, end_timestamp: Optional[str]
    ) -> List["pyarrow.RecordBatch"]:
        """
        Reads incremental records from the Hudi table between the given timestamps.

        Parameters:
            start_timestamp (str): The start timestamp (exclusive).
            end_timestamp (Optional[str]): The end timestamp (inclusive).

        Returns:
            List[pyarrow.RecordBatch]: A list of record batches containing incremental records.
        """
        ...

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
