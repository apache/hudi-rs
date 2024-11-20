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
    A reader for a group of Hudi file slices. Allows reading of records from the base file in a Hudi table.

    Attributes:
        base_uri (str): The base URI of the Hudi table.
        options (Optional[Dict[str, str]]): Additional options for reading the file group.
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
        Reads the data from the base file given a relative path.

        Parameters:
            relative_path (str): The relative path to the base file.

        Returns:
            pyarrow.RecordBatch: A batch of records read from the base file.
        """
        ...

@dataclass(init=False)
class HudiFileSlice:
    """
    Represents a file slice in a Hudi table. A file slice includes information about the base file,
    the partition it belongs to, and associated metadata.

    Attributes:
        file_group_id (str): The ID of the file group this slice belongs to.
        partition_path (str): The path of the partition containing this file slice.
        commit_time (str): The commit time of this file slice.
        base_file_name (str): The name of the base file.
        base_file_size (int): The size of the base file.
        num_records (int): The number of records in the base file.
        size_bytes (int): The size of the file slice in bytes.
    """

    file_group_id: str
    partition_path: str
    commit_time: str
    base_file_name: str
    base_file_size: int
    num_records: int
    size_bytes: int

    def base_file_relative_path(self) -> str:
        """
        Returns the relative path of the base file for this file slice.

        Returns:
            str: The relative path of the base file.
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
    def create_file_group_reader(self) -> HudiFileGroupReader:
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
