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

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Union, List, Optional, Dict

import pyarrow
from hudi._internal import BindingHudiTable, HudiFileSlice


@dataclass(init=False)
class HudiTable:

    def __init__(
            self,
            table_uri: Union[str, Path, "os.PathLike[str]"],
            storage_options: Optional[Dict[str, str]] = None,
    ):
        self._table = BindingHudiTable(str(table_uri), storage_options)

    def get_schema(self) -> "pyarrow.Schema":
        return self._table.get_schema()

    def split_file_slices(self, n: int) -> List[List[HudiFileSlice]]:
        return self._table.split_file_slices(n)

    def get_file_slices(self) -> List[HudiFileSlice]:
        return self._table.get_file_slices()

    def read_file_slice(self, base_file_relative_path: str) -> "pyarrow.RecordBatch":
        return self._table.read_file_slice(base_file_relative_path)

    def read_snapshot(self) -> List["pyarrow.RecordBatch"]:
        return self._table.read_snapshot()

    def read_snapshot_as_of(self, timestamp: str) -> List["pyarrow.RecordBatch"]:
        return self._table.read_snapshot_as_of(timestamp)
