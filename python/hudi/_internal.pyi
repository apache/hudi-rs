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

from typing import List, Dict, Optional

import pyarrow

__version__: str


def rust_core_version() -> str: ...


class HudiFileSlice:
    file_group_id: str
    partition_path: str
    commit_time: str
    base_file_name: str
    base_file_size: int
    num_records: int

    def base_file_relative_path(self) -> str: ...

class BindingHudiTable:

    def __init__(
            self,
            table_uri: str,
            storage_options: Optional[Dict[str, str]] = None,
    ): ...

    def schema(self) -> "pyarrow.Schema": ...

    def get_latest_file_slices(self) -> List[HudiFileSlice]: ...

    def read_file_slice(self, relative_path) -> List["pyarrow.RecordBatch"]: ...
