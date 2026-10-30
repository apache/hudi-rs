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

"""Apache Hudi for Python, backed by the native Rust implementation hudi-rs.

Start with :class:`HudiTable`: ``HudiTable.create`` makes a table and ``HudiTable(base_uri)``
opens an existing one; both return a handle that reads, writes, and, on a merge-on-read table,
compacts (:meth:`HudiTable.compact`, experimental).

Resources:

- Quick start: https://hudi.apache.org/docs/python-rust-quick-start-guide, and the shorter
  README quick start at https://github.com/apache/hudi-rs#quick-start.
- Python API reference: the docstrings in the bundled type stubs, ``hudi/_internal.pyi``
  (https://github.com/apache/hudi-rs/blob/main/python/hudi/_internal.pyi).
- Rust API reference: https://docs.rs/hudi (also https://docs.rs/hudi-core and
  https://docs.rs/hudi-datafusion).
- Runnable Rust example:
  https://github.com/apache/hudi-rs/blob/main/crates/core/examples/table_writes.rs.
- Reader semantics: https://github.com/apache/hudi-rs/blob/main/docs/reader-spec.md.
  Writer design: https://github.com/apache/hudi-rs/blob/main/docs/writer-design.md.
- Packages: https://pypi.org/project/hudi/ and https://crates.io/crates/hudi.
  Source: https://github.com/apache/hudi-rs.
- Hudi concepts (timeline, table types, query types): https://hudi.apache.org/docs/overview.
"""

from hudi._config import HudiPlanConfig, HudiReadConfig, HudiTableConfig
from hudi._internal import (
    HudiCompactionResult,
    HudiCoreError,
    HudiDataFusionDataSource,
    HudiFileGroupReader,
    HudiFileSlice,
    HudiInstant,
    HudiQueryType,
    HudiReadOptions,
    HudiRecordBatchStream,
    HudiTable,
    HudiTimeline,
    HudiWriteResult,
)
from hudi._internal import __version__ as __version__
from hudi.table.builder import HudiTableBuilder

__all__ = [
    "HudiCompactionResult",
    "HudiCoreError",
    "HudiDataFusionDataSource",
    "HudiFileGroupReader",
    "HudiFileSlice",
    "HudiInstant",
    "HudiQueryType",
    "HudiPlanConfig",
    "HudiReadConfig",
    "HudiReadOptions",
    "HudiRecordBatchStream",
    "HudiTable",
    "HudiTableBuilder",
    "HudiTableConfig",
    "HudiTimeline",
    "HudiWriteResult",
]
