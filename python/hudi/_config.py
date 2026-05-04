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
# mypy: ignore-errors
"""Hudi config enums reflected from the Rust core at import time.

The `enum.Enum` members are constructed from `hudi._internal._config_keys()`,
so the variant names and key strings have a single source of truth in Rust.
mypy is disabled for this module because its strict-mode Enum analyzer
requires literal members and cannot follow the dynamic construction below.
"""

from enum import Enum

from hudi._internal import _config_keys

_keys = _config_keys()

HudiTableConfig = Enum(
    "HudiTableConfig",
    _keys["HudiTableConfig"],
    type=str,
    module=__name__,
    qualname="HudiTableConfig",
)
HudiTableConfig.__doc__ = (
    "Configurations for Hudi tables, most of them are persisted in `hoodie.properties`."
)

HudiReadConfig = Enum(
    "HudiReadConfig",
    _keys["HudiReadConfig"],
    type=str,
    module=__name__,
    qualname="HudiReadConfig",
)
HudiReadConfig.__doc__ = "Configurations for reading Hudi tables."

HudiPlanConfig = Enum(
    "HudiPlanConfig",
    _keys["HudiPlanConfig"],
    type=str,
    module=__name__,
    qualname="HudiPlanConfig",
)
HudiPlanConfig.__doc__ = "Configurations for query planning in Hudi."

__all__ = ["HudiPlanConfig", "HudiReadConfig", "HudiTableConfig"]
