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
from dataclasses import dataclass, field
from typing import Dict, Optional

from hudi._internal import HudiTable, build_hudi_table


@dataclass
class HudiTableBuilder:
    """
    A builder class for constructing a HudiTable object with customizable options.

    Attributes:
        base_uri (str): The base URI of the Hudi table.
        options (Dict[str, str]): Both hudi and storage options for building the table.
        hudi_options (Dict[str, str]): Hudi configuration options.
        storage_options (Dict[str, str]): Storage-related options.
    """

    base_uri: str
    hudi_options: Dict[str, str] = field(default_factory=dict)
    storage_options: Dict[str, str] = field(default_factory=dict)
    options: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_base_uri(cls, base_uri: str) -> "HudiTableBuilder":
        """
        Initializes a HudiTableBuilder using the base URI of the Hudi table.

        Parameters:
            base_uri (str): The base URI of the Hudi table.

        Returns:
            HudiTableBuilder: An instance of the builder.
        """
        builder = cls(base_uri)
        return builder

    def _add_options(
        self, options: Dict[str, str], category: Optional[str] = None
    ) -> None:
        target_attr = getattr(self, f"{category}_options") if category else self.options
        target_attr.update(options)

    def with_hudi_option(self, k: str, v: str) -> "HudiTableBuilder":
        """
        Adds a Hudi option to the builder.

        Parameters:
            k (str): The key of the option.
            v (str): The value of the option.

        Returns:
            HudiTableBuilder: The builder instance.
        """
        self._add_options({k: v}, "hudi")
        return self

    def with_hudi_options(self, hudi_options: Dict[str, str]) -> "HudiTableBuilder":
        """
        Adds Hudi options to the builder.

        Parameters:
            hudi_options (Dict[str, str]): Hudi options to be applied.

        Returns:
            HudiTableBuilder: The builder instance.
        """
        self._add_options(hudi_options, "hudi")
        return self

    def with_storage_option(self, k: str, v: str) -> "HudiTableBuilder":
        """
        Adds a storage option to the builder.

        Parameters:
            k (str): The key of the option.
            v (str): The value of the option.

        Returns:
            HudiTableBuilder: The builder instance.
        """
        self._add_options({k: v}, "storage")
        return self

    def with_storage_options(
        self, storage_options: Dict[str, str]
    ) -> "HudiTableBuilder":
        """
        Adds storage-related options for configuring the table.

        Parameters:
            storage_options (Dict[str, str]): Storage-related options to be applied.

        Returns:
            HudiTableBuilder: The builder instance.
        """
        self._add_options(storage_options, "storage")
        return self

    def with_option(self, k: str, v: str) -> "HudiTableBuilder":
        """
        Adds a generic option to the builder.

        Parameters:
            k (str): The key of the option.
            v (str): The value of the option.

        Returns:
            HudiTableBuilder: The builder instance.
        """
        self._add_options({k: v})
        return self

    def with_options(self, options: Dict[str, str]) -> "HudiTableBuilder":
        """
        Adds general options for configuring the HudiTable.

        Parameters:
            options (Dict[str, str]): General options to be applied, can pass hudi and storage options.

        Returns:
            HudiTableBuilder: The builder instance.
        """
        self._add_options(options)
        return self

    def build(self) -> "HudiTable":
        """
        Constructs and returns a HudiTable object with the specified options.

        Returns:
            HudiTable: The constructed HudiTable object.
        """
        return build_hudi_table(
            self.base_uri, self.hudi_options, self.storage_options, self.options
        )
