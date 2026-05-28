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
from enum import Enum
from typing import Dict, Optional, Union

from hudi._config import HudiPlanConfig, HudiReadConfig, HudiTableConfig
from hudi._internal import HudiTable, build_hudi_table

ConfigKey = Union[str, HudiTableConfig, HudiReadConfig, HudiPlanConfig]


def _coerce_key(k: ConfigKey) -> str:
    return k.value if isinstance(k, Enum) else k


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

        Examples:
            >>> from hudi import HudiTableBuilder
            >>> builder = HudiTableBuilder.from_base_uri("s3://my-bucket/trips")
            >>> builder.base_uri
            's3://my-bucket/trips'
            >>> builder.hudi_options
            {}
            >>> builder.storage_options
            {}
        """
        builder = cls(base_uri)
        return builder

    def _add_options(
        self, options: Dict[str, str], category: Optional[str] = None
    ) -> None:
        target_attr = getattr(self, f"{category}_options") if category else self.options
        target_attr.update(options)

    def with_hudi_option(self, k: ConfigKey, v: str) -> "HudiTableBuilder":
        """
        Adds a Hudi option to the builder.

        Parameters:
            k (Union[str, HudiTableConfig, HudiReadConfig]): The key, as a string or enum member.
            v (str): The value of the option.

        Returns:
            HudiTableBuilder: The builder instance.

        Examples:
            >>> from hudi import HudiTableBuilder
            >>> builder = HudiTableBuilder.from_base_uri("s3://my-bucket/trips")
            >>> builder = builder.with_hudi_option("read.timeline.num-instants", "5")
            >>> builder.hudi_options
            {'read.timeline.num-instants': '5'}
        """
        self._add_options({_coerce_key(k): v}, "hudi")
        return self

    def with_hudi_options(self, hudi_options: Dict[str, str]) -> "HudiTableBuilder":
        """
        Adds Hudi options to the builder.

        Parameters:
            hudi_options (Dict[str, str]): Hudi options to be applied.

        Returns:
            HudiTableBuilder: The builder instance.

        Examples:
            >>> from hudi import HudiTableBuilder
            >>> builder = HudiTableBuilder.from_base_uri("s3://my-bucket/trips")
            >>> builder = builder.with_hudi_options({
            ...     "read.timeline.num-instants": "5",
            ...     "read.use.new.log.record.reader": "true",
            ... })
            >>> sorted(builder.hudi_options.items())
            [('read.timeline.num-instants', '5'), ('read.use.new.log.record.reader', 'true')]
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

        Examples:
            >>> from hudi import HudiTableBuilder
            >>> builder = HudiTableBuilder.from_base_uri("s3://my-bucket/trips")
            >>> builder = builder.with_storage_option("aws.region", "us-east-1")
            >>> builder.storage_options
            {'aws.region': 'us-east-1'}
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

        Examples:
            >>> from hudi import HudiTableBuilder
            >>> builder = HudiTableBuilder.from_base_uri("s3://my-bucket/trips")
            >>> builder = builder.with_storage_options({"aws.region": "us-east-1"})
            >>> builder.storage_options
            {'aws.region': 'us-east-1'}
        """
        self._add_options(storage_options, "storage")
        return self

    def with_option(self, k: ConfigKey, v: str) -> "HudiTableBuilder":
        """
        Adds a generic option to the builder.

        Parameters:
            k (Union[str, HudiTableConfig, HudiReadConfig]): The key, as a string or enum member.
            v (str): The value of the option.

        Returns:
            HudiTableBuilder: The builder instance.

        Examples:
            >>> from hudi import HudiTableBuilder
            >>> builder = HudiTableBuilder.from_base_uri("s3://my-bucket/trips")
            >>> builder = builder.with_option("aws.region", "eu-west-1")
            >>> builder.options
            {'aws.region': 'eu-west-1'}
        """
        self._add_options({_coerce_key(k): v})
        return self

    def with_options(self, options: Dict[str, str]) -> "HudiTableBuilder":
        """
        Adds general options for configuring the HudiTable.

        Parameters:
            options (Dict[str, str]): General options to be applied, can pass hudi and storage options.

        Returns:
            HudiTableBuilder: The builder instance.

        Examples:
            >>> from hudi import HudiTableBuilder
            >>> builder = HudiTableBuilder.from_base_uri("s3://my-bucket/trips")
            >>> builder = builder.with_options({"aws.region": "ap-southeast-1"})
            >>> builder.options
            {'aws.region': 'ap-southeast-1'}
        """
        self._add_options(options)
        return self

    def build(self) -> "HudiTable":
        """
        Constructs and returns a HudiTable object with the specified options.

        Returns:
            HudiTable: The constructed HudiTable object.

        Examples:
            >>> from hudi import HudiTableBuilder
            >>> table = (
            ...     HudiTableBuilder.from_base_uri("s3://my-bucket/trips")
            ...     .with_hudi_option("read.timeline.num-instants", "5")
            ...     .with_storage_option("aws.region", "us-east-1")
            ...     .build()
            ... )  # doctest: +SKIP
        """
        return build_hudi_table(
            self.base_uri, self.hudi_options, self.storage_options, self.options
        )
