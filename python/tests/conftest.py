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
import zipfile
from pathlib import Path

import pytest


def _extract_testing_table(zip_file_path, target_path) -> str:
    with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
        zip_ref.extractall(target_path)
    return os.path.join(target_path, "trips_table")


@pytest.fixture(
    params=[
        "0.x_cow_partitioned",
    ]
)
def get_sample_table(request, tmp_path) -> str:
    fixture_path = "tests/table"
    table_name = request.param
    zip_file_path = Path(fixture_path).joinpath(f"{table_name}.zip")
    return _extract_testing_table(zip_file_path, tmp_path)
