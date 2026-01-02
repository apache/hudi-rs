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

from datafusion import SessionContext

from hudi import HudiDataFusionDataSource


def test_datafusion_table_registry(get_sample_table):
    table_path = get_sample_table

    table = HudiDataFusionDataSource(
        table_path, [("hoodie.read.use.read_optimized.mode", "true")]
    )
    ctx = SessionContext()
    ctx.register_table("trips", table)
    df = ctx.sql("SELECT  city from trips order by city desc limit 1").to_arrow_table()
    assert df.to_pylist() == [{"city": "sao_paulo"}]
