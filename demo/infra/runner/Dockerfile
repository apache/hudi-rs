# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

FROM rust:1.84

RUN apt-get update && apt-get install -y cmake python3-dev python3-venv curl

RUN curl -LsSf https://astral.sh/uv/install.sh | sh

RUN uv venv /opt/.venv

ENV PATH="/root/.local/bin:$PATH"

ENV VIRTUAL_ENV=/opt/.venv

WORKDIR /opt

CMD tail -f /dev/null
