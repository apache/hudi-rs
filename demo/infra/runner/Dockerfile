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

FROM rust:1.87

RUN apt-get update && apt-get install -y cmake curl ca-certificates

# Install uv to a globally accessible location (not /root/.local/bin)
# This allows the container to run as any user (HOST_UID:HOST_GID)
ADD https://astral.sh/uv/install.sh /uv-installer.sh
RUN UV_INSTALL_DIR=/usr/local/bin sh /uv-installer.sh && rm /uv-installer.sh

# Install Python to a shared location accessible by any user
ENV UV_PYTHON_INSTALL_DIR=/opt/python
RUN uv python install 3.13

RUN uv python list

RUN uv venv /opt/.venv

ENV VIRTUAL_ENV=/opt/.venv
ENV PATH="/opt/.venv/bin:$PATH"

RUN uv pip install --only-binary=all 'pyarrow==20.0.0'

# Make Python installation and venv accessible to any user (needed for HOST_UID:HOST_GID mapping)
# Python needs read+execute, venv needs read+write+execute for pip install
RUN chmod -R a+rX /opt/python && chmod -R a+rwX /opt/.venv

WORKDIR /opt

CMD tail -f /dev/null
