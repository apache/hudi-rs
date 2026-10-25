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

FROM rust:1.94

RUN apt-get update && apt-get install -y cmake curl ca-certificates protobuf-compiler clang libclang-dev

ADD https://astral.sh/uv/install.sh /uv-installer.sh

RUN UV_UNMANAGED_INSTALL=/usr/local/bin sh /uv-installer.sh && rm /uv-installer.sh

# The runner uses the host UID, so Python must be accessible outside /root.
ENV UV_PYTHON_INSTALL_DIR=/opt/python

RUN uv python install --no-bin 3.13 && uv venv --python 3.13 /opt/.venv

ENV VIRTUAL_ENV=/opt/.venv
ENV PATH="/opt/.venv/bin:$PATH"

RUN uv pip install --only-binary=all 'pyarrow==20.0.0'

WORKDIR /opt

ARG RUNNER_UID=1000
ARG RUNNER_GID=1000

# make develop installs into this environment as the host user.
RUN chown -R "${RUNNER_UID}:${RUNNER_GID}" /opt/.venv

USER ${RUNNER_UID}:${RUNNER_GID}

# Fail early rather than discovering missing dependencies during integration tests.
RUN uv --version \
    && python -c 'import pyarrow' \
    && python -c 'import ctypes, glob; ctypes.CDLL(glob.glob("/usr/lib/llvm-*/lib/libclang.so")[0])' \
    && test -w /opt/.venv/bin \
    && test -w /opt/.venv/lib/python3.13/site-packages

CMD tail -f /dev/null
