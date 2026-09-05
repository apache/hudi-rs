#!/usr/bin/env bash
#
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
#
# Install the system build dependencies for a manylinux_2_28 wheel.
#
# Both release steps and the CI job that guards them run this one script, so a
# change cannot land in some of them and not others.
#
# clang-devel: librocksdb-sys generates its bindings with bindgen, which needs
# libclang. The distro clang is current on the 2_28 (AlmaLinux 8) base, so the
# standard install is enough; bindgen finds it with no environment handed in.
#
# protoc: the distro package is still older than the
# --experimental_allow_proto3_optional flag prost-build passes when it compiles
# lance-encoding/lance-file's .proto files, so install a pinned official
# release. The wheels this produces are published and signed, so the archive is
# verified against a pinned sha256 rather than trusted from the download.
#
# perl-IPC-Cmd: needed by openssl.

set -euo pipefail

dnf install -y clang-devel perl-IPC-Cmd

PROTOC_VERSION=36.1

case "$(uname -m)" in
  x86_64)
    protoc_arch=x86_64
    protoc_sha256=c4bc672d9d49214dc8cafdceadf4df92182d6ca8e3ec65a56b2d7de5602669b4
    ;;
  aarch64)
    protoc_arch=aarch_64
    protoc_sha256=237a68856edf1bd28b6204bddd0596c1cf46d298bc29c620012540b2e44c73e7
    ;;
  *)
    echo "unsupported architecture: $(uname -m)" >&2
    exit 1
    ;;
esac

curl -fsSL -o /tmp/protoc.zip \
  "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-${protoc_arch}.zip"
echo "${protoc_sha256}  /tmp/protoc.zip" | sha256sum -c -

unzip -qo /tmp/protoc.zip -d /usr/local bin/protoc 'include/*'
chmod +x /usr/local/bin/protoc
protoc --version
clang --version | head -n 1
