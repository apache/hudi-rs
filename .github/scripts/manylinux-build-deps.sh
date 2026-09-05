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
# Install the system build dependencies for a manylinux wheel.
#
# Both release steps and the CI job that guards them run this one script, so a
# change cannot land in some of them and not others.
#
# protoc: the distro package is 2.5.0 on the CentOS 7 base, which predates the
# --experimental_allow_proto3_optional flag prost-build passes when it compiles
# lance-encoding/lance-file's .proto files.
#
# libclang: librocksdb-sys generates its bindings with bindgen, which needs a
# libclang the base image does not carry; the distro clang is 3.4, older than
# bindgen supports. bindgen also needs clang's builtin headers (stdbool.h and
# friends), which libclang cannot locate on its own when loaded from the SCL
# prefix, so both are handed over explicitly. The exports only reach the build
# if this script is SOURCED from before-script-linux, not executed: a step-level
# `env:` never enters the container (maturin-action forwards only its own
# variables), while the before-script runs in the same shell as the build - the
# long-standing CFLAGS_aarch64 export below relies on the same behavior.
#
# perl-IPC-Cmd: needed by openssl.

set -euo pipefail

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

# The wheels this produces are published and signed, so verify the archive
# rather than trusting whatever the download returned.
echo "${protoc_sha256}  /tmp/protoc.zip" | sha256sum -c -

unzip -qo /tmp/protoc.zip -d /usr/local bin/protoc 'include/*'
chmod +x /usr/local/bin/protoc
protoc --version

yum install -y llvm-toolset-7.0-clang llvm-toolset-7.0-clang-libs perl-IPC-Cmd

# libclang.so links against the LLVM runtime in the same prefix, so make the
# loader aware of it rather than relying on a symlink out of the prefix.
libclang_dir=/opt/rh/llvm-toolset-7.0/root/usr/lib64
test -e "$libclang_dir/libclang.so" || {
  echo "libclang.so not found under $libclang_dir" >&2
  exit 1
}

# Register the prefix so the LLVM runtime libclang links against resolves.
echo "$libclang_dir" >/etc/ld.so.conf.d/llvm-toolset-7.0.conf
ldconfig

clang_include=$(ls -d "$libclang_dir"/clang/*/include 2>/dev/null | head -n 1)
test -n "$clang_include" || {
  echo "clang builtin headers not found under $libclang_dir/clang" >&2
  exit 1
}

export LIBCLANG_PATH="$libclang_dir"
export BINDGEN_EXTRA_CLANG_ARGS="-I$clang_include"
echo "libclang: $LIBCLANG_PATH"
echo "clang builtin headers: $clang_include"
