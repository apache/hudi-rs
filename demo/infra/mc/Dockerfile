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

# MinIO stopped serving client binaries from dl.min.io (the release and
# archive paths now answer 410 Gone), so the client is taken from its
# container image instead. Pinned: the tag is the only thing that makes
# this build reproducible.
FROM quay.io/minio/mc:RELEASE.2025-08-13T08-35-41Z AS mc

FROM alpine

RUN apk update && apk add --no-cache \
	ca-certificates bash unzip

COPY --from=mc /usr/bin/mc /usr/local/bin/mc

WORKDIR /opt/data
