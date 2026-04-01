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
# Sync local code to a GCP VM and rebuild the benchmark binary.
#
# Usage:
#   bash benchmark/tpch/infra/gcp/sync.sh <vm-name> <zone>
#
# Example:
#   bash benchmark/tpch/infra/gcp/sync.sh bench-vm us-central1-a
#
set -euo pipefail

VM_NAME="${1:?Usage: sync.sh <vm-name> <zone>}"
ZONE="${2:?Usage: sync.sh <vm-name> <zone>}"

REPO_ROOT="$(cd "$(dirname "$0")/../../../.." && pwd)"

echo "==> Syncing code to $VM_NAME..."
rsync -az --progress \
  --exclude='target/' \
  --exclude='.git/' \
  --exclude='.context/' \
  -e "gcloud compute ssh --zone=$ZONE -- -o StrictHostKeyChecking=no" \
  "$REPO_ROOT/" "$VM_NAME":~/hudi-rs/

echo "==> Building on VM..."
gcloud compute ssh "$VM_NAME" --zone="$ZONE" -- \
  "cd ~/hudi-rs && . \$HOME/.cargo/env && cargo build -p tpch --release"

echo ""
echo "Ready. Connect with:"
echo "  gcloud compute ssh $VM_NAME --zone=$ZONE"
