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
# Sync local code to an AWS EC2 instance over SSM and rebuild the benchmark
# binary. Connects through Session Manager, so the instance needs no public
# IP or open SSH port; it does need the AmazonSSMManagedInstanceCore policy
# and an SSH key authorized for the login user (e.g. the instance's key pair).
#
# Requires locally: AWS CLI with the Session Manager plugin, and credentials
# for the instance's account.
#
# Usage:
#   bash benchmark/tpch/infra/aws/sync.sh <instance-id> <region> [ssh-key-path]
#
# Example:
#   bash benchmark/tpch/infra/aws/sync.sh i-0123456789abcdef0 us-west-2 ~/.ssh/bench.pem
#
set -euo pipefail

INSTANCE_ID="${1:?Usage: sync.sh <instance-id> <region> [ssh-key-path]}"
REGION="${2:?Usage: sync.sh <instance-id> <region> [ssh-key-path]}"
SSH_KEY="${3:-}"
SSH_USER="${SSH_USER:-ec2-user}"

REPO_ROOT="$(cd "$(dirname "$0")/../../../.." && pwd)"

PROXY_CMD="aws ssm start-session --target %h --region $REGION --document-name AWS-StartSSHSession --parameters portNumber=%p"

SSH_OPTS=(-o StrictHostKeyChecking=no -o "ProxyCommand=$PROXY_CMD")
if [[ -n "$SSH_KEY" ]]; then
  SSH_OPTS+=(-i "$SSH_KEY")
fi

RSYNC_SSH="ssh -o StrictHostKeyChecking=no -o 'ProxyCommand=$PROXY_CMD'"
if [[ -n "$SSH_KEY" ]]; then
  RSYNC_SSH+=" -i '$SSH_KEY'"
fi

echo "==> Syncing code to $INSTANCE_ID..."
rsync -az --progress \
  --exclude='target/' \
  --exclude='.git/' \
  --exclude='.context/' \
  -e "$RSYNC_SSH" \
  "$REPO_ROOT/" "$SSH_USER@$INSTANCE_ID":~/hudi-rs/

echo "==> Building on the instance..."
ssh "${SSH_OPTS[@]}" "$SSH_USER@$INSTANCE_ID" \
  "cd ~/hudi-rs && . \$HOME/.cargo/env && cargo build -p tpch --release"

echo ""
echo "Ready. Connect with:"
printf '  ssh'
printf ' %q' "${SSH_OPTS[@]}"
printf ' %s\n' "$SSH_USER@$INSTANCE_ID"
