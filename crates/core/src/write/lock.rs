/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
//! Write-path locking (Java `LockProvider` / `TransactionManager`).
//!
//! Every write takes two short-lived critical sections:
//! 1. lock → generate instant time → create requested/inflight → unlock;
//!    the actual work (data files, MDT log files) runs unlocked.
//! 2. lock → generate completion time → complete the action (MDT
//!    deltacommit + data instant) → bookkeeping (marker cleanup, timeline
//!    archival) → unlock.
//!
//! Only [`InProcessLockProvider`] exists today (Java's single-writer default);
//! the trait is the seam for external providers (DynamoDB, ZK, ...) later.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use futures::future::BoxFuture;

use crate::Result;
use crate::config::table::HudiTableConfig;
use crate::table::Table;

/// A held table lock; releases on drop.
pub struct LockLease {
    _inner: Box<dyn std::any::Any + Send>,
}

/// Java `LockProvider`: mutual exclusion for timeline-mutating critical
/// sections. Implementations must be safe to share across writes.
pub trait LockProvider: Send + Sync + std::fmt::Debug {
    /// Acquire the table lock, waiting as needed. The lease releases on drop.
    fn lock(&self) -> BoxFuture<'_, Result<LockLease>>;
}

/// Java `InProcessLockProvider`: one lock per table base path, shared across
/// all writers in this process.
#[derive(Debug)]
pub struct InProcessLockProvider {
    base_path: String,
}

impl InProcessLockProvider {
    pub fn new(base_path: impl Into<String>) -> Self {
        Self {
            base_path: base_path.into(),
        }
    }

    fn lock_for_base_path(&self) -> Arc<tokio::sync::Mutex<()>> {
        static LOCKS: OnceLock<Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>> =
            OnceLock::new();
        let registry = LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
        let mut map = match registry.lock() {
            Ok(map) => map,
            // A poisoned registry only means another thread panicked while
            // inserting; the map itself is still usable.
            Err(poisoned) => poisoned.into_inner(),
        };
        map.entry(self.base_path.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }
}

impl LockProvider for InProcessLockProvider {
    fn lock(&self) -> BoxFuture<'_, Result<LockLease>> {
        let lock = self.lock_for_base_path();
        Box::pin(async move {
            let guard = lock.lock_owned().await;
            Ok(LockLease {
                _inner: Box::new(guard),
            })
        })
    }
}

/// The lock provider for a table. Always in-process today; later this reads
/// `hoodie.write.lock.provider` to construct external providers.
pub(crate) fn lock_provider_for(table: &Table) -> Arc<dyn LockProvider> {
    let base_path: String = table
        .hudi_configs
        .get_or_default(HudiTableConfig::BasePath)
        .into();
    Arc::new(InProcessLockProvider::new(base_path))
}
