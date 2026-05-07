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
 */

//! Mirrors Java `org.apache.hudi.expression.ArrayData`.

use std::any::Any;

/// Read-only view over an array-like value. Mirrors Java's `ArrayData`.
pub trait ArrayData: Send + Sync {
    fn num_elements(&self) -> usize;
    fn get(&self, pos: usize) -> Option<&dyn Any>;
}
