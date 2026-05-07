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

//! Mirrors Java `org.apache.hudi.expression.StructLike`.

use std::any::Any;

/// Read-only view over a record-like value. Mirrors Java's `StructLike`.
///
/// Java has `int numFields()` + `<T> T get(int pos, Class<T> javaClass)`.
/// Rust uses `&dyn Any` so callers can downcast.
pub trait StructLike: Send + Sync {
    fn num_fields(&self) -> usize;
    fn get(&self, pos: usize) -> Option<&dyn Any>;
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockStruct(Vec<Option<Box<dyn std::any::Any + Send + Sync>>>);

    impl StructLike for MockStruct {
        fn num_fields(&self) -> usize { self.0.len() }
        fn get(&self, pos: usize) -> Option<&dyn std::any::Any> {
            self.0[pos].as_deref().map(|v| v as &dyn std::any::Any)
        }
    }

    #[test]
    fn struct_like_basic() {
        let s = MockStruct(vec![
            Some(Box::new(42i32)),
            Some(Box::new("hello".to_string())),
            None,
        ]);
        assert_eq!(s.num_fields(), 3);
        assert!(s.get(2).is_none());
    }
}
