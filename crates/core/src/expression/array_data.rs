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

use crate::expression::struct_like::StructLike;
use std::any::Any;

/// Concrete `StructLike` implementation backed by a vector of boxed `Any`
/// values. Mirrors Java `ArrayData(List<Object> data)`.
pub struct ArrayData {
    data: Vec<Box<dyn Any + Send + Sync>>,
}

impl ArrayData {
    pub fn new(data: Vec<Box<dyn Any + Send + Sync>>) -> Self {
        Self { data }
    }
}

impl StructLike for ArrayData {
    fn num_fields(&self) -> usize {
        self.data.len()
    }

    fn get(&self, pos: usize) -> Option<&dyn Any> {
        self.data.get(pos).map(|b| b.as_ref() as &dyn Any)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn array_data_implements_struct_like() {
        let arr = ArrayData::new(vec![
            Box::new(42i32) as Box<dyn Any + Send + Sync>,
            Box::new("hello".to_string()),
        ]);
        assert_eq!(arr.num_fields(), 2);
        assert_eq!(
            arr.get(0).and_then(|v| v.downcast_ref::<i32>()).copied(),
            Some(42)
        );
        assert!(arr.get(2).is_none());
    }
}
