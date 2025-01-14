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
use bytes::Bytes;
use object_store::{ObjectMeta, ObjectStore};
use std::io::{BufReader, Cursor, Result};
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

#[derive(Debug)]
pub struct StorageReader {
    reader: BufReader<Cursor<Bytes>>,
}

impl StorageReader {
    pub async fn new(object_store: Arc<dyn ObjectStore>, object_meta: ObjectMeta) -> Result<Self> {
        let get_result = object_store.get(&object_meta.location).await?;
        // TODO change to use stream
        let bytes = get_result.bytes().await?;
        let reader = BufReader::with_capacity(bytes.len(), Cursor::new(bytes));
        Ok(Self { reader })
    }
}

impl Read for StorageReader {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.reader.read(buf)
    }
}

impl Seek for StorageReader {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        self.reader.seek(pos)
    }
}
