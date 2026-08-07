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
use crate::storage::OBJECT_STORE_RUNTIME;
use bytes::Bytes;
use object_store::path::Path as ObjPath;
use object_store::{ObjectMeta, ObjectStore};
use std::io::{Error, ErrorKind, Read, Result, Seek, SeekFrom};
use std::sync::Arc;

/// How much of a file a streaming reader keeps resident at once.
///
/// Sweeping a log file's block headers touches small reads scattered across the
/// file and seeks forward past block content, so a window this size is read
/// once and reused for many headers. Peak buffering is one window regardless of
/// how large the file is.
const STREAM_WINDOW_SIZE: u64 = 16 * 1024 * 1024;

/// Fetch a byte range synchronously, from any context.
///
/// See [`OBJECT_STORE_RUNTIME`] for why the work is spawned onto a shared
/// runtime and waited on over a channel rather than driven with `block_on`.
fn get_range_blocking(
    object_store: &Arc<dyn ObjectStore>,
    location: &ObjPath,
    offset: u64,
    length: u64,
) -> Result<Bytes> {
    let end = offset.checked_add(length).ok_or_else(|| {
        Error::other(format!(
            "ranged read offset {offset} + length {length} overflows u64"
        ))
    })?;
    let store = object_store.clone();
    let location = location.clone();
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    OBJECT_STORE_RUNTIME.spawn(async move {
        let res = store.get_range(&location, offset..end).await;
        // The receiver is gone only if the caller panicked.
        let _ = tx.send(res);
    });
    rx.recv()
        .map_err(|_| Error::other("ranged object-store read task dropped"))?
        .map_err(|e| Error::other(format!("ranged object-store read failed: {e}")))
}

/// Reads one log block's content range without holding the file.
///
/// A block that has been admitted needs its own bytes and nothing else. This
/// carries an object-store handle and a path — no file bytes — so a block can
/// keep one cheaply and fetch its range when it is actually read. Cloning bumps
/// an `Arc` and copies a path.
#[derive(Clone, Debug)]
pub struct LogBlockFetcher {
    object_store: Arc<dyn ObjectStore>,
    location: ObjPath,
}

impl LogBlockFetcher {
    pub fn new(object_store: Arc<dyn ObjectStore>, location: ObjPath) -> Self {
        Self {
            object_store,
            location,
        }
    }

    /// Read `[offset, offset + length)` in one ranged request.
    pub fn read_content(&self, offset: u64, length: u64) -> Result<Bytes> {
        get_range_blocking(&self.object_store, &self.location, offset, length)
    }
}

/// A seekable reader over a file in an object store.
///
/// [`StorageReader::new`] fetches the whole file up front, which is what a
/// small file or a full read wants. [`StorageReader::new_streaming`] fetches
/// bounded windows as the cursor moves, so walking a large file's structure
/// never holds more than one window.
#[derive(Debug)]
pub struct StorageReader {
    object_store: Arc<dyn ObjectStore>,
    location: ObjPath,
    file_len: u64,
    /// Cursor position within the file.
    pos: u64,
    /// Set when the whole file is resident; `None` puts the reader in streaming
    /// mode.
    whole: Option<Bytes>,
    /// Streaming mode: the resident window and the offset it starts at. Empty
    /// until the first read forces a fetch.
    window: Bytes,
    window_start: u64,
}

impl StorageReader {
    /// Read the entire file into one buffer.
    pub async fn new(object_store: Arc<dyn ObjectStore>, object_meta: ObjectMeta) -> Result<Self> {
        let get_result = object_store
            .get(&object_meta.location)
            .await
            .map_err(|e| Error::other(format!("object-store get failed: {e}")))?;
        let bytes = get_result
            .bytes()
            .await
            .map_err(|e| Error::other(format!("object-store read failed: {e}")))?;
        let file_len = bytes.len() as u64;
        Ok(Self {
            object_store,
            location: object_meta.location,
            file_len,
            pos: 0,
            whole: Some(bytes),
            window: Bytes::new(),
            window_start: 0,
        })
    }

    /// Open a reader that fetches bounded windows on demand.
    ///
    /// Nothing is read until the first [`Read::read`].
    pub fn new_streaming(object_store: Arc<dyn ObjectStore>, object_meta: ObjectMeta) -> Self {
        Self {
            object_store,
            location: object_meta.location,
            file_len: object_meta.size,
            pos: 0,
            whole: None,
            window: Bytes::new(),
            window_start: 0,
        }
    }

    /// A fetcher over the same file, for reading a block's content later
    /// without re-opening it.
    pub fn block_fetcher(&self) -> LogBlockFetcher {
        LogBlockFetcher::new(self.object_store.clone(), self.location.clone())
    }

    /// Refill the window if the cursor has moved outside it. No-op past the end.
    fn ensure_window(&mut self) -> Result<()> {
        if self.pos >= self.file_len {
            return Ok(());
        }
        let in_window = self.pos >= self.window_start
            && self.pos < self.window_start + self.window.len() as u64;
        if in_window {
            return Ok(());
        }
        let start = self.pos;
        let end = (start + STREAM_WINDOW_SIZE).min(self.file_len);
        self.window = get_range_blocking(&self.object_store, &self.location, start, end - start)?;
        self.window_start = start;
        Ok(())
    }
}

impl Read for StorageReader {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        if buf.is_empty() || self.pos >= self.file_len {
            return Ok(0);
        }
        if let Some(ref whole) = self.whole {
            let start = self.pos as usize;
            let end = (start + buf.len()).min(whole.len());
            let n = end - start;
            buf[..n].copy_from_slice(&whole[start..end]);
            self.pos += n as u64;
            return Ok(n);
        }
        self.ensure_window()?;
        let off = (self.pos - self.window_start) as usize;
        let avail = self.window.len() - off;
        let n = avail.min(buf.len());
        buf[..n].copy_from_slice(&self.window[off..off + n]);
        self.pos += n as u64;
        Ok(n)
    }
}

impl Seek for StorageReader {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        let new_pos = match pos {
            SeekFrom::Start(p) => p as i128,
            SeekFrom::End(p) => self.file_len as i128 + p as i128,
            SeekFrom::Current(p) => self.pos as i128 + p as i128,
        };
        if new_pos < 0 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "seek to a negative position",
            ));
        }
        self.pos = new_pos as u64;
        Ok(self.pos)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::local::LocalFileSystem;
    use std::io::Write;

    /// A temp file of `len` bytes with a repeating pattern, plus the store and
    /// metadata needed to open it either way.
    fn make_store(len: usize) -> (Arc<dyn ObjectStore>, ObjectMeta, Vec<u8>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.bin");
        let bytes: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
        std::fs::File::create(&path)
            .unwrap()
            .write_all(&bytes)
            .unwrap();

        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(&dir).unwrap());
        let location = ObjPath::from("data.bin");
        let meta = OBJECT_STORE_RUNTIME
            .block_on(async { store.head(&location).await })
            .unwrap();
        (store, meta, bytes, dir)
    }

    fn read_all(reader: &mut StorageReader, chunk: usize) -> Vec<u8> {
        let mut out = Vec::new();
        let mut buf = vec![0u8; chunk];
        loop {
            let n = reader.read(&mut buf).unwrap();
            if n == 0 {
                break;
            }
            out.extend_from_slice(&buf[..n]);
        }
        out
    }

    /// The two modes have to be indistinguishable to a caller. A streaming read
    /// serves out of a window and refills as the cursor advances, which is where
    /// an off-by-one would show up.
    #[test]
    fn test_streaming_read_matches_eager_across_window_refills() {
        // Larger than one window, so the read spans several refills.
        let len = (STREAM_WINDOW_SIZE as usize) * 2 + 4096;
        let (store, meta, expected, _dir) = make_store(len);

        let mut eager = OBJECT_STORE_RUNTIME
            .block_on(StorageReader::new(store.clone(), meta.clone()))
            .unwrap();
        assert_eq!(read_all(&mut eager, 64 * 1024), expected);

        let mut streaming = StorageReader::new_streaming(store, meta);
        assert_eq!(read_all(&mut streaming, 64 * 1024), expected);
    }

    /// Seeking backwards has to refill, and a seek past the end has to read
    /// nothing rather than fetch. Both are what a block scan does when it walks
    /// back to a header or jumps past a block it is skipping.
    #[test]
    fn test_streaming_seek_backwards_and_past_end() {
        let len = (STREAM_WINDOW_SIZE as usize) + 1024;
        let (store, meta, expected, _dir) = make_store(len);
        let mut reader = StorageReader::new_streaming(store, meta);

        // Force a window near the end, then walk back to the start.
        reader.seek(SeekFrom::Start(len as u64 - 512)).unwrap();
        let mut tail = [0u8; 512];
        reader.read_exact(&mut tail).unwrap();
        assert_eq!(&tail[..], &expected[len - 512..]);

        reader.seek(SeekFrom::Start(0)).unwrap();
        let mut head = [0u8; 512];
        reader.read_exact(&mut head).unwrap();
        assert_eq!(&head[..], &expected[..512]);

        reader.seek(SeekFrom::End(0)).unwrap();
        assert_eq!(reader.read(&mut [0u8; 16]).unwrap(), 0);

        assert!(reader.seek(SeekFrom::Current(-(len as i64) - 10)).is_err());
    }

    /// A block fetcher reads only its own range, and does so without the file
    /// ever being resident.
    #[test]
    fn test_block_fetcher_reads_only_its_range() {
        let len = 8192;
        let (store, meta, expected, _dir) = make_store(len);
        let reader = StorageReader::new_streaming(store, meta);

        let content = reader.block_fetcher().read_content(1000, 256).unwrap();
        assert_eq!(&content[..], &expected[1000..1256]);
    }
}
