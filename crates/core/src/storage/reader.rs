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
use crate::config::HudiConfigs;
use crate::storage::{OBJECT_STORE_RUNTIME, OBJECT_STORE_RUNTIME_THREAD_NAME};
use bytes::Bytes;
use object_store::path::Path as ObjPath;
use object_store::{ObjectMeta, ObjectStore};
use std::io::{Error, ErrorKind, Read, Result, Seek, SeekFrom};
use std::sync::Arc;

/// How much of a file a streaming reader keeps resident at once, when the table
/// does not say.
///
/// Sweeping a log file's block headers touches small reads scattered across the
/// file and seeks forward past block content, so a window this size is read
/// once and reused for many headers. Peak buffering is one window regardless of
/// how large the file is.
///
/// 16 MiB is what Hudi defaults [`CONFIG_DFS_BUFFER_MAX_SIZE`] to, so a table
/// that sets nothing reads the same amount here as it would there.
pub const DEFAULT_STREAM_WINDOW_SIZE: u64 = 16 * 1024 * 1024;

/// Hudi's buffer size for reading a log file.
///
/// Java hands this to the filesystem client when it opens a log file
/// (`HoodieLogFileReader` → `HoodieStorage.openSeekable` → `FileSystem.open`),
/// so it bounds how much of the file one reader holds. This crate buffers a
/// window itself rather than delegating to a filesystem client, but the knob
/// means the same thing to whoever sets it: the memory one log-file read may
/// occupy. Matching Hudi's spelling means a table tuned for one reader is tuned
/// for the other.
///
/// It exists because the cost scales with concurrency, not with one read — Java
/// drops it to 1 MiB under MapReduce for exactly that reason, and this crate
/// reads file slices concurrently (`hoodie.read.file.slice.read.concurrency`).
pub const CONFIG_DFS_BUFFER_MAX_SIZE: &str = "hoodie.memory.dfs.buffer.max.size";

/// The streaming window size a set of configs asks for.
///
/// Absent means [`DEFAULT_STREAM_WINDOW_SIZE`]. A value that is not a positive
/// byte count is an error rather than a silent fallback: someone setting this is
/// tuning memory, and quietly giving them 16 MiB when they asked for 1 MiB is
/// the failure this config exists to prevent.
pub fn stream_window_size(hudi_configs: &HudiConfigs) -> Result<u64> {
    let Some(raw) = hudi_configs
        .as_options()
        .get(CONFIG_DFS_BUFFER_MAX_SIZE)
        .cloned()
    else {
        return Ok(DEFAULT_STREAM_WINDOW_SIZE);
    };
    match raw.trim().parse::<u64>() {
        Ok(0) | Err(_) => Err(Error::other(format!(
            "{CONFIG_DFS_BUFFER_MAX_SIZE} must be a positive integer byte count, got '{raw}'"
        ))),
        Ok(size) => Ok(size),
    }
}

/// Whether the calling thread is one of [`OBJECT_STORE_RUNTIME`]'s workers.
///
/// Recognised by thread name, which is the only handle available: a worker has no
/// marker a caller can query, and `Handle::try_current()` cannot be compared to a
/// `Runtime`'s own handle.
pub(crate) fn in_object_store_runtime() -> bool {
    std::thread::current()
        .name()
        .is_some_and(|name| name.starts_with(OBJECT_STORE_RUNTIME_THREAD_NAME))
}

/// Fetch a byte range synchronously, from any context.
///
/// See [`OBJECT_STORE_RUNTIME`] for why the work is spawned onto a shared
/// runtime and waited on over a channel rather than driven with `block_on`.
///
/// # Where this may be called from
///
/// A blocking-pool thread (`spawn_blocking`) or an ordinary sync thread. Both are
/// fine: the fetch runs on [`OBJECT_STORE_RUNTIME`], so the wait always ends.
///
/// **Not** from an async task on a worker thread — that blocks the worker for a
/// whole round trip, starving every other task on that runtime, which is the
/// "no blocking I/O in async" rule. Nothing here can detect that in general, so
/// it remains the caller's obligation.
///
/// **Never** from an [`OBJECT_STORE_RUNTIME`] worker: the wait would depend on a
/// task that can only be scheduled on the workers, and with enough concurrent
/// callers every worker blocks and none of their tasks can ever run — a genuine
/// deadlock rather than mere starvation. That case *is* detectable, and is
/// refused below with a message rather than left to hang. It is unreachable
/// today; the guard is here so wiring a new caller cannot make it reachable
/// silently.
fn get_range_blocking(
    object_store: &Arc<dyn ObjectStore>,
    location: &ObjPath,
    offset: u64,
    length: u64,
) -> Result<Bytes> {
    if in_object_store_runtime() {
        return Err(Error::other(format!(
            "a blocking ranged read was issued from an object-store runtime worker, which would \
             deadlock: the wait can only be satisfied by a task on that same runtime. Drive this \
             read from a blocking-pool thread instead (path '{location}')"
        )));
    }
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

    /// The file these ranges are read from, so a caller batching across blocks
    /// can group them by the file they belong to.
    pub fn location(&self) -> &ObjPath {
        &self.location
    }

    /// Read several ranges from this file in one call.
    ///
    /// `get_ranges` coalesces ranges that sit close together into a single
    /// request, so a run of adjacent blocks costs one round trip rather than
    /// one each. Reading them one at a time is the same bytes and many more
    /// round trips, which is what dominates on object storage.
    ///
    /// Async, unlike [`Self::read_content`]: the caller is expected to have a
    /// runtime, and going through the blocking bridge here would serialise the
    /// very requests this exists to overlap.
    pub async fn read_contents(&self, ranges: &[std::ops::Range<u64>]) -> Result<Vec<Bytes>> {
        self.object_store
            .get_ranges(&self.location, ranges)
            .await
            .map_err(|e| {
                Error::other(format!(
                    "batched ranged read of {} range(s) from '{}' failed: {e}",
                    ranges.len(),
                    self.location
                ))
            })
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
    /// How much to fetch per window refill. See [`stream_window_size`].
    window_size: u64,
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
            window_size: DEFAULT_STREAM_WINDOW_SIZE,
        })
    }

    /// Open a reader that fetches bounded windows on demand.
    ///
    /// `window_size` is how much is fetched per refill and therefore the peak
    /// this reader buffers; see [`stream_window_size`] for where it comes from.
    ///
    /// Nothing is read until the first [`Read::read`].
    pub fn new_streaming(
        object_store: Arc<dyn ObjectStore>,
        object_meta: ObjectMeta,
        window_size: u64,
    ) -> Self {
        Self {
            object_store,
            location: object_meta.location,
            file_len: object_meta.size,
            pos: 0,
            whole: None,
            window: Bytes::new(),
            window_start: 0,
            window_size,
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
        let end = (start + self.window_size).min(self.file_len);
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

    /// A blocking ranged read issued from an object-store runtime worker must be
    /// refused, because it would deadlock rather than merely block: the wait can
    /// only be satisfied by a task on the very runtime whose worker is waiting.
    ///
    /// Asserted from inside the runtime, since the guard keys off the calling
    /// thread. The negative control matters as much as the positive: an ordinary
    /// thread must still be allowed through, or the guard would break every real
    /// caller.
    #[test]
    fn test_blocking_read_from_the_object_store_runtime_is_refused() {
        let len = 4096;
        let (store, meta, expected, _dir) = make_store(len);

        // Positive: on a worker of that runtime, refused with a message naming
        // the hazard.
        let err = OBJECT_STORE_RUNTIME
            .block_on(async {
                let store = store.clone();
                let location = meta.location.clone();
                tokio::task::spawn(async move {
                    assert!(
                        in_object_store_runtime(),
                        "a spawned task must run on an object-store worker"
                    );
                    get_range_blocking(&store, &location, 0, 16).err()
                })
                .await
                .unwrap()
            })
            .expect("must be refused, not attempted");
        let msg = err.to_string();
        assert!(
            msg.contains("deadlock"),
            "the error must say why it refused, got: {msg}"
        );

        // Negative control: an ordinary thread reads normally.
        assert!(!in_object_store_runtime());
        let bytes = get_range_blocking(&store, &meta.location, 0, 16).unwrap();
        assert_eq!(bytes.as_ref(), &expected[..16]);
    }

    /// The two modes have to be indistinguishable to a caller. A streaming read
    /// serves out of a window and refills as the cursor advances, which is where
    /// an off-by-one would show up.
    #[test]
    fn test_streaming_read_matches_eager_across_window_refills() {
        // Larger than one window, so the read spans several refills.
        let len = (DEFAULT_STREAM_WINDOW_SIZE as usize) * 2 + 4096;
        let (store, meta, expected, _dir) = make_store(len);

        let mut eager = OBJECT_STORE_RUNTIME
            .block_on(StorageReader::new(store.clone(), meta.clone()))
            .unwrap();
        assert_eq!(read_all(&mut eager, 64 * 1024), expected);

        let mut streaming = StorageReader::new_streaming(store, meta, DEFAULT_STREAM_WINDOW_SIZE);
        assert_eq!(read_all(&mut streaming, 64 * 1024), expected);
    }

    /// Seeking backwards has to refill, and a seek past the end has to read
    /// nothing rather than fetch. Both are what a block scan does when it walks
    /// back to a header or jumps past a block it is skipping.
    #[test]
    fn test_streaming_seek_backwards_and_past_end() {
        let len = (DEFAULT_STREAM_WINDOW_SIZE as usize) + 1024;
        let (store, meta, expected, _dir) = make_store(len);
        let mut reader = StorageReader::new_streaming(store, meta, DEFAULT_STREAM_WINDOW_SIZE);

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

    /// Absent config reads the same amount Hudi would: 16 MiB.
    #[test]
    fn test_stream_window_size_defaults_to_hudis_own_default() {
        let empty: Vec<(&str, &str)> = vec![];
        assert_eq!(
            stream_window_size(&HudiConfigs::new(empty)).unwrap(),
            DEFAULT_STREAM_WINDOW_SIZE
        );
        assert_eq!(DEFAULT_STREAM_WINDOW_SIZE, 16 * 1024 * 1024);
    }

    /// The whole point of the knob: a caller tuning memory down must actually
    /// get less, not the default. Reading a file larger than the window they
    /// asked for proves the window, not just the field.
    #[test]
    fn test_configured_window_is_used_for_refills() {
        let window = 4096u64;
        let configs = HudiConfigs::new([(CONFIG_DFS_BUFFER_MAX_SIZE, window.to_string())]);
        assert_eq!(stream_window_size(&configs).unwrap(), window);

        let len = (window as usize) * 3 + 17;
        let (store, meta, expected, _dir) = make_store(len);
        let mut reader = StorageReader::new_streaming(store, meta, window);

        // Same bytes as an eager read, across several refills of the smaller
        // window.
        assert_eq!(read_all(&mut reader, 512), expected);
        assert!(
            reader.window.len() as u64 <= window,
            "a refill fetched {} bytes for a {window}-byte window",
            reader.window.len()
        );
    }

    /// A value that is not a positive byte count is rejected rather than
    /// silently replaced by the default — someone setting this is bounding
    /// memory, and handing them 16 MiB when they asked for less is the failure
    /// the config exists to prevent.
    #[test]
    fn test_unusable_window_size_is_rejected() {
        for bad in ["0", "-1", "16MB", ""] {
            let configs = HudiConfigs::new([(CONFIG_DFS_BUFFER_MAX_SIZE, bad)]);
            let err = stream_window_size(&configs)
                .expect_err("'{bad}' is not a positive byte count and must not be accepted");
            assert!(
                format!("{err}").contains(CONFIG_DFS_BUFFER_MAX_SIZE),
                "the error must name the config, got: {err}"
            );
        }
    }

    /// A block fetcher reads only its own range, and does so without the file
    /// ever being resident.
    #[test]
    fn test_block_fetcher_reads_only_its_range() {
        let len = 8192;
        let (store, meta, expected, _dir) = make_store(len);
        let reader = StorageReader::new_streaming(store, meta, DEFAULT_STREAM_WINDOW_SIZE);

        let content = reader.block_fetcher().read_content(1000, 256).unwrap();
        assert_eq!(&content[..], &expected[1000..1256]);
    }
}
