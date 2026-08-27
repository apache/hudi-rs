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
use bytes::Bytes;
use object_store::path::Path as ObjPath;
use object_store::{ObjectMeta, ObjectStore};
use std::io::{Error, ErrorKind, Result};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

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

/// Size below which an HFile is read whole rather than in ranges.
///
/// Hudi's own key and meaning: `HFileReaderFactory.createInputStream` reads the
/// whole file when it is under this, and only opens a seekable stream above it.
pub const CONFIG_HFILE_WHOLE_READ_MAX_SIZE_MB: &str = "hoodie.metadata.file.cache.max.size.mb";

/// 50 MB, matching Hudi's default for [`CONFIG_HFILE_WHOLE_READ_MAX_SIZE_MB`].
///
/// Taken from Hudi rather than derived here, deliberately. Measured on a local
/// filesystem the crossover is about 2 MB for a point lookup and a full scan is a
/// wash above roughly 512 KB — but a local `head` and range read are nearly free,
/// where on object storage each is a round trip and a ranged open pays three
/// before the first data byte. So the local number is a floor, not the answer, and
/// a constant set from it would be far too low for the deployment that matters.
pub const DEFAULT_HFILE_WHOLE_READ_MAX_SIZE_MB: u64 = 50;

/// The size below which an HFile is read whole even when the caller named keys.
///
/// A separate, much smaller bound because a scan and a seek want opposite things,
/// and the reader knows which it is doing before it opens the file. Reading whole
/// never costs a scan anything, since a scan touches every byte either way. A seek
/// touches one block, so reading whole makes it pay for the file: measured on
/// generated HFiles, a ranged seek is 1.54x faster at 8 KB (fixed cost dominates)
/// but a whole-file seek is 1.24x slower at 2 MB, 3.66x at 8 MB and 9.40x at 32 MB.
/// 512 KB is the largest size measured at which whole does not lose.
///
/// This is where Hudi is deliberately not followed. Its single threshold is
/// `hoodie.metadata.file.cache.max.size.mb` and the name says why it can be coarse:
/// the whole read populates a cache, so one file read is amortised over many
/// lookups. There is no such cache here, so the read would be repaid on every
/// lookup instead.
///
/// The number is a local measurement and therefore conservative. A local `head`
/// and range read are nearly free, where each is a round trip on object storage, so
/// the real crossover there is higher and this bound gives up part of the available
/// win rather than risking the regression above.
pub const HFILE_WHOLE_READ_WITH_KEYS_MAX_SIZE: u64 = 512 * 1024;

/// The size below which an HFile should be read whole, in bytes.
pub fn hfile_whole_read_max_size(hudi_configs: &HudiConfigs) -> Result<u64> {
    let mb = match hudi_configs
        .as_options()
        .get(CONFIG_HFILE_WHOLE_READ_MAX_SIZE_MB)
        .map(|raw| raw.trim().parse::<u64>())
    {
        None => DEFAULT_HFILE_WHOLE_READ_MAX_SIZE_MB,
        // Zero is meaningful: always read in ranges, never whole.
        Some(Ok(mb)) => mb,
        Some(Err(_)) => {
            return Err(Error::other(format!(
                "{CONFIG_HFILE_WHOLE_READ_MAX_SIZE_MB} must be a non-negative integer \
                 count of megabytes"
            )));
        }
    };
    Ok(mb.saturating_mul(1024 * 1024))
}

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

/// Fetch `[offset, offset + length)` in one ranged request.
async fn get_range(
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
    object_store
        .get_range(location, offset..end)
        .await
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
    /// What this fetcher has actually read, shared across clones so a caller
    /// holding one clone can account for reads made through another.
    reads: Arc<FetchCounts>,
}

/// What a fetcher read, as opposed to what it was asked for.
///
/// Both numbers are what this side of the API can observe, and neither is a
/// count of what a remote store transferred. Read them with that in mind.
///
/// **`bytes` is the total length of the buffers the store returned**, so it is
/// the number to judge a narrowed read on — but only exactly on a backend that
/// reads each range as asked. `LocalFileSystem` does
/// (`object_store::local`, which overrides `get_ranges` to read range by range),
/// which is why tests measure real reductions against it. The **default**
/// `get_ranges` runs `coalesce_ranges` with a 1 MiB threshold, merging ranges
/// closer than that into one GET and slicing the result, so on the cloud
/// backends `bytes` is the total *asked for* and the transfer can be larger. The
/// practical consequence is worth stating: selecting blocks only reduces cloud
/// transfer when the selected blocks sit more than that threshold apart.
///
/// **`calls` is not a request count.** It counts calls made into the
/// object-store API; coalescing and any internal fan-out are invisible from
/// here. A full scan of a thousand adjacent blocks reports one call, and a
/// scattered three-block seek also reports one, so `calls` must not be read as
/// "the seek did not reduce round trips".
#[derive(Debug, Default)]
pub struct FetchCounts {
    calls: AtomicU64,
    bytes: AtomicU64,
}

impl FetchCounts {
    /// Calls made into the object-store API. See the type's note: this is not a
    /// count of GETs.
    pub fn calls(&self) -> u64 {
        self.calls.load(Ordering::Relaxed)
    }

    /// Total length of the buffers the object store returned. See the type's
    /// note before treating this as bytes transferred.
    pub fn bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }

    fn record(&self, calls: u64, bytes: u64) {
        self.calls.fetch_add(calls, Ordering::Relaxed);
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
    }
}

impl LogBlockFetcher {
    pub fn new(object_store: Arc<dyn ObjectStore>, location: ObjPath) -> Self {
        Self {
            object_store,
            location,
            reads: Arc::new(FetchCounts::default()),
        }
    }

    /// What this fetcher has read so far.
    pub fn reads(&self) -> &FetchCounts {
        &self.reads
    }

    /// Read `[offset, offset + length)` in one ranged request.
    pub async fn read_content(&self, offset: u64, length: u64) -> Result<Bytes> {
        let bytes = get_range(&self.object_store, &self.location, offset, length).await?;
        self.reads.record(1, bytes.len() as u64);
        Ok(bytes)
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
    pub async fn read_contents(&self, ranges: &[std::ops::Range<u64>]) -> Result<Vec<Bytes>> {
        let fetched = self
            .object_store
            .get_ranges(&self.location, ranges)
            .await
            .map_err(|e| {
                Error::other(format!(
                    "batched ranged read of {} range(s) from '{}' failed: {e}",
                    ranges.len(),
                    self.location
                ))
            })?;
        // One call, whatever coalescing and fan-out did underneath, and the
        // bytes it handed back. Counting `ranges.len()` would count the plan.
        self.reads
            .record(1, fetched.iter().map(|b| b.len() as u64).sum());
        Ok(fetched)
    }
}

/// A seekable reader over a file in an object store.
///
/// [`StorageReader::new`] fetches the whole file up front, which is what a
/// small file or a full read wants. [`StorageReader::new_streaming`] fetches
/// bounded windows as the cursor moves, so walking a large file's structure
/// never holds more than one window.
///
/// Reads are `async` inherent methods rather than `std::io::Read`: every byte
/// comes from an object store, and a synchronous `Read` over an async store can
/// only be had by blocking some thread on a runtime, which is a contract the
/// compiler cannot check and every caller has to be told about.
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
    /// Nothing is read until the first read.
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

    /// Length of the whole file, known without reading any of it.
    pub fn file_len(&self) -> u64 {
        self.file_len
    }

    /// Where the cursor sits.
    pub fn position(&self) -> u64 {
        self.pos
    }

    /// Move the cursor. No I/O, and a position past the end of the file is
    /// allowed: it fails at the next read, which is what lets a caller skip past
    /// a block's content without paying for it.
    pub fn seek_to(&mut self, pos: u64) {
        self.pos = pos;
    }

    /// Whether `[start, end)` lies inside the resident window.
    fn window_covers(&self, start: u64, end: u64) -> bool {
        start >= self.window_start && end <= self.window_start + self.window.len() as u64
    }

    /// Whether `[start, end)` can be handed out without another request.
    ///
    /// True for a whole-file reader, and for a streaming one whose current window
    /// already spans the range. A caller that would otherwise defer a read can ask
    /// this first and take the bytes it already paid for: a log file smaller than
    /// one window is fetched entirely by the first fill, so deferring its blocks'
    /// content buys nothing and costs a second request.
    pub fn has_resident(&self, start: u64, end: u64) -> bool {
        if self.whole.is_some() {
            return end <= self.file_len;
        }
        self.window_covers(start, end)
    }

    /// Fetch a window starting at the cursor.
    async fn fill_window(&mut self) -> Result<()> {
        let end = self.pos.saturating_add(self.window_size).min(self.file_len);
        self.window =
            get_range(&self.object_store, &self.location, self.pos, end - self.pos).await?;
        self.window_start = self.pos;
        Ok(())
    }

    /// Read the next `len` bytes, advancing the cursor.
    ///
    /// A read that would run past the end of the file is
    /// [`ErrorKind::UnexpectedEof`] and consumes nothing, which is how a caller
    /// walking blocks recognises the end of the last one.
    ///
    /// Nothing is copied when the bytes are already resident: both the whole-file
    /// buffer and the streaming window hand out `Bytes` slices of themselves.
    pub async fn read_bytes(&mut self, len: u64) -> Result<Bytes> {
        if len == 0 {
            return Ok(Bytes::new());
        }
        let end = self.pos.checked_add(len).ok_or_else(|| {
            Error::other(format!(
                "read of {len} byte(s) at offset {} overflows u64",
                self.pos
            ))
        })?;
        if end > self.file_len {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                format!(
                    "read of {len} byte(s) at offset {} runs past the end of '{}' ({} bytes)",
                    self.pos, self.location, self.file_len
                ),
            ));
        }

        if let Some(whole) = self.whole.as_ref() {
            let bytes = whole.slice(self.pos as usize..end as usize);
            self.pos = end;
            return Ok(bytes);
        }

        // Longer than a window: fetch exactly this range and leave the window
        // alone, rather than growing what the reader keeps resident to fit one
        // read. A log block's content can be larger than the window a caller
        // tuned for sweeping headers, and it has to be contiguous to decode.
        if len > self.window_size {
            let bytes = get_range(&self.object_store, &self.location, self.pos, len).await?;
            self.pos = end;
            return Ok(bytes);
        }

        if !self.window_covers(self.pos, end) {
            // A refilled window starts at the cursor and runs `window_size`
            // bytes or to the end of the file, whichever comes first, so it
            // covers `len` — the end-of-file case was refused above.
            self.fill_window().await?;
        }
        let off = (self.pos - self.window_start) as usize;
        let resident = self.window.len() - off;
        if resident < len as usize {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                format!(
                    "a window fetched at offset {} holds {resident} byte(s), short of the {len} \
                     asked for: '{}' was truncated while it was being read",
                    self.pos, self.location
                ),
            ));
        }
        let bytes = self.window.slice(off..off + len as usize);
        self.pos = end;
        Ok(bytes)
    }

    /// Fill `buf` from the cursor, advancing it. See [`Self::read_bytes`] for
    /// what happens at the end of the file.
    pub async fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        if buf.is_empty() {
            return Ok(());
        }
        let bytes = self.read_bytes(buf.len() as u64).await?;
        buf.copy_from_slice(&bytes);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::local::LocalFileSystem;
    use std::io::Write;

    /// A temp file of `len` bytes with a repeating pattern, plus the store and
    /// metadata needed to open it either way.
    async fn make_store(
        len: usize,
    ) -> (Arc<dyn ObjectStore>, ObjectMeta, Vec<u8>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.bin");
        let bytes: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
        std::fs::File::create(&path)
            .unwrap()
            .write_all(&bytes)
            .unwrap();

        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(&dir).unwrap());
        let location = ObjPath::from("data.bin");
        let meta = store.head(&location).await.unwrap();
        (store, meta, bytes, dir)
    }

    /// Read the whole file in `chunk`-sized reads.
    async fn read_all(reader: &mut StorageReader, chunk: u64) -> Vec<u8> {
        let mut out = Vec::new();
        loop {
            let want = chunk.min(reader.file_len() - reader.position());
            if want == 0 {
                return out;
            }
            out.extend_from_slice(&reader.read_bytes(want).await.unwrap());
        }
    }

    /// The two modes have to be indistinguishable to a caller. A streaming read
    /// serves out of a window and refills as the cursor advances, which is where
    /// an off-by-one would show up.
    #[tokio::test]
    async fn test_streaming_read_matches_eager_across_window_refills() {
        // Larger than one window, so the read spans several refills.
        let len = (DEFAULT_STREAM_WINDOW_SIZE as usize) * 2 + 4096;
        let (store, meta, expected, _dir) = make_store(len).await;

        let mut eager = StorageReader::new(store.clone(), meta.clone())
            .await
            .unwrap();
        assert_eq!(read_all(&mut eager, 64 * 1024).await, expected);

        let mut streaming = StorageReader::new_streaming(store, meta, DEFAULT_STREAM_WINDOW_SIZE);
        assert_eq!(read_all(&mut streaming, 64 * 1024).await, expected);
    }

    /// Seeking backwards has to refill, and a read that runs off the end has to
    /// fail rather than come back short. Both are what a block scan does when it
    /// walks back to a header or jumps past a block it is skipping.
    #[tokio::test]
    async fn test_streaming_seek_backwards_and_past_end() {
        let len = (DEFAULT_STREAM_WINDOW_SIZE as usize) + 1024;
        let (store, meta, expected, _dir) = make_store(len).await;
        let mut reader = StorageReader::new_streaming(store, meta, DEFAULT_STREAM_WINDOW_SIZE);

        // Force a window near the end, then walk back to the start.
        reader.seek_to(len as u64 - 512);
        let tail = reader.read_bytes(512).await.unwrap();
        assert_eq!(&tail[..], &expected[len - 512..]);

        reader.seek_to(0);
        let head = reader.read_bytes(512).await.unwrap();
        assert_eq!(&head[..], &expected[..512]);

        // At the end of the file, and past it, a read is refused rather than
        // short — a caller walking blocks reads the refusal as the end.
        reader.seek_to(len as u64);
        assert_eq!(
            reader.read_bytes(16).await.unwrap_err().kind(),
            ErrorKind::UnexpectedEof
        );
        reader.seek_to(len as u64 * 2);
        assert_eq!(
            reader.read_bytes(1).await.unwrap_err().kind(),
            ErrorKind::UnexpectedEof
        );
        // Nothing was consumed, so the cursor still says where the caller was.
        assert_eq!(reader.position(), len as u64 * 2);
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
    #[tokio::test]
    async fn test_configured_window_is_used_for_refills() {
        let window = 4096u64;
        let configs = HudiConfigs::new([(CONFIG_DFS_BUFFER_MAX_SIZE, window.to_string())]);
        assert_eq!(stream_window_size(&configs).unwrap(), window);

        let len = (window as usize) * 3 + 17;
        let (store, meta, expected, _dir) = make_store(len).await;
        let mut reader = StorageReader::new_streaming(store, meta, window);

        // Same bytes as an eager read, across several refills of the smaller
        // window.
        assert_eq!(read_all(&mut reader, 512).await, expected);
        assert!(
            reader.window.len() as u64 <= window,
            "a refill fetched {} bytes for a {window}-byte window",
            reader.window.len()
        );
    }

    /// A read longer than the window is served whole and without leaving that
    /// much resident. This is the case a log block's content hits: it can be
    /// larger than the window tuned for sweeping headers, and it has to be
    /// contiguous to decode.
    #[tokio::test]
    async fn test_read_longer_than_the_window_does_not_grow_it() {
        let window = 4096u64;
        let len = (window as usize) * 4;
        let (store, meta, expected, _dir) = make_store(len).await;
        let mut reader = StorageReader::new_streaming(store, meta, window);

        // Prime a window, so the oversized read below has one to leave alone.
        reader.read_bytes(16).await.unwrap();
        assert_eq!(reader.window.len() as u64, window);

        reader.seek_to(1000);
        let big = reader.read_bytes(window * 2).await.unwrap();
        assert_eq!(&big[..], &expected[1000..1000 + (window * 2) as usize]);
        assert_eq!(reader.position(), 1000 + window * 2);
        assert!(
            reader.window.len() as u64 <= window,
            "an oversized read must not grow the resident window, now {} bytes",
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
    #[tokio::test]
    async fn test_block_fetcher_reads_only_its_range() {
        let len = 8192;
        let (store, meta, expected, _dir) = make_store(len).await;
        let reader = StorageReader::new_streaming(store, meta, DEFAULT_STREAM_WINDOW_SIZE);

        let content = reader
            .block_fetcher()
            .read_content(1000, 256)
            .await
            .unwrap();
        assert_eq!(&content[..], &expected[1000..1256]);
    }
}
