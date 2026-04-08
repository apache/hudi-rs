
👤 USER:
for hudi commit time ordering, here are some pointers

org.apache.hudi.common.table.read.buffer.TestKeyBasedFileGroupRecordBuffer

  ---
  Part 1: Annotated Code Walkthrough

  The Pipeline

  FileSlice = BaseFile (parquet) + LogFile1 (commit1) + LogFile2 (commit2) + ...

  The processing has 3 phases: (A) scan log files into a map, (B) merge base records with the map, (C) emit remaining log-only records.

  ---
  **Phase A: Scan Log Blocks into records Map**

  Log blocks are processed in commit order (oldest → newest). For each block, the buffer calls into KeyBasedFileGroupRecordBuffer:
```java
  Data blocks — processDataBlock() (line 73):
  // For each record in the log data block:
  try (ClosableIterator<T> recordIterator = recordsIteratorSchemaPair.getLeft()) {
      while (recordIterator.hasNext()) {
          T nextRecord = recordIterator.next();
          boolean isDelete = recordContext.isDeleteRecord(nextRecord, deleteContext);
          // Wrap the engine record into a BufferedRecord (extracts key, ordering value, etc.)
          BufferedRecord<T> bufferedRecord = BufferedRecords.fromEngineRecord(
              nextRecord, schema, readerContext.getRecordContext(), orderingFieldNames, isDelete);
          // Merge into the map ↓
          processNextDataRecord(bufferedRecord, bufferedRecord.getRecordKey());
      }
  }

  processNextDataRecord() (line 105) — the core delta merge:
  public void processNextDataRecord(BufferedRecord<T> record, Serializable recordKey) {
      BufferedRecord<T> existingRecord = records.get(recordKey);  // Look up existing entry for this key
      totalLogRecords++;
      // ┌─────────────────────────────────────────────────────────────────┐
      // │ For COMMIT_TIME_ORDERING, deltaMerge always returns newRecord. │
      // │ The new record unconditionally overwrites the old one.         │
      // └─────────────────────────────────────────────────────────────────┘
      bufferedRecordMerger.deltaMerge(record, existingRecord)
          .ifPresent(bufferedRecord ->
              records.put(recordKey, bufferedRecord.toBinary(readerContext.getRecordContext())));
  }

  Delete blocks — processDeleteBlock() (line 113):
  public void processDeleteBlock(HoodieDeleteBlock deleteBlock) {
      Iterator<DeleteRecord> it = Arrays.stream(deleteBlock.getRecordsToDelete()).iterator();
      while (it.hasNext()) {
          DeleteRecord record = it.next();
          processNextDeletedRecord(record, record.getRecordKey());
      }
  }

  processNextDeletedRecord() (line 122):
  public void processNextDeletedRecord(DeleteRecord deleteRecord, Serializable recordIdentifier) {
      BufferedRecord<T> existingRecord = records.get(recordIdentifier);
      totalLogRecords++;
      // ┌──────────────────────────────────────────────────────────────────┐
      // │ For COMMIT_TIME_ORDERING, deltaMerge always returns the delete. │
      // │ The delete unconditionally overwrites any existing record.      │
      // └──────────────────────────────────────────────────────────────────┘
      Option<DeleteRecord> recordOpt = bufferedRecordMerger.deltaMerge(deleteRecord, existingRecord);
      recordOpt.ifPresent(deleteRec ->
          records.put(recordIdentifier, BufferedRecords.fromDeleteRecord(deleteRec, ...)));
  }

  The CommitTimeRecordMerger (line 105 of BufferedRecordMergerFactory.java):
  private static class CommitTimeRecordMerger<T> implements BufferedRecordMerger<T> {
      // Data vs data: always pick new
      public Option<BufferedRecord<T>> deltaMerge(BufferedRecord<T> newRecord, BufferedRecord<T> existingRecord) {
          return Option.of(newRecord);
      }
      // Delete vs data: always pick delete
      public Option<DeleteRecord> deltaMerge(DeleteRecord deleteRecord, BufferedRecord<T> existingRecord) {
          return Option.of(deleteRecord);
      }
      // Base vs log: always pick log (newer commit)
      public BufferedRecord<T> finalMerge(BufferedRecord<T> olderRecord, BufferedRecord<T> newerRecord) {
          return newerRecord;
      }
  }
```
  After Phase A, records is a Map<recordKey, BufferedRecord> holding the last-writer-wins version of each key that appeared in any log block.

  ---
**Phase B: Iterate Base File, Merge with Log Map**
```java
  doHasNext() in KeyBasedFileGroupRecordBuffer (line 142):
  protected boolean doHasNext() throws IOException {
      // Phase B: walk through every base file record
      while (baseFileIterator.hasNext()) {
          if (hasNextBaseRecord(baseFileIterator.next())) {
              return true;
          }
      }
      // Phase C: emit remaining log-only records (inserts from logs)
      return hasNextLogRecord();
  }

  hasNextBaseRecord(T baseRecord) (line 135):
  protected boolean hasNextBaseRecord(T baseRecord) {
      String recordKey = readerContext.getRecordContext().getRecordKey(baseRecord, readerSchema);
      // Remove from log map (so it won't be emitted again in Phase C)
      BufferedRecord<T> logRecordInfo = records.remove(recordKey);
      return hasNextBaseRecord(baseRecord, logRecordInfo);
  }

  hasNextBaseRecord(T baseRecord, BufferedRecord<T> logRecordInfo) in FileGroupRecordBuffer (line 238):
  protected boolean hasNextBaseRecord(T baseRecord, BufferedRecord<T> logRecordInfo) {
      if (logRecordInfo != null) {
          // ┌─────────────────────────────────────────────────────────────────────┐
          // │ This key exists in BOTH the base file and log map.                 │
          // │ finalMerge() picks the winner.                                     │
          // │ For COMMIT_TIME_ORDERING: always returns logRecordInfo (the newer) │
          // └─────────────────────────────────────────────────────────────────────┘
          BufferedRecord<T> baseRecordInfo = BufferedRecords.fromEngineRecord(
              baseRecord, readerSchema, readerContext.getRecordContext(), orderingFieldNames, false);
          BufferedRecord<T> mergeResult = bufferedRecordMerger.finalMerge(baseRecordInfo, logRecordInfo);

          // ┌──────────────────────────────────────────────────────────────────┐
          // │ processUpdate decides what to emit:                             │
          // │   - if mergeResult.isDelete() and !emitDeletes → return null    │
          // │   - if mergeResult.isDelete() and emitDeletes  → emit delete   │
          // │   - otherwise → emit the merged record as UPDATE_AFTER          │
          // └──────────────────────────────────────────────────────────────────┘
          nextRecord = updateProcessor.processUpdate(
              logRecordInfo.getRecordKey(), baseRecordInfo, mergeResult, mergeResult.isDelete());
          return nextRecord != null;
      }

      // No log entry for this key → emit the base file record as-is
      nextRecord = bufferedRecordConverter.convert(readerContext.getRecordContext().seal(baseRecord));
      return true;
  }
```
  ---
  **Phase C: Emit Log-Only Records (New Inserts from Logs)**

  hasNextLogRecord() in FileGroupRecordBuffer (line 255):
```java
  protected boolean hasNextLogRecord() {
      if (logRecordIterator == null) {
          initializeLogRecordIterator();  // iterates over remaining entries in records map
      }
      while (logRecordIterator.hasNext()) {
          BufferedRecord<T> nextRecordInfo = logRecordIterator.next();
          // ┌──────────────────────────────────────────────────────────────────┐
          // │ previousRecord = null (no base file match for this key)         │
          // │ For non-deletes: emitted as INSERT                              │
          // │ For deletes: skipped (unless emitDeletes=true)                  │
          // └──────────────────────────────────────────────────────────────────┘
          nextRecord = updateProcessor.processUpdate(
              nextRecordInfo.getRecordKey(), null, nextRecordInfo, nextRecordInfo.isDelete());
          if (nextRecord != null) {
              return true;
          }
      }
      return false;
  }
```

❯ which column is used for deciding commit time?                                                                                                                          

  Searched for 2 patterns (ctrl+o to expand)

⏺ The answer is: no column is compared. The ordering is implicit, not column-based.

  With COMMIT_TIME_ORDERING, the CommitTimeRecordMerger unconditionally returns the newer record — it never reads any field value. The "commit time ordering" comes from
  the order in which log blocks are processed, not from comparing a column.

  Here's the chain:

  1. Log blocks are read in commit order — the file group reader iterates log files/blocks sequentially from oldest to newest commit
  2. Each record overwrites the previous — deltaMerge always returns the new record:
  public Option<BufferedRecord<T>> deltaMerge(BufferedRecord<T> newRecord, BufferedRecord<T> existingRecord) {
      return Option.of(newRecord);  // no field comparison
  }
  3. Log wins over base file — finalMerge always returns the log record (which came from a later commit than the base file):
  public BufferedRecord<T> finalMerge(BufferedRecord<T> olderRecord, BufferedRecord<T> newerRecord) {
      return newerRecord;  // no field comparison
  }

  So the "commit time" is encoded in the processing order of log blocks, not in any metadata column like _hoodie_commit_time or _hoodie_commit_seqno. Those meta fields
  exist in the data but are not consulted by this merger.



my question is
during phase A, does it mean it mains a running map, the map content keeps being updated as it iterate records inside a log file and across log files?

I would like to see the full chain from reading 1 single records from the log file and it directly update the running map inline


🤖 ASSISTANT:
Yes, the map is a running accumulator — every record read from every log block immediately mutates it in-place. Let me trace the exact call chain.

🤖 ASSISTANT:
Now I have the full picture. Here is the complete call chain from one record in a log file to the map being updated:

---

## Full Chain: 1 Record → Map Update

**`scanInternal()` in `BaseHoodieLogRecordReader`** (line 192)

This runs in **two passes** before any record hits the map:

**Pass 1** (line 249–317): Forward-scan all log blocks across all log files. Builds:
- `orderedInstantsList` — instant times in arrival order (oldest → newest)
- `instantToBlocksMap` — maps each instant time to its list of blocks
- Any rollback targets are removed from both structures

**Pass 2** (line 336–375): Iterates `orderedInstantsList` in **reverse** (newest → oldest), adding blocks to `currentInstantLogBlocks` deque with `addLast`. The deque ends up with newest instants at the head, oldest at the tail.

**`processQueuedBlocksForInstant(currentInstantLogBlocks, ...)` at line 385** drains the deque using `pollLast()` — so it processes oldest instants first. For each block:

```
BaseHoodieLogRecordReader.processQueuedBlocksForInstant()  [line 428]
  └─ recordBuffer.processDataBlock(dataBlock, keySpecOpt)  [line 437]
```

**`KeyBasedFileGroupRecordBuffer.processDataBlock()` at line 73**: opens an iterator over records inside that one block. For each record in the iterator:

```java
T nextRecord = recordIterator.next();                         // read raw engine record
boolean isDelete = recordContext.isDeleteRecord(...);
BufferedRecord<T> bufferedRecord = BufferedRecords.fromEngineRecord(
    nextRecord, schema, recordContext, orderingFieldNames, isDelete);   // wrap into BufferedRecord
processNextDataRecord(bufferedRecord, bufferedRecord.getRecordKey());   // hit the map
```

**`processNextDataRecord()` at line 105** — the map mutation:

```java
BufferedRecord<T> existingRecord = records.get(recordKey);   // look up current map entry
totalLogRecords++;
bufferedRecordMerger.deltaMerge(record, existingRecord)      // for COMMIT_TIME: always returns record
    .ifPresent(r -> records.put(recordKey, r.toBinary(...)));  // ← inline map update
```

---

## The Critical Insight on Ordering

The deque is drained with `pollLast()`, and the deque was filled in reverse-instant order with `addLast`. That means:

- **i1 (oldest)** is at the tail → polled first → written to map first
- **i2** overwrites i1's entries
- **i3 (newest)** overwrites i2's entries → final map state

This is how commit-time ordering works without comparing any field: the **insertion order into the map is commit order**, and `deltaMerge` unconditionally returns the new record, so the last write (newest commit) survives.

---

## Answer to Your Question

Yes, `records` is a single running map that is mutated **inline, record by record, block by block, across all log files**. There is no intermediate buffering between reading a record and writing it to the map. The commit ordering is baked into the processing order, not into any per-record timestamp comparison.


👤 USER:
where is the start of the scan of log files, and base files. show me code and generate call stack tree

🤖 ASSISTANT:
Now let me find the `FileGroupRecordBufferLoader` default implementation — that's where `scanInternal` gets triggered:


🤖 ASSISTANT:
Now I have the complete picture. Here is the full call stack tree:

---

## Call Stack Tree: Log File Scan + Base File Scan

### Entry Point — Engine calls `HoodieFileGroupReader`

```
HoodieFileGroupReader.getClosableIterator()          [HoodieFileGroupReader.java:301]
  └─ getBufferedRecordIterator(IteratorMode)          [line 291]
       └─ initRecordIterators()                       [line 292]
            │
            ├─── makeBaseFileIterator()               [line 129]  ← BASE FILE SCAN (iterator created here)
            │      └─ readerContext.getFileRecordIterator(...)     [line 157/163]
            │           (returns a lazy Parquet/HFile iterator — not yet consumed)
            │
            └─── recordBufferLoader.getRecordBuffer(...) [line 134]  ← LOG FILE SCAN (eagerly consumed here)
                  │
                  └─ DefaultFileGroupRecordBufferLoader.getRecordBuffer()   [DefaultFileGroupRecordBufferLoader.java:54]
                        ├─ new KeyBasedFileGroupRecordBuffer(...)            [line 78]
                        └─ scanLogFiles(readerContext, storage, inputSplit, ..., recordBuffer)  [line 81]
                             │
                             └─ LogScanningRecordBufferLoader.scanLogFiles() [LogScanningRecordBufferLoader.java:37]
                                  └─ HoodieMergedLogRecordReader.newBuilder()...build()  [line 41-52]
                                       └─ HoodieMergedLogRecordReader constructor        [HoodieMergedLogRecordReader.java:66]
                                            └─ performScan()                             [line 75]  (forceFullScan=true)
                                                 └─ BaseHoodieLogRecordReader.scanInternal()  [BaseHoodieLogRecordReader.java:192]
                                                      │
                                                      ├─ [Pass 1] HoodieLogFormatReader.next() × N blocks  [line 249-317]
                                                      │     (reads block headers, builds instantToBlocksMap, strips rollbacks)
                                                      │
                                                      ├─ [Pass 2] orderedInstantsList reverse iteration   [line 336-375]
                                                      │     (resolves compaction, builds currentInstantLogBlocks deque)
                                                      │
                                                      └─ processQueuedBlocksForInstant(deque)             [line 385]
                                                           └─ [for each block, oldest→newest via pollLast]
                                                                ├─ recordBuffer.processDataBlock(dataBlock, keySpecOpt)   [line 437]
                                                                │    └─ KeyBasedFileGroupRecordBuffer.processDataBlock()  [line 73]
                                                                │         └─ [for each record in block]
                                                                │              BufferedRecords.fromEngineRecord(...)
                                                                │              └─ processNextDataRecord(bufferedRecord, key)  [line 105]
                                                                │                   └─ records.put(key, deltaMerge(new, existing))  ← MAP UPDATED
                                                                │
                                                                └─ recordBuffer.processDeleteBlock(deleteBlock)           [line 440]
                                                                     └─ KeyBasedFileGroupRecordBuffer.processDeleteBlock() [line 113]
                                                                          └─ processNextDeletedRecord(deleteRecord, key)   [line 122]
                                                                               └─ records.put(key, fromDeleteRecord(...))  ← MAP UPDATED
```

After `initRecordIterators()` returns, `records` map is fully populated. The base file iterator is set on the buffer but **not yet consumed**.

---

### Consumer Loop — Engine iterates `HoodieFileGroupReaderIterator`

```
HoodieFileGroupReaderIterator.hasNext()              [HoodieFileGroupReader.java:332]
  └─ HoodieFileGroupReader.hasNext()                 [line 243]
       └─ recordBuffer.hasNext()                     [line 247]
            └─ KeyBasedFileGroupRecordBuffer.doHasNext()           [line 142]
                 │
                 ├─ [Phase B] while baseFileIterator.hasNext()     [line 146]
                 │     └─ hasNextBaseRecord(baseFileIterator.next())  [line 147]  ← BASE FILE CONSUMED HERE
                 │          └─ records.remove(recordKey)               (lookup + evict from map)
                 │          └─ finalMerge(baseRecord, logRecord)       (log wins for COMMIT_TIME)
                 │          └─ updateProcessor.processUpdate(...)
                 │
                 └─ [Phase C] hasNextLogRecord()                   [line 153]
                       (drains remaining map entries = log-only inserts)
```

---

### Key Timing Distinction

| What | When |
|---|---|
| Log file scan (`scanInternal`) | **Eagerly, upfront** — all log blocks fully consumed into `records` map before the caller gets back an iterator |
| Base file scan (`baseFileIterator`) | **Lazily, on demand** — Parquet iterator created eagerly but records only read as consumer calls `hasNext()`/`next()` |


👤 USER:
zoom into the phase A where we read and merge all log files. when is org.apache.hudi.common.table.log.block.HoodieLogBlock#content populated? does it hold all log file content in memory?


🤖 ASSISTANT:
Now I have the complete picture. Here is the full answer:

---

## When is `content` populated?

There are **two paths** depending on the block version:

### Path A — Old blocks (`DEFAULT_VERSION`)
`HoodieLogFileReader.readBlock()` line 167–168:
```java
boolean shouldReadLazily = nextBlockVersion.getVersion() != HoodieLogFormatVersion.DEFAULT_VERSION;
// → for DEFAULT_VERSION: shouldReadLazily = false
Option<byte[]> content = HoodieLogBlock.tryReadContent(inputStream, contentLength, shouldReadLazily);
```
`tryReadContent` (`HoodieLogBlock.java:332`):
```java
if (readLazily) { inputStream.seek(...); return Option.empty(); }  // ← skip
byte[] content = new byte[contentLength];
inputStream.readFully(content, 0, contentLength);                  // ← OLD PATH: eagerly read here
return Option.of(content);
```
Content bytes land in `content` during Pass 1 of `scanInternal`, inline while scanning.

### Path B — Modern blocks (version > DEFAULT) — the normal case
`shouldReadLazily = true` → `tryReadContent` just **seeks past the bytes** and returns `Option.empty()`. The block object is constructed with:
- `content = Option.empty()`
- `inputStreamSupplier` = lambda that re-opens the file
- `blockContentLocation` = {file path, contentPositionInLogFile, blockSize, blockEndPos}

Content bytes are loaded later in `HoodieDataBlock.readRecordsFromBlockPayload()` (`line 272`):
```java
if (readBlockLazily && !getContent().isPresent()) {
    inflate();   // ← content loaded HERE, on demand when getRecordIterator() is called
}
try {
    return deserializeRecords(getContent().get(), type);
} finally {
    deflate();   // ← content bytes released immediately after deserialization
}
```
`inflate()` (`HoodieLogBlock.java:401`) seeks back into the file and reads the block:
```java
content = Option.of(new byte[(int) this.getBlockContentLocation().get().getBlockSize()]);
inputStream.seek(this.getBlockContentLocation().get().getContentPositionInLogFile());
inputStream.readFully(content.get(), 0, content.get().length);
```

---

## Does it hold all log file content in memory?

**No.** The design is explicitly block-by-block. Here is what lives in memory at each phase:

| Phase | What's in memory |
|---|---|
| Pass 1 (`logFormatReaderWrapper.next()` loop) | Block metadata only (headers/footers + `blockContentLocation`). Content bytes skipped for modern blocks. All block objects from all log files accumulate in `instantToBlocksMap`. |
| Pass 2 (building deque) | Same block objects, just reordered. No new allocations. |
| `processQueuedBlocksForInstant` — for each block | `inflate()` reads **one block's** `byte[]` into heap → `deserializeRecords` parses it → `deflate()` drops the `byte[]` reference → GC can reclaim it → next block |
| Growing throughout Phase A | The `records` map accumulates merged `BufferedRecord` entries (one per unique key seen so far) |

So at peak within Phase A, memory holds:
1. All lightweight block header/metadata objects (small)
2. **One block's raw `byte[]`** (can be large — a single data block can be 100s of MB)
3. The `records` map built so far

The `deflate()` call immediately after `deserializeRecords` is the key: it drops the `byte[]` so the GC can reclaim the space before the next block is inflated. The design deliberately avoids holding more than one block's worth of raw bytes at a time.

---

## Call stack for content loading (modern blocks)

```
processQueuedBlocksForInstant(deque)              [BaseHoodieLogRecordReader.java:428]
  └─ recordBuffer.processDataBlock(dataBlock)      [line 437]
       └─ KeyBasedFileGroupRecordBuffer.processDataBlock()  [line 73]
            └─ getRecordsIterator(dataBlock, keySpecOpt)    [line 74]
                 └─ HoodieDataBlock.getRecordIterator(type) [HoodieDataBlock.java:157]
                      └─ readRecordsFromBlockPayload(type)  [line 272]
                           ├─ inflate()     ← content byte[] allocated + readFully() from disk
                           ├─ deserializeRecords(content.get(), type)  ← parse records from bytes
                           └─ deflate()     ← content = Option.empty(), byte[] eligible for GC
```


👤 USER:
for table version 9 what's the condition for that org.apache.hudi.common.table.log.block.HoodieLogBlock#content population?


🤖 ASSISTANT:
Now I have everything. Here is the precise answer:

---

## `content` population for table version 9

### Step 1 — Pass 1 (`readBlock()`): content is **never** populated

The decision at `HoodieLogFileReader.readBlock()` line 167 is driven by the **log format version**, not the table version:

```java
boolean shouldReadLazily = nextBlockVersion.getVersion() != HoodieLogFormatVersion.DEFAULT_VERSION;
//                          log format version 1           !=  0  (DEFAULT)
//                          → true for ALL blocks written by table version 9
Option<byte[]> content = HoodieLogBlock.tryReadContent(inputStream, contentLength, shouldReadLazily);
// → seeks past the bytes, returns Option.empty()
```

`HoodieLogFormat.CURRENT_VERSION = 1` and `DEFAULT_VERSION = 0`. Table version 9 always writes blocks at format version 1 → `shouldReadLazily = true` → content bytes are **skipped** in the stream. Every block object enters `instantToBlocksMap` with `content = Option.empty()`.

---

### Step 2 — Pass 1 filter: which blocks survive to `processQueuedBlocksForInstant`

This is where table version 9 **diverges** from versions < 8. The filter in `scanInternal` at line 269:

```java
if (logBlock.getBlockType() != COMMAND_BLOCK) {
    if (this.tableVersion.lesserThan(HoodieTableVersion.EIGHT) && !allowInflightInstants) {
        // ← for version 9, this entire block is SKIPPED
        HoodieTimeline commitsTimeline = ...
        if (commitsTimeline.filterInflights().containsInstant(instantTime)
            || !commitsTimeline.filterCompletedInstants()...) {
            continue;  // inflight blocks filtered OUT here (only for versions < 8)
        }
    }
}
```

For version 9 (`!lesserThan(EIGHT)` → `true`), the inflight/uncommitted check is **never executed**. The only filters that apply are:

| Filter | Applies to version 9? |
|---|---|
| `CORRUPT_BLOCK` → skip | Yes |
| `instantTime > latestInstantTime` → skip | Yes |
| Rollback removal (via `COMMAND_BLOCK`) | Yes |
| `instantRange` filter | Yes (if set) |
| Inflight/uncommitted timeline check | **No — bypassed entirely** |

---

### Step 3 — `inflate()`: content **is** populated here

For each block that survived the filters and was dequeued in `processQueuedBlocksForInstant`:

```
processQueuedBlocksForInstant → recordBuffer.processDataBlock(block)
  → KeyBasedFileGroupRecordBuffer.getRecordsIterator(dataBlock)
      → dataBlock.getEngineRecordIterator(readerContext)       [HoodieDataBlock.java:241]
           → readRecordsFromBlockPayload(readerContext)         [line 306]
                if (readBlockLazily && !getContent().isPresent())  // true for version 9 blocks
                    inflate()   ← content = new byte[blockSize]; readFully(...)
                deserializeRecords(readerContext, getContent().get())
                deflate()       ← content = Option.empty(), byte[] eligible for GC
```

`inflate()` (`HoodieLogBlock.java:401`) opens a **new stream** via `inputStreamSupplier`, seeks to `contentPositionInLogFile`, and reads exactly `blockSize` bytes:

```java
content = Option.of(new byte[(int) this.getBlockContentLocation().get().getBlockSize()]);
inputStream.seek(this.getBlockContentLocation().get().getContentPositionInLogFile());
inputStream.readFully(content.get(), 0, content.get().length);
```

---

### Summary: what's unique to version 9

```
Pass 1 loop: block arrives from log file
    │
    ├─ instantTime > latestInstantTime?  → skip (drop, content never populated)
    ├─ CORRUPT_BLOCK?                   → skip
    ├─ COMMAND_BLOCK rollback target?   → removed from map
    ├─ outside instantRange?            → skip
    │
    │  ← inflight timeline check is NOT applied (version 9 skips it)
    │
    └─ passes all filters → added to instantToBlocksMap with content = Option.empty()

processQueuedBlocksForInstant:
    └─ inflate() → content byte[] allocated and filled ← only moment content is populated
    └─ deserializeRecords()
    └─ deflate() → content dropped
```

**Bottom line:** for version 9, `content` is populated exactly once per block — inside `inflate()` during `processQueuedBlocksForInstant` — and only for blocks that are non-corrupt, non-rolled-back, within `latestInstantTime`, and within `instantRange`. Inflight blocks are **not** pre-filtered (unlike versions < 8), so they too will have `inflate()` called and their content loaded into memory.

