
👤 USER:
introduce me the file group for table version 9. I only care parquet as base file avro as log file.
- log file types, what do they stand for, what are the special properties each one contains, also what kind of information can be extracted from the file - schema?commit time?data?
- log file name and base file name what are the information they contain.
- how do they relate to notions of file group and file slice
- would a file slice contain both committed and uncommited code change? If so, how they are differentiated.
- for committed changes within a file slice, how do we know which changes comes from which instant time / completion time
please read relevant tests, file group reader code and other places extensively to give a comprehensive intro covering these questions. I expect raw code pieces with annotation to serve as support of every claim you make.


🤖 ASSISTANT:
  🤖 Agent: Explore log file types and properties — I need a comprehensive understanding of Hudi log file types for table version 9 (Parquet base, Avro log). Research these questions thoroughly:

1. What are the different log block types? Find the enum


🤖 ASSISTANT:
  🤖 Agent: Explore file naming conventions — I need to understand Hudi file naming conventions for table version 9 (TV9). Research:

1. Base file (Parquet) naming - what information is encoded in the file name?
2. Log file (Avro) naming - what i


🤖 ASSISTANT:
  🤖 Agent: Explore file group and file slice — I need to understand the relationship between file groups, file slices, base files, and log files in Hudi table version 9. Research:

1. What is a HoodieFileGroup? What does it contain?
2. What is a F


🤖 ASSISTANT:
  🤖 Agent: Explore file group reader and tests — I need to understand how Hudi reads file groups in table version 9, specifically the record buffer and merge logic. Research:

1. How does the file group reader work? What's the reading flow?
2. How a


🤖 ASSISTANT:
Now let me read the key source files to get precise code snippets for the response.


🤖 ASSISTANT:
  📖 Read: /Users/zhanyeha/hudi-oss/hudi-common/src/main/java/org/apache/hudi/common/table/log/block/HoodieLogBlock.java


🤖 ASSISTANT:
  📖 Read: /Users/zhanyeha/hudi-oss/hudi-common/src/main/java/org/apache/hudi/common/model/HoodieFileGroup.java


🤖 ASSISTANT:
  📖 Read: /Users/zhanyeha/hudi-oss/hudi-common/src/main/java/org/apache/hudi/common/model/FileSlice.java


🤖 ASSISTANT:
  📖 Read: /Users/zhanyeha/hudi-oss/hudi-common/src/main/java/org/apache/hudi/common/table/log/block/HoodieLogBlock.java


🤖 ASSISTANT:
  📖 Read: /Users/zhanyeha/hudi-oss/hudi-common/src/main/java/org/apache/hudi/common/fs/FSUtils.java


🤖 ASSISTANT:
  📖 Read: /Users/zhanyeha/hudi-oss/hudi-common/src/main/java/org/apache/hudi/common/table/log/AbstractHoodieLogRecordScanner.java


🤖 ASSISTANT:
  📖 Read: /Users/zhanyeha/hudi-oss/hudi-common/src/main/java/org/apache/hudi/common/table/log/AbstractHoodieLogRecordScanner.java


🤖 ASSISTANT:
  📖 Read: /Users/zhanyeha/hudi-oss/hudi-common/src/main/java/org/apache/hudi/common/table/read/buffer/KeyBasedFileGroupRecordBuffer.java


🤖 ASSISTANT:
  📖 Read: /Users/zhanyeha/hudi-oss/hudi-common/src/main/java/org/apache/hudi/common/table/read/buffer/KeyBasedFileGroupRecordBuffer.java


🤖 ASSISTANT:
  📖 Read: /Users/zhanyeha/hudi-oss/hudi-common/src/main/java/org/apache/hudi/common/table/read/buffer/FileGroupRecordBuffer.java


🤖 ASSISTANT:
  📖 Read: /Users/zhanyeha/hudi-oss/hudi-common/src/main/java/org/apache/hudi/common/table/read/buffer/FileGroupRecordBuffer.java


🤖 ASSISTANT:
Now let me read the test file that the user has modified, and check the log file scanner's handling of COMMAND_BLOCK for rollback.


🤖 ASSISTANT:
  📖 Read: /Users/zhanyeha/hudi-oss/hudi-common/src/test/java/org/apache/hudi/common/table/read/buffer/TestKeyBasedFileGroupRecordBuffer.java


🤖 ASSISTANT:
  📖 Read: /Users/zhanyeha/hudi-oss/hudi-common/src/main/java/org/apache/hudi/common/model/HoodieLogFile.java


🤖 ASSISTANT:
Now I have all the information I need. Let me compose the comprehensive answer.

---

# File Groups in Hudi Table Version 9 (Parquet Base + Avro Log)

## 1. Log Block Types

Defined in `HoodieLogBlock.java:187-219`:

```java
public enum HoodieLogBlockType {
    COMMAND_BLOCK(":command", HoodieTableVersion.ONE),
    DELETE_BLOCK(":delete", HoodieTableVersion.ONE),
    CORRUPT_BLOCK(":corrupted", HoodieTableVersion.ONE),
    AVRO_DATA_BLOCK("avro", HoodieTableVersion.ONE),
    HFILE_DATA_BLOCK("hfile", HoodieTableVersion.ONE),
    PARQUET_DATA_BLOCK("parquet", HoodieTableVersion.FOUR),
    CDC_DATA_BLOCK("cdc", HoodieTableVersion.SIX);
}
```

For TV9 with Parquet base + Avro log, the relevant block types are:

| Block Type | What It Stores | Key Properties |
|---|---|---|
| **AVRO_DATA_BLOCK** | Upsert data records (binary Avro) | Schema in header, record positions bitmap, ordering values per record |
| **DELETE_BLOCK** | Array of `DeleteRecord` (key + ordering value) | Uses `HoodieDeleteRecordList` Avro format (v3) |
| **COMMAND_BLOCK** | No data content — metadata-only | `COMMAND_BLOCK_TYPE` header (currently only `ROLLBACK_BLOCK`), plus `TARGET_INSTANT_TIME` to identify which instant to rollback |
| **CORRUPT_BLOCK** | Raw corrupted bytes | Created when block size mismatch is detected during scanning |
| **CDC_DATA_BLOCK** | Change-data-capture records | Extends `HoodieAvroDataBlock`, appended to `.cdc` suffixed log files |

### Header Metadata Each Block Can Carry

Defined in `HoodieLogBlock.java:225-242`:

```java
public enum HeaderMetadataType {
    INSTANT_TIME(HoodieTableVersion.ONE),            // commit instant that wrote this block
    TARGET_INSTANT_TIME(HoodieTableVersion.ONE),     // for COMMAND_BLOCK: the instant being rolled back
    SCHEMA(HoodieTableVersion.ONE),                  // writer Avro schema JSON string
    COMMAND_BLOCK_TYPE(HoodieTableVersion.ONE),      // ordinal of HoodieCommandBlockTypeEnum
    COMPACTED_BLOCK_TIMES(HoodieTableVersion.FIVE),  // comma-separated instants merged by log-compaction
    RECORD_POSITIONS(HoodieTableVersion.SIX),        // Base64-encoded Roaring64NavigableMap bitmap
    BLOCK_IDENTIFIER(HoodieTableVersion.SIX),        // unique block ID
    IS_PARTIAL(HoodieTableVersion.EIGHT),            // "true" if partial update block
    BASE_FILE_INSTANT_TIME_OF_RECORD_POSITIONS(HoodieTableVersion.EIGHT); // base file instant for positions
}
```

**What can be extracted from a log file:**
- **Schema** — via `SCHEMA` header (writer Avro schema JSON string)
- **Commit time** — via `INSTANT_TIME` header (the instant that wrote the block)
- **Data records** — via `HoodieDataBlock.getEngineRecordIterator()`
- **Delete keys** — via `HoodieDeleteBlock.getRecordsToDelete()`
- **Record positions** — via `getRecordPositions()` (Roaring bitmap of positions in the base file this block updates)

Evidence from `HoodieLogBlock.java:143-148`:
```java
public Roaring64NavigableMap getRecordPositions() throws IOException {
    if (!logBlockHeader.containsKey(HeaderMetadataType.RECORD_POSITIONS)) {
        return new Roaring64NavigableMap();
    }
    return LogReaderUtils.decodeRecordPositionsHeader(logBlockHeader.get(HeaderMetadataType.RECORD_POSITIONS));
}
```

---

## 2. File Name Anatomy

### Base File (Parquet)

Format from `FSUtils.java:120-121`:
```java
public static String makeBaseFileName(String instantTime, String writeToken, String fileId, String fileExtension) {
    return String.format("%s_%s_%s%s", fileId, writeToken, instantTime, fileExtension);
}
```

Pattern: **`{fileId}_{writeToken}_{instantTime}.parquet`**

Example: `136281f3-c24e-423b-a65a-95dbfbddce1d_1-0-1_20240101120000.parquet`

- **fileId**: UUID identifying the file group
- **writeToken**: `{taskPartitionId}-{stageId}-{taskAttemptId}` (from `FSUtils.java:116-117`)
- **instantTime**: the commit timestamp when this base file was created

Extraction via `FSUtils.java:133-142`:
```java
public static String getCommitTime(String fullFileName) {
    if (isLogFile(fullFileName)) {
        return fullFileName.split("_")[1].split("\\.", 2)[0];  // 2nd segment for logs
    }
    return fullFileName.split("_")[2].split("\\.", 2)[0];      // 3rd segment for base files
}
```

### Log File (Avro)

Pattern from `FSUtils.java:81-82`:
```java
public static final Pattern LOG_FILE_PATTERN =
    Pattern.compile("^\\.([^._]+)_([^.]*)\\.(log|archive)\\.(\\d+)(_((\\d+)-(\\d+)-(\\d+))(\\.cdc)?)?$");
```

Format: **`.{fileId}_{deltaCommitTime}.log.{logVersion}_{writeToken}`**

Example: `.136281f3-c24e-423b-a65a-95dbfbddce1d_20240101130000.log.1_1-0-1`

Parsed in `HoodieLogFile.java:111-122`:
```java
private void parseFieldsFromPath() {
    Matcher matcher = LOG_FILE_PATTERN.matcher(getPath().getName());
    this.fileId = matcher.group(1);            // same fileId as its base file
    this.deltaCommitTime = matcher.group(2);   // instant that wrote this log
    this.fileExtension = matcher.group(3);     // "log" or "archive"
    this.logVersion = Integer.parseInt(matcher.group(4)); // version number (monotonically increasing)
    this.logWriteToken = matcher.group(6);     // writeToken for disambiguation
    this.suffix = matcher.group(10);           // ".cdc" for CDC log files, else empty
}
```

Key difference: log files start with `.` prefix and encode `deltaCommitTime` as the 2nd segment (not 3rd like base files).

---

## 3. File Group and File Slice Relationship

### HoodieFileGroup

From `HoodieFileGroup.java:42-47`:
```java
/**
 * A set of data/base files + set of log files, that make up a unit for all operations.
 */
public class HoodieFileGroup implements Serializable {
    private final HoodieFileGroupId fileGroupId;  // (partitionPath, fileId)
    private final TreeMap<String, FileSlice> fileSlices;  // keyed by baseInstantTime, reverse-sorted
    private final HoodieTimeline timeline;
    private final Option<HoodieInstant> lastInstant;  // high watermark
}
```

A **file group** is identified by `(partitionPath, fileId)`. It's a container of **file slices** ordered by time.

### FileSlice

From `FileSlice.java:37-43`:
```java
/**
 * Within a file group, a slice is a combination of data file written at a commit time
 * and list of log files, containing changes to the data file from that commit time.
 */
public class FileSlice implements Serializable {
    private final HoodieFileGroupId fileGroupId;
    private final String baseInstantTime;          // the instant that created this slice
    private HoodieBaseFile baseFile;               // 0 or 1 parquet file
    private final TreeSet<HoodieLogFile> logFiles; // 0..N log files, reverse-sorted by version
}
```

### How they relate — visually:

```
FileGroup (fileId = "abc-123", partition = "2024/01/01")
  │
  ├── FileSlice (baseInstantTime = "t3")   ← latest after compaction
  │     ├── baseFile: abc-123_1-0-1_t3.parquet
  │     ├── logFile: .abc-123_t4.log.1_1-0-1
  │     └── logFile: .abc-123_t5.log.2_1-0-1
  │
  └── FileSlice (baseInstantTime = "t1")   ← older (kept for time-travel)
        └── baseFile: abc-123_1-0-1_t1.parquet
```

**How a log file is assigned to a file slice** — `HoodieFileGroup.java:129-148`:

```java
public String getBaseInstantTime(CompletionTimeQueryView completionTimeQueryView, HoodieLogFile logFile) {
    if (fileSlices.isEmpty()) {
        return logFile.getDeltaCommitTime();  // no base file yet
    }
    Option<String> completionTimeOpt = completionTimeQueryView
        .getCompletionTime(fileSlices.firstKey(), logFile.getDeltaCommitTime());
    if (completionTimeOpt.isPresent()) {
        // find the largest base instant time <= log's completion time
        for (String commitTime : fileSlices.keySet()) {
            if (compareTimestamps(completionTimeOpt.get(), GREATER_THAN_OR_EQUALS, commitTime)) {
                return commitTime;
            }
        }
        return logFile.getDeltaCommitTime();
    }
    // pending log file → belongs to latest file slice
    return fileSlices.firstKey();
}
```

The `CompletionTimeQueryView` resolves the log file's `deltaCommitTime` to an actual completion time, then finds the most recent base file slice that was created before that completion time.

---

## 4. Committed vs Uncommitted Changes Within a File Slice

### At the file-slice level

A file slice is considered committed based on `HoodieFileGroup.java:158-164`:

```java
private boolean isFileSliceCommitted(FileSlice slice) {
    if (!compareTimestamps(slice.getBaseInstantTime(), LESSER_THAN_OR_EQUALS,
                           lastInstant.get().requestedTime())) {
        return false;
    }
    return timeline.containsOrBeforeTimelineStarts(slice.getBaseInstantTime());
}
```

- `getAllFileSlices()` returns **only committed** slices (line 183-188)
- `getAllFileSlicesIncludingInflight()` returns **all** slices including pending ones (line 169-171)

### At the log-block level (within a file slice)

A single file slice can contain log files from both committed and inflight instants. The **log record scanner** filters them during read.

From `AbstractHoodieLogRecordScanner.java:309-333`:

```java
final String instantTime = logBlock.getLogBlockHeader().get(INSTANT_TIME);

// Skip blocks from future instants (beyond the reader's high watermark)
if (logBlock.isDataOrDeleteBlock()
    && compareTimestamps(logBlock.getLogBlockHeader().get(INSTANT_TIME),
                         GREATER_THAN, this.latestInstantTime)) {
    continue;
}

// For table version < 8: explicitly filter uncommitted blocks
if (this.tableVersion.lesserThan(HoodieTableVersion.EIGHT) && !allowInflightInstants) {
    if (!getOrCreateCompletedInstantsTimeline().containsOrBeforeTimelineStarts(instantTime)
        || getOrCreateInflightInstantsTimeline().containsInstant(instantTime)) {
        // hit an uncommitted block from a failed write — skip it
        continue;
    }
}

// Optional instant range filter
if (instantRange.isPresent() && !instantRange.get().isInRange(instantTime)) {
    continue;
}
```

For **TV9 (>= EIGHT)**: the `tableVersion.lesserThan(HoodieTableVersion.EIGHT)` check is `false`, so uncommitted blocks are **not** filtered by this path. Instead, the pending log file is simply assigned to the latest file slice by `getBaseInstantTime()` returning `fileSlices.firstKey()` (the "pending" branch at line 147).

### Rollback of uncommitted changes

Failed writes are cleaned up via `COMMAND_BLOCK` with `ROLLBACK_BLOCK` type. From `AbstractHoodieLogRecordScanner.java:349-363`:

```java
case COMMAND_BLOCK:
    HoodieCommandBlock commandBlock = (HoodieCommandBlock) logBlock;
    if (commandBlock.getType().equals(HoodieCommandBlockTypeEnum.ROLLBACK_BLOCK)) {
        String targetInstantForCommandBlock =
            logBlock.getLogBlockHeader().get(TARGET_INSTANT_TIME);
        targetRollbackInstants.add(targetInstantForCommandBlock);
        orderedInstantsList.remove(targetInstantForCommandBlock);
        instantToBlocksMap.remove(targetInstantForCommandBlock);
    }
```

So a rollback block with `TARGET_INSTANT_TIME = "t5"` causes all data/delete blocks from instant "t5" to be removed from the processing queue.

---

## 5. Tracking Which Instant Each Change Comes From

### Blocks are grouped by instant time

The scanner groups blocks into `instantToBlocksMap` keyed by their `INSTANT_TIME` header (`AbstractHoodieLogRecordScanner.java:336-347`):

```java
case AVRO_DATA_BLOCK:
case PARQUET_DATA_BLOCK:
case DELETE_BLOCK:
    List<HoodieLogBlock> logBlocksList =
        instantToBlocksMap.getOrDefault(instantTime, new ArrayList<>());
    if (logBlocksList.isEmpty()) {
        orderedInstantsList.add(instantTime);  // track arrival order
    }
    logBlocksList.add(logBlock);
    instantToBlocksMap.put(instantTime, logBlocksList);
```

### Log-compaction tracking

When a log-compacted block is encountered, the `COMPACTED_BLOCK_TIMES` header tells us which original instants were merged into it. From `AbstractHoodieLogRecordScanner.java:396-425`:

```java
if (firstBlock.getLogBlockHeader().containsKey(COMPACTED_BLOCK_TIMES)) {
    // Map each original instant to the compacted block's instant
    Arrays.stream(firstBlock.getLogBlockHeader().get(COMPACTED_BLOCK_TIMES).split(","))
        .forEach(originalInstant -> {
            String finalInstant = blockTimeToCompactionBlockTimeMap
                .getOrDefault(instantTime, instantTime);
            blockTimeToCompactionBlockTimeMap.put(originalInstant, finalInstant);
        });
} else {
    String compactedFinalInstantTime = blockTimeToCompactionBlockTimeMap.get(instantTime);
    if (compactedFinalInstantTime == null) {
        // Not compacted: add blocks directly to processing queue
        logBlocks.forEach(block -> currentInstantLogBlocks.addLast(block));
        validBlockInstants.add(instantTime);  // ← tracked for later reference
    }
}
```

### At the merge level — instant time is NOT used for merging

Within the record buffer, merging is driven by **ordering values** (e.g., `ts` field), **not** instant times. The instant time decides *which blocks are read*; the ordering value decides *which record wins*.

Evidence from `KeyBasedFileGroupRecordBuffer.java:105-109`:

```java
public void processNextDataRecord(BufferedRecord<T> record, Serializable recordKey) throws IOException {
    BufferedRecord<T> existingRecord = records.get(recordKey);
    // deltaMerge uses ordering values, not instant times
    bufferedRecordMerger.deltaMerge(record, existingRecord)
        .ifPresent(bufferedRecord -> records.put(recordKey, bufferedRecord.toBinary(...)));
}
```

And for the final base-vs-log merge (`FileGroupRecordBuffer.java:238-249`):

```java
protected boolean hasNextBaseRecord(T baseRecord, BufferedRecord<T> logRecordInfo) throws IOException {
    if (logRecordInfo != null) {
        BufferedRecord<T> baseRecordInfo = BufferedRecords.fromEngineRecord(baseRecord, ...);
        BufferedRecord<T> mergeResult = bufferedRecordMerger.finalMerge(baseRecordInfo, logRecordInfo);
        nextRecord = updateProcessor.processUpdate(...);
        return nextRecord != null;
    }
    // No log record for this key — return base record as-is
    nextRecord = bufferedRecordConverter.convert(readerContext.getRecordContext().seal(baseRecord));
    return true;
}
```

The three-phase output from `doHasNext()` (`KeyBasedFileGroupRecordBuffer.java:142-154`):

```java
protected boolean doHasNext() throws IOException {
    // Phase A+B: iterate base file, merge with log map matches (updates) or return as-is
    while (baseFileIterator.hasNext()) {
        if (hasNextBaseRecord(baseFileIterator.next())) {
            return true;
        }
    }
    // Phase C: remaining log-only records (inserts from log)
    return hasNextLogRecord();
}
```

### Concrete example from the test

From `TestKeyBasedFileGroupRecordBuffer.java:78-109`:

```java
// Base file:               Log block (single block, records processed in order):
//   (rk=1, counter=1, ts=1)    (rk=1, counter=2, ts=1) ← ts equal → stored
//   (rk=2, counter=1, ts=1)    (rk=2, counter=1, ts=2) ← ts=2 > none → stored
//   (rk=3, counter=1, ts=1)    (rk=2, counter=1, ts=0) ← ts=0 < ts=2 → REJECTED
//                               (rk=3, counter=1, ts=2) ← ts=2 > none → stored
//                               (rk=3, counter=3, ts=1) ← DELETE ts=1 < ts=2 → REJECTED
```

The test shows how in EVENT_TIME_ORDERING mode, the ordering field (`ts`) determines merge outcomes — not the position within the log block or the instant time.

---

## Summary

| Concept | What It Is |
|---|---|
| **File Group** | All files sharing the same `(partitionPath, fileId)` — the unit of all reads/writes |
| **File Slice** | One base file + its associated log files, anchored at a `baseInstantTime` |
| **Base File** | Parquet file — compact snapshot at a point in time. Name encodes `fileId`, `writeToken`, `instantTime` |
| **Log File** | Append-only delta file (Avro blocks). Name encodes `fileId`, `deltaCommitTime`, `logVersion`, `writeToken` |
| **Log Block** | Atomic unit within a log file — carries `INSTANT_TIME`, `SCHEMA`, record data, positions |
| **Committed vs uncommitted** | Determined at scan time: TV<8 checks timeline; TV9 uses rollback blocks + latestInstantTime watermark |
| **Instant attribution** | Each log block's `INSTANT_TIME` header identifies its source commit; log-compacted blocks carry `COMPACTED_BLOCK_TIMES` listing merged instants |
| **Merge winner** | Decided by ordering field values (EVENT_TIME) or write order (COMMIT_TIME) — **not** by instant time |


👤 USER:
Format: .{fileId}_{deltaCommitTime}.log.{logVersion}_{writeToken}                                                                            
is deltacommit time of a log file the instant time that write the file? is it instant time or completion time in v9


🤖 ASSISTANT:
Good question. Let me trace exactly how the log file name is constructed.

Now I have the full picture. The `instantTime` passed to the writer is the **requested time** (the time the commit was initiated), not the completion time. Let me verify this is also what goes into the log file name.

The chain is clear:
1. `BaseActionExecutor.instantTime` = requested time (from `createNewInstantTime()`)
2. `HoodieWriteHandle.instantTime` = same requested time (passed through constructor)
3. `HoodieLogFormat.WriterBuilder.withInstantTime(instantTime)` = same requested time
4. `FSUtils.makeLogFileName(..., instantTime, ...)` = same requested time → becomes `deltaCommitTime` in the file name

---

**Yes, `deltaCommitTime` in the log file name is the instant's *requested time* (a.k.a. instant time), not the completion time.**

Here's the evidence chain:

1. The write handle receives `instantTime` which is the **requested time** — the time generated when the commit is first initiated:

   `BaseActionExecutor.java:63`:
   ```java
   this.instantTime = instantTime;  // this is the requested time from createNewInstantTime()
   ```

2. It flows directly into the log format writer:

   `HoodieWriteHandle.java:291-294`:
   ```java
   return HoodieLogFormat.newWriterBuilder()
       .onParentPath(...)
       .withFileId(fileId)
       .withInstantTime(instantTime)  // ← still the requested time
   ```

3. Which uses it as the file name's `deltaCommitTime`:

   `HoodieLogFormat.java:281`:
   ```java
   FSUtils.makeLogFileName(logFileId, fileExtension, instantTime, logVersion, logWriteToken)
   //                                                ^^^^^^^^^^^
   //                                   this becomes deltaCommitTime in the file name
   ```

4. The **completion time** is only assigned later, when the commit is finalized (transition from INFLIGHT to COMPLETED state). It's stored in the timeline metadata (`HoodieInstant.completionTime`), not in the file name.

This means for a log file `.abc-123_20240101130000.log.1_1-0-1`:
- `20240101130000` = the **requested time** — when the write operation was initiated
- The **completion time** (when it actually committed) is only known from the timeline, not from the file name
- `CompletionTimeQueryView.getCompletionTime(baseInstant, deltaCommitTime)` resolves the requested time → completion time mapping at read time


👤 USER:
HoodieFileGroup

  From HoodieFileGroup.java:42-47:                                                                                                             
  /**
   * A set of data/base files + set of log files, that make up a unit for all operations.                                                      
   */                                                                                    
  public class HoodieFileGroup implements Serializable {                                                                                       
      private final HoodieFileGroupId fileGroupId;  // (partitionPath, fileId)
      private final TreeMap<String, FileSlice> fileSlices;  // keyed by baseInstantTime, reverse-sorted                                        
      private final HoodieTimeline timeline;                                                                                                   
      private final Option<HoodieInstant> lastInstant;  // high watermark                                                                      
  }                                                                                                                                            
                                                                                                                                               
  A file group is identified by (partitionPath, fileId). It's a container of file slices ordered by time.

 "file slices ordered by time." request time or completion time?


🤖 ASSISTANT:
**Requested time.**

The TreeMap key comes from `HoodieBaseFile.getCommitTime()`, which is parsed from the base file name.

From `HoodieFileGroup.java:108-113`:
```java
public void addBaseFile(HoodieBaseFile dataFile) {
    if (!fileSlices.containsKey(dataFile.getCommitTime())) {
        fileSlices.put(dataFile.getCommitTime(), new FileSlice(fileGroupId, dataFile.getCommitTime()));
    }
    fileSlices.get(dataFile.getCommitTime()).setBaseFile(dataFile);
}
```

And `dataFile.getCommitTime()` is extracted from the 3rd segment of `{fileId}_{writeToken}_{instantTime}.parquet` via `FSUtils.getCommitTime()` (`FSUtils.java:138`):

```java
return fullFileName.split("_")[2].split("\\.", 2)[0];
```

That `instantTime` in the base file name is the requested time, as we traced earlier. So the TreeMap is keyed by **requested time**, and file slices are ordered by requested time.


👤 USER:
FileSlice       
                                                                                                                                               
  From FileSlice.java:37-43:
  /**
   * Within a file group, a slice is a combination of data file written at a commit time
   * and list of log files, containing changes to the data file from that commit time.  
   */                                                                                 
  public class FileSlice implements Serializable {                                                                                             
      private final HoodieFileGroupId fileGroupId;
      private final String baseInstantTime;          // the instant that created this slice                                                    
      private HoodieBaseFile baseFile;               // 0 or 1 parquet file                                                                    
      private final TreeSet<HoodieLogFile> logFiles; // 0..N log files, reverse-sorted by version
  } 

0..N log files, reverse-sorted by version, what is this version, how it is generated, who would consume this version, what's the effect it can have

🤖 ASSISTANT:
Now I have the complete picture. Here's the answer:

---

The `logVersion` is the `.{N}` portion of the log file name (e.g., `.abc_t1.log.1`, `.abc_t1.log.2`).

### How it's generated

It starts at `LOGFILE_BASE_VERSION = 1` and increments on **rollover**. Rollover happens when a log file exceeds a size threshold, or when a file-already-exists conflict occurs.

From `HoodieLogFormatWriter.java:206-218`:
```java
private void rolloverIfNeeded() throws IOException {
    if (getCurrentSize() > sizeThreshold) {
        log.info("CurrentSize {} has reached threshold {}. Rolling over to the next version",
                 getCurrentSize(), sizeThreshold);
        rollOver();
    }
}

private void rollOver() throws IOException {
    closeStream();
    this.logFile = logFile.rollOver(rolloverLogWriteToken);  // version + 1
    this.closed = false;
}
```

And `rollOver()` on `HoodieLogFile` (`HoodieLogFile.java:181-188`):
```java
public HoodieLogFile rollOver(String logWriteToken) {
    return new HoodieLogFile(new StoragePath(path.getParent(),
        FSUtils.makeLogFileName(fileId, extension, deltaCommitTime, logVersion + 1, logWriteToken)));
}
```

Rollover also happens on **write-token collision** — when two tasks try to write the same log file simultaneously (`HoodieLogFormatWriter.java:97-116`):
```java
if (storage.exists(logFile.getPath())) {
    rollOver();                          // file already exists → bump version
}
// ...
} catch (FileAlreadyExistsException ignored) {
    rollOver();                          // race condition → bump version
} catch (RemoteException re) {
    if (re.getClassName().contentEquals(AlreadyBeingCreatedException.class.getName())) {
        rollOver();                      // another task writing → bump version
    }
}
```

**For TV >= 8**, the initial version always starts at base version 1 (no storage listing needed):

```java
// HoodieLogFormat.java:247-251
boolean useBaseVersion = tableVersion.greaterThanOrEquals(HoodieTableVersion.EIGHT)
    && logWriteToken != null;
if (useBaseVersion) {
    logVersion = HoodieLogFile.LOGFILE_BASE_VERSION;  // always 1
}
```

This works because TV8+ uses distinct `writeToken` per task, so version conflicts are resolved by rollover at write time rather than pre-computing the next version from storage (which was the expensive pre-TV8 approach).

### Who consumes it — the sort order

The `LogFileComparator` uses version as a **secondary sort key** (after `deltaCommitTime`). From `HoodieLogFile.java:214-240`:

```java
public int compare(HoodieLogFile o1, HoodieLogFile o2) {
    String deltaCommitTime1 = o1.getDeltaCommitTime();
    String deltaCommitTime2 = o2.getDeltaCommitTime();

    if (deltaCommitTime1.equals(deltaCommitTime2)) {
        if (o1.getLogVersion() == o2.getLogVersion()) {
            int compareWriteToken = getWriteTokenComparator()
                .compare(o1.getLogWriteToken(), o2.getLogWriteToken());
            if (compareWriteToken == 0) {
                return o1.getSuffix().compareTo(o2.getSuffix());
            }
            return compareWriteToken;       // 3rd tiebreaker: writeToken
        }
        return Integer.compare(o1.getLogVersion(), o2.getLogVersion()); // 2nd: logVersion
    }
    return deltaCommitTime1.compareTo(deltaCommitTime2);                // 1st: deltaCommitTime
}
```

The `FileSlice` stores log files in a `TreeSet` with the **reverse** comparator (`HoodieLogFile.getReverseLogFileComparator()`), so within a file slice, log files are ordered **highest version first**.

### Effect

The version establishes **write order within the same instant**. When a single write task produces more data than the size threshold, it rolls over to version 2, 3, etc. The log scanner then processes blocks in version order (ascending via the forward comparator), ensuring blocks are replayed in the order they were written. This matters because block processing order affects the merge result in `COMMIT_TIME_ORDERING` mode (last-writer-wins).


👤 USER:
how does filterUncommittedFiles work?

🤖 ASSISTANT:
Here's how it works end-to-end:

### `filterUncommittedFiles` (line 602)

```java
private Stream<FileSlice> filterUncommittedFiles(FileSlice fileSlice, boolean includeEmptyFileSlice) {
    Option<HoodieBaseFile> committedBaseFile = fileSlice.getBaseFile().isPresent()
        && completionTimeQueryView.isCompleted(fileSlice.getBaseInstantTime())
        ? fileSlice.getBaseFile() : Option.empty();

    List<HoodieLogFile> committedLogFiles = fileSlice.getLogFiles()
        .filter(logFile -> completionTimeQueryView.isCompleted(logFile.getDeltaCommitTime()))
        .collect(Collectors.toList());

    // ... rebuild FileSlice with only committed files
}
```

It delegates to `completionTimeQueryView.isCompleted(requestedTime)`. For TV9, this is `CompletionTimeQueryViewV2`.

### `CompletionTimeQueryViewV2.isCompleted()` (line 100-103)

```java
public boolean isCompleted(String instantTime) {
    return this.instantTimeToCompletionTimeMap.containsKey(instantTime)
        || isArchived(instantTime);
}
```

Two checks:
1. Is the requested time in the `instantTimeToCompletionTimeMap`?
2. Or is it archived (older than the first active instant)?

### How the map gets populated — `load()` (line 296-301)

```java
private void load() {
    this.metaClient.getActiveTimeline()
        .filterCompletedInstants().getInstantsAsStream()
        .forEach(instant -> setCompletionTime(instant.requestedTime(), instant.getCompletionTime()));
}
```

**This is where the "completed" semantics live.** It iterates the active timeline filtered to **completed instants only** (`filterCompletedInstants()`), and builds a map of `requestedTime -> completionTime`. Only instants that have transitioned to the COMPLETED state get into this map.

### `setCompletionTime()` (line 308-314)

```java
private void setCompletionTime(String beginInstantTime, String completionTime) {
    if (completionTime == null) {
        completionTime = beginInstantTime;  // meta-server fallback
    }
    this.instantTimeToCompletionTimeMap.putIfAbsent(beginInstantTime, completionTime);
}
```

### `getCompletionTime()` with lazy loading (line 166-177)

```java
public Option<String> getCompletionTime(String instantTime) {
    String completionTime = this.instantTimeToCompletionTimeMap.get(instantTime);
    if (completionTime != null) {
        return Option.of(completionTime);
    }
    if (compareTimestamps(instantTime, GREATER_THAN_OR_EQUALS, this.cursorInstant)) {
        return Option.empty();  // instant is still pending
    }
    // Older than cursor → might be in archived timeline, lazy-load it
    loadCompletionTimeIncrementally(instantTime);
    return Option.ofNullable(this.instantTimeToCompletionTimeMap.get(instantTime));
}
```

Three-tier lookup:
1. **In-memory map** — populated from active timeline's completed instants
2. **Cursor check** — if requested time >= cursor (first active instant), it's pending → return empty
3. **Archived timeline** — lazy-load from archive files if older than cursor

### Summary

```
filterUncommittedFiles(fileSlice)
  │
  ├── completionTimeQueryView.isCompleted("20240101130000")  // log file's deltaCommitTime (= requested time)
  │     │
  │     ├── instantTimeToCompletionTimeMap.containsKey("20240101130000")
  │     │     │
  │     │     └── populated by: activeTimeline.filterCompletedInstants()  ← HERE is the COMPLETED state check
  │     │                          .forEach(instant -> map.put(instant.requestedTime(), instant.completionTime()))
  │     │
  │     └── OR isArchived("20240101130000")
  │           └── instantTime < firstNonSavepointCommit  (archived = definitely completed)
  │
  └── if not completed → log file excluded from FileSlice
```

The map is keyed by **requested time** but only populated from instants that have `state == COMPLETED`. So `map.containsKey(requestedTime)` effectively answers "has this instant completed?" without needing to compare completion times — the mere presence in the map is proof of completion.

👤 USER:
now with all these corrections, please answer the question of log block v.s. log file again with all the details we covered so far

🤖 ASSISTANT:
## Log File vs Log Block

### Relationship: 1 log file : N blocks, all from the same instant

A log file is a physical file on storage. Log blocks are the logical units inside it, written sequentially with a binary framing protocol.

**All blocks within a single log file share the same `INSTANT_TIME`**, because:

1. Each instant creates its own writer with its own `instantTime` as the file name's `deltaCommitTime`:

   ```java
   // HoodieWriteHandle.java:294
   .withInstantTime(instantTime)  // → becomes deltaCommitTime in file name
   ```

2. The writer never appends to an existing file — it always creates new:

   ```java
   // HoodieLogFormatWriter.java:98-99
   if (storage.exists(logFile.getPath())) {
       rollOver();  // bump version, never append
   }
   // line 222-223
   this.output = storage.create(this.logFile.getPath(), false, ...);  // false = create-only
   ```

3. The block header `INSTANT_TIME` is set once per writer session:

   ```java
   // HoodieAppendHandle.java:484
   header.put(HeaderMetadataType.INSTANT_TIME, instantTime);  // same for all blocks
   ```

### What block types can coexist in one log file

Since a log file belongs to one instant, and an instant performs one type of action:

| Combination | Can coexist? | Why |
|---|---|---|
| DATA_BLOCK + DELETE_BLOCK | Yes | Same instant writes both upserts and deletes in one `appendBlocks()` call |
| DATA_BLOCK + COMMAND_BLOCK | No | COMMAND_BLOCK is written by a rollback instant, which is a separate instant with its own log file |
| DATA_BLOCK + DATA_BLOCK | Yes | Same instant, multiple blocks due to size threshold rollover within a file, or multiple flushes |

Evidence for DATA + DELETE coexistence — `HoodieAppendHandle.java:482-520`:

```java
protected void appendDataAndDeleteBlocks(Map<HeaderMetadataType, String> header, boolean appendDeleteBlocks) {
    header.put(HeaderMetadataType.INSTANT_TIME, instantTime);
    List<HoodieLogBlock> blocks = new ArrayList<>(2);

    if (recordList.size() > 0) {
        blocks.add(getDataBlock(header, ...));       // DATA_BLOCK
    }
    if (appendDeleteBlocks && deleteRecordList.size() > 0) {
        blocks.add(new HoodieDeleteBlock(deleteRecordList, header));  // DELETE_BLOCK
    }

    writer.appendBlocks(blocks);  // both written to same log file
}
```

### One instant can produce multiple log files

When a single write exceeds the size threshold, the writer rolls over to a new version:

```java
// HoodieLogFormatWriter.java:206-211
private void rolloverIfNeeded() throws IOException {
    if (getCurrentSize() > sizeThreshold) {
        rollOver();  // closes current file, bumps logVersion
    }
}

// HoodieLogFile.java:181-188
public HoodieLogFile rollOver(String logWriteToken) {
    return new HoodieLogFile(new StoragePath(path.getParent(),
        FSUtils.makeLogFileName(fileId, extension, deltaCommitTime, logVersion + 1, logWriteToken)));
}
```

This produces multiple files with the same `deltaCommitTime` but incrementing `logVersion`:

```
.abc_t1.log.1_1-0-1   ← blocks until size threshold
.abc_t1.log.2_1-0-1   ← overflow blocks, same instant t1
```

Also for TV8+, concurrent tasks writing to the same file group produce different files with the same `deltaCommitTime` but different `writeToken`:

```
.abc_t1.log.1_1-0-1   ← task partition 1
.abc_t1.log.1_2-0-1   ← task partition 2
```

### On-disk binary layout of a log file

```
Log File: .{fileId}_{deltaCommitTime}.log.{version}_{writeToken}
┌────────────────────────────────────────-─────────┐
│  Block 1: AVRO_DATA_BLOCK (INSTANT_TIME = t1)    │
│  ┌─────────────────────────────────────────────┐ │
│  │ MAGIC (6 bytes): '#HUDI#'                   │ │
│  │ BLOCK SIZE (8 bytes, long)                  │ │
│  │ LOG FORMAT VERSION (4 bytes, int) = 3       │ │
│  │ BLOCK TYPE ORDINAL (4 bytes, int) = 3       │ │
│  │ HEADER METADATA:                            │ │
│  │   count(4) + for each:                      │ │
│  │     ordinal(4) + size(4) + value(UTF-8)     │ │
│  │   includes: INSTANT_TIME, SCHEMA,           │ │
│  │     RECORD_POSITIONS, IS_PARTIAL, etc.      │ │
│  │ CONTENT SIZE (8 bytes, long)                │ │
│  │ CONTENT: Avro binary encoded records        │ │
│  │ FOOTER METADATA (currently empty)           │ │
│  │ TOTAL BLOCK LENGTH (8 bytes, reverse ptr)   │ │
│  └─────────────────────────────────────────────┘ │
├─────────────────────────────────-────────────────┤
│  Block 2: DELETE_BLOCK (INSTANT_TIME = t1)       │
│  ┌─────────────────────────────────────────────┐ │
│  │ MAGIC: '#HUDI#'                             │ │
│  │ ... same framing ...                        │ │
│  │ BLOCK TYPE ORDINAL = 1                      │ │
│  │ CONTENT: HoodieDeleteRecordList (Avro v3)   │ │
│  │   each: recordKey + partitionPath +         │ │
│  │         orderingValue                       │ │
│  └─────────────────────────────────────────────┘ │
└───────────────────────────────────────────────-──┘
```

### Consequence for filtering

Since all blocks in a log file share the same instant, `filterUncommittedFiles` filtering at the **log file level** is equivalent to filtering at the block level:

```java
// AbstractTableFileSystemView.java:604
List<HoodieLogFile> committedLogFiles = fileSlice.getLogFiles()
    .filter(logFile -> completionTimeQueryView.isCompleted(logFile.getDeltaCommitTime()))
    .collect(Collectors.toList());
```

If `deltaCommitTime` is not completed → the entire file (all blocks inside) is uncommitted. No block-level ambiguity exists.

### Summary

```
Instant t1 ──writes──> .abc_t1.log.1_1-0-1  [DATA_BLOCK(t1), DELETE_BLOCK(t1)]
                   └──> .abc_t1.log.2_1-0-1  [DATA_BLOCK(t1)]  (size rollover)

Instant t2 ──writes──> .abc_t2.log.1_2-0-1  [DATA_BLOCK(t2)]

Rollback   ──writes──> .abc_t3.log.1_1-0-1  [COMMAND_BLOCK(t3, TARGET=t1)]
```


👤 USER:
when will we have "compacted log block"? who generates that, what does it compact, what is the relation between what is been compacted and the output file? are they in the same file slice? will this cause a record being duplicated in 2 places and how would readers handle it properly?
Please give a comprehensive intro on this part.


🤖 ASSISTANT:
  🤖 Agent: Explore log compaction end to end — I need a comprehensive understanding of "log compaction" in Hudi (not regular compaction which merges log files into a new base file — this is specifically LOG compaction which merges log blocks into 

---

## What is Log Compaction?

**Regular compaction** merges log files + base file → new base file. **Log compaction** merges log blocks → new compacted log block. It stays within the log layer — no new base file is produced.

## Who Generates It?

`FileGroupReaderBasedAppendHandle.doAppend()` (line 76-104):

```java
public void doAppend() {
    // 1. Collect log files from the compaction operation
    Stream<HoodieLogFile> logFiles = operation.getDeltaFileNames().stream().map(logFileName ->
        new HoodieLogFile(new StoragePath(FSUtils.constructAbsolutePath(
            config.getBasePath(), operation.getPartitionPath()), logFileName)));

    // 2. Read all log records via HoodieFileGroupReader (no base file)
    try (HoodieFileGroupReader<T> fileGroupReader = HoodieFileGroupReader.<T>newBuilder()
        .withLogFiles(logFiles)
        .withBaseFileOption(Option.empty())   // ← no base file, logs only
        .withEmitDelete(true)                 // ← preserve deletes
        .build()) {

        // 3. Get merged records from logs
        recordItr = new CloseableMappingIterator<>(
            fileGroupReader.getLogRecordsOnly(), ...);

        // 4. Stamp the output block with which instants were compacted
        header.put(HeaderMetadataType.COMPACTED_BLOCK_TIMES,
            StringUtils.join(fileGroupReader.getValidBlockInstants(), ","));

        // 5. Write to new log file via parent class (HoodieAppendHandle)
        super.doAppend();
    }
}
```

The key line is **line 97-98**: the compacted block's header carries `COMPACTED_BLOCK_TIMES = "t1,t2,t3"` — the comma-separated list of original instant times whose blocks were merged.

## What Does It Compact?

Input: multiple log files from the **same file slice** (same fileId, same partition).
Output: a **new** log file in the **same file slice**, with `deltaCommitTime` = the log compaction instant time.

```
BEFORE log compaction:
FileSlice (baseInstantTime = t0)
  ├── base: abc_1-0-1_t0.parquet
  ├── log:  .abc_t1.log.1_1-0-1   ← blocks from instant t1
  ├── log:  .abc_t2.log.1_2-0-1   ← blocks from instant t2
  └── log:  .abc_t3.log.1_3-0-1   ← blocks from instant t3

AFTER log compaction (instant t4):
FileSlice (baseInstantTime = t0)
  ├── base: abc_1-0-1_t0.parquet
  ├── log:  .abc_t1.log.1_1-0-1   ← STILL EXISTS (not deleted)
  ├── log:  .abc_t2.log.1_2-0-1   ← STILL EXISTS
  ├── log:  .abc_t3.log.1_3-0-1   ← STILL EXISTS
  └── log:  .abc_t4.log.1_4-0-1   ← NEW compacted log file
                                      COMPACTED_BLOCK_TIMES = "t1,t2,t3"
```

**Yes, the original log files remain on disk.** The same records exist in both the old log files (t1, t2, t3) and the new compacted log file (t4). This is where the duplication problem arises.

## How the Reader Avoids Double-Counting

The scanner in `AbstractHoodieLogRecordScanner.scanInternal()` uses a two-pass algorithm.

**Pass 1 (forward, lines 302-368):** Read all blocks from all log files, group by `INSTANT_TIME`:

```
orderedInstantsList = [t1, t2, t3, t4]
instantToBlocksMap  = {
    t1 → [dataBlock from .abc_t1.log],
    t2 → [dataBlock from .abc_t2.log],
    t3 → [dataBlock from .abc_t3.log],
    t4 → [compactedBlock from .abc_t4.log]  ← has COMPACTED_BLOCK_TIMES="t1,t2,t3"
}
```

**Pass 2 (reverse, lines 387-426):** Iterate newest-to-oldest, building a dedup map:

```java
// Iterate: t4, t3, t2, t1

// t4: has COMPACTED_BLOCK_TIMES header → build the map
//     blockTimeToCompactionBlockTimeMap = {t1→t4, t2→t4, t3→t4}
//     (t4 itself is NOT added to the processing queue yet)

// t3: lookup in map → compactedFinalInstantTime = t4
//     t4 not yet in instantTimesIncluded → ADD t4's blocks to queue
//     instantTimesIncluded = {t4}

// t2: lookup in map → compactedFinalInstantTime = t4
//     t4 already in instantTimesIncluded → SKIP

// t1: lookup in map → compactedFinalInstantTime = t4
//     t4 already in instantTimesIncluded → SKIP
```

**Result:** Only t4's compacted block is processed. t1, t2, t3 blocks are skipped entirely.

The code with annotations (lines 395-425):

```java
if (firstBlock.getLogBlockHeader().containsKey(COMPACTED_BLOCK_TIMES)) {
    // COMPACTED BLOCK: don't process it yet, just record the mapping
    Arrays.stream(firstBlock.getLogBlockHeader().get(COMPACTED_BLOCK_TIMES).split(","))
        .forEach(originalInstant -> {
            String finalInstant = blockTimeToCompactionBlockTimeMap
                .getOrDefault(instantTime, instantTime);
            blockTimeToCompactionBlockTimeMap.put(originalInstant, finalInstant);
            //  t1 → t4,  t2 → t4,  t3 → t4
        });
} else {
    String compactedFinalInstantTime = blockTimeToCompactionBlockTimeMap.get(instantTime);
    if (compactedFinalInstantTime == null) {
        // NOT compacted — process normally
        logBlocks.forEach(block -> currentInstantLogBlocks.addLast(block));
        validBlockInstants.add(instantTime);
    } else if (!instantTimesIncluded.contains(compactedFinalInstantTime)) {
        // First time seeing this compacted group — add the COMPACTED block instead
        logBlocks = instantToBlocksMap.get(compactedFinalInstantTime);  // ← t4's blocks
        logBlocks.forEach(block -> currentInstantLogBlocks.addLast(block));
        instantTimesIncluded.add(compactedFinalInstantTime);
    }
    // else: compacted block already included — skip this original block
}
```

The reverse iteration order is important: the compacted block (t4, newest) is encountered first, which sets up the map. Then when older originals (t3, t2, t1) are encountered, the map redirects them to t4's already-included compacted block.

### Chained log compaction

The `getOrDefault(instantTime, instantTime)` on line 400 handles chaining — if t4 itself was later compacted into t7:

```java
String finalInstant = blockTimeToCompactionBlockTimeMap.getOrDefault(instantTime, instantTime);
// If t4→t7 already in map, then t1→t7 (not t1→t4)
```

This ensures that even with multiple rounds of log compaction, each original block maps to the most recent compacted block.

