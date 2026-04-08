
👤 USER:
BaseHoodieLogRecordReader.scanInternal() It seems like the scan internal function will not just blindly read all the log for a given file group. So I have two questions. First is how does it know which file group it should process?

🤖 ASSISTANT:
Let me trace the inputs from the very top to understand what controls the scope of `scanInternal`.

🤖 ASSISTANT:
Now I have everything needed. Here's the full answer:

---

## External Inputs

There are **4 key inputs** that control what `scanInternal` reads. They are set long before `scanInternal` is called:

### Input 1: `logFiles` — **Which file group, and which log files**

The scope is pre-determined by the `FileSlice`, which the engine (Spark/Flink) selects via the file system view. The chain is:

```
Engine (Spark/Flink) selects a FileSlice for a file group
  └─ HoodieFileGroupReader.Builder.withFileSlice(fileSlice)     [HoodieFileGroupReader.java:396]
       ├─ this.baseFileOption = fileSlice.getBaseFile()
       ├─ this.logFiles = fileSlice.getLogFiles()                ← Stream<HoodieLogFile>
       └─ this.partitionPath = fileSlice.getPartitionPath()
```

The `FileSlice` object already holds exactly the log files for **one file group** (`FileSlice.java:67`):

```java
// FileSlice.java:67 — already scoped to one fileGroupId
private final TreeSet<HoodieLogFile> logFiles;  // sorted by getReverseLogFileComparator()
```

A `HoodieLogFile` encodes the file group identity in its **filename**:
```
.fileId_deltaCommitTime_logVersion_writeToken.log
```
So all log files in a `FileSlice` share the same `fileId`.

These log files flow through:
```
HoodieFileGroupReader.Builder.build()                     [line 521]
  └─ new InputSplit(baseFileOption, Either.left(logFiles), ...)  [line 521]
       └─ InputSplit constructor                                  [InputSplit.java:56]
            // ┌──────────────────────────────────────────────────────────────┐
            // │ Sorts log files AND filters out CDC log files               │
            // │ Sort order: deltaCommitTime ASC → logVersion ASC            │
            // │            → writeToken ASC → suffix ASC                    │
            // └──────────────────────────────────────────────────────────────┘
            this.logFiles = recordsToMerge.asLeft()
                .sorted(HoodieLogFile.getLogFileComparator())
                .filter(logFile -> !logFile.getFileName().endsWith(CDC_LOGFILE_SUFFIX))
                .collect(Collectors.toList());
```

Then into the log reader builder:
```
LogScanningRecordBufferLoader.scanLogFiles()              [LogScanningRecordBufferLoader.java:41]
  └─ HoodieMergedLogRecordReader.newBuilder()
       .withLogFiles(inputSplit.getLogFiles())             ← the sorted list
       .build()
```

And into `HoodieMergedLogRecordReader` constructor → `BaseHoodieLogRecordReader` stores it as `this.logFiles`.

### Input 2: `latestInstantTime` — **High-watermark for block filtering**

```java
// BaseHoodieLogRecordReader.java:144
this.latestInstantTime = readerContext.getLatestCommitTime();
```

Set by:
```
HoodieFileGroupReader constructor                         [HoodieFileGroupReader.java:113]
  └─ readerContext.setLatestCommitTime(latestCommitTime)
```

### Input 3: `instantRange` — **Optional incremental range filter**

```java
// BaseHoodieLogRecordReader constructor, stored as this.instantRange
// LogScanningRecordBufferLoader.java:47
.withInstantRange(readerContext.getInstantRange())
```

### Input 4: `allowInflightInstants` — **Whether to include uncommitted blocks**

```java
// LogScanningRecordBufferLoader.java:50
.withAllowInflightInstants(readerParameters.allowInflightInstants())
```


👤 USER:
for commit time ordering, it requires we process the data based on their completion time, under this scenario, show me how we enforce such order of processing records across log files

## There is no completion-time sorting of log files within a file slice on the merge/read path
```
⏺ FileSlice.<init>()
  │   logFiles = new TreeSet<>(HoodieLogFile.getReverseLogFileComparator())
  │   // stored: [t3_v1, t2_v1, t1_v2, t1_v1]  (descending by deltaCommitTime, then version)
  │
  FileSlice.addLogFile(logFile)      // called per log file discovered
  │   logFiles.add(logFile)          // TreeSet auto-sorts on insert
  │
  └── HoodieFileGroupReader.constructInputSplits()
      └── InputSplit.<init>(baseFile, logFileStream, ...)
          │   // InputSplit.java:56
          │   this.logFiles = logFileStream
          │       .sorted(HoodieLogFile.getLogFileComparator())   // RE-SORT: ascending
          │       .filter(not CDC)
          │       .collect(toList())
          │   // result: [t1_v1, t1_v2, t2_v1, t3_v1]  (ascending by deltaCommitTime, then version)
          │
          └── HoodieFileGroupReader.readLogFiles()
              └── LogScanningRecordBufferLoader.scanLogFiles(inputSplit, recordBuffer)
                  │
                  └── HoodieMergedLogRecordReader.newBuilder()
                      │   .withLogFiles(inputSplit.getLogFiles())   // [t1_v1, t1_v2, t2_v1, t3_v1]
                      │   .withRecordBuffer(recordBuffer)
                      │   .build()
                      │
                      └── HoodieMergedLogRecordReader.<init>()
                          └── BaseHoodieLogRecordReader.scanInternal()
                              │
                              │── HoodieLogFormatReader.<init>(logFiles)
                              │   │   // receives [t1_v1, t1_v2, t2_v1, t3_v1]
                              │   └── currentReader = new HoodieLogFileReader(logFiles.remove(0))  // t1_v1
                              │
                              │── [FORWARD PASS] ──────────────────────────────────────────
                              │   while (logFormatReaderWrapper.hasNext())
                              │   │   // iterates: t1_v1 → t1_v2 → t2_v1 → t3_v1
                              │   │   logBlock = logFormatReaderWrapper.next()
                              │   │   instantTime = logBlock.header.get(INSTANT_TIME)
                              │   │
                              │   │   // first time seeing an instant → append to orderedInstantsList
                              │   │   orderedInstantsList.add(instantTime)
                              │   │   instantToBlocksMap.get(instantTime).add(logBlock)
                              │   │
                              │   // result:
                              │   //   orderedInstantsList = [t1, t2, t3]
                              │   //   instantToBlocksMap  = {t1→[A,B], t2→[C], t3→[D,E]}
                              │
                              │── [REVERSE PASS] ──────────────────────────────────────────
                              │   for (i = orderedInstantsList.size()-1; i >= 0; i--)  // t3, t2, t1
                              │   │   logBlocks = instantToBlocksMap.get(instantTime)
                              │   │   Collections.reverse(logBlocks)
                              │   │   logBlocks.forEach(b -> currentInstantLogBlocks.addLast(b))
                              │   │
                              │   // deque after each iteration:
                              │   //   i=2 (t3): front [E, D] back
                              │   //   i=1 (t2): front [E, D, C] back
                              │   //   i=0 (t1): front [E, D, C, B, A] back
                              │
                              └── [PROCESS] ────────────────────────────────────────────────
                                  processQueuedBlocksForInstant(currentInstantLogBlocks)
                                  │
                                  while (!logBlocks.isEmpty())
                                  │   lastBlock = logBlocks.pollLast()
                                  │   // poll order: A(t1) → B(t1) → C(t2) → D(t3) → E(t3)
                                  │
                                  ├── processDataBlock(dataBlock)
                                  │   └── while (recordIterator.hasNext())
                                  │       └── processNextDataRecord(record, key)
                                  │           │   existing = records.get(key)
                                  │           │   merged = bufferedRecordMerger.deltaMerge(record, existing)
                                  │           └── records.put(key, merged)
                                  │
                                  └── processDeleteBlock(deleteBlock)
                                      └── for each DeleteRecord
                                          └── processNextDeletedRecord(deleteRecord, key)
                                              │   existing = records.get(key)
                                              │   merged = bufferedRecordMerger.deltaMerge(deleteRecord, existing)
                                              └── records.put(key, merged)
```
Key takeaway: The double-reversal (reverse iteration of instants + addLast in the deque + pollLast to consume) produces oldest-first
processing. The last writer to records.put(key, ...) is always the newest deltaCommitTime, so it wins when the merger favors the incoming
record.

### Step 1: FileSlice — reverse-sorted TreeSet

```java
// FileSlice.java:77
this.logFiles = new TreeSet<>(HoodieLogFile.getReverseLogFileComparator());
```

Log files stored **descending by deltaCommitTime** (aka requested/instant time). The `LogFileComparator` (HoodieLogFile.java:215-240) sorts by: deltaCommitTime → logVersion → writeToken → suffix. The reversed version flips this.

### Step 2: InputSplit — re-sorted to forward order

```java
// InputSplit.java:56
this.logFiles = recordsToMerge.asLeft()
    .sorted(HoodieLogFile.getLogFileComparator())  // forward: oldest deltaCommitTime first
    .filter(...)
    .collect(Collectors.toList());
```

Result: `[log_t1_v1, log_t1_v2, log_t2_v1, log_t3_v1, ...]` — ascending by deltaCommitTime, then logVersion.

### Step 3: HoodieLogFormatReader — sequential iteration, oldest first

```java
// HoodieLogFormatReader.java:61
HoodieLogFile nextLogFile = logFiles.remove(0);  // takes from front = oldest

// hasNext() line 87: same pattern
HoodieLogFile nextLogFile = logFiles.remove(0);
```

Blocks are yielded in the order they appear within each file, file by file, oldest deltaCommitTime first.

### Step 4: Forward scan — builds orderedInstantsList

```java
// BaseHoodieLogRecordReader.java:249-310
while (logFormatReaderWrapper.hasNext()) {
    HoodieLogBlock logBlock = logFormatReaderWrapper.next();
    String instantTime = logBlock.getLogBlockHeader().get(INSTANT_TIME);
    // ...
    // Line 292: first appearance of an instant → added to list
    orderedInstantsList.add(instantTime);
    // Line 293: all blocks for that instant accumulated
    logBlocksList.add(logBlock);
    instantToBlocksMap.put(instantTime, logBlocksList);
}
```

`orderedInstantsList` = `[t1, t2, t3]` — order of first block appearance = deltaCommitTime ascending.

`instantToBlocksMap`:
- `t1 → [block_A, block_B]` (in file-read order)
- `t2 → [block_C]`
- `t3 → [block_D, block_E]`

### Step 5: Reverse pass — builds the deque

```java
// BaseHoodieLogRecordReader.java:336-375
for (int i = orderedInstantsList.size() - 1; i >= 0; i--) {  // t3, t2, t1
    List<HoodieLogBlock> logBlocks = instantToBlocksMap.get(instantTime);
    Collections.reverse(logBlocks);          // [block_D, block_E] → [block_E, block_D]
    logBlocks.forEach(block -> currentInstantLogBlocks.addLast(block));
}
```

Iteration order: **t3 first, then t2, then t1**. Each instant's blocks are reversed then appended to the **back** of the deque.

Deque state after the loop (front → back):

```
front: [block_E, block_D, block_C, block_B, block_A] :back
        ←── t3 ──→  ←t2→  ←──── t1 ────→
```

### Step 6: Processing — pollLast (oldest instant first)

```java
// BaseHoodieLogRecordReader.java:432
HoodieLogBlock lastBlock = logBlocks.pollLast();
```

Poll order from the back: **block_A → block_B → block_C → block_D → block_E**

This means: **t1's blocks processed first (in original file order), then t2, then t3.**

The `Collections.reverse` + `addLast` + `pollLast` pattern is effectively a double-reversal that restores original file-read order within each instant, while the reverse iteration of instants + pollLast produces oldest-instant-first processing.

### Step 7: LogScanningRecordBufferLoader → recordBuffer

```java
// LogScanningRecordBufferLoader.java:44,49
.withLogFiles(inputSplit.getLogFiles())   // passes the forward-sorted list
.withRecordBuffer(recordBuffer)           // the buffer receives processDataBlock/processDeleteBlock calls
```

The scanner calls `recordBuffer.processDataBlock()` / `recordBuffer.processDeleteBlock()` directly during `processQueuedBlocksForInstant()` — there's no separate buffering step.

### Step 8: KeyBasedFileGroupRecordBuffer — record-level merge

```java
// KeyBasedFileGroupRecordBuffer.java:105-110
public void processNextDataRecord(BufferedRecord<T> record, Serializable recordKey) {
    BufferedRecord<T> existingRecord = records.get(recordKey);  // lookup by key
    totalLogRecords++;
    bufferedRecordMerger.deltaMerge(record, existingRecord)
        .ifPresent(bufferedRecord -> records.put(recordKey, bufferedRecord.toBinary(...)));
}

// KeyBasedFileGroupRecordBuffer.java:122-128
public void processNextDeletedRecord(DeleteRecord deleteRecord, Serializable recordIdentifier) {
    BufferedRecord<T> existingRecord = records.get(recordIdentifier);
    totalLogRecords++;
    Option<DeleteRecord> recordOpt = bufferedRecordMerger.deltaMerge(deleteRecord, existingRecord);
    recordOpt.ifPresent(deleteRec -> records.put(recordIdentifier, BufferedRecords.fromDeleteRecord(...)));
}
```

For each record in each block, the buffer does a **key-based lookup** in the `records` map, then calls `bufferedRecordMerger.deltaMerge(incoming, existing)`. The merge result replaces the existing entry.

---

### Net effect

| Processing order | What |
|---|---|
| 1st | t1's blocks (oldest deltaCommitTime), in original file-read order |
| 2nd | t2's blocks |
| 3rd | t3's blocks (newest deltaCommitTime) |

For a given key that appears in multiple instants, `deltaMerge` is called with:
1. `deltaMerge(t1_record, null)` → stores t1_record
2. `deltaMerge(t2_record, t1_record)` → stores merge result
3. `deltaMerge(t3_record, t2_result)` → stores final result

With `CommitTimeRecordMerger` (which always returns the incoming record), **the latest deltaCommitTime wins**. With other mergers, the outcome depends on ordering field comparison within `deltaMerge`.

**Processing order through the deque:**

```
Deque after reverse pass:

  front                                          back
  ┌──────────┬──────────┬──────────┬───--───┬──────────┐
  │ blkE(t3) │ blkD(t3) │ blkC(t2) │blkB(t1)│ blkA(t1) │
  └──────────┴──────────┴──────────┴─────--─┴──────────┘
                                              ↑ pollLast()

  Poll order:  blkA(t1) → blkB(t1) → blkC(t2) → blkD(t3) → blkE(t3)
               oldest instant first ──────────────► newest instant last
```

For a key `K` appearing in t1 and t3:
```
deltaMerge(t1_record, null)        → records["K"] = t1_record
deltaMerge(t3_record, t1_record)   → records["K"] = merge_result   // latest instant is "incoming"
```