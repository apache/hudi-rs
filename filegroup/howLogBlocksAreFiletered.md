
---

## How `scanInternal` Filters Blocks — Annotated Code

Inside `scanInternal`, each block read from the log files goes through **4 gates** before it can enter `instantToBlocksMap`:

```java
// BaseHoodieLogRecordReader.java, scanInternal(), Pass 1

while (logFormatReaderWrapper.hasNext()) {                    // iterates across ALL log files sequentially
    HoodieLogBlock logBlock = logFormatReaderWrapper.next();  // read one block
    final String instantTime = logBlock.getLogBlockHeader().get(INSTANT_TIME);  // each block stamps its commit

    // ┌──────────────────────────────────────────────────┐
    // │  GATE 1: Corrupt blocks → skip silently          │
    // └──────────────────────────────────────────────────┘
    if (logBlock.getBlockType().equals(CORRUPT_BLOCK)) {
        continue;
    }

    // ┌──────────────────────────────────────────────────────────────────────┐
    // │  GATE 2: Future blocks → skip                                       │
    // │  Block's instant time > latestInstantTime (reader's high-watermark) │
    // │  These are from commits that haven't been acknowledged yet           │
    // └──────────────────────────────────────────────────────────────────────┘
    if (logBlock.isDataOrDeleteBlock()
        && compareTimestamps(instantTime, GREATER_THAN, this.latestInstantTime)) {
        continue;
    }

    // ┌──────────────────────────────────────────────────────────────────────┐
    // │  GATE 3: Inflight/uncommitted blocks (table version < 8 only)        │
    // │  If the commit is still inflight OR not on the completed timeline,   │
    // │  skip it — it's from a failed/incomplete write                       │
    // └──────────────────────────────────────────────────────────────────────┘
    if (logBlock.getBlockType() != COMMAND_BLOCK) {
        if (this.tableVersion.lesserThan(HoodieTableVersion.EIGHT) && !allowInflightInstants) {
            HoodieTimeline commitsTimeline = this.hoodieTableMetaClient.getCommitsTimeline();
            if (commitsTimeline.filterInflights().containsInstant(instantTime)
                || !commitsTimeline.filterCompletedInstants().containsOrBeforeTimelineStarts(instantTime)) {
                continue;  // uncommitted block from failed write
            }
        }

        // ┌──────────────────────────────────────────────────────────────────┐
        // │  GATE 4: Instant range filter (for incremental queries)          │
        // │  If an instantRange is configured, only process blocks within it │
        // └──────────────────────────────────────────────────────────────────┘
        if (instantRange.isPresent() && !instantRange.get().isInRange(instantTime)) {
            continue;
        }
    }

    // ═══════════════════════════════════════════════════════
    //  Block passed all gates — classify it
    // ═══════════════════════════════════════════════════════
    switch (logBlock.getBlockType()) {
        case AVRO_DATA_BLOCK:
        case HFILE_DATA_BLOCK:
        case PARQUET_DATA_BLOCK:
        case DELETE_BLOCK:
            // Add to instantToBlocksMap, keyed by instantTime
            // Track arrival order in orderedInstantsList
            break;
        case COMMAND_BLOCK:
            // Rollback → remove the target instant from both maps
            targetRollbackInstants.add(targetInstant);
            orderedInstantsList.remove(targetInstant);
            instantToBlocksMap.remove(targetInstant);
            break;
    }
}
```

---

## Visual: End-to-End Flow

```
┌─────────────────────────────────────────────────────────────────────────────--────┐
│                           ENGINE (Spark / Flink)                                  │
 │                                                                                  │
│  FileSystemView  ──selects──►  FileSlice {                                        │
│                                  fileGroupId: (partition="p1", fileId="fg-001")   │
│                                  baseFile: "fg-001_001_0_123.parquet"             │
│                                  logFiles: [                                      │
│                                    ".fg-001_20250101.log.1",                      │
│                                    ".fg-001_20250102.log.1",                      │
│                                    ".fg-001_20250103.log.1"                       │
│                                  ]                                                │
│                                }                                                  │
└───────────────────────────────────────┬──────────────────────────────────────────┘
                                        │
                                        ▼
┌──────────────────────────────────────────────────────────────────────────────── ─┐
│                      HoodieFileGroupReader.initRecordIterators()                 │
│                                                                                  │
│  ┌─ makeBaseFileIterator() ──► lazy Parquet iterator (not consumed yet)           │
│  │                                                                               │
│  └─ recordBufferLoader.getRecordBuffer()                                         │
│       │                                                                          │
│       ▼                                                                          │
│     InputSplit sorts log files:                                                  │
│       .fg-001_20250101.log.1  (oldest)                                           │
│       .fg-001_20250102.log.1                                                     │
│       .fg-001_20250103.log.1  (newest)                                           │
│                                                                                  │
│     Passes them + latestInstantTime + instantRange to:                           │
└───────────────────────────────────────┬──────────────────────────────────────────┘
                                        │
                                        ▼
┌──────────────────────────────────────────────────────────────────────────────── ─┐
│                    HoodieMergedLogRecordReader → scanInternal()                  │
│                                                                                  │
│  External inputs available:                                                      │
│    • logFiles          = [log1, log2, log3]  (pre-sorted, one file group only)   │
│    • latestInstantTime = "20250103"          (high-watermark)                    │
│    • instantRange      = Option.empty()      (or [startTime, endTime])           │
│    • allowInflight     = false               (skip uncommitted blocks)           │
│    • tableVersion      = 6/7/8               (controls inflight check path)      │
│    • metaClient        → timeline            (for inflight/completed checks)     │
│                                                                                  │
│  HoodieLogFormatReader reads blocks sequentially across log files:               │
│                                                                                  │
│    log1: [B1(i=20250101)]                                                        │
│    log2: [B2(i=20250102), B3(i=20250102)]                                        │
│    log3: [B4(i=20250103), R1(target=20250102)]  ← rollback                       │
│                                                                                  │
│  Pass 1: Forward scan with 4 gates                                               │
│                                                                                  │
│    B1 ──► GATE1:ok GATE2:ok GATE3:ok GATE4:ok ──► instantToBlocksMap             │
│    B2 ──► GATE1:ok GATE2:ok GATE3:ok GATE4:ok ──► instantToBlocksMap             │
│    B3 ──► GATE1:ok GATE2:ok GATE3:ok GATE4:ok ──► instantToBlocksMap             │
│    B4 ──► GATE1:ok GATE2:ok GATE3:ok GATE4:ok ──► instantToBlocksMap             │
│    R1 ──► COMMAND_BLOCK: removes "20250102" from maps                            │
│                                                                                  │
│    instantToBlocksMap after pass 1:                                              │
│      "20250101" → [B1]                                                           │
│      "20250103" → [B4]                                                           │
│    orderedInstantsList: ["20250101", "20250103"]                                 │
│                                                                                  │
│  Pass 2: Reverse iteration → build deque                                         │
│                                                                                  │
│    i=1: "20250103" → addLast(B4) to deque                                        │
│    i=0: "20250101" → addLast(B1) to deque                                        │
│    deque: [B4, B1]                                                               │
│                                                                                  │
│  processQueuedBlocksForInstant: pollLast() drains oldest→newest                  │
│                                                                                  │
│    pollLast → B1 (i=20250101) → processDataBlock → records map updated           │
│    pollLast → B4 (i=20250103) → processDataBlock → records map updated           │
│                                                                                  │
└───────────────────────────────────────────────────────────────────────────-──────┘
```

---

## Summary: Answers to Your Two Questions

**Q1: How does it know which file group to process?**

`scanInternal` doesn't pick a file group — it is **told**. The file group scoping happens entirely upstream:

```
Engine → FileSystemView → selects FileSlice (scoped to one fileGroupId)
  → HoodieFileGroupReader.Builder.withFileSlice(fileSlice)
    → InputSplit sorts & stores fileSlice.getLogFiles()
      → LogScanningRecordBufferLoader passes inputSplit.getLogFiles() to the reader builder
        → BaseHoodieLogRecordReader stores as this.logFiles
          → scanInternal() reads exactly those files, nothing more
```

All log files in a `FileSlice` share the same `fileId` embedded in their filename (e.g., `.fg-001_20250101.log.1`). The `FileSlice` is a pre-scoped container — one file group, one partition.

**Q2: How does it decide which log blocks to include?**

Four gates, applied per-block during Pass 1 of `scanInternal`:

| Gate | Input Used | What It Filters |
|---|---|---|
| **1. Corrupt** | Block type | Drops `CORRUPT_BLOCK` |
| **2. Future** | `latestInstantTime` | Drops blocks with `INSTANT_TIME > latestInstantTime` |
| **3. Inflight** | `metaClient.getCommitsTimeline()` + `allowInflightInstants` + `tableVersion` | Drops blocks from failed/inflight writes (table version < 8 only) |
| **4. Range** | `instantRange` | Drops blocks outside `[startTime, endTime]` (incremental queries) |

After Pass 1, rollback command blocks retroactively remove their target instants from the surviving set. Pass 2 then handles log-compaction deduplication (replacing original blocks with their compacted equivalents).


👤 USER:
what is the semantic of instantToBlocksMap? we have covered how it is produced, now check it's consumer code to give the answer


🤖 ASSISTANT:
The consumer is Pass 2 of `scanInternal`. Let me re-read it with focus on how `instantToBlocksMap` is consumed:


🤖 ASSISTANT:
  📖 Read: /Users/zhanyeha/hudi-oss/hudi-common/src/main/java/org/apache/hudi/common/table/log/BaseHoodieLogRecordReader.java


🤖 ASSISTANT:
The consumer code uses `instantToBlocksMap` in exactly **3 places**, all in Pass 2:

```java
// USE 1 (line 338): Look up the blocks for the current instant being visited
List<HoodieLogBlock> instantsBlocks = instantToBlocksMap.get(instantTime);

// USE 2 (line 357): For a non-compacted instant, get its blocks to add to the processing deque
List<HoodieLogBlock> logBlocks = instantToBlocksMap.get(instantTime);

// USE 3 (line 369): For a compacted instant, get the COMPACTED block's blocks (not the original's)
List<HoodieLogBlock> logBlocks = instantToBlocksMap.get(compactedFinalInstantTime);
```

## Semantic

**`instantToBlocksMap` is a grouping of log blocks by the commit instant that produced them.**

- **Key**: the commit instant time (e.g., `"20250101"`) — extracted from each block's `INSTANT_TIME` header
- **Value**: the ordered list of `HoodieLogBlock`s written by that commit, in the order they appeared in the log files

Its purpose is to make log blocks **addressable by commit instant**, so that Pass 2 can:

1. **Skip entire commits** — if an instant was compacted into another, skip the original's blocks entirely and use the compacted block's blocks instead (USE 3 at line 369 vs USE 2 at line 357)
2. **Retrieve the right replacement** — when a data block's instant has been compacted (e.g., `i1 → i3`), the map lets you jump directly to `instantToBlocksMap.get("i3")` to get the compacted block that subsumes it

Without this grouping, you'd have a flat list of blocks and no way to "swap out" an original commit's blocks for their compacted replacement. The map is essentially an **indirection layer** that decouples "which instants to process" (`orderedInstantsList`) from "what blocks to actually read" (`instantToBlocksMap`).

### Concrete example

```
Log files contain:  B1(i1), B2(i2), CB(i3, compacted=[i1,i2]), B3(i4)

instantToBlocksMap after Pass 1:
  "i1" → [B1]
  "i2" → [B2]
  "i3" → [CB]      ← compacted block with header COMPACTED_BLOCK_TIMES="i1,i2"
  "i4" → [B3]

orderedInstantsList: ["i1", "i2", "i3", "i4"]

Pass 2 (reverse iteration):
  i4: normal block, not compacted → add B3 to deque
  i3: has COMPACTED_BLOCK_TIMES → updates map: i1→i3, i2→i3 (register replacement)
  i2: compacted → looks up compactedFinalInstantTime = "i3"
      → instantToBlocksMap.get("i3") → [CB]     ← USE 3: fetches the COMPACTED block, not B2
      → adds CB to deque, marks i3 as included
  i1: compacted → looks up "i3" → already included → skip

Final deque:  [B3, CB]  (newest at head, oldest at tail)
pollLast order: CB → B3  (oldest commit first → newest commit last)
```

The key insight: `instantToBlocksMap.get(instantTime)` is called with **either the original instant or the compacted instant**, depending on context. The map serves as a lookup table where the compaction logic can substitute one set of blocks for another by simply changing which key it looks up.

## Call Stack Tree

```
HoodieMergedLogRecordReader.<init>()                           // :75
  └─ performScan()                                             // :95
       └─ BaseHoodieLogRecordReader.scanInternal()             // :192
            ├─ new HoodieLogFormatReader(storage, logFiles, …) // :203
            │    └─ logFiles.remove(0) → new HoodieLogFileReader(file₀)  // :61-63
            │
            ├─ PHASE 1: Forward scan (while logFormatReaderWrapper.hasNext())  // :249
            │    ├─ HoodieLogFormatReader.hasNext()            // :79
            │    │    └─ currentReader.hasNext()               // delegates to HoodieLogFileReader
            │    │    └─ if exhausted → logFiles.remove(0)     // :87  ← advances to next log file
            │    │         └─ new HoodieLogFileReader(nextFile)
            │    ├─ HoodieLogFormatReader.next() → HoodieLogBlock  // :101
            │    ├─ GATE 1-4 filtering                         // :259-281
            │    └─ switch(blockType) → populate maps          // :284-316
            │
            ├─ PHASE 2: Reverse traversal of orderedInstantsList  // :336
            │    └─ builds currentInstantLogBlocks (Deque)
            │
            └─ processQueuedBlocksForInstant(…)                // :383-386
```

## Phase 1: Forward Scan — Annotated Code

Given pre-sorted log files `[log1, log2, log3]`:

```java
// ──────────────────────────────────────────────────────────────────────
// INITIALIZATION: HoodieLogFormatReader chains the sorted log files
// into one sequential block stream.
// ──────────────────────────────────────────────────────────────────────
// HoodieLogFormatReader.java:50-64
HoodieLogFormatReader(storage, logFiles, …) {
    // logFiles = [log1, log2, log3]  (already sorted by caller)
    this.logFiles = logFiles;           // mutable list, acts as a queue
    HoodieLogFile nextLogFile = logFiles.remove(0);  // pops log1
    this.currentReader = new HoodieLogFileReader(storage, nextLogFile, …);
    // remaining: logFiles = [log2, log3]
}

// HoodieLogFormatReader.java:79-97
// hasNext() auto-advances to the next log file when current is exhausted:
boolean hasNext() {
    if (currentReader.hasNext()) return true;        // still blocks in current file
    else if (!logFiles.isEmpty()) {
        HoodieLogFile nextLogFile = logFiles.remove(0);  // pop next file
        this.currentReader = new HoodieLogFileReader(…, nextLogFile, …);
        return hasNext();  // recursive — keeps advancing until blocks found or list empty
    }
    return false;
}
```

```java
// ──────────────────────────────────────────────────────────────────────
// BaseHoodieLogRecordReader.java:232-239  — fresh data structures
// ──────────────────────────────────────────────────────────────────────
Set<String>                    targetRollbackInstants = new HashSet<>();
Map<String, List<HoodieLogBlock>> instantToBlocksMap  = new HashMap<>();
List<String>                   orderedInstantsList     = new ArrayList<>();

// ──────────────────────────────────────────────────────────────────────
// BaseHoodieLogRecordReader.java:249-317  — FORWARD SCAN
// Reads blocks sequentially: log1:[B1] → log2:[B2,B3] → log3:[B4,R1]
// ──────────────────────────────────────────────────────────────────────
while (logFormatReaderWrapper.hasNext()) {
    HoodieLogBlock logBlock = logFormatReaderWrapper.next();
    String instantTime = logBlock.getLogBlockHeader().get(INSTANT_TIME);

    // ════════ GATE 1: Corrupt block ════════  (:259-262)
    if (logBlock.getBlockType() == CORRUPT_BLOCK) {
        continue;   // silently skip
    }

    // ════════ GATE 2: Future block ════════  (:264-267)
    // Block's instant > latestInstantTime ("20250103") → skip
    if (logBlock.isDataOrDeleteBlock()
        && compareTimestamps(instantTime, GREATER_THAN, this.latestInstantTime)) {
        continue;
    }

    // ════════ GATE 3: Inflight / uncommitted (table version < 8 only) ════════  (:269-277)
    if (logBlock.getBlockType() != COMMAND_BLOCK) {
        if (tableVersion < V8 && !allowInflightInstants) {
            HoodieTimeline commits = metaClient.getCommitsTimeline();
            if (commits.filterInflights().containsInstant(instantTime)
                || !commits.filterCompletedInstants()
                           .containsOrBeforeTimelineStarts(instantTime)) {
                continue;  // uncommitted / unknown instant → skip
            }
        }

        // ════════ GATE 4: Instant range ════════  (:278-281)
        if (instantRange.isPresent() && !instantRange.get().isInRange(instantTime)) {
            continue;  // outside [startTime, endTime] window
        }
    }

    // ──────── Block passed all gates → classify & store ────────  (:284-316)
    switch (logBlock.getBlockType()) {

        case AVRO_DATA_BLOCK:
        case PARQUET_DATA_BLOCK:
        case HFILE_DATA_BLOCK:
        case DELETE_BLOCK:
            List<HoodieLogBlock> blocks =
                instantToBlocksMap.getOrDefault(instantTime, new ArrayList<>());
            if (blocks.isEmpty()) {
                // First block for this instant → record arrival order
                orderedInstantsList.add(instantTime);
            }
            blocks.add(logBlock);
            instantToBlocksMap.put(instantTime, blocks);
            break;

        case COMMAND_BLOCK:
            HoodieCommandBlock cmd = (HoodieCommandBlock) logBlock;
            if (cmd.getType() == ROLLBACK_BLOCK) {
                String target = logBlock.getLogBlockHeader().get(TARGET_INSTANT_TIME);
                targetRollbackInstants.add(target);
                // ★ Nuke the rolled-back instant from BOTH structures
                orderedInstantsList.remove(target);
                instantToBlocksMap.remove(target);
            }
            break;
    }
}
```

### Concrete Example Trace

```
logFiles (sorted): [log1, log2, log3]

log1: [B1(i=20250101)]
log2: [B2(i=20250102), B3(i=20250102)]
log3: [B4(i=20250103), R1(target=20250102)]

 Block │ Gates      │ Action                           │ orderedInstantsList      │ instantToBlocksMap
───────┼────────────┼──────────────────────────────────┼──────────────────────────┼──────────────────────────
 B1    │ all pass   │ new instant → add to both        │ ["20250101"]             │ {101→[B1]}
 B2    │ all pass   │ new instant → add to both        │ ["20250101","20250102"]  │ {101→[B1], 102→[B2]}
 B3    │ all pass   │ existing instant → append block  │ ["20250101","20250102"]  │ {101→[B1], 102→[B2,B3]}
 B4    │ all pass   │ new instant → add to both        │ [..,"20250102","20250103"]│ {101→[B1], 102→[B2,B3], 103→[B4]}
 R1    │ COMMAND    │ remove("20250102") from both     │ ["20250101","20250103"]  │ {101→[B1], 103→[B4]}
```

**Key design points:**

- `orderedInstantsList` tracks **arrival order** (first-block-seen order), not sorted timestamp order — though in practice they tend to coincide since log files are sorted.
- `instantToBlocksMap` is a multi-map: one instant can have many blocks (e.g. B2+B3 both at `20250102`).
- Rollback blocks bypass Gates 2-4 (only Gate 1 applies) because the gate checks are wrapped in `if (logBlock.getBlockType() != COMMAND_BLOCK)` at line 269.
- `List.remove(Object)` on `orderedInstantsList` is an O(n) scan — fine because the list is small (one entry per commit instant in the file group).

────────────────────────────────────────────────────────────────────────────────
👤 USER:
what are the invariants between
currentInstantLogBlocks, instantTimesIncluded and validBlockInstants

────────────────────────────────────────────────────────────────────────────────
🤖 ASSISTANT:
Here's the Phase 2 loop again for reference (lines 326–375), then the invariants:

```java
Set<String> instantTimesIncluded = new HashSet<>();        // :326
Map<String, String> blockTimeToCompactionBlockTimeMap = …; // :330

for (int i = orderedInstantsList.size() - 1; i >= 0; i--) {   // :336  (reverse)
    String instantTime = orderedInstantsList.get(i);
    List<HoodieLogBlock> instantsBlocks = instantToBlocksMap.get(instantTime);
    HoodieLogBlock firstBlock = instantsBlocks.get(0);

    if (firstBlock has COMPACTED_BLOCK_TIMES header) {         // :345
        // This IS a compacted block — update the mapping, but don't enqueue anything yet
        update blockTimeToCompactionBlockTimeMap;
    } else {
        String compacted = blockTimeToCompactionBlockTimeMap.get(instantTime);

        if (compacted == null) {                               // :355  — not compacted
            reverse(blocks); blocks → currentInstantLogBlocks.addLast(…)
            instantTimesIncluded.add(instantTime);
            validBlockInstants.add(instantTime);

        } else if (instantTimesIncluded.contains(compacted)) { // :365  — compacted & already included
            continue;                                          // deduplicate

        } else {                                               // :368  — compacted & not yet included
            blocks of compacted instant → currentInstantLogBlocks.addLast(…)
            instantTimesIncluded.add(compactedFinalInstantTime);
            validBlockInstants.add(compactedFinalInstantTime);
        }
    }
}
```

## The three structures and their roles

| Structure | Type | Scope | Purpose |
|---|---|---|---|
| `currentInstantLogBlocks` | `Deque<HoodieLogBlock>` | instance field | The final ordered queue of blocks to be merged by `processQueuedBlocksForInstant` |
| `instantTimesIncluded` | `Set<String>` | local to Phase 2 | Dedup guard — prevents the same compacted instant from being enqueued twice |
| `validBlockInstants` | `List<String>` | instance field | Records which instant times actually contributed blocks, exposed via getter for downstream consumers |

## Invariants

**Invariant 1: `instantTimesIncluded` and `validBlockInstants` contain exactly the same instant strings.**

Every code path that adds to one adds to the other in the same `if` branch:
- Line 360–361: `instantTimesIncluded.add(instantTime)` + `validBlockInstants.add(instantTime)`
- Line 372–373: `instantTimesIncluded.add(compactedFinalInstantTime)` + `validBlockInstants.add(compactedFinalInstantTime)`

They differ only in data structure — `Set` vs `List` — because they serve different purposes: one for O(1) membership checks during the loop, the other for ordered output after the loop.

**Invariant 2: Every instant in `instantTimesIncluded`/`validBlockInstants` has its blocks in `currentInstantLogBlocks`, and vice versa.**

A block is only added to `currentInstantLogBlocks` (lines 359, 371) in the same branch that adds the instant to the other two structures. There is no path that enqueues blocks without recording the instant, and no path that records an instant without enqueuing its blocks.

**Invariant 3: Each instant appears in `instantTimesIncluded` at most once (dedup).**

The compaction path at line 365 checks `instantTimesIncluded.contains(compactedFinalInstantTime)` before enqueuing. This prevents a scenario like:

```
orderedInstantsList: [B1(i1), B2(i2), M1(i3, compacted=[i1,i2])]
```

When iterating in reverse: M1 updates the mapping (i1→i3, i2→i3). Then encountering i2 would enqueue i3's blocks. Without the guard, encountering i1 would enqueue i3's blocks again. The `instantTimesIncluded` set prevents this double-enqueue.

**Invariant 4: `currentInstantLogBlocks` is ordered latest-first (reverse chronological).**

The deque is **latest-first, earliest-last** — the exact opposite of what you described. Here's the proof:

## 1. Filling: reverse iteration + `addLast`

```java
// :336 — iterates orderedInstantsList from END to START (latest → earliest)
for (int i = orderedInstantsList.size() - 1; i >= 0; i--) {
    String instantTime = orderedInstantsList.get(i);
    List<HoodieLogBlock> logBlocks = instantToBlocksMap.get(instantTime);

    // :358-359 — reverses blocks within the instant, then appends to TAIL
    Collections.reverse(logBlocks);
    logBlocks.forEach(block -> currentInstantLogBlocks.addLast(block));
}
```

## 2. Draining: `pollLast`

```java
// :431-432 — consumes from the TAIL
// poll the element at the bottom of the stack since that's the order it was inserted
HoodieLogBlock lastBlock = logBlocks.pollLast();
```

## Concrete trace

```
orderedInstantsList = ["20250101", "20250103"]
instantToBlocksMap  = { "20250101"→[B1], "20250103"→[B4] }

Reverse iteration:
  i=1  instant="20250103"  blocks=[B4] → reverse → [B4] → addLast
       deque: [ B4 ]
                head       tail

  i=0  instant="20250101"  blocks=[B1] → reverse → [B1] → addLast
       deque: [ B4,  B1 ]
                head  tail
```

The deque now has **latest at head, earliest at tail**.

`processQueuedBlocksForInstant` calls `pollLast()` in a loop, so it drains **tail-first**:

```
pollLast → B1 (earliest instant, processed first)
pollLast → B4 (latest instant, processed second)
```

## Multi-block instant

```
orderedInstantsList = ["20250101", "20250102"]
instantToBlocksMap  = { "20250101"→[B1], "20250102"→[B2, B3] }
                                          (B2 was seen before B3 in forward scan)

Reverse iteration:
  i=1  instant="20250102"  blocks=[B2,B3] → reverse → [B3,B2] → addLast
       deque: [ B3, B2 ]

  i=0  instant="20250101"  blocks=[B1] → reverse → [B1] → addLast
       deque: [ B3, B2, B1 ]

pollLast order: B1 → B2 → B3
```

## Summary

| Property | Value |
|---|---|
| Contains all valid (non-rolled-back, gate-passing) blocks? | Yes |
| Head of deque | Latest instant, last block within that instant |
| Tail of deque | Earliest instant, first block within that instant |
| `pollLast` drain order | **Earliest-instant-first, and within an instant, earliest-block-first** (i.e. original forward-scan order) |

The `Collections.reverse()` + `addLast` during reverse iteration is a trick to reconstruct forward chronological order at the tail. The deque is used as a stack-like structure where the "bottom" (tail) holds the oldest data — and `pollLast` drains it in exactly the original log-file order.

**Invariant 5: Compacted block instants replace their constituent instants.**

If i1 and i2 were compacted into M1(i3), then `validBlockInstants` will contain `i3` (not i1 or i2), and `currentInstantLogBlocks` will contain the blocks from `instantToBlocksMap.get(i3)`. The original instants i1/i2 are effectively "absorbed" — their iteration in the loop hits the `continue` at line 365/366.


────────────────────────────────────────────────────────────────────────────────
👤 USER:
cool, could you also summarize the invariant for orderedInstantsList and instantToBlocksMap from pass 1

────────────────────────────────────────────────────────────────────────────────
🤖 ASSISTANT:
## Pass 1 Invariants for `orderedInstantsList` and `instantToBlocksMap`

**Invariant 1: `orderedInstantsList` contains exactly the keys of `instantToBlocksMap`.**

Every mutation site maintains this:

```java
// :289-295 — ADD: both structures updated together
if (logBlocksList.isEmpty()) {
    orderedInstantsList.add(instantTime);   // add key to list
}
logBlocksList.add(logBlock);
instantToBlocksMap.put(instantTime, logBlocksList);  // add key to map

// :307-309 — REMOVE: both structures updated together
orderedInstantsList.remove(targetInstantForCommandBlock);
instantToBlocksMap.remove(targetInstantForCommandBlock);
```

There is no code path that touches one without the other.

**Invariant 2: `orderedInstantsList` has no duplicates.**

The `add` to the list only fires when `logBlocksList.isEmpty()` (:290) — i.e. only on the **first** block seen for a given instant. Subsequent blocks for the same instant hit the `getOrDefault` path, find a non-empty list, and skip the list add.

**Invariant 3: `orderedInstantsList` preserves first-seen arrival order.**

Instants appear in the order their first block was encountered in the forward scan. Since `HoodieLogFormatReader` drains log files in sorted order and blocks within a file are sequential, this is effectively **log-file order → block-position-within-file order**.

**Invariant 4: `instantToBlocksMap` values preserve per-instant block encounter order.**

For a given instant, blocks are appended via `logBlocksList.add(logBlock)` (:294) in the order the forward scan sees them. If an instant spans multiple log files (e.g. B2 in log2, B5 in log3, same instant), the list reflects that cross-file order.

**Invariant 5: Rolled-back instants are fully purged — no tombstones.**

A rollback doesn't mark entries as invalid; it removes them entirely from both structures (:308-309). Downstream code (Phase 2) never needs to check `targetRollbackInstants` to filter — the maps are already clean.

**Invariant 6: Every value list in `instantToBlocksMap` is non-empty.**

An entry is created only when a block is added (:294-295). Removal is all-or-nothing via rollback (:309). There is no path that empties a list without removing the key. (Phase 2 relies on this — line 340 throws if it finds an empty list.)

────────────────────────────────────────────────────────────────────────────────
👤 USER:
also for any ordered data structure within the 2, how the order relates to block position within log files plus the log file order in the input log file list, cover that invariant as well

────────────────────────────────────────────────────────────────────────────────
🤖 ASSISTANT:
## Ordering Invariant: How `orderedInstantsList` and `instantToBlocksMap` value lists relate to physical log file / block position

### Setup: the physical read order

`HoodieLogFormatReader` yields blocks in a strict sequence determined by two axes:

1. **Log file order**: files are consumed left-to-right from the input `logFiles` list (which the caller pre-sorts)
2. **Block position within a file**: blocks are read sequentially from byte offset 0 to EOF

This gives a total order over all blocks. Call it the **physical scan order**:

```
logFiles = [log1, log2, log3]

physical scan order:
  log1:B1  <  log2:B2  <  log2:B3  <  log3:B4  <  log3:R1
  ───────     ─────────────────────    ─────────────────────
  file 0          file 1                   file 2
```

### Invariant for `orderedInstantsList`: first-seen order tracks physical scan order

An instant is added to `orderedInstantsList` when its **first** gate-passing block is encountered (:290-292). Since the forward scan reads blocks in physical scan order, the list order is:

> **`orderedInstantsList[i]` appeared (first gate-passing block) earlier in physical scan order than `orderedInstantsList[j]` for all `i < j`.**

This is *not* the same as timestamp-sorted order. A counterexample:

```
log1: [B1(i=20250103)]       ← written by a late-arriving writer
log2: [B2(i=20250101)]       ← written by an earlier writer, but to a later log file

orderedInstantsList = ["20250103", "20250101"]
                        ↑ first-seen             ↑ second-seen
```

In practice this is rare because log files are typically ordered by base instant, but the code does **not** sort by timestamp — it relies purely on physical encounter order.

### Invariant for `instantToBlocksMap` value lists: strict physical scan order within an instant

Blocks for the same instant are appended via `logBlocksList.add(logBlock)` (:294) as they appear in the forward scan. So:

> **For a given instant `t`, `instantToBlocksMap.get(t)` is ordered by physical scan order — earlier file first, and within the same file, earlier byte offset first.**

Evidence — there is only one code path that appends:

```java
// :289-295 — the ONLY mutation that adds blocks
List<HoodieLogBlock> logBlocksList = instantToBlocksMap.getOrDefault(instantTime, new ArrayList<>());
if (logBlocksList.isEmpty()) {
    orderedInstantsList.add(instantTime);
}
logBlocksList.add(logBlock);          // ← ArrayList.add = append at tail
instantToBlocksMap.put(instantTime, logBlocksList);
```

`ArrayList.add` appends at the tail. The forward scan visits blocks in physical scan order. So the list is physical scan order.

Concrete example with one instant spanning two files:

```
log1: [B1(i=20250102)]
log2: [B2(i=20250102), B3(i=20250102)]

instantToBlocksMap.get("20250102") = [B1, B2, B3]
                                      ↑    ↑    ↑
                                    log1  log2  log2
                                    (physical scan order preserved)
```

### How Phase 2 uses these ordering guarantees

Phase 2 reverses both:

```java
// iterate instants latest-first (reverse of orderedInstantsList)
for (int i = orderedInstantsList.size() - 1; i >= 0; i--) {
    List<HoodieLogBlock> logBlocks = instantToBlocksMap.get(instantTime);
    Collections.reverse(logBlocks);                    // ← reverse physical order within instant
    logBlocks.forEach(block -> currentInstantLogBlocks.addLast(block));
}
```

Then `processQueuedBlocksForInstant` calls `pollLast()`, which re-reverses the drain order back to physical scan order. The double reversal (once in Phase 2 fill, once in `pollLast` drain) means **blocks are ultimately processed in their original physical scan order** — earliest file, earliest position first.



👤 USER:
fgClassDesign


👤 USER:
org.apache.hudi.common.table.read.buffer.FileGroupRecordBufferLoader#getRecordBuffer, what are 2 items in the pair stands for


🤖 ASSISTANT:
Looking at `DefaultFileGroupRecordBufferLoader.getRecordBuffer()` at line 81:

```java
return Pair.of(recordBuffer, scanLogFiles(readerContext, storage, inputSplit, hoodieTableMetaClient, props,
    readerParameters, readStats, recordBuffer));
```

And `LogScanningRecordBufferLoader.scanLogFiles()` at line 60:

```java
return logRecordReader.getValidBlockInstants();
```

So the pair is:

| Position | Type | What it is |
|---|---|---|
| **Left** | `HoodieFileGroupRecordBuffer<T>` | The record buffer (e.g., `KeyBasedFileGroupRecordBuffer`) — already populated with all log records after `scanLogFiles` returns |
| **Right** | `List<String>` | `validBlockInstants` — the list of commit instant times from log blocks that were actually processed (excluding rolled-back, corrupt, or filtered-out blocks) |

The `validBlockInstants` list is collected during `scanInternal` Pass 2 (`BaseHoodieLogRecordReader.java:361,371`), where each non-compacted or final-compacted instant gets added. It's then used by log-compaction to know which instants were read, enabling optimized log-block scans in subsequent operations. You can see the consumer back in `HoodieFileGroupReader.initRecordIterators()` at line 137:

```java
validBlockInstants = initializationResult.getRight();
```


👤 USER:
Where is the base file content?


🤖 ASSISTANT:
The base file content is **not** in the pair. It's separate.

Looking at `HoodieFileGroupReader.initRecordIterators()` (line 128):

```java
private void initRecordIterators() throws IOException {
    ClosableIterator<T> iter = makeBaseFileIterator();           // ← base file iterator created here
    if (inputSplit.hasNoRecordsToMerge()) {
        this.baseFileIterator = new CloseableMappingIterator<>(...);  // no logs → just use base file directly
    } else {
        this.baseFileIterator = iter;
        Pair<HoodieFileGroupRecordBuffer<T>, List<String>> initializationResult = 
            recordBufferLoader.getRecordBuffer(...);             // ← log scan happens here, returns buffer + instants
        recordBuffer = initializationResult.getLeft();
        validBlockInstants = initializationResult.getRight();
        recordBuffer.setBaseFileIterator(baseFileIterator);      // ← base file iterator injected into buffer
    }
}
```

The base file content lives in `this.baseFileIterator` — a lazy Parquet iterator created by `makeBaseFileIterator()` at line 129. It's **not** part of the `getRecordBuffer` return pair at all. Instead, it's handed to the buffer separately via `recordBuffer.setBaseFileIterator(baseFileIterator)` at line 138.

The base file is only consumed later, lazily, when the consumer calls `recordBuffer.hasNext()` → `doHasNext()` → `baseFileIterator.next()` during Phase B.

So to summarize where each piece of data lives after `initRecordIterators()`:

| Data | Where | When consumed |
|---|---|---|
| Log file records | `recordBuffer.records` map (already populated) | Phase B (merge with base) + Phase C (log-only inserts) |
| Base file records | `baseFileIterator` (lazy, untouched) | Phase B, one at a time via `hasNext()`/`next()` |
| Valid block instants | `validBlockInstants` list | Used by log-compaction later |

