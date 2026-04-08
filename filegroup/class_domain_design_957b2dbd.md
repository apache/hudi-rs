────────────────────────────────────────────────────────────────────────────────
👤 USER:
Given the call stack tree which gives you a procedural view for reading log files of a file slice

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

Limit ourselves to the procedure window from HoodieFileGroupReader to DefaultFileGroupRecordBufferLoader.getRecordBuffer, show me class/domain interaction with visualization. There should be general purpose util class, info they contain/functionality they provide(how they transform input/constructor input to the output made available via API consumed by other classes' caller), then application level class that assembles util for target functionality.

We can give data members, class names, functionality descriptions.Then we should make sure functionality x provided by class A how that is consumed by the other class B.

Also the relevant hiearchy of interfaces should be annotated within the actual child class and how we end up picking that child class.

────────────────────────────────────────────────────────────────────────────────
🤖 ASSISTANT:
Let me read the relevant source files to understand the class interactions.

────────────────────────────────────────────────────────────────────────────────
🤖 ASSISTANT:
  🤖 Agent: Explore HoodieFileGroupReader

────────────────────────────────────────────────────────────────────────────────
🤖 ASSISTANT:
  🤖 Agent: Explore InputSplit and ReaderParameters

────────────────────────────────────────────────────────────────────────────────
🤖 ASSISTANT:
Now I have a complete picture. Here's the class/domain interaction visualization:

---

## Class/Domain Interaction: `HoodieFileGroupReader` → `DefaultFileGroupRecordBufferLoader.getRecordBuffer`

### Layer 1: General-Purpose Utility / Data Classes

These are "bags of data" or simple transformers — no application logic, just holding/converting info.

```
┌─────────────────────────────────────────────────────────────────────┐
│                     UTILITY / DATA CLASSES                          │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌─────────────────────────┐    ┌──────────────────────────────┐   │
│  │     InputSplit           │    │     ReaderParameters          │   │
│  │─────────────────────────│    │──────────────────────────────│   │
│  │ baseFileOption: Option   │    │ useRecordPosition: boolean   │   │
│  │ logFiles: List<LogFile>  │    │ emitDelete: boolean          │   │
│  │ partitionPath: String    │    │ sortOutput: boolean          │   │
│  │ start: long              │    │ allowInflightInstants: bool   │   │
│  │ length: long             │    │──────────────────────────────│   │
│  │─────────────────────────│    │ Constructed via Builder       │   │
│  │ hasNoRecordsToMerge()   │    │ Pure query object — no logic  │   │
│  │ hasLogFiles()            │    └──────────────────────────────┘   │
│  │ getBaseFileOption()      │                                       │
│  └─────────────────────────┘    ┌──────────────────────────────┐   │
│                                  │     HoodieReadStats           │   │
│  ┌─────────────────────────┐    │──────────────────────────────│   │
│  │  BufferedRecordConverter │    │ numInserts/Updates/Deletes   │   │
│  │─────────────────────────│    │ totalLogReadTimeMs            │   │
│  │ Converts engine record T│    │ totalLogRecords/Blocks/Files │   │
│  │ → BufferedRecord<T>     │    │ totalCorrupt/RollbackBlocks  │   │
│  │─────────────────────────│    │──────────────────────────────│   │
│  │ createConverter(         │    │ Mutable stats accumulator    │   │
│  │   iteratorMode,          │    │ Written by: LogScanning,     │   │
│  │   schema, recordCtx,    │    │   UpdateProcessor, buffer    │   │
│  │   orderingFields)       │    │ Read by: caller after scan   │   │
│  │ → adapt based on mode:  │    └──────────────────────────────┘   │
│  │   ENGINE_RECORD → wrap  │                                       │
│  │   RECORD_KEY → extract  │                                       │
│  └─────────────────────────┘                                       │
└─────────────────────────────────────────────────────────────────────┘
```

### Layer 2: Context / Service-Provider Class

```
┌─────────────────────────────────────────────────────────────────────────┐
│              CONTEXT / SERVICE PROVIDER (abstract)                       │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │              HoodieReaderContext<T>                               │   │
│  │  (abstract — engine-specific subclass e.g. SparkReaderContext)   │   │
│  │─────────────────────────────────────────────────────────────────│   │
│  │ storageConfiguration: StorageConfiguration                       │   │
│  │ tableConfig: HoodieTableConfig                                   │   │
│  │ mergeMode: RecordMergeMode                                       │   │
│  │ recordMerger: Option<HoodieRecordMerger>                        │   │
│  │ schemaHandler: FileGroupReaderSchemaHandler<T>                   │   │
│  │ recordContext: RecordContext<T>                                   │   │
│  │ iteratorMode: IteratorMode  (mutable — set before iteration)     │   │
│  │ instantRangeOpt: Option<InstantRange>                            │   │
│  │ keyFilterOpt: Option<Predicate>                                  │   │
│  │─────────────────────────────────────────────────────────────────│   │
│  │ PROVIDES TO CONSUMERS:                                           │   │
│  │  • getFileRecordIterator(path,start,len,schema,..) → Iterator<T>│   │
│  │    [abstract — Spark impl reads Parquet via ColumnarBatchReader] │   │
│  │  • getMergeMode() → RecordMergeMode                              │   │
│  │  • getSchemaHandler() → schemas for read/write                   │   │
│  │  • getRecordContext() → RecordContext for seal/key extraction     │   │
│  │  • getRecordMerger() → merger for combining base+log records     │   │
│  │  • getInstantRange() → time-based filtering of log blocks        │   │
│  │  • applyInstantRangeFilter(iter) → filtered iterator             │   │
│  │  • getRecordSizeEstimator() → for spillable map sizing           │   │
│  │  • getRecordSerializer() → for spillable map disk spill          │   │
│  └─────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
```

### Layer 3: Application-Level Classes — Assembly & Orchestration

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                    APPLICATION-LEVEL ORCHESTRATION                            │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │                    HoodieFileGroupReader<T>                             │  │
│  │  THE TOP-LEVEL ORCHESTRATOR — owns the full read pipeline              │  │
│  │────────────────────────────────────────────────────────────────────────│  │
│  │ readerContext ──────────────→ HoodieReaderContext<T>                    │  │
│  │ inputSplit ─────────────────→ InputSplit                                │  │
│  │ readerParameters ───────────→ ReaderParameters                          │  │
│  │ recordBufferLoader ─────────→ FileGroupRecordBufferLoader<T> (strategy)│  │
│  │ readStats ──────────────────→ HoodieReadStats                           │  │
│  │ bufferedRecordConverter ────→ BufferedRecordConverter<T>                 │  │
│  │ baseFileIterator ───────────→ ClosableIterator<T>  (lazy parquet scan) │  │
│  │ recordBuffer ───────────────→ HoodieFileGroupRecordBuffer<T> (from ↓)  │  │
│  │────────────────────────────────────────────────────────────────────────│  │
│  │ getClosableIterator()                                                   │  │
│  │   └→ getBufferedRecordIterator(ENGINE_RECORD)                           │  │
│  │        ├→ readerContext.setIteratorMode(ENGINE_RECORD)                   │  │
│  │        └→ initRecordIterators()                                         │  │
│  │             ├→ makeBaseFileIterator()                                    │  │
│  │             │    Uses: readerContext.getFileRecordIterator(...)           │  │
│  │             │    Uses: BufferedRecordConverter.createConverter(...)       │  │
│  │             │    Output: this.baseFileIterator = ClosableIterator<T>     │  │
│  │             │                                                            │  │
│  │             └→ IF has records to merge:                                  │  │
│  │                  recordBufferLoader.getRecordBuffer(                     │  │
│  │                    readerContext, storage, inputSplit,                    │  │
│  │                    orderingFieldNames, metaClient, props,                │  │
│  │                    readerParameters, readStats, callback)                │  │
│  │                  → receives Pair<recordBuffer, validBlockInstants>       │  │
│  │                  → recordBuffer.setBaseFileIterator(baseFileIterator)    │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│       │                                                                      │
│       │ delegates to (via strategy interface)                                │
│       ▼                                                                      │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │          DefaultFileGroupRecordBufferLoader<T>                         │  │
│  │  ASSEMBLER — picks the right buffer impl + triggers log scan           │  │
│  │────────────────────────────────────────────────────────────────────────│  │
│  │  implements: FileGroupRecordBufferLoader<T>                            │  │
│  │  extends:    LogScanningRecordBufferLoader                             │  │
│  │  pattern:    Singleton (stateless)                                      │  │
│  │────────────────────────────────────────────────────────────────────────│  │
│  │  getRecordBuffer(readerContext, storage, inputSplit, ...)               │  │
│  │    │                                                                    │  │
│  │    ├─ STEP 1: Read config to determine merge strategy                   │  │
│  │    │   isSkipMerge ← props[MERGE_TYPE] == "skip_merge"                 │  │
│  │    │   partialUpdateModeOpt ← tableConfig                               │  │
│  │    │                                                                    │  │
│  │    ├─ STEP 2: Create UpdateProcessor<T> via factory                     │  │
│  │    │   UpdateProcessor.create(readStats, readerContext,                  │  │
│  │    │     emitDeletes, callback, props)                                   │  │
│  │    │   → StandardUpdateProcessor | PayloadUpdateProcessor               │  │
│  │    │     optionally wrapped in CallbackProcessor                        │  │
│  │    │                                                                    │  │
│  │    ├─ STEP 3: Instantiate buffer (STRATEGY SELECTION)  ──────────┐     │  │
│  │    │   see decision tree below                                    │     │  │
│  │    │                                                              │     │  │
│  │    └─ STEP 4: scanLogFiles(..., recordBuffer) ← inherited        │     │  │
│  │         builds HoodieMergedLogRecordReader                        │     │  │
│  │         which calls recordBuffer.processDataBlock()               │     │  │
│  │         and recordBuffer.processDeleteBlock()                     │     │  │
│  │         populating the buffer's ExternalSpillableMap              │     │  │
│  │         → returns validBlockInstants                              │     │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────────┘
```

### Buffer Strategy Selection (Decision Tree in `DefaultFileGroupRecordBufferLoader`)

```
                    ┌─────────────────────────┐
                    │  isSkipMerge?            │
                    │  (MERGE_TYPE="skip_merge")│
                    └──────┬──────────┬────────┘
                       YES │          │ NO
                           ▼          ▼
            ┌──────────────────┐  ┌──────────────────┐
            │ Unmerged          │  │ sortOutputs()?    │
            │ FileGroupRecord   │  └───┬──────────┬───┘
            │ Buffer            │  YES │          │ NO
            └──────────────────┘      ▼          ▼
                        ┌──────────────────┐  ┌──────────────────────┐
                        │ SortedKeyBased    │  │ useRecordPosition()  │
                        │ FileGroupRecord   │  │ && baseFile present? │
                        │ Buffer            │  └───┬──────────┬──────┘
                        └──────────────────┘   YES │          │ NO
                                                   ▼          ▼
                                  ┌──────────────────┐  ┌──────────────────┐
                                  │ PositionBased     │  │ ★ KeyBased       │
                                  │ FileGroupRecord   │  │ FileGroupRecord  │
                                  │ Buffer            │  │ Buffer (DEFAULT) │
                                  └──────────────────┘  └──────────────────┘
```

### Interface Hierarchy for Record Buffers

```
    «interface»                              «interface»
    HoodieFileGroupRecordBuffer<T>           FileGroupRecordBufferLoader<T>
    ┌──────────────────────────────┐         ┌────────────────────────────────┐
    │ getBufferType()              │         │ getRecordBuffer(...)            │
    │ processDataBlock(...)        │         │   → Pair<Buffer, Instants>     │
    │ processNextDataRecord(...)   │         ├────────────────────────────────┤
    │ processDeleteBlock(...)      │         │ static createDefault()         │
    │ setBaseFileIterator(...)     │         │   → DefaultFileGroupRecordBuf… │
    │ hasNext() / next()           │         │ static createStreamingRecords… │
    │ close()                      │         │ static createReusable(ctx)     │
    └──────────────┬───────────────┘         └──────────────┬─────────────────┘
                   │ implements                              │ implements
                   ▼                                         ▼
    «abstract»                               ┌────────────────────────────────┐
    FileGroupRecordBuffer<T>                 │ DefaultFileGroupRecordBuffer   │
    ┌──────────────────────────────┐         │ Loader<T>                      │
    │ records: ExternalSpillableMap│         │────────────────────────────────│
    │ readerContext                 │         │ extends LogScanningRecord      │
    │ bufferedRecordMerger         │         │         BufferLoader           │
    │ updateProcessor              │         │ Singleton (INSTANCE)           │
    │ baseFileIterator             │         └────────────────────────────────┘
    │──────────────────────────────│                          │
    │ abstract doHasNext()         │                          │ extends
    │ hasNext() → doHasNext()      │                          ▼
    │ hasNextBaseRecord(rec)       │         «abstract»
    │ hasNextLogRecord()           │         LogScanningRecordBufferLoader
    └──────────────┬───────────────┘         ┌────────────────────────────────┐
                   │ extends                 │ scanLogFiles(ctx, storage,     │
          ┌────────┼────────┬────────┐       │   inputSplit, metaClient,      │
          ▼        ▼        ▼        ▼       │   props, params, stats, buf)  │
    ┌──────────┐┌────────┐┌────────┐┌──────┐ │ → builds HoodieMergedLog      │
    │KeyBased  ││Sorted  ││Position││Unmrgd│ │   RecordReader, which calls   │
    │FGRecord  ││KeyBased││Based   ││FGRec │ │   buffer.processDataBlock()   │
    │Buffer    ││FGRBuf  ││FGRBuf  ││Buf   │ │ → returns validBlockInstants  │
    │──────────││────────││────────││──────│ └────────────────────────────────┘
    │KEY_BASED ││KEY_BASE││POS_BAS ││UNMRGD│
    │_MERGE    ││D_MERGE ││ED_MERGE││      │
    └──────────┘└────────┘└────────┘└──────┘
```

### Data Flow: How Class A's Output Feeds Class B

```
 ① HoodieFileGroupReader.initRecordIterators()
    ─────────────────────────────────────────────
    CALLS: readerContext.getFileRecordIterator(basePath, start, len, dataSchema, reqSchema, storage)
    GETS:  ClosableIterator<T>  ← lazy Parquet row iterator (not yet consumed)
    STORES: this.baseFileIterator

 ② HoodieFileGroupReader.initRecordIterators()
    ─────────────────────────────────────────────
    CALLS: recordBufferLoader.getRecordBuffer(
             readerContext,      ← provides: mergeMode, schemaHandler, recordMerger, 
                                    sizeEstimator, serializer, instantRange
             storage,            ← passed through to log reader
             inputSplit,         ← provides: logFiles list, baseFileOption, partitionPath
             orderingFieldNames, ← for merge ordering
             metaClient,         ← provides: tableConfig (partialUpdateMode)
             props,              ← provides: MERGE_TYPE config
             readerParameters,   ← provides: sortOutputs(), useRecordPosition(), 
                                    emitDeletes(), allowInflightInstants()
             readStats,          ← mutable accumulator, written during scan
             callback)           ← optional update callback

 ③ DefaultFileGroupRecordBufferLoader.getRecordBuffer()
    ─────────────────────────────────────────────────────
    READS FROM readerContext:
      • getMergeMode()                → decides UpdateProcessor variant
      • (via schemaHandler later)     → schema for record conversion in buffer
    READS FROM metaClient:
      • tableConfig.getPartialUpdateMode() → passed to buffer constructor
    READS FROM props:
      • HoodieReaderConfig.MERGE_TYPE → isSkipMerge decision
    READS FROM readerParameters:
      • sortOutputs()           → SortedKeyBased branch
      • useRecordPosition()     → PositionBased branch
      • emitDeletes()           → passed to UpdateProcessor
      • allowInflightInstants() → passed to log scanner

    CREATES: UpdateProcessor<T> via UpdateProcessor.create(
               readStats, readerContext, emitDeletes, callback, props)
             → StandardUpdateProcessor (default)
             → PayloadUpdateProcessor (if CUSTOM merge + non-meta payload)
             → optionally wrapped in CallbackProcessor

    CREATES: FileGroupRecordBuffer<T> (one of 4 variants)
             constructor receives: readerContext, metaClient, mergeMode,
               partialUpdateModeOpt, props, orderingFieldNames, updateProcessor
             → internally creates ExternalSpillableMap for log records

 ④ LogScanningRecordBufferLoader.scanLogFiles()  [called by ③]
    ───────────────────────────────────────────────
    READS FROM inputSplit:
      • getLogFiles()       → list of log files to scan
      • getPartitionPath()  → partition context
    READS FROM readerContext:
      • getInstantRange()   → time-based block filtering
    READS FROM props:
      • MAX_DFS_STREAM_BUFFER_SIZE → I/O buffer sizing

    CREATES: HoodieMergedLogRecordReader (via builder)
             → receives the recordBuffer instance
             → calls recordBuffer.processDataBlock() / processDeleteBlock()
               for each log block encountered
             → this EAGERLY populates recordBuffer.records (the spillable map)

    WRITES TO readStats:
      totalLogReadTimeMs, totalUpdatedRecordsCompacted,
      totalLogFilesCompacted, totalLogRecords, totalLogBlocks,
      totalCorruptLogBlock, totalRollbackBlocks

    RETURNS: validBlockInstants (List<String>)

 ⑤ Back in HoodieFileGroupReader.initRecordIterators()
    ─────────────────────────────────────────────────────
    STORES: recordBuffer  (now populated with all log records)
    STORES: validBlockInstants
    CALLS:  recordBuffer.setBaseFileIterator(baseFileIterator)
            → the buffer now owns both data sources for lazy merge iteration
```

### Summary Table

| Class | Role | Key Inputs | Key Outputs | Consumed By |
|-------|------|-----------|-------------|-------------|
| `InputSplit` | Data holder | baseFile, logFiles, partition, byte range | Accessors only | FileGroupReader, BufferLoader, LogScanning |
| `ReaderParameters` | Data holder | 4 booleans (position, delete, sort, inflight) | Accessors only | FileGroupReader, BufferLoader |
| `HoodieReadStats` | Mutable accumulator | — | insert/update/delete counts, log metrics | Written by LogScanning + UpdateProcessor; read by caller |
| `BufferedRecordConverter` | Factory + converter | iteratorMode, schema, recordCtx | `convert(T) → BufferedRecord<T>` | FileGroupRecordBuffer during merge iteration |
| `HoodieReaderContext<T>` | Abstract service provider | storageConfig, tableConfig, schemas | fileRecordIterator, mergeMode, merger, sizeEstimator, serializer | FileGroupReader (base file iter), BufferLoader (merge config), Buffer (merge ops) |
| `UpdateProcessor<T>` | Strategy (factory-created) | readStats, readerCtx, emitDeletes, callback | `processUpdate(key, prev, merged, isDel) → record` | FileGroupRecordBuffer during base+log merge |
| `HoodieFileGroupReader<T>` | **Top orchestrator** | all of the above via Builder | `ClosableIterator<T>` of merged records | Engine (Spark/Flink query) |
| `DefaultFileGroupRecordBufferLoader<T>` | **Assembler** (singleton) | readerCtx, inputSplit, params, props | Pair<populated buffer, validInstants> | FileGroupReader.initRecordIterators() |
| `LogScanningRecordBufferLoader` | **Log scan executor** | inputSplit.logFiles, buffer, readerCtx | populates buffer + returns validInstants | DefaultFileGroupRecordBufferLoader (via inheritance) |
| `KeyBasedFileGroupRecordBuffer<T>` | **Merge buffer** (default) | log records (eagerly loaded), base iter (lazy) | merged `BufferedRecord<T>` stream via hasNext/next | FileGroupReader iteration loop |

────────────────────────────────────────────────────────────────────────────────
👤 USER:
cool, now given the even lower layer procedural view
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



let's do the same analysis

────────────────────────────────────────────────────────────────────────────────
🤖 ASSISTANT:
  🤖 Agent: Explore log scanning classes

────────────────────────────────────────────────────────────────────────────────
🤖 ASSISTANT:
Here's the full class/domain interaction visualization for the log-scanning layer:

---

## Class/Domain Interaction: `DefaultFileGroupRecordBufferLoader.getRecordBuffer` → Log Scan Internals

### Layer 1: Low-Level I/O & Data Containers

These classes are pure data holders or I/O primitives — no merge logic, no application decisions.

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                    I/O & DATA CONTAINERS                                     │
│                                                                              │
│  ┌──────────────────────────────┐     ┌──────────────────────────────────┐  │
│  │   HoodieLogFormatReader       │     │   HoodieLogBlock (abstract)      │  │
│  │──────────────────────────────│     │──────────────────────────────────│  │
│  │ logFiles: List<HoodieLogFile> │     │ logBlockHeader: Map<HeaderType,  │  │
│  │ currentReader: LogFileReader  │     │                      String>     │  │
│  │ storage: HoodieStorage        │     │ blockContentLocation: Option     │  │
│  │ readerSchema: HoodieSchema    │     │ content: Option<byte[]>          │  │
│  │ bufferSize: int               │     │ readBlockLazily: boolean         │  │
│  │──────────────────────────────│     │──────────────────────────────────│  │
│  │ Iterates across multiple log  │     │ HeaderMetadataType:              │  │
│  │ files sequentially, yielding  │     │   INSTANT_TIME                   │  │
│  │ HoodieLogBlock objects one    │     │   TARGET_INSTANT_TIME            │  │
│  │ at a time via hasNext/next    │     │   SCHEMA                         │  │
│  │                               │     │   COMMAND_BLOCK_TYPE             │  │
│  │ Consumed by:                  │     │   COMPACTED_BLOCK_TIMES          │  │
│  │   BaseHoodieLogRecordReader   │     │   RECORD_POSITIONS               │  │
│  │   .scanInternal() Pass 1      │     │──────────────────────────────────│  │
│  └──────────────────────────────┘     │ getBlockType() → LogBlockType    │  │
│                                        │ isDataOrDeleteBlock()             │  │
│                                        │ isCompactedLogBlock()             │  │
│                                        │ getRecordPositions() → Bitmap    │  │
│                                        └───────────┬──────────────────────┘  │
│                                           extends  │                         │
│                              ┌─────────────────────┼──────────────┐          │
│                              ▼                     ▼              ▼          │
│  ┌──────────────────────────────┐  ┌─────────────────────────────────────┐  │
│  │   HoodieDataBlock (abstract)  │  │   HoodieDeleteBlock                 │  │
│  │──────────────────────────────│  │─────────────────────────────────────│  │
│  │ records: Option<List<Record>> │  │ recordsToDelete: DeleteRecord[]     │  │
│  │ keyFieldName: String          │  │─────────────────────────────────────│  │
│  │ readerSchema: HoodieSchema    │  │ getRecordsToDelete() → DeleteRec[]  │  │
│  │──────────────────────────────│  │ getBlockType() → DELETE_BLOCK        │  │
│  │ getEngineRecordIterator(ctx)  │  │                                     │  │
│  │   → ClosableIterator<T>       │  │ Consumed by:                        │  │
│  │ getSchema() → writer schema   │  │   recordBuffer.processDeleteBlock() │  │
│  │ containsPartialUpdates()      │  └─────────────────────────────────────┘  │
│  │ getBlockType() →              │                                           │
│  │   AVRO/PARQUET/HFILE_DATA_BLK│  ┌─────────────────────────────────────┐  │
│  │                               │  │   DeleteRecord                      │  │
│  │ Consumed by:                  │  │─────────────────────────────────────│  │
│  │   recordBuffer                │  │ hoodieKey: HoodieKey                │  │
│  │     .processDataBlock()       │  │   (recordKey + partitionPath)       │  │
│  └──────────────────────────────┘  │ orderingValue: Comparable<?>         │  │
│                                     │─────────────────────────────────────│  │
│                                     │ Pure value object.                   │  │
│                                     │ Created from: HoodieDeleteBlock      │  │
│                                     │ Consumed by: processNextDeletedRec() │  │
│                                     └─────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────────┘
```

### Layer 2: Record Representation & Merge Primitives

```
┌──────────────────────────────────────────────────────────────────────────────┐
│              RECORD REPRESENTATION & MERGE PRIMITIVES                         │
│                                                                              │
│  ┌──────────────────────────────────┐  ┌──────────────────────────────────┐  │
│  │   BufferedRecord<T>               │  │   BufferedRecords (factory)      │  │
│  │──────────────────────────────────│  │──────────────────────────────────│  │
│  │ recordKey: String                 │  │ Static factory methods:          │  │
│  │ record: T  (engine-native row)    │  │                                  │  │
│  │ orderingValue: Comparable         │  │ fromEngineRecord(T, schema,      │  │
│  │ schemaId: Integer                 │  │   recordCtx, orderingFields,     │  │
│  │ hoodieOperation: HoodieOperation  │  │   isDelete)                      │  │
│  │──────────────────────────────────│  │   → extracts recordKey via ctx   │  │
│  │ isDelete()                        │  │   → extracts orderingVal via ctx │  │
│  │ toBinary(recordCtx)               │  │   → wraps in BufferedRecord<T>   │  │
│  │   → serializes T for disk spill   │  │                                  │  │
│  │ seal(recordCtx) → finalize record │  │ fromDeleteRecord(deleteRecord,   │  │
│  │ replaceRecord(T) → swap payload   │  │   recordCtx)                     │  │
│  │──────────────────────────────────│  │   → creates BufferedRecord with  │  │
│  │ Universal currency flowing through│  │     null record, isDelete=true   │  │
│  │ the entire merge pipeline.        │  └──────────────────────────────────┘  │
│  │ Created by: BufferedRecords       │                                        │
│  │ Stored in: ExternalSpillableMap   │  ┌──────────────────────────────────┐  │
│  │ Merged by: BufferedRecordMerger   │  │   DeleteContext                  │  │
│  │ Emitted by: FileGroupRecordBuffer │  │──────────────────────────────────│  │
│  └──────────────────────────────────┘  │ customDeleteMarkerKeyValue:      │  │
│                                         │   Option<Pair<String,String>>    │  │
│  ┌──────────────────────────────────┐  │ hasBuiltInDeleteField: boolean   │  │
│  │   «interface»                     │  │ hoodieOperationPos: int          │  │
│  │   BufferedRecordMerger<T>         │  │──────────────────────────────────│  │
│  │──────────────────────────────────│  │ Knows HOW to detect deletes      │  │
│  │ deltaMerge(newRec, existingRec)   │  │ from schema metadata.            │  │
│  │   → Option<BufferedRecord<T>>     │  │ Consumed by: FileGroupRecord-    │  │
│  │   Log-vs-log merge: keeps latest  │  │   Buffer constructor             │  │
│  │   by orderingValue                │  │                                  │  │
│  │                                   │  └──────────────────────────────────┘  │
│  │ deltaMerge(deleteRec, existingRec)│                                        │
│  │   → Option<DeleteRecord>          │  ┌──────────────────────────────────┐  │
│  │   Delete-vs-existing merge        │  │   ExternalSpillableMap<K,V>      │  │
│  │                                   │  │   implements Map<K,V>            │  │
│  │ finalMerge(olderRec, newerRec)    │  │──────────────────────────────────│  │
│  │   → BufferedRecord<T>             │  │ maxInMemorySizeInBytes: long     │  │
│  │   Base-file-vs-log merge (later)  │  │ inMemoryMap: HashMap<K,V>       │  │
│  │──────────────────────────────────│  │ diskBasedMap: DiskMap<K,V>       │  │
│  │ Consumed by:                      │  │   (ROCKS_DB or BITCASK)          │  │
│  │   KeyBasedFileGroupRecordBuffer   │  │ currentInMemoryMapSize: long     │  │
│  │   in processNextDataRecord() and  │  │──────────────────────────────────│  │
│  │   processNextDeletedRecord()      │  │ put(key, val):                   │  │
│  └──────────────────────────────────┘  │   if mem full → spill to disk    │  │
│                                         │   if key exists in mem → update  │  │
│                                         │ get(key):                         │  │
│                                         │   check mem first, then disk     │  │
│                                         │──────────────────────────────────│  │
│                                         │ Backing store for all log records│  │
│                                         │ in FileGroupRecordBuffer.records │  │
│                                         │ K = Serializable (record key)    │  │
│                                         │ V = BufferedRecord<T>            │  │
│                                         └──────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────────┘
```

### Layer 3: Application-Level — Scan Orchestration & Buffer Population

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                    SCAN ORCHESTRATION & BUFFER POPULATION                         │
│                                                                                  │
│  ┌────────────────────────────────────────────────────────────────────────────┐  │
│  │           BaseHoodieLogRecordReader<T>  (abstract)                         │  │
│  │  THE SCAN ENGINE — two-pass algorithm over log blocks                      │  │
│  │────────────────────────────────────────────────────────────────────────────│  │
│  │ logFiles: List<HoodieLogFile>         instantRange: Option<InstantRange>   │  │
│  │ storage: HoodieStorage                forceFullScan: boolean               │  │
│  │ recordBuffer: HoodieFileGroupRecordBuffer<T>  ← target for processed data │  │
│  │ currentInstantLogBlocks: Deque<HoodieLogBlock> ← staging area             │  │
│  │ validBlockInstants: List<String>      ← output: which instants survived   │  │
│  │ latestInstantTime: String             payloadClassFQN: String             │  │
│  │ readerSchema: HoodieSchema            reverseReader: boolean              │  │
│  │ totalLogFiles/Records/Blocks/Rollbacks/CorruptBlocks: AtomicLong (stats)  │  │
│  │────────────────────────────────────────────────────────────────────────────│  │
│  │                                                                            │  │
│  │  scanInternal(keySpecOpt, skipProcessingBlocks):                           │  │
│  │                                                                            │  │
│  │  ┌─ PASS 1: Sequential block collection ──────────────────────────────┐   │  │
│  │  │  reader = new HoodieLogFormatReader(storage, logFiles, ...)         │   │  │
│  │  │  while reader.hasNext():                                            │   │  │
│  │  │    block = reader.next()   ← yields HoodieLogBlock                  │   │  │
│  │  │    │                                                                │   │  │
│  │  │    ├─ CORRUPT_BLOCK → totalCorruptBlocks++, skip                    │   │  │
│  │  │    ├─ ROLLBACK (COMMAND_BLOCK) →                                    │   │  │
│  │  │    │    targetInstant = header[TARGET_INSTANT_TIME]                  │   │  │
│  │  │    │    targetRollbackInstants.add(targetInstant)                    │   │  │
│  │  │    │    totalRollbacks++                                             │   │  │
│  │  │    └─ DATA or DELETE block →                                        │   │  │
│  │  │         blockInstant = header[INSTANT_TIME]                          │   │  │
│  │  │         if NOT rolled back AND within instantRange:                  │   │  │
│  │  │           instantToBlocksMap[blockInstant].add(block)                │   │  │
│  │  │           totalLogBlocks++                                           │   │  │
│  │  └────────────────────────────────────────────────────────────────────┘   │  │
│  │                                                                            │  │
│  │  ┌─ PASS 2: Reverse-chronological instant processing ─────────────────┐   │  │
│  │  │  orderedInstantsList = instantToBlocksMap.keySet() sorted            │   │  │
│  │  │  for instant in REVERSE(orderedInstantsList):                        │   │  │
│  │  │    blocks = instantToBlocksMap[instant]                              │   │  │
│  │  │    │                                                                │   │  │
│  │  │    ├─ if block.isCompactedLogBlock():                               │   │  │
│  │  │    │    compactedInstants = header[COMPACTED_BLOCK_TIMES]            │   │  │
│  │  │    │    → skip all blocks from those compacted instants             │   │  │
│  │  │    │    (compaction already includes their data)                     │   │  │
│  │  │    │                                                                │   │  │
│  │  │    └─ add blocks to currentInstantLogBlocks deque                   │   │  │
│  │  │       validBlockInstants.add(instant)                               │   │  │
│  │  │                                                                     │   │  │
│  │  │  processQueuedBlocksForInstant(currentInstantLogBlocks, keySpec)     │   │  │
│  │  └────────────────────────────────────────────────────────────────────┘   │  │
│  │                                                                            │  │
│  │  ┌─ BLOCK DISPATCH: processQueuedBlocksForInstant(deque, keySpec) ────┐   │  │
│  │  │  while deque not empty:                                             │   │  │
│  │  │    block = deque.pollLast()  ← oldest block first                   │   │  │
│  │  │    │                                                                │   │  │
│  │  │    ├─ if DATA block:                                                │   │  │
│  │  │    │    recordBuffer.processDataBlock(block, keySpecOpt)  ──────────┼───┼──→ [Buffer]
│  │  │    │                                                                │   │  │
│  │  │    └─ if DELETE block:                                              │   │  │
│  │  │         recordBuffer.processDeleteBlock(block)  ────────────────────┼───┼──→ [Buffer]
│  │  └────────────────────────────────────────────────────────────────────┘   │  │
│  └────────────────────────────────────────────────────────────────────────────┘  │
│       │ extends                                                                  │
│       ▼                                                                          │
│  ┌────────────────────────────────────────────────────────────────────────────┐  │
│  │          HoodieMergedLogRecordReader<T>                                     │  │
│  │  Thin subclass — adds timing + builder entry point                         │  │
│  │────────────────────────────────────────────────────────────────────────────│  │
│  │ implements: Iterable<BufferedRecord<T>>, Closeable                         │  │
│  │ extends:    BaseHoodieLogRecordReader<T>                                   │  │
│  │────────────────────────────────────────────────────────────────────────────│  │
│  │ timer: HoodieTimer                                                         │  │
│  │ numMergedRecordsInLog: long                                                │  │
│  │ totalTimeTakenToReadAndMergeBlocks: long                                   │  │
│  │────────────────────────────────────────────────────────────────────────────│  │
│  │ performScan():                                                             │  │
│  │   timer.startTimer()                                                       │  │
│  │   scanInternal(keySpec, false)   ← delegates to parent                     │  │
│  │   totalTimeTaken = timer.endTimer()                                        │  │
│  │   numMergedRecords = recordBuffer.size()                                   │  │
│  │────────────────────────────────────────────────────────────────────────────│  │
│  │ Created by: LogScanningRecordBufferLoader.scanLogFiles() via Builder       │  │
│  │ Builder wires: readerContext, storage, logFiles, instantRange,             │  │
│  │   partition, recordBuffer, metaClient, allowInflightInstants               │  │
│  └────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                  │
│       ┌──── receives blocks from scan engine via processDataBlock/Delete ────┐   │
│       ▼                                                                      │   │
│  ┌────────────────────────────────────────────────────────────────────────────┐  │
│  │          KeyBasedFileGroupRecordBuffer<T>                                   │  │
│  │  THE MERGE BUFFER — accumulates & deduplicates log records by key          │  │
│  │────────────────────────────────────────────────────────────────────────────│  │
│  │ «interface» HoodieFileGroupRecordBuffer<T>                                 │  │
│  │   └─ «abstract» FileGroupRecordBuffer<T>                                  │  │
│  │        └─ KeyBasedFileGroupRecordBuffer<T>  (this class)                   │  │
│  │────────────────────────────────────────────────────────────────────────────│  │
│  │ records: ExternalSpillableMap<Serializable, BufferedRecord<T>>             │  │
│  │ bufferedRecordMerger: BufferedRecordMerger<T>                              │  │
│  │ readerContext: HoodieReaderContext<T>                                       │  │
│  │ deleteContext: DeleteContext                                                │  │
│  │ totalLogRecords: long                                                      │  │
│  │────────────────────────────────────────────────────────────────────────────│  │
│  │                                                                            │  │
│  │ processDataBlock(dataBlock, keySpecOpt):                                   │  │
│  │   iter = getRecordsIterator(dataBlock, keySpecOpt)                         │  │
│  │     → dataBlock.getEngineRecordIterator(readerContext)                      │  │
│  │     → optionally applies schema evolution transformer                      │  │
│  │   for each engine record T:                                                │  │
│  │     bufferedRec = BufferedRecords.fromEngineRecord(                         │  │
│  │       record, schema, recordCtx, orderingFields, isDelete)                 │  │
│  │     processNextDataRecord(bufferedRec, recordKey)                          │  │
│  │                                                                            │  │
│  │ processNextDataRecord(newRecord, key):                                     │  │
│  │   existingRecord = records.get(key)     ← spillable map lookup             │  │
│  │   totalLogRecords++                                                        │  │
│  │   bufferedRecordMerger                                                     │  │
│  │     .deltaMerge(newRecord, existingRecord)                                 │  │
│  │     .ifPresent(merged →                                                    │  │
│  │       records.put(key, merged.toBinary(recordCtx)))                        │  │
│  │              ↑                     ↑                                       │  │
│  │              │                     └─ serialize T for possible disk spill   │  │
│  │              └─ spillable map: in-memory up to threshold, then to disk     │  │
│  │                                                                            │  │
│  │ processDeleteBlock(deleteBlock):                                           │  │
│  │   for deleteRecord in deleteBlock.getRecordsToDelete():                    │  │
│  │     processNextDeletedRecord(deleteRecord, recordKey)                      │  │
│  │                                                                            │  │
│  │ processNextDeletedRecord(deleteRecord, key):                               │  │
│  │   existingRecord = records.get(key)                                        │  │
│  │   bufferedRecordMerger                                                     │  │
│  │     .deltaMerge(deleteRecord, existingRecord)                              │  │
│  │     .ifPresent(surviving →                                                 │  │
│  │       records.put(key, BufferedRecords.fromDeleteRecord(                   │  │
│  │         surviving, recordCtx)))                                            │  │
│  └────────────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### Data Flow: Step-by-Step Record Journey

```
 ① HoodieLogFormatReader.next()
    ──────────────────────────────
    INPUT:  raw bytes from log file on storage
    OUTPUT: HoodieLogBlock (with lazy content — header already parsed)
    ─────────────────────────────────────────────────────────────────
    Key header fields extracted:
      INSTANT_TIME           → which commit wrote this block
      TARGET_INSTANT_TIME    → (rollback blocks) which instant to undo
      COMPACTED_BLOCK_TIMES  → (compacted blocks) which instants were folded in
      SCHEMA                 → writer schema for data blocks


 ② BaseHoodieLogRecordReader.scanInternal() — Pass 1
    ──────────────────────────────────────────────────
    INPUT:  stream of HoodieLogBlock from ①
    OUTPUT: instantToBlocksMap: Map<String, List<HoodieLogBlock>>
            targetRollbackInstants: Set<String>

    LOGIC:  Classifies each block:
            CORRUPT  → counted, skipped
            COMMAND (rollback) → target instant added to rollback set
            DATA/DELETE → if not rolled back & within instantRange,
                          grouped by instant time


 ③ BaseHoodieLogRecordReader.scanInternal() — Pass 2
    ──────────────────────────────────────────────────
    INPUT:  instantToBlocksMap from ②
    OUTPUT: currentInstantLogBlocks: Deque<HoodieLogBlock>  (ordered oldest→newest)
            validBlockInstants: List<String>

    LOGIC:  Iterates instants NEWEST→OLDEST:
            If a block is a compacted log block, marks its constituent
            instants as "already included" → those instants' blocks get skipped.
            Surviving blocks are pushed onto the deque.
            Net effect: deduplication at the instant level.


 ④ BaseHoodieLogRecordReader.processQueuedBlocksForInstant()
    ──────────────────────────────────────────────────────────
    INPUT:  deque of HoodieLogBlock (oldest first via pollLast)
    OUTPUT: recordBuffer is mutated (records map populated)

    DISPATCHES:
      HoodieDataBlock   → recordBuffer.processDataBlock(block, keySpec)
      HoodieDeleteBlock → recordBuffer.processDeleteBlock(block)


 ⑤ KeyBasedFileGroupRecordBuffer.processDataBlock()
    ─────────────────────────────────────────────────
    INPUT:  HoodieDataBlock + optional KeySpec filter
    USES:   dataBlock.getEngineRecordIterator(readerContext)
              → deserializes block bytes into engine-native rows (T)
            composeEvolvedSchemaTransformer(dataBlock)
              → if writer schema ≠ reader schema, creates projection function
            BufferedRecords.fromEngineRecord(record, schema, ctx, orderingFields, isDelete)
              → extracts recordKey and orderingValue from the engine row
              → wraps as BufferedRecord<T>
    CALLS:  processNextDataRecord(bufferedRecord, recordKey)


 ⑥ KeyBasedFileGroupRecordBuffer.processNextDataRecord()
    ─────────────────────────────────────────────────────
    INPUT:  BufferedRecord<T> (new from log), Serializable key
    USES:   records.get(key)  → ExternalSpillableMap lookup
              → checks HashMap first (O(1)), then DiskMap if spilled
            bufferedRecordMerger.deltaMerge(newRecord, existingRecord)
              → compares orderingValue: keeps the one with higher ordering
              → if existingRecord is null, new record wins trivially
    OUTPUT: records.put(key, merged.toBinary(recordCtx))
              → toBinary() serializes the engine record T for disk-spill safety
              → ExternalSpillableMap: if inMemorySize > threshold, spills to disk
                (RocksDB or BitCask depending on config)


 ⑦ KeyBasedFileGroupRecordBuffer.processDeleteBlock()
    ──────────────────────────────────────────────────
    INPUT:  HoodieDeleteBlock
    USES:   deleteBlock.getRecordsToDelete() → DeleteRecord[]
              each DeleteRecord has: recordKey, partitionPath, orderingValue
    CALLS:  processNextDeletedRecord(deleteRecord, recordKey)


 ⑧ KeyBasedFileGroupRecordBuffer.processNextDeletedRecord()
    ────────────────────────────────────────────────────────
    INPUT:  DeleteRecord + Serializable key
    USES:   records.get(key)  → existing entry lookup
            bufferedRecordMerger.deltaMerge(deleteRecord, existingRecord)
              → compares ordering: delete wins if ordering ≥ existing
              → returns Option<DeleteRecord> (surviving delete)
    OUTPUT: records.put(key, BufferedRecords.fromDeleteRecord(surviving, recordCtx))
              → creates BufferedRecord with record=null, isDelete=true
```

### How Class A's Functionality Is Consumed by Class B

| Producer (Class A) | What It Provides | Consumer (Class B) | How B Uses It |
|---|---|---|---|
| `HoodieLogFormatReader` | `next()` → `HoodieLogBlock` | `BaseHoodieLogRecordReader.scanInternal()` Pass 1 | Reads header to classify block type, build `instantToBlocksMap` |
| `HoodieLogBlock` | `header[INSTANT_TIME]` | `BaseHoodieLogRecordReader` | Groups blocks by instant, filters by `instantRange` |
| `HoodieLogBlock` | `header[TARGET_INSTANT_TIME]` | `BaseHoodieLogRecordReader` | Identifies which instant a rollback block undoes |
| `HoodieLogBlock` | `header[COMPACTED_BLOCK_TIMES]` | `BaseHoodieLogRecordReader` Pass 2 | Marks compacted instants to skip (already folded in) |
| `HoodieDataBlock` | `getEngineRecordIterator(ctx)` → `Iterator<T>` | `KeyBasedFGRecordBuffer.processDataBlock()` | Deserializes block bytes into engine-native rows for merge |
| `HoodieDataBlock` | `getSchema()` (writer schema) | `FileGroupRecordBuffer.composeEvolvedSchemaTransformer()` | Detects schema evolution, builds projection function if needed |
| `HoodieDeleteBlock` | `getRecordsToDelete()` → `DeleteRecord[]` | `KeyBasedFGRecordBuffer.processDeleteBlock()` | Iterates delete entries to merge against existing map entries |
| `BufferedRecords` (factory) | `fromEngineRecord(T,...)` → `BufferedRecord<T>` | `KeyBasedFGRecordBuffer.processDataBlock()` | Wraps raw engine row with key + ordering for map insertion |
| `BufferedRecords` (factory) | `fromDeleteRecord(del, ctx)` → `BufferedRecord<T>` | `KeyBasedFGRecordBuffer.processNextDeletedRecord()` | Creates tombstone entry (record=null) for map insertion |
| `BufferedRecordMerger` | `deltaMerge(new, existing)` → `Option<BufferedRecord>` | `KeyBasedFGRecordBuffer.processNextDataRecord()` | Resolves conflicts: latest-by-ordering wins |
| `Buff

────────────────────────────────────────────────────────────────────────────────
🤖 ASSISTANT:
Here is the full class/domain interaction visualization for the lower layer:

---

## Class/Domain Interaction: `DefaultFileGroupRecordBufferLoader.getRecordBuffer` → `scanInternal`

### Layer 1: General-Purpose Utility / Data Classes

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                      UTILITY / DATA / VALUE CLASSES                          │
│                                                                              │
│  ┌──────────────────────────────┐   ┌──────────────────────────────────────┐ │
│  │       DeleteRecord            │   │       BufferedRecord<T>              │ │
│  │──────────────────────────────│   │──────────────────────────────────────│ │
│  │ hoodieKey: HoodieKey          │   │ recordKey: String                    │ │
│  │ orderingValue: Comparable     │   │ record: T   (engine-native row)     │ │
│  │──────────────────────────────│   │ orderingValue: Comparable            │ │
│  │ Immutable value object.       │   │ schemaId: Integer                    │ │
│  │ Factory: create(key, part,    │   │ hoodieOperation: HoodieOperation    │ │
│  │   orderingVal)                │   │──────────────────────────────────────│ │
│  │                               │   │ isDelete() → checks operation       │ │
│  │ CONSUMED BY:                  │   │ toBinary(ctx) → serialize record T  │ │
│  │  • HoodieDeleteBlock (array)  │   │   for spillable map storage         │ │
│  │  • BufferedRecords factory    │   │ seal(ctx) → finalize for output     │ │
│  │  • BufferedRecordMerger       │   │                                      │ │
│  │    (deltaMerge overload)      │   │ CONSUMED BY: everything — this is   │ │
│  └──────────────────────────────┘   │   the universal record envelope     │ │
│                                      └──────────────────────────────────────┘ │
│                                                                              │
│  ┌──────────────────────────────┐   ┌──────────────────────────────────────┐ │
│  │       DeleteContext           │   │       KeySpec (interface)            │ │
│  │──────────────────────────────│   │──────────────────────────────────────│ │
│  │ customDeleteMarkerKeyValue    │   │ getKeys(): List<String>             │ │
│  │ hasBuiltInDeleteField         │   │ isFullKey(): boolean                │ │
│  │ hoodieOperationPos            │   │──────────────────────────────────────│ │
│  │ readerSchema                  │   │ Constructed from readerContext's     │ │
│  │──────────────────────────────│   │ keyFilterOpt (predicate → key list) │ │
│  │ Built from: props + schema    │   │                                      │ │
│  │ Tells: "how do I detect a     │   │ CONSUMED BY:                        │ │
│  │  delete in a record?"         │   │  • BaseHoodieLogRecordReader        │ │
│  │                               │   │    → passed to processDataBlock()   │ │
│  │ CONSUMED BY:                  │   │  • HoodieDataBlock                  │ │
│  │  • FileGroupRecordBuffer      │   │    → filters records during deser   │ │
│  │    (delete detection in merge)│   └──────────────────────────────────────┘ │
│  └──────────────────────────────┘                                            │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────────┐ │
│  │              ExternalSpillableMap<Serializable, BufferedRecord<T>>       │ │
│  │──────────────────────────────────────────────────────────────────────────│ │
│  │ maxInMemorySizeInBytes: long          inMemoryMap: HashMap               │ │
│  │ diskBasedMap: DiskMap (lazy)          currentInMemoryMapSize: long       │ │
│  │ keySizeEstimator: SizeEstimator       valueSizeEstimator: SizeEstimator │ │
│  │ diskMapType: ROCKS_DB | BITCASK       valueSerializer: CustomSerializer │ │
│  │──────────────────────────────────────────────────────────────────────────│ │
│  │ put(key, value):                                                         │ │
│  │   • If key exists in-memory → update in-place (no spill check)          │ │
│  │   • If new key + memory full → spill to disk via DiskMap                │ │
│  │   • Otherwise → insert to inMemoryMap                                    │ │
│  │ get(key): check inMemoryMap first → then diskBasedMap                   │ │
│  │──────────────────────────────────────────────────────────────────────────│ │
│  │ Sizing fed by: readerContext.getRecordSizeEstimator()                    │ │
│  │                readerContext.getRecordSerializer()                        │ │
│  │                                                                          │ │
│  │ CONSUMED BY: FileGroupRecordBuffer as `records` field                   │ │
│  │   → all processDataBlock / processDeleteBlock writes go here            │ │
│  │   → all merge reads (hasNextBaseRecord) pull from here                  │ │
│  └──────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────────┐ │
│  │                        BufferedRecords  (static factory)                 │ │
│  │──────────────────────────────────────────────────────────────────────────│ │
│  │ fromEngineRecord(T record, schema, recordCtx, orderingFields, isDel)    │ │
│  │   → extracts recordKey, orderingValue from engine record T              │ │
│  │   → wraps into BufferedRecord<T>                                         │ │
│  │                                                                          │ │
│  │ fromDeleteRecord(DeleteRecord, recordCtx)                                │ │
│  │   → converts DeleteRecord → BufferedRecord<T> (record=null, isDel=true) │ │
│  │                                                                          │ │
│  │ CONSUMED BY: KeyBasedFileGroupRecordBuffer.processDataBlock()            │ │
│  │              KeyBasedFileGroupRecordBuffer.processNextDeletedRecord()    │ │
│  └──────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────────┐ │
│  │               BufferedRecordMerger<T>  (interface)                       │ │
│  │──────────────────────────────────────────────────────────────────────────│ │
│  │ deltaMerge(newRecord, existingRecord) → Option<BufferedRecord<T>>       │ │
│  │   "given a NEW log record and an EXISTING buffered record,              │ │
│  │    produce the merged result (or empty if record should be dropped)"    │ │
│  │                                                                          │ │
│  │ deltaMerge(deleteRecord, existingRecord) → Option<DeleteRecord>         │ │
│  │   "given a DELETE and an existing record, decide outcome"               │ │
│  │                                                                          │ │
│  │ finalMerge(olderBaseRec, newerLogRec) → BufferedRecord<T>               │ │
│  │   "merge a base-file record with its corresponding log record"          │ │
│  │                                                                          │ │
│  │ CONSUMED BY: KeyBasedFileGroupRecordBuffer                              │ │
│  │   processNextDataRecord → deltaMerge (log-vs-log within buffer)         │ │
│  │   processNextDeletedRecord → deltaMerge (delete-vs-existing)            │ │
│  │   hasNextBaseRecord → finalMerge (base-vs-log at read time)             │ │
│  └──────────────────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────────────────┘
```

### Layer 2: Log File I/O — Block Reading Infrastructure

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                    LOG FILE I/O & BLOCK INFRASTRUCTURE                        │
│                                                                              │
│        «abstract»  HoodieLogBlock                                            │
│        ┌─────────────────────────────────────────────────┐                   │
│        │ logBlockHeader: Map<HeaderMetadataType, String>  │                   │
│        │ logBlockFooter: Map<FooterMetadataType, String>  │                   │
│        │ content: Option<byte[]>  (lazy-inflatable)       │                   │
│        │ blockContentLocation: Option<ContentLocation>    │                   │
│        │ readBlockLazily: boolean                         │                   │
│        │─────────────────────────────────────────────────│                   │
│        │ getBlockType() → HoodieLogBlockType              │                   │
│        │ inflate() / deflate()  (lazy content mgmt)       │                   │
│        │ getRecordPositions() → Roaring64NavigableMap     │                   │
│        │ isCompactedLogBlock() → checks COMPACTED_BLOCK_  │                   │
│        │   TIMES header                                   │                   │
│        │ HeaderMetadataType.INSTANT_TIME → commit instant │                   │
│        │ HeaderMetadataType.TARGET_INSTANT_TIME → rollback│                   │
│        └───────────────┬──────────────────┬──────────────┘                   │
│                        │                  │                                   │
│             ┌──────────┘                  └──────────┐                       │
│             ▼                                         ▼                       │
│  «abstract» HoodieDataBlock                HoodieDeleteBlock                 │
│  ┌──────────────────────────────┐   ┌──────────────────────────────┐        │
│  │ records: Option<List<Record>>│   │ recordsToDelete: DeleteRecord│        │
│  │ keyFieldName: String         │   │   []                          │        │
│  │ readerSchema: HoodieSchema   │   │──────────────────────────────│        │
│  │ enablePointLookups: boolean  │   │ getRecordsToDelete()          │        │
│  │──────────────────────────────│   │   → lazy deserialize from    │        │
│  │ getEngineRecordIterator(ctx) │   │     content bytes            │        │
│  │   → deserializes block bytes │   │ getBlockType()               │        │
│  │     into Iterator<T> of      │   │   → DELETE_BLOCK             │        │
│  │     engine-native records    │   │                               │        │
│  │ getRecordIterator(keys,      │   │ CONSUMED BY:                 │        │
│  │   fullKey, type)             │   │   processQueuedBlocksFor-    │        │
│  │   → filtered by KeySpec      │   │   Instant → recordBuffer.   │        │
│  │──────────────────────────────│   │   processDeleteBlock(this)   │        │
│  │ Subtypes (by format):        │   └──────────────────────────────┘        │
│  │  AVRO_DATA_BLOCK             │                                            │
│  │  HFILE_DATA_BLOCK            │    HoodieLogBlockType enum:               │
│  │  PARQUET_DATA_BLOCK          │    ┌──────────────────────┐               │
│  │  CDC_DATA_BLOCK              │    │ COMMAND_BLOCK         │               │
│  │                               │    │ DELETE_BLOCK          │               │
│  │ CONSUMED BY:                  │    │ CORRUPT_BLOCK         │               │
│  │   processQueuedBlocksFor-    │    │ AVRO_DATA_BLOCK       │               │
│  │   Instant → recordBuffer.   │    │ HFILE_DATA_BLOCK      │               │
│  │   processDataBlock(this,     │    │ PARQUET_DATA_BLOCK    │               │
│  │     keySpecOpt)               │    │ CDC_DATA_BLOCK        │               │
│  └──────────────────────────────┘    └──────────────────────┘               │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐    │
│  │              HoodieLogFormatReader                                    │    │
│  │  implements: HoodieLogFormat.Reader                                   │    │
│  │──────────────────────────────────────────────────────────────────────│    │
│  │ logFiles: List<HoodieLogFile>     currentReader: HoodieLogFileReader │    │
│  │ storage: HoodieStorage            readerSchema: HoodieSchema         │    │
│  │ bufferSize: int                   recordKeyField: String             │    │
│  │──────────────────────────────────────────────────────────────────────│    │
│  │ hasNext() / next() → HoodieLogBlock                                  │    │
│  │   Iterates across ALL log files sequentially.                        │    │
│  │   Each next() returns one deserialized HoodieLogBlock                │    │
│  │   (could be DataBlock, DeleteBlock, CommandBlock, CorruptBlock)      │    │
│  │                                                                      │    │
│  │ CONSUMED BY: BaseHoodieLogRecordReader.scanInternal() Pass 1         │    │
│  └──────────────────────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────────────────┘
```

### Layer 3: Application-Level — Log Scan Orchestration & Buffer Population

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                     APPLICATION-LEVEL ORCHESTRATION                                │
│                                                                                    │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │       BaseHoodieLogRecordReader<T>   (abstract — the scan engine)            │  │
│  │──────────────────────────────────────────────────────────────────────────────│  │
│  │ logFiles: List<HoodieLogFile>         storage: HoodieStorage                 │  │
│  │ readerContext: HoodieReaderContext<T>  hoodieTableMetaClient                  │  │
│  │ recordBuffer: HoodieFileGroupRecordBuffer<T>  ← INJECTED, written to        │  │
│  │ instantRange: Option<InstantRange>    latestInstantTime: String              │  │
│  │ forceFullScan: boolean                reverseReader: boolean                 │  │
│  │ bufferSize: int                       validBlockInstants: List<String>       │  │
│  │ currentInstantLogBlocks: Deque<HoodieLogBlock>  (staging area for Pass 2)   │  │
│  │                                                                              │  │
│  │ ── Scan counters (AtomicLong) ──                                             │  │
│  │ totalLogFiles, totalLogBlocks, totalLogRecords                               │  │
│  │ totalRollbacks, totalCorruptBlocks                                           │  │
│  │──────────────────────────────────────────────────────────────────────────────│  │
│  │                                                                              │  │
│  │ scanInternal(keySpecOpt, skipProcessingBlocks):                              │  │
│  │ ┌────────────────────────────────────────────────────────────────────────┐   │  │
│  │ │  PASS 1: Sequential block reading via HoodieLogFormatReader            │   │  │
│  │ │  ─────────────────────────────────────────────────────────────────────  │   │  │
│  │ │  while (logFormatReader.hasNext()):                                     │   │  │
│  │ │    block = logFormatReader.next()  → HoodieLogBlock                    │   │  │
│  │ │                                                                        │   │  │
│  │ │    switch (block.getBlockType()):                                       │   │  │
│  │ │      CORRUPT_BLOCK → totalCorruptBlocks++, skip                        │   │  │
│  │ │      COMMAND_BLOCK → if ROLLBACK:                                      │   │  │
│  │ │        targetInstant = header[TARGET_INSTANT_TIME]                      │   │  │
│  │ │        targetRollbackInstants.add(targetInstant)                        │   │  │
│  │ │        totalRollbacks++                                                 │   │  │
│  │ │      DATA_BLOCK / DELETE_BLOCK →                                        │   │  │
│  │ │        blockInstant = header[INSTANT_TIME]                              │   │  │
│  │ │        instantToBlocksMap.computeIfAbsent(blockInstant).add(block)     │   │  │
│  │ │                                                                        │   │  │
│  │ │  OUTPUT: instantToBlocksMap:  Map<String, List<HoodieLogBlock>>        │   │  │
│  │ │          targetRollbackInstants: Set<String>                            │   │  │
│  │ │          orderedInstantsList: ordered keys of instantToBlocksMap        │   │  │
│  │ └────────────────────────────────────────────────────────────────────────┘   │  │
│  │                              │                                               │  │
│  │                              ▼                                               │  │
│  │ ┌────────────────────────────────────────────────────────────────────────┐   │  │
│  │ │  PASS 2: Reverse-chronological instant resolution                      │   │  │
│  │ │  ─────────────────────────────────────────────────────────────────────  │   │  │
│  │ │  for each instant in orderedInstantsList (REVERSE order, newest→oldest):│   │  │
│  │ │                                                                        │   │  │
│  │ │    SKIP if instant ∈ targetRollbackInstants (rolled back)              │   │  │
│  │ │    SKIP if instant filtered out by instantRange                        │   │  │
│  │ │    SKIP if instant > latestInstantTime && !allowInflightInstants       │   │  │
│  │ │                                                                        │   │  │
│  │ │    for each block in instant's block list:                              │   │  │
│  │ │      if block.isCompactedLogBlock():                                    │   │  │
│  │ │        compactedInstants = header[COMPACTED_BLOCK_TIMES].split(",")    │   │  │
│  │ │        → remove those instants from orderedInstantsList                │   │  │
│  │ │          (they are superseded by this compaction)                       │   │  │
│  │ │                                                                        │   │  │
│  │ │      currentInstantLogBlocks.addLast(block)  ← accumulate             │   │  │
│  │ │                                                                        │   │  │
│  │ │    validBlockInstants.add(instant)                                      │   │  │
│  │ │                                                                        │   │  │
│  │ │  OUTPUT: currentInstantLogBlocks: Deque<HoodieLogBlock>                │   │  │
│  │ │    (ordered newest-instant-first because of reverse iteration,          │   │  │
│  │ │     within each instant blocks are in log-file order)                   │   │  │
│  │ │          validBlockInstants: List<String>                               │   │  │
│  │ └────────────────────────────────────────────────────────────────────────┘   │  │
│  │                              │                                               │  │
│  │                              ▼                                               │  │
│  │ ┌────────────────────────────────────────────────────────────────────────┐   │  │
│  │ │  PASS 3: processQueuedBlocksForInstant(currentInstantLogBlocks)        │   │  │
│  │ │  ─────────────────────────────────────────────────────────────────────  │   │  │
│  │ │  while (!logBlocks.isEmpty()):                                          │   │  │
│  │ │    block = logBlocks.pollLast()  ← OLDEST first (FIFO from bottom)     │   │  │
│  │ │                                                                        │   │  │
│  │ │    if (block instanceof HoodieDataBlock):                               │   │  │
│  │ │      recordBuffer.processDataBlock(dataBlock, keySpecOpt) ──────┐      │   │  │
│  │ │                                                                  │      │   │  │
│  │ │    else if (block instanceof HoodieDeleteBlock):                 │      │   │  │
│  │ │      recordBuffer.processDeleteBlock(deleteBlock) ──────────────┤      │   │  │
│  │ │                                                                  │      │   │  │
│  │ │    (other block types silently skipped)                   DELEGATES     │   │  │
│  │ │                                                           TO BUFFER    │   │  │
│  │ └──────────────────────────────────────────────────────────────┼──────┘   │  │
│  └───────────────────────────────────────────────────────────────┼──────────┘  │
│                                                                   │            │
│  ┌────────────────────────────────────────────────────────────────┼──────────┐  │
│  │       HoodieMergedLogRecordReader<T>                           │          │  │
│  │  extends BaseHoodieLogRecordReader<T>                          │          │  │
│  │  implements Iterable<BufferedRecord<T>>, Closeable             │          │  │
│  │──────────────────────────────────────────────────────────────────────────│  │
│  │ timer: HoodieTimer                                                       │  │
│  │ numMergedRecordsInLog: long                                              │  │
│  │ totalTimeTakenToReadAndMergeBlocks: long                                 │  │
│  │──────────────────────────────────────────────────────────────────────────│  │
│  │ performScan():                                                           │  │
│  │   timer.startTimer()                                                     │  │
│  │   super.scanInternal(keySpecOpt, skipProcessingBlocks=false)             │  │
│  │   totalTimeTakenToReadAndMergeBlocks = timer.endTimer()                  │  │
│  │   numMergedRecordsInLog = recordBuffer.getTotalLogRecords()              │  │
│  │                                                                          │  │
│  │ Thin wrapper: adds timing + stats on top of base scan logic             │  │
│  │ Built via Builder pattern in LogScanningRecordBufferLoader              │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
│                                                                   │            │
│       ┌───────────────────────────────────────────────────────────┘            │
│       │  recordBuffer.processDataBlock / processDeleteBlock                   │
│       ▼                                                                        │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │      KeyBasedFileGroupRecordBuffer<T>                                    │  │
│  │  extends FileGroupRecordBuffer<T>                                        │  │
│  │  implements HoodieFileGroupRecordBuffer<T>                               │  │
│  │──────────────────────────────────────────────────────────────────────────│  │
│  │ records: ExternalSpillableMap<Serializable, BufferedRecord<T>>           │  │
│  │ bufferedRecordMerger: BufferedRecordMerger<T>                            │  │
│  │ readerContext: HoodieReaderContext<T>                                     │  │
│  │ totalLogRecords: long                                                     │  │
│  │──────────────────────────────────────────────────────────────────────────│  │
│  │                                                                          │  │
│  │ processDataBlock(dataBlock, keySpecOpt):                                 │  │
│  │ ┌──────────────────────────────────────────────────────────────────────┐ │  │
│  │ │ 1. iter = getRecordsIterator(dataBlock, keySpecOpt)                  │ │  │
│  │ │      → dataBlock.getEngineRecordIterator(readerContext)              │ │  │
│  │ │      → optionally applies schema evolution transformer              │ │  │
│  │ │      → optionally applies KeySpec filter (point lookups)            │ │  │
│  │ │                                                                      │ │  │
│  │ │ 2. for each engine record T in iter:                                 │ │  │
│  │ │      bufferedRec = BufferedRecords.fromEngineRecord(                 │ │  │
│  │ │        record, writerSchema, recordCtx, orderingFields, isDelete)   │ │  │
│  │ │        → extracts: recordKey, orderingValue from record T           │ │  │
│  │ │        → wraps into BufferedRecord<T>                                │ │  │
│  │ │                                                                      │ │  │
│  │ │      processNextDataRecord(bufferedRec, recordKey)                   │ │  │
│  │ └──────────────────────────────────────────────────────────────────────┘ │  │
│  │                                                                          │  │
│  │ processNextDataRecord(newRecord, key):                                   │  │
│  │ ┌──────────────────────────────────────────────────────────────────────┐ │  │
│  │ │ existing = records.get(key)  ← may be null (first time) or previous │ │  │
│  │ │ totalLogRecords++                                                    │ │  │
│  │ │ merged = bufferedRecordMerger.deltaMerge(newRecord, existing)        │ │  │
│  │ │   → if existing==null: returns newRecord                             │ │  │
│  │ │   → if both present: merges by ordering value / merge strategy       │ │  │
│  │ │   → Option.empty() means "drop this record"                          │ │  │
│  │ │ merged.ifPresent(rec →                                               │ │  │
│  │ │   records.put(key, rec.toBinary(recordContext)))                     │ │  │
│  │ │     ↑ toBinary serializes the engine record T so it can spill       │ │  │
│  │ │       to disk in ExternalSpillableMap if memory is full              │ │  │
│  │ └──────────────────────────────────────────────────────────────────────┘ │  │
│  │                                                                          │  │
│  │ processDeleteBlock(deleteBlock):                                         │  │
│  │ ┌──────────────────────────────────────────────────────────────────────┐ │  │
│  │ │ deleteRecords = deleteBlock.getRecordsToDelete()                     │ │  │
│  │ │   → lazy deserialization of byte[] → DeleteRecord[]                  │ │  │
│  │ │                                                                      │ │  │
│  │ │ for each DeleteRecord:                                               │ │  │
│  │ │   key = deleteRecord.getRecordKey()                                  │ │  │
│  │ │   processNextDeletedRecord(deleteRecord, key)                        │ │  │
│  │ └──────────────────────────────────────────────────────────────────────┘ │  │
│  │                                                                          │  │
│  │ processNextDeletedRecord(deleteRecord, key):                             │  │
│  │ ┌──────────────────────────────────────────────────────────────────────┐ │  │
│  │ │ existing = records.get(key)                                          │ │  │
│  │ │ totalLogRecords++                                                    │ │  │
│  │ │ mergedDel = bufferedRecordMerger.deltaMerge(deleteRecord, existing)  │ │  │
│  │ │   → decides: does delete win over existing record?                   │ │  │
│  │ │   → returns Option<DeleteRecord> (present = delete wins)             │ │  │
│  │ │                                                                      │ │  │
│  │ │ if mergedDel.isPresent():                                            │ │  │
│  │ │   records.put(key, BufferedRecords.fromDeleteRecord(                 │ │  │
│  │ │     mergedDel.get(), recordContext))                                  │ │  │
│  │ │     → creates BufferedRecord with record=null, isDelete=true         │ │  │
│  │ └──────────────────────────────────────────────────────────────────────┘ │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### Interface Hierarchy (annotated at the implementing class)

```
  «interface» HoodieFileGroupRecordBuffer<T>
       │
       │ implements
       ▼
  «abstract» FileGroupRecordBuffer<T>
  ┌──────────────────────────────────────────────────┐
  │ Owns: ExternalSpillableMap, BufferedRecordMerger, │
  │   DeleteContext, UpdateProcessor, baseFileIterator│
  │ Template methods: doHasNext() [abstract],         │
  │   hasNextBaseRecord(), hasNextLogRecord()          │
  └──────────────┬────────────┬────────────┬─────────┘
                 │            │            │
       ┌─────────┘    ┌───────┘    ┌───────┘
       ▼              ▼            ▼
  KeyBased       PositionBased  Unmerged         SortedKeyBased
  FGRecordBuf    FGRecordBuf    FGRecordBuf      FGRecordBuf
                                                  (extends KeyBased)

  HOW WE PICK KeyBased (in DefaultFileGroupRecordBufferLoader):
    !isSkipMerge && !sortOutputs() && !(useRecordPosition() && baseFilePresent)
    → falls through all checks → DEFAULT branch → new KeyBasedFileGroupRecordBuffer(...)
```

```
  «abstract» HoodieLogBlock
       │
       ├──── «abstract» HoodieDataBlock
       │         │
       │         ├── HoodieAvroDataBlock     (AVRO_DATA_BLOCK)
       │         ├── HoodieHFileDataBlock    (HFILE_DATA_BLOCK)
       │         ├── HoodieParquetDataBlock  (PARQUET_DATA_BLOCK)
       │         └── HoodieCDCDataBlock      (CDC_DATA_BLOCK)
       │
       ├──── HoodieDeleteBlock               (DELETE_BLOCK)
       ├──── HoodieCommandBlock              (COMMAND_BLOCK — rollback markers)
       └──── HoodieCorruptBlock              (CORRUPT_BLOCK)
```

```
  «abstract» BaseHoodieLogRecordReader<T>
       │
       │ extends
       ▼
  HoodieMergedLogRecordReader<T>
  (the only concrete subclass in this path — selected directly
   via HoodieMergedLogRecordReader.newBuilder()...build() in
   LogScanningRecordBufferLoader.scanLogFiles())
```

### End-to-End Data Flow Summary

```
 ① LogScanningRecordBufferLoader.scanLogFiles()
      creates HoodieMergedLogRecordReader via builder
      passes: storage, logFiles, bufferSize, instantRange,
              partitionPath, recordBuffer, allowInflightInstants

 ② HoodieMergedLogRecordReader constructor → performScan()
      timer.start()
      calls: super.scanInternal(keySpecOpt, false)

 ③ BaseHoodieLogRecordReader.scanInternal()  PASS 1
      creates: HoodieLogFormatReader(storage, logFiles, schema, ...)
      reads: ALL blocks across all log files sequentially
      builds: instantToBlocksMap{instant → [blocks]}
              targetRollbackInstants{instant}

 ④ scanInternal()  PASS 2
      iterates: instants in REVERSE chronological order
      filters: rolled-back instants, out-of-range instants
      resolves: compacted blocks (removes superseded instants)
      accumulates: currentInstantLogBlocks deque
      records: validBlockInstants

 ⑤ scanInternal()  PASS 3 — processQueuedBlocksForInstant()
      drains deque OLDEST→NEWEST (pollLast):
        DataBlock  → recordBuffer.processDataBlock(block, keySpec)
        DeleteBlock → recordBuffer.processDeleteBlock(block)

 ⑥ KeyBasedFileGroupRecordBuffer.processDataBlock()
      deserializes: HoodieDataBlock → Iterator<T> engine records
      for each record:
        wraps: BufferedRecords.fromEngineRecord() → BufferedRecord<T>
        merges: bufferedRecordMerger.deltaMerge(new, existing)
        stores: records.put(key, merged.toBinary())
                  └→ ExternalSpillableMap: in-memory or spills to disk

 ⑦ KeyBasedFileGroupRecordBuffer.processDeleteBlock()
      deserializes: HoodieDeleteBlock → DeleteRecord[]
      for each delete:
        merges: bufferedRecordMerger.deltaMerge(delete, existing)
        stores: records.put(key, BufferedRecords.fromDeleteRecord(...))

 ⑧ Back in performScan():
      totalTimeTaken = timer.end()
      numMergedRecords = recordBuffer.getTotalLogRecords()

 ⑨ Back in LogScanningRecordBufferLoader:
      populates HoodieReadStats from reader counters
      returns: validBlockInstants → caller
      recordBuffer is now fully populated with all log deltas
```
