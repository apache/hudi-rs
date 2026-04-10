# hudi internal RecordContext

The recordContext is the engine-specific "glue" that lets the engine-agnostic FileGroupRecordBuffer and BufferedRecordMerger infrastructure manipulate Spark InternalRow objects without knowing anything about Spark's type system.


## Call Stack Tree: Record Types Through HoodieFileGroupReader

```
HoodieFileGroupReader.getClosableIterator()           → ClosableIterator<T>
HoodieFileGroupReader.getClosableHoodieRecordIterator() → ClosableIterator<HoodieRecord<T>>
HoodieFileGroupReader.getClosableKeyIterator()        → ClosableIterator<String>
HoodieFileGroupReader.getClosableBufferedRecordIterator() → ClosableIterator<BufferedRecord<T>>
  │
  └─ getBufferedRecordIterator(mode)
       │
       └─ initRecordIterators()
            │
            ├─── [BRANCH A: no merge needed — base file only]
            │         makeBaseFileIterator()
            │           │
            │           └─ readerContext.getFileRecordIterator(...)
            │                └─ reads parquet file
            │                   TYPE: ClosableIterator<T>           ← raw engine record (e.g. InternalRow)
            │                   │
            │                   └─ [if no log files] CloseableMappingIterator
            │                        └─ RecordContext.seal(T)
            │                           TYPE: T (copied/frozen)
            │
            └─── [BRANCH B: MOR — base + log files]
                      │
                      ├─── [1. BASE FILE reading]
                      │         makeBaseFileIterator()
                      │           └─ readerContext.getFileRecordIterator(...)
                      │                TYPE: ClosableIterator<T>    ← raw engine record
                      │
                      └─── [2. LOG FILE scanning]
                                LogScanningRecordBufferLoader.scanLogFiles()
                                  └─ HoodieMergedLogRecordReader.build()
                                       └─ [for each log block]
                                            │
                                            ├─── [DATA BLOCK]
                                            │         HoodieDataBlock.getEngineRecordIterator(readerContext)
                                            │           └─ deserializeRecords(readerContext, content)
                                            │                TYPE: ClosableIterator<T>   ← raw engine record
                                            │                │
                                            │                └─ [schema evolution transformer applied]
                                            │                   RecordContext.projectRecord(from, to, renames)
                                            │                   TYPE: T (projected)
                                            │                   │
                                            │       KeyBasedFileGroupRecordBuffer.processDataBlock()
                                            │         └─ RecordContext.isDeleteRecord(T, deleteContext)
                                            │              BufferedRecords.fromEngineRecord(T, schema, ...)
                                            │                └─ RecordContext.getRecordKey(T)
                                            │                   RecordContext.encodeAvroSchema(schema)
                                            │                   RecordContext.getOrderingValue(T, schema, fields)
                                            │                   TYPE: BufferedRecord<T>  ← Hudi internal record
                                            │                   │
                                            │                   └─ bufferedRecordMerger.deltaMerge(new, existing)
                                            │                        TYPE: BufferedRecord<T>  (merged)
                                            │                        │
                                            │                        └─ BufferedRecord.toBinary(recordContext)
                                            │                             RecordContext.toBinaryRow(schema, T)
                                            │                             RecordContext.seal(T)
                                            │                        stored in:
                                            │                        ExternalSpillableMap<key, BufferedRecord<T>>
                                            │
                                            └─── [DELETE BLOCK]
                                                      HoodieDeleteBlock.getRecordsToDelete()
                                                      TYPE: DeleteRecord[]           ← Hudi delete record
                                                      │
                                                      └─ BufferedRecords.fromDeleteRecord(DeleteRecord, ...)
                                                           RecordContext.getOrderingValue(DeleteRecord)
                                                           TYPE: BufferedRecord<T>   ← isDelete=true, record=null
                                                           │
                                                           └─ stored in:
                                                              ExternalSpillableMap<key, BufferedRecord<T>>
```

---

```
[3. MERGE PHASE — doHasNext() drives the merge]
  │
  KeyBasedFileGroupRecordBuffer.doHasNext()
    │
    ├─── [base record has matching log record]
    │         baseFileIterator.next()
    │         TYPE: T                                        ← raw engine record from base file
    │           │
    │           └─ BufferedRecords.fromEngineRecord(T, schema, ...)
    │                TYPE: BufferedRecord<T>                 ← wraps base record
    │                │
    │                └─ bufferedRecordMerger.finalMerge(base: BufferedRecord<T>, log: BufferedRecord<T>)
    │                     │
    │                     ├─ [event/commit time ordering]
    │                     │    TYPE: BufferedRecord<T>       ← higher ordering value wins
    │                     │
    │                     └─ [custom merger / payload merger]
    │                          RecordContext.decodeAvroSchema(schemaId)
    │                          RecordContext.convertToAvroRecord(T, schema)
    │                          → merge via HoodieRecordMerger (in Avro space)
    │                          RecordContext.convertAvroRecord(IndexedRecord)
    │                          TYPE: BufferedRecord<T>       ← merged result
    │                          │
    │                          └─ UpdateProcessor.processUpdate(key, base, merged, isDelete)
    │                               │
    │                               ├─ [delete] RecordContext.getDeleteRow(key)
    │                               │    TYPE: BufferedRecord<T>  (isDelete=true, T=delete row)
    │                               │
    │                               └─ [non-delete] BufferedRecord.seal(recordContext)
    │                                    RecordContext.seal(T)
    │                                    TYPE: BufferedRecord<T>  ← final merged, sealed
    │
    └─── [log-only record — no base file match]
              records iterator → BufferedRecord<T>
                └─ UpdateProcessor.processUpdate(key, null, logRecord, isDelete)
                     TYPE: BufferedRecord<T>                 ← insert or delete
```

---

```
[4. OUTPUT — HoodieFileGroupReader.next()]
  │
  BufferedRecord<T>
    │
    └─ outputConverter.apply(T)  [if schema projection needed]
         RecordContext.projectRecord(from, to)
         TYPE: BufferedRecord<T>  (projected T inside)
         │
         ├─ getClosableIterator()
         │    BufferedRecord::getRecord
         │    TYPE: T                                        ← raw engine record (InternalRow)
         │
         ├─ getClosableHoodieRecordIterator()
         │    RecordContext.constructFinalHoodieRecord(BufferedRecord<T>)
         │      └─ RecordContext.constructHoodieRecord(BufferedRecord<T>, partitionPath)
         │           TYPE: HoodieRecord<T>                  ← HoodieSparkRecord or HoodieEmptyRecord
         │
         ├─ getClosableKeyIterator()
         │    BufferedRecord::getRecordKey
         │    TYPE: String                                   ← record key only
         │
         └─ getClosableBufferedRecordIterator()
              TYPE: BufferedRecord<T>                        ← raw internal record with metadata
```


# Object Construction

---
RecordContext Construction Chain

Starting from your buildReaderWithPartitionValues code:

Step 1: SparkFileFormatInternalRowReaderContext instantiation
```
val readerContext = new SparkFileFormatInternalRowReaderContext(
    fileGroupBaseFileReader.value, filters, requiredFilters, storageConf, metaClient.getTableConfig)
```
The constructor at SparkFileFormatInternalRowReaderContext.scala:63-68 passes the recordContext up to the superclass:
```
class SparkFileFormatInternalRowReaderContext(...)
extends BaseSparkInternalRowReaderContext(
    storageConfiguration, tableConfig,
    SparkFileFormatInternalRecordContext.apply(tableConfig))  // <-- recordContext created here
```
Step 2: SparkFileFormatInternalRecordContext.apply(tableConfig)

At SparkFileFormatInternalRecordContext.scala:74:
```
def apply(tableConfig: HoodieTableConfig): SparkFileFormatInternalRecordContext =
    new BaseSparkInternalRecordContext(tableConfig) with SparkFileFormatInternalRecordContext
```
This creates a BaseSparkInternalRecordContext mixed with the SparkFileFormatInternalRecordContext trait. The trait adds:
- supportsParquetRowIndex → true if Spark >= 3.5
- convertAvroRecord / convertToAvroRecord → Avro ↔ InternalRow via Spark deserializers

Step 3: BaseSparkInternalRecordContext(tableConfig)

At BaseSparkInternalRecordContext.java:58-59:
```
protected BaseSparkInternalRecordContext(HoodieTableConfig tableConfig) {
    super(tableConfig, new DefaultJavaTypeConverter());
}
```
Step 4: RecordContext(tableConfig, typeConverter)

At RecordContext.java:71-75:
```
protected RecordContext(HoodieTableConfig tableConfig, JavaTypeConverter typeConverter) {
    this.typeConverter = typeConverter;
    this.recordKeyExtractor = tableConfig.populateMetaFields()
        ? metadataKeyExtractor()        // reads _hoodie_record_key meta field
        : virtualKeyExtractor(tableConfig.getRecordKeyFields());  // reads actual PK columns
}
```
This is where the key-extraction strategy is wired: either reading _hoodie_record_key from position 2 in the row (meta fields mode) or constructing the key from configured record key columns (virtual keys mode).

Step 5: recordContext is stored in HoodieReaderContext

BaseSparkInternalRowReaderContext calls super(storageConfig, tableConfig, Option.empty(), Option.empty(), recordContext) which flows to HoodieReaderContext:101-107:
```
protected HoodieReaderContext(..., RecordContext<T> recordContext) {
    ...
    this.recordContext = recordContext;
}
```
Step 6: Late initialization — ordering value converter

When the HoodieFileGroupReader constructor calls readerContext.setSchemaHandler(...) at line 119-121, BaseSparkInternalRowReaderContext.setSchemaHandler() (line 96-103)
fires:

```
@Override
public void setSchemaHandler(FileGroupReaderSchemaHandler<InternalRow> schemaHandler) {
    super.setSchemaHandler(schemaHandler);
    List<String> orderingFieldNames = HoodieRecordUtils.getOrderingFieldNames(getMergeMode(), tableConfig);
    Schema schema = schemaHandler.getRequiredSchema();
    ((BaseSparkInternalRecordContext) recordContext).initOrderingValueConverter(schema, orderingFieldNames);
}
```

This initializes OrderingValueEngineTypeConverter inside recordContext. For commit-time ordering, orderingFieldNames is empty, so this converter is effectively a no-op (it will never be called because ordering values are always the default 0).

Step 7: Partition path set

Back in HoodieFileGroupReader constructor at line 106:

`readerContext.getRecordContext().setPartitionPath(inputSplit.getPartitionPath());`

This lets recordContext stamp partition path onto HoodieRecord objects it constructs later

────────────────────────────────────────────────────────────────────────────────
👤 USER:
in hoodie file group reader we have reader context which contains recordContext.                                                          
  could you walk through consumers within the fg reader and see what are the set of APIs that actually got consumed

────────────────────────────────────────────────────────────────────────────────
🤖 ASSISTANT:
Here's a walkthrough of the `RecordContext` API surface and what's actually consumed:

## RecordContext - Full API (31 methods)

Defined in `hudi-common/src/main/java/org/apache/hudi/common/engine/RecordContext.java`

## Consumed APIs by Layer

### Directly in `HoodieFileGroupReader` (6 methods)

| Method | Line | Purpose |
|--------|------|---------|
| `setPartitionPath()` | :104 | Set partition path from input split |
| `supportsParquetRowIndex()` | :118 | Pick schema handler type |
| `seal()` | :132 | Seal records from base file iterator |
| `convertValueToEngineType()` | :200 | Convert partition field values |
| `constructFinalHoodieRecord()` | :309 | Build final `HoodieRecord` for output iterator |

### Via `BufferedRecords` / `BufferedRecordConverter`

- `extractDataFromRecord()`
- `getRecordKey()`
- `encodeAvroSchema()`
- `getOrderingValue()` (multiple overloads)
- `convertOrderingValueToEngineType()`

### Via `UpdateProcessor`

- `getDeleteRow()`
- `decodeAvroSchema()`
- `convertToAvroRecord()`
- `convertAvroRecord()`
- `seal()` (again, via `BufferedRecord.seal()`)

### Via `PartialUpdateHandler`

- `getValue()`
- `constructEngineRecord()`
- `getTypeConverter()`

### Via `KeyBasedFileGroupRecordBuffer`

- `isDeleteRecord()`

### Via `HoodieReaderContext` (instant range filtering)

- `getMetaFieldValue()`

---

## Summary

Of the 31 defined methods, roughly **17-18 distinct methods** are actually exercised across the FG reader pipeline. The remaining ones (e.g., `projectRecord`, `toBinaryRow`, `getSchemaFromBufferRecord`, `extractRecordPosition`) are called only by engine-specific subclasses or are utility helpers used outside the core FG reader flow.

The FG reader itself is thin — it delegates almost all heavy lifting to `BufferedRecords`, `UpdateProcessor`, `PartialUpdateHandler`, and `BufferedRecordConverter`, which is where most of the `RecordContext` surface area is consumed.

────────────────────────────────────────────────────────────────────────────────
👤 USER:
for each of the API, show me call stack tree starting from hoodie file group reader API

────────────────────────────────────────────────────────────────────────────────
🤖 ASSISTANT:
Here are the call stack trees rooted at `HoodieFileGroupReader` for each consumed `RecordContext` API:

---

## 1. `setPartitionPath()`
```
HoodieFileGroupReader.<init>()                          :104
  -> readerContext.getRecordContext()
    -> RecordContext.setPartitionPath(partitionPath)
```

## 2. `supportsParquetRowIndex()`
```
HoodieFileGroupReader.<init>()                          :118
  -> readerContext.getRecordContext()
    -> RecordContext.supportsParquetRowIndex()
```

## 3. `seal()`

**Path A — base file only (no merge)**
```
HoodieFileGroupReader.initRecordIterators()             :132
  -> CloseableMappingIterator.apply()
    -> readerContext.getRecordContext()
      -> RecordContext.seal(rec)
```

**Path B — buffer hasNext**
```
FileGroupRecordBuffer.hasNextBaseRecord()               :244
  -> readerContext.getRecordContext()
    -> RecordContext.seal(baseRecord)
```

**Path C — unmerged buffer**
```
UnmergedFileGroupRecordBuffer.doHasNext()               :70,88
  -> readerContext.getRecordContext()
    -> RecordContext.seal(next)
```

**Path D — via BufferedRecord**
```
BufferedRecord.seal()                                   :100
  -> RecordContext.seal(record)
```

**Path E — UpdateProcessor non-delete**
```
UpdateProcessor.StandardUpdateProcessor.handleNonDeletes() :118
  -> BufferedRecord.seal(readerContext.getRecordContext())
    -> RecordContext.seal(record)
```

## 4. `convertValueToEngineType()`

**Path A — bootstrap partition values**
```
HoodieFileGroupReader.makeBootstrapBaseFileIterator()   :200
  -> readerContext.getRecordContext()
    -> RecordContext.convertValueToEngineType(partitionValues[i])
```

**Path B — via convertOrderingValueToEngineType (see #10)**

## 5. `constructFinalHoodieRecord()`
```
HoodieFileGroupReader.getClosableHoodieRecordIterator() :309
  -> CloseableMappingIterator.apply()
    -> readerContext.getRecordContext()
      -> RecordContext.constructFinalHoodieRecord(bufferedRecord)
```

## 6. `extractDataFromRecord()`

**Path A**
```
BufferedRecords.fromHoodieRecord()                      :44
  -> RecordContext.extractDataFromRecord(record, schema, props)
```

**Path B**
```
StreamingFileGroupRecordBufferLoader.loadRecords()      :89
  -> RecordContext.extractDataFromRecord(hoodieRecord, schema, props)
```

## 7. `getRecordKey()`

**Path A — from engine record via converter**
```
HoodieFileGroupReader.initRecordIterators()
  -> BufferedRecordConverter.createConverter()          :56
    -> RecordContext.getRecordKey(record, readerSchema)
```

**Path B — from HoodieRecord**
```
BufferedRecords.fromHoodieRecord()                      :45
  -> RecordContext.getRecordKey(data, schema)
```

**Path C — from base record in key-based buffer**
```
KeyBasedFileGroupRecordBuffer.hasNextBaseRecord()       :138
  -> readerContext.getRecordContext()
    -> RecordContext.getRecordKey(baseRecord, readerSchema)
```

## 8. `encodeAvroSchema()`

```
BufferedRecords.fromHoodieRecord()                      :46
  -> RecordContext.encodeAvroSchema(schema)

BufferedRecords.fromEngineRecord() [overload 2]         :58
  -> RecordContext.encodeAvroSchema(schema)

BufferedRecords.fromEngineRecord() [overload 3]         :64
  -> RecordContext.encodeAvroSchema(schema)
```

## 9. `getOrderingValue(T, Schema, List<String>)`
```
BufferedRecords.fromEngineRecord() [overload 2]         :59
  -> RecordContext.getOrderingValue(record, schema, orderingFieldNames)

BufferedRecords.fromEngineRecord() [overload 3]         :65
  -> RecordContext.getOrderingValue(record, schema, orderingFieldNames)
```

## 10. `getOrderingValue(DeleteRecord)`
```
BufferedRecords.fromDeleteRecord() [overload 1]         :70
  -> RecordContext.getOrderingValue(deleteRecord)

BufferedRecords.fromDeleteRecord() [overload 2]         :75
  -> RecordContext.getOrderingValue(deleteRecord)
```

## 11. `convertOrderingValueToEngineType()`
```
BufferedRecords.fromHoodieRecord()                      :48
  -> RecordContext.convertOrderingValueToEngineType(orderingValue)
    -> RecordContext.convertValueToEngineType(value)    [for ArrayComparable]
```

## 12. `getDeleteRow()`
```
UpdateProcessor.StandardUpdateProcessor.processUpdate() :96
  -> readerContext.getRecordContext()
    -> RecordContext.getDeleteRow(recordKey)
```

## 13. `decodeAvroSchema()`

**Path A — payload update**
```
UpdateProcessor.PayloadUpdateProcessor.handleNonDeletes() :137
  -> readerContext.getRecordContext()
    -> RecordContext.decodeAvroSchema(mergedRecord.getSchemaId())
```

**Path B — merger factory**
```
BufferedRecordMergerFactory.[merger impl]               :528
  -> RecordContext.getSchemaFromBufferRecord(bufferedRecord)
    -> RecordContext.decodeAvroSchema(record.getSchemaId())
```

## 14. `convertToAvroRecord()`

**Path A — payload update**
```
UpdateProcessor.PayloadUpdateProcessor.handleNonDeletes() :138
  -> readerContext.getRecordContext()
    -> RecordContext.convertToAvroRecord(mergedRecord.getRecord(), schema)
```

**Path B — merger factory**
```
BufferedRecordMergerFactory.[merger impl]               :529
  -> RecordContext.convertToAvroRecord(bufferedRecord.getRecord(), schema)
```

## 15. `convertAvroRecord()`

**Path A — payload update rewrite**
```
UpdateProcessor.PayloadUpdateProcessor.handleNonDeletes() :147
  -> readerContext.getRecordContext()
    -> RecordContext.convertAvroRecord(rewrittenRecord.getData())
```

**Path B — custom payload merger**
```
BufferedRecordMergerFactory.CustomPayloadRecordMerger.deltaMerge() :480
  -> RecordContext.convertAvroRecord(mergedRecord.toIndexedRecord(...).getData())
```

**Path C — custom record merger**
```
BufferedRecordMergerFactory.CustomRecordMerger.finalMerge() :505
  -> RecordContext.convertAvroRecord(indexedRecord)
```

## 16. `getValue()`
```
PartialUpdateHandler.reconcileDefaultValues()           :120,122
  -> RecordContext.getValue(high.getRecord(), highSchema, fieldName)
  -> RecordContext.getValue(low.getRecord(), lowSchema, fieldName)

PartialUpdateHandler.reconcileMarkerValues()            :164,168
  -> RecordContext.getValue(high.getRecord(), highSchema, fieldName)
  -> RecordContext.getValue(low.getRecord(), lowSchema, fieldName)
```

## 17. `constructEngineRecord()`
```
PartialUpdateHandler.reconcileDefaultValues()           :131
  -> RecordContext.constructEngineRecord(newSchema, fieldVals)

PartialUpdateHandler.reconcileMarkerValues()            :177
  -> RecordContext.constructEngineRecord(newSchema, fieldVals)
```

## 18. `getTypeConverter()`
```
PartialUpdateHandler.reconcileMarkerValues()            :167
  -> RecordContext.getTypeConverter()
    -> JavaTypeConverter.castToString(newValue)
```

## 19. `isDeleteRecord()`

**Path A**
```
KeyBasedFileGroupRecordBuffer.processDataBlock()        :99
  -> readerContext.getRecordContext()
    -> RecordContext.isDeleteRecord(nextRecord, deleteContext)
```

**Path B**
```
PositionBasedFileGroupRecordBuffer.processDataBlock()   :149
  -> readerContext.getRecordContext()
    -> RecordContext.isDeleteRecord(evolvedNextRecord, deleteContext)
```

**Path C**
```
StreamingFileGroupRecordBufferLoader.loadRecords()      :100
  -> RecordContext.isDeleteRecord(data, deleteContext)
```

## 20. `getMetaFieldValue()` — called internally by `isDeleteRecord`
```
RecordContext.isDeleteRecord(record, deleteContext)
  -> RecordContext.isDeleteHoodieOperation(record, schema)  :280
    -> RecordContext.getMetaFieldValue(record, hoodieOperationPos)
```

---

**Key structural insight**: `HoodieFileGroupReader` itself touches only 5 APIs directly (`setPartitionPath`, `supportsParquetRowIndex`, `seal`, `convertValueToEngineType`, `constructFinalHoodieRecord`). The remaining 15 APIs are reached through two delegation chains:
- **Record buffering path**: `BufferedRecords` / `BufferedRecordConverter` / buffer impls
- **Merge/update path**: `UpdateProcessor` → `BufferedRecordMergerFactory` → `PartialUpdateHandler`

────────────────────────────────────────────────────────────────────────────────
👤 USER:
to construct a reader context for spark, what are the input parameters

────────────────────────────────────────────────────────────────────────────────
🤖 ASSISTANT:
  🔍 Grep: pattern="class.*ReaderContext.*extends HoodieReaderContext" path=.

────────────────────────────────────────────────────────────────────────────────
🤖 ASSISTANT:
  📖 Read: /Users/zhanyeha/hudi-internal/hudi-client/hudi-spark-client/src/main/scala/org/apache/hudi/BaseSparkInternalRowReaderContext.java

────────────────────────────────────────────────────────────────────────────────
🤖 ASSISTANT:
  🔍 Grep: pattern="class BaseSparkInternalRecordContext" path=.

────────────────────────────────────────────────────────────────────────────────
🤖 ASSISTANT:
  🔍 Grep: pattern="class.*extends.*BaseSparkInternalRowReaderContext" path=.

────────────────────────────────────────────────────────────────────────────────
🤖 ASSISTANT:
Now I have the full picture. Here's the answer:

---

## Constructor chain for Spark reader context

The concrete class is `SparkFileFormatInternalRowReaderContext`, which requires **5 parameters**:

```scala
// SparkFileFormatInternalRowReaderContext.scala:58-63
new SparkFileFormatInternalRowReaderContext(
    baseFileReader: SparkColumnarFileReader,   // Spark parquet reader (PartitionedFile → Iterator[InternalRow])
    filters: Seq[Filter],                      // pushdown filters (optional, may be pushed into reader)
    requiredFilters: Seq[Filter],              // filters always applied, even during merging
    storageConfiguration: StorageConfiguration[_],
    tableConfig: HoodieTableConfig
)
```

Internally its constructor immediately builds a `SparkFileFormatInternalRecordContext(tableConfig)` and passes everything up the chain:

```
SparkFileFormatInternalRowReaderContext(baseFileReader, filters, requiredFilters, storageConfig, tableConfig)
  -> BaseSparkInternalRowReaderContext(storageConfig, tableConfig, new SparkFileFormatInternalRecordContext(tableConfig))
    -> HoodieReaderContext(storageConfig, tableConfig, Option.empty(), Option.empty(), recordContext)

SparkFileFormatInternalRecordContext(tableConfig)
  -> BaseSparkInternalRecordContext(tableConfig)
    -> RecordContext(tableConfig, new DefaultJavaTypeConverter())
```

## What each parameter drives

| Parameter | Where used |
|-----------|-----------|
| `baseFileReader` | `getFileRecordIterator()` — reads base parquet files and log blocks |
| `filters` | Applied when no log files and no bootstrap merge needed |
| `requiredFilters` | Always applied; combined with `filters` into `allFilters` and `bootstrapSafeFilters` |
| `storageConfiguration` | Stored in `HoodieReaderContext`, passed to storage I/O |
| `tableConfig` | Drives `baseFileFormat`, record key extraction strategy (`populateMetaFields` vs virtual keys), merge mode, table version |

## What `tableConfig` determines inside `RecordContext`

At `RecordContext.<init>` (:70-74):
- If `tableConfig.populateMetaFields()` → uses `metadataKeyExtractor()` (reads `_hoodie_record_key` meta field directly)
- Otherwise → uses `virtualKeyExtractor(tableConfig.getRecordKeyFields())` (computes key from user-defined key fields)

So the **minimum required inputs** to construct a working Spark reader context are:
1. A `SparkColumnarFileReader` (the Spark parquet reader callback)
2. `StorageConfiguration` (Hadoop conf wrapper)
3. `HoodieTableConfig` (from `.hoodie/hoodie.properties`)
4. Optionally: pushdown `Filter`s

────────────────────────────────────────────────────────────────────────────────
👤 USER:
could you tell me how the record context is related to the record type returned by hoodie file group reader

────────────────────────────────────────────────────────────────────────────────
🤖 ASSISTANT:
Now I have the full picture. Here's the relationship:

---

## How RecordContext relates to the output record type

`HoodieFileGroupReader<T>` exposes **three public iterator methods**, and the output type of each is determined by the `IteratorMode` passed to `getBufferedRecordIterator()`. `RecordContext` sits at the boundary where the internal `BufferedRecord<T>` gets projected into the caller-facing type.

```
HoodieFileGroupReader<T>
        │
        ▼
getBufferedRecordIterator(mode)  ─── always produces ClosableIterator<BufferedRecord<T>>
        │
        ├─ getClosableIterator()           → ClosableIterator<T>
        │       maps: BufferedRecord::getRecord       ← raw engine record, RecordContext not involved
        │
        ├─ getClosableHoodieRecordIterator() → ClosableIterator<HoodieRecord<T>>
        │       maps: recordContext.constructFinalHoodieRecord(bufferedRecord)   ← RecordContext involved
        │
        └─ getClosableKeyIterator()        → ClosableIterator<String>
                maps: BufferedRecord::getRecordKey    ← RecordContext not involved
```

---

## The central role: `constructFinalHoodieRecord`

This is the only RecordContext method that bridges `BufferedRecord<T>` → `HoodieRecord<T>`. Looking at `RecordContext.java`:

```java
// RecordContext.java:147
public HoodieRecord<T> constructFinalHoodieRecord(BufferedRecord<T> bufferedRecord) {
    if (readerContext.getIteratorMode() == IteratorMode.HOODIE_RECORD) {
        return constructHoodieRecord(bufferedRecord);   // abstract — engine specific
    }
    ...
}
```

And in `BaseSparkInternalRecordContext.java:81-95`, Spark's implementation of `constructHoodieRecord`:

```java
// wraps InternalRow into HoodieSparkRecord (or HoodieEmptyRecord for deletes)
HoodieKey hoodieKey = new HoodieKey(bufferedRecord.getRecordKey(), partitionPath);
if (bufferedRecord.isDelete()) {
    return new HoodieEmptyRecord<>(hoodieKey, ...);
}
return new HoodieSparkRecord(hoodieKey, row, cachedSchema, false, ...);
```

---

## The internal currency: `BufferedRecord<T>`

Everything inside the reader pipeline — buffering, merging, log replay — works in terms of `BufferedRecord<T>`, not raw `T` or `HoodieRecord<T>`. `BufferedRecord` carries:

| Field | Source |
|-------|--------|
| `record: T` | Raw engine record (e.g. `InternalRow`) |
| `recordKey: String` | Extracted via `RecordContext.getRecordKey()` |
| `orderingValue` | Extracted via `RecordContext.getOrderingValue()` |
| `schemaId` | Encoded via `RecordContext.encodeAvroSchema()` |
| `isDelete` | Flagged via `RecordContext.isDeleteRecord()` |

`RecordContext` is what **populates** the `BufferedRecord` on ingestion and **unwraps** it into the output type on egress.

---

## Summary diagram

```
File / Log
    │
    │  getFileRecordIterator() → raw T (InternalRow)
    ▼
RecordContext.seal(T)                    ← copy/freeze raw record
RecordContext.getRecordKey(T)            ┐
RecordContext.encodeAvroSchema()         │  BufferedRecordConverter wraps T
RecordContext.getOrderingValue(T)        │  into BufferedRecord<T>
RecordContext.isDeleteRecord(T)          ┘
    │
    │  BufferedRecord<T>  (internal currency throughout merge pipeline)
    │
    ├── merge / update path (UpdateProcessor, PartialUpdateHandler, etc.)
    │       RecordContext.convertToAvroRecord()
    │       RecordContext.convertAvroRecord()
    │       RecordContext.constructEngineRecord()
    │       ...
    │
    ▼
RecordContext.constructFinalHoodieRecord(BufferedRecord<T>)   ← egress
    │
    ├── getClosableHoodieRecordIterator()  → HoodieRecord<T>  (HoodieSparkRecord / HoodieEmptyRecord)
    ├── getClosableIterator()              → T                 (raw InternalRow, bypasses RecordContext)
    └── getClosableKeyIterator()           → String            (bypasses RecordContext)
```

**Key point**: `RecordContext` is the type adapter between the engine's native record (`T`) and Hudi's record model. It is invoked at ingestion to enrich raw engine records into `BufferedRecord`, throughout the merge pipeline to transform them, and at egress only when the caller wants `HoodieRecord<T>` — the raw-`T` and key-only paths skip it entirely.

────────────────────────────────────────────────────────────────────────────────
👤 USER:
could you tell me how the record type that hoodie file group reader returns and what are the intermediate     
  format it uses at various stages to store the records extracted from log file and base file. Show me in a     
  call stack tree so I know both the stage and the record type

────────────────────────────────────────────────────────────────────────────────
🤖 ASSISTANT:
  📂 Glob: hudi-common/src/main/java/org/apache/hudi/common/table/read/buffer/*.java

────────────────────────────────────────────────────────────────────────────────
🤖 ASSISTANT:
Now I have everything I need. Here is the full call stack tree showing both stage and record type at each level:

---

---

## Summary: record type at each stage

| Stage | Record type | Notes |
|-------|------------|-------|
| Base file read | `T` (`InternalRow`) | Raw from parquet reader |
| Log data block deserialization | `T` | Deserialized by `HoodieDataBlock.deserializeRecords()` |
| Log data block after schema evolution | `T` (projected) | `RecordContext.projectRecord()` applied |
| Log delete block | `DeleteRecord` | Avro-free; key + ordering value only |
| After wrapping for buffer | `BufferedRecord<T>` | Enriched with key, ordering, schemaId, delete flag |
| In-buffer storage (delta merge done) | `BufferedRecord<T>` (binary form) | `toBinary()` converts inner `T` to `UnsafeRow` for spillable map |
| After base+log final merge | `BufferedRecord<T>` | Merged result; inner `T` is result of merger |
| After `UpdateProcessor` | `BufferedRecord<T>` (sealed) | `seal()` copies/freezes the inner `T` |
| Output — `getClosableIterator` | `T` | Inner engine record unwrapped |
| Output — `getClosableHoodieRecordIterator` | `HoodieRecord<T>` | Wrapped in `HoodieSparkRecord` / `HoodieEmptyRecord` |
| Output — `getClosableKeyIterator` | `String` | Only the record key |
| Output — `getClosableBufferedRecordIterator` | `BufferedRecord<T>` | Full internal representation |
