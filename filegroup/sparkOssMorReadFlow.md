# Analyze phase
Analyzer rule `class FindDataSourceTable(sparkSession: SparkSession) extends Rule[LogicalPlan]`
```
  def resolveRelation(checkFilesExist: Boolean = true): BaseRelation = {
    val relation = (providingInstance(), userSpecifiedSchema) match {
      // TODO: Throw when too much is given.
      case (dataSource: SchemaRelationProvider, Some(schema)) =>
        dataSource.createRelation(sparkSession.sqlContext, caseInsensitiveOptions, schema)
```
-> `org.apache.hudi.DefaultSource#createRelation`. It also decides RO v.s. RT.
```

  def createRelation(sqlContext: SQLContext,
                     metaClient: HoodieTableMetaClient,
                     schema: StructType,
                     parameters: Map[String, String]): BaseRelation = {
          case (MERGE_ON_READ, QUERY_TYPE_SNAPSHOT_OPT_VAL, false) =>
            if (isNotMetadataTable) {
              new HoodieMergeOnReadSnapshotHadoopFsRelationFactory(
                sqlContext, metaClient, parameters, userSchema, isBootstrap = false).build()
            } else {
              new MergeOnReadSnapshotRelation(sqlContext, parameters, metaClient, userSchema)
            }
```
# Physical planning phase

## Create file source scan exec
```
case class FileSourceScanExec(
    @transient override val relation: HadoopFsRelation,
    override val output: Seq[Attribute],
    override val requiredSchema: StructType,
    override val partitionFilters: Seq[Expression],
    override val optionalBucketSet: Option[BitSet],
    override val optionalNumCoalescedBuckets: Option[Int],
    override val dataFilters: Seq[Expression],
    override val tableIdentifier: Option[TableIdentifier],
    override val disableBucketedScan: Boolean = false)
  extends FileSourceScanLike {
```
### what base files to read:
org.apache.spark.sql.execution.FileSourceScanLike#dynamicallySelectedPartitions

Internally hoodie file index first gets all file slices, then use this func to extract base file / 1 log file to build spark `FileStatus` object
```
  def convertFileSliceToPartitionDirectory(partitionValues: InternalRow,
                                           fileSlice: FileSlice,
                                           options: Map[String, String]): PartitionDirectory = {
    val logFileEstimationFraction = options.getOrElse(HoodieStorageConfig.LOGFILE_TO_PARQUET_COMPRESSION_RATIO_FRACTION.key(),
      HoodieStorageConfig.LOGFILE_TO_PARQUET_COMPRESSION_RATIO_FRACTION.defaultValue()).toDouble
    // 1. Generate a delegate file for file slice, which spark uses to optimize rdd partition parallelism based on data such as file size
    //    - For file slice only has base file, we directly use the base file size as delegate file size
    //    - For file slice has log file, we estimate the delegate file size based on the log file size and option(base file) size
    val estimationFileSize = fileSlice.getTotalFileSizeAsParquetFormat(logFileEstimationFraction)
    val fileInfo = if (fileSlice.getBaseFile.isPresent) {
      fileSlice.getBaseFile.get().getPathInfo
    } else {
      fileSlice.getLogFiles.findAny().get().getPathInfo
    }
    // create a delegate file status based on the file size estimation
    val delegateFile = new FileStatus(estimationFileSize, fileInfo.isDirectory, 0, fileInfo.getBlockSize, fileInfo.getModificationTime, new Path(fileInfo.getPath.toUri))

    // 2. Generate a partition directory based on the delegate file and partition values
    if (fileSlice.hasLogFiles || fileSlice.hasBootstrapBase) {
      // should read as file slice, so we need to create a mapping from fileId to file slice
      sparkAdapter.getSparkPartitionedFileUtils.newPartitionDirectory(
        sparkAdapter.createPartitionFileSliceMapping(partitionValues, Map(fileSlice.getFileId -> fileSlice)), Seq(delegateFile))
    } else {
      sparkAdapter.getSparkPartitionedFileUtils.newPartitionDirectory(partitionValues, Seq(delegateFile))
    }
  }
```

### MOR how to figure out file group based on the parquet files

relation.fileFormat.buildReaderWithPartitionValues - offloaded to the HoodieMergeOnReadSnapshotHadoopFsRelation:

```
  lazy val inputRDD: RDD[InternalRow] = {
    val options = relation.options +
      (FileFormat.OPTION_RETURNING_BATCH -> supportsColumnar.toString)
    val readFile: (PartitionedFile) => Iterator[InternalRow] =
      relation.fileFormat.buildReaderWithPartitionValues(  <-------------- returns func consuming individual `FileStatus` from `PartitionDirectory`
        sparkSession = relation.sparkSession,
        dataSchema = relation.dataSchema,
        partitionSchema = relation.partitionSchema,
        requiredSchema = requiredSchema,
        filters = pushedDownFilters,
        options = options,
        hadoopConf = relation.sparkSession.sessionState.newHadoopConfWithOptions(relation.options))

    val readRDD = if (bucketedScan) {
      createBucketedReadRDD(relation.bucketSpec.get, readFile, dynamicallySelectedPartitions)
    } else {
      createReadRDD(readFile, dynamicallySelectedPartitions)
    }
    sendDriverMetrics()
    readRDD
  }
```

buildReaderWithPartitionValues method return this reader func, where given parquet file path, extract file group id and use that to figure out all log files
Inside that we build file group reader as well.
```
    (file: PartitionedFile) => {
      // executor
      val storageConf = new HadoopStorageConfiguration(broadcastedStorageConf.value.value)
      val iter = file.partitionValues match {
        // Snapshot or incremental queries.
        case fileSliceMapping: HoodiePartitionFileSliceMapping =>
          val fileGroupName = FSUtils.getFileIdFromFilePath(sparkAdapter
            .getSparkPartitionedFileUtils.getPathFromPartitionedFile(file))
          fileSliceMapping.getSlice(fileGroupName) match {
            case Some(fileSlice) if !isCount && (requiredSchema.nonEmpty || fileSlice.getLogFiles.findAny().isPresent) =>
              val readerContext = new SparkFileFormatInternalRowReaderContext(fileGroupBaseFileReader.value, filters, requiredFilters, storageConf, metaClient.getTableConfig)
              readerContext.setEnableLogicalTimestampFieldRepair(storageConf.getBoolean(ENABLE_LOGICAL_TIMESTAMP_REPAIR, true))
              val props = metaClient.getTableConfig.getProps
              options.foreach(kv => props.setProperty(kv._1, kv._2))
              props.put(HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE.key(), String.valueOf(maxMemoryPerCompaction))
              val baseFileLength = if (fileSlice.getBaseFile.isPresent) {
                fileSlice.getBaseFile.get.getFileSize
              } else {
                0
              }
              val reader = HoodieFileGroupReader.newBuilder()
                .withReaderContext(readerContext)
                .withHoodieTableMetaClient(metaClient)
                .withLatestCommitTime(queryTimestamp)
                .withFileSlice(fileSlice)
                .withDataSchema(dataSchema)
                .withRequestedSchema(requestedSchema)
                .withInternalSchema(internalSchemaOpt)
                .withProps(props)
                .withStart(file.start)
                .withLength(baseFileLength)
                .withShouldUseRecordPosition(shouldUseRecordPosition)
                .build()
              // Append partition values to rows and project to output schema
              appendPartitionAndProject(
                reader.getClosableIterator,
                requestedStructType,
                remainingPartitionSchema,
                outputSchema,
                fileSliceMapping.getPartitionValues,
                fixedPartitionIndexes)
```

### RDD parallelism (file spliting)
For MOR it is not splitable, 1 parquet file belongs to 1 spark task. 1 file group has 1 parquet file. So 1 file group is handled by 1 spark task at a time.
```
  override def isSplitable(sparkSession: SparkSession,
                           options: Map[String, String],
                           path: Path): Boolean = {
    // NOTE: When we have and only the base file that needs to be read with normal reading mode,
    // we can consider the current format to be equivalent to `org.apache.spark.sql.execution.datasources.parquet.ParquetFormat`.
    // Naturally, we can maintain the same `isSplitable` logic as the upper-level format.
    // This will enable us to take advantage of spark's file splitting capability.
    // For overly large single files, we can use multiple concurrent tasks to read them, thereby reducing the overall job reading time consumption
    val superSplitable = super.isSplitable(sparkSession, options, path)
    val splitable = !isMOR && !isIncremental && !isBootstrap && superSplitable
    logInfo(s"isSplitable: $splitable, super.isSplitable: $superSplitable, isMOR: $isMOR, isIncremental: $isIncremental, isBootstrap: $isBootstrap")
    splitable
  }
```

# Chat
 ~/hudi-oss 17df0307-624c-4a31-9d42-c108efee6f9b
────────────────────────────────────────────────────────────────────────────────
👤 USER:
def convertFileSliceToPartitionDirectory(partitionValues: InternalRow,
                                           fileSlice: FileSlice,
                                           options: Map[String, String]): PartitionDirectory = {
    val logFileEstimationFraction = options.getOrElse(HoodieStorageConfig.LOGFILE_TO_PARQUET_COMPRESSION_RATIO_FRACTION.key(),
      HoodieStorageConfig.LOGFILE_TO_PARQUET_COMPRESSION_RATIO_FRACTION.defaultValue()).toDouble
    // 1. Generate a delegate file for file slice, which spark uses to optimize rdd partition parallelism based on data such as file size
    //    - For file slice only has base file, we directly use the base file size as delegate file size
    //    - For file slice has log file, we estimate the delegate file size based on the log file size and option(base file) size
    val estimationFileSize = fileSlice.getTotalFileSizeAsParquetFormat(logFileEstimationFraction)
    val fileInfo = if (fileSlice.getBaseFile.isPresent) {
      fileSlice.getBaseFile.get().getPathInfo
    } else {
      fileSlice.getLogFiles.findAny().get().getPathInfo
    }
    // create a delegate file status based on the file size estimation
    val delegateFile = new FileStatus(estimationFileSize, fileInfo.isDirectory, 0, fileInfo.getBlockSize, fileInfo.getModificationTime, new Path(fileInfo.getPath.toUri))

    // 2. Generate a partition directory based on the delegate file and partition values
    if (fileSlice.hasLogFiles || fileSlice.hasBootstrapBase) {
      // should read as file slice, so we need to create a mapping from fileId to file slice
      sparkAdapter.getSparkPartitionedFileUtils.newPartitionDirectory(
        sparkAdapter.createPartitionFileSliceMapping(partitionValues, Map(fileSlice.getFileId -> fileSlice)), Seq(delegateFile))
    } else {
      sparkAdapter.getSparkPartitionedFileUtils.newPartitionDirectory(partitionValues, Seq(delegateFile))
    }
  }

could you explain for log file only file slice, eventually what does createPartitionFileSliceMapping do and how eventually it is consumed by


buildReaderWithPartitionValues method return this reader func, where given parquet file path, extract file group id and use that to figure out all log files
Inside that we build file group reader as well.

hudi/hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/execution/datasources/parquet/HoodieFileGroupReaderBasedFileFormat.scala
```
    (file: PartitionedFile) => {
      // executor
      val storageConf = new HadoopStorageConfiguration(broadcastedStorageConf.value.value)
      val iter = file.partitionValues match {
        // Snapshot or incremental queries.
        case fileSliceMapping: HoodiePartitionFileSliceMapping =>
          val fileGroupName = FSUtils.getFileIdFromFilePath(sparkAdapter
            .getSparkPartitionedFileUtils.getPathFromPartitionedFile(file))
          fileSliceMapping.getSlice(fileGroupName) match {
            case Some(fileSlice) if !isCount && (requiredSchema.nonEmpty || fileSlice.getLogFiles.findAny().isPresent) =>
              val readerContext = new SparkFileFormatInternalRowReaderContext(fileGroupBaseFileReader.value, filters, requiredFilters, storageConf, metaClient.getTableConfig)
              readerContext.setEnableLogicalTimestampFieldRepair(storageConf.getBoolean(ENABLE_LOGICAL_TIMESTAMP_REPAIR, true))
              val props = metaClient.getTableConfig.getProps
              options.foreach(kv => props.setProperty(kv._1, kv._2))
              props.put(HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE.key(), String.valueOf(maxMemoryPerCompaction))
              val baseFileLength = if (fileSlice.getBaseFile.isPresent) {
                fileSlice.getBaseFile.get.getFileSize
              } else {
                0
              }
              val reader = HoodieFileGroupReader.newBuilder()
                .withReaderContext(readerContext)
                .withHoodieTableMetaClient(metaClient)
                .withLatestCommitTime(queryTimestamp)
                .withFileSlice(fileSlice)
                .withDataSchema(dataSchema)
                .withRequestedSchema(requestedSchema)
                .withInternalSchema(internalSchemaOpt)
                .withProps(props)
                .withStart(file.start)
                .withLength(baseFileLength)
                .withShouldUseRecordPosition(shouldUseRecordPosition)
                .build()
              // Append partition values to rows and project to output schema
              appendPartitionAndProject(
                reader.getClosableIterator,
                requestedStructType,
                remainingPartitionSchema,
                outputSchema,
                fileSliceMapping.getPartitionValues,
                fixedPartitionIndexes)

────────────────────────────────────────────────────────────────────────────────
🤖 ASSISTANT:
Let me trace the key pieces.

────────────────────────────────────────────────────────────────────────────────
🤖 ASSISTANT:
  🤖 Agent: Trace createPartitionFileSliceMapping

────────────────────────────────────────────────────────────────────────────────
🤖 ASSISTANT:
Here's the full picture for **log-file-only file slices**:

## 1. `createPartitionFileSliceMapping` — what it does

It's simple: it wraps a `Map[String, FileSlice]` (keyed by **file group ID**) into a special `InternalRow` subclass (`HoodiePartitionFileSliceMapping`). The mapping is created with exactly one entry:

```scala
Map(fileSlice.getFileId -> fileSlice)
```

The `getSlice(fileId)` method is just `slices.get(fileId)` — a HashMap lookup.

## 2. The trick — smuggling the FileSlice through Spark's partition machinery

Spark's `PartitionDirectory` holds `(partitionValues: InternalRow, files: Seq[FileStatus])`. Normally `partitionValues` is just partition column values. Hudi **replaces** it with `HoodiePartitionFileSliceMapping`, which extends `InternalRow` so Spark accepts it, but also carries the fileId→FileSlice map.

The `delegateFile` (a `FileStatus`) is just a stand-in so Spark can do its split/parallelism planning based on estimated file size. For a log-only slice, the delegate file path is the **log file path**.

## 3. How it's consumed at read time

When `buildReaderWithPartitionValues` returns the reader function:

1. Spark calls `(file: PartitionedFile) => ...` with a `PartitionedFile` derived from the delegate `FileStatus`
2. The code pattern-matches `file.partitionValues` as `HoodiePartitionFileSliceMapping` — this tells it "this needs file-group-level reading"
3. It extracts the file group ID from the delegate file path:
   ```scala
   val fileGroupName = FSUtils.getFileIdFromFilePath(getPathFromPartitionedFile(file))
   ```
   For a log file like `.f-0_20250101010101.log.0_0-0-0`, `getFileIdFromLogPath` uses a regex to extract `f-0` (group 1 of the log file pattern)
4. It looks up the FileSlice: `fileSliceMapping.getSlice(fileGroupName)` → `Some(fileSlice)`
5. It builds a `HoodieFileGroupReader` with that FileSlice, which knows about all the log files (and no base file), and reads/merges them

## Key insight

The entire mechanism is a way to **pass the FileSlice through Spark's file-based planning infrastructure** without Spark knowing about it. Spark sees a normal `PartitionDirectory` with a file and partition values. Hudi's reader function then unpacks the hidden FileSlice mapping from the "partition values" to get the actual log files it needs to read.

────────────────────────────────────────────────────────────────────────────────
👤 USER:
do you know at the time we build file group reader, how these input are populated?
                                                                                                                                               
  Input 2: latestInstantTime — High-watermark for block filtering                                                                              
                                                                                                                                               
  // BaseHoodieLogRecordReader.java:144                                                                                                        
  this.latestInstantTime = readerContext.getLatestCommitTime();
                                                                                                                                               
  Set by:
  HoodieFileGroupReader constructor                         [HoodieFileGroupReader.java:113]                                                   
    └─ readerContext.setLatestCommitTime(latestCommitTime)                                  
                                                                                                                                               
  Input 3: instantRange — Optional incremental range filter                                                                                    
                                                                                                                                               
  // BaseHoodieLogRecordReader constructor, stored as this.instantRange                                                                        
  // LogScanningRecordBufferLoader.java:47                                                                                                     
  .withInstantRange(readerContext.getInstantRange())                                                                                           
  
  Input 4: allowInflightInstants — Whether to include uncommitted blocks                                                                       
                  
  // LogScanningRecordBufferLoader.java:50                                                                                                     
  .withAllowInflightInstants(readerParameters.allowInflightInstants())

────────────────────────────────────────────────────────────────────────────────
🤖 ASSISTANT:
  🤖 Agent: Trace file group reader inputs

────────────────────────────────────────────────────────────────────────────────
🤖 ASSISTANT:
Here's how each is populated in the Spark snapshot read path:

---

### Input 2: `latestCommitTime` (→ `latestInstantTime` in log reader)

**Value:** `queryTimestamp` from `HoodieFileGroupReaderBasedFileFormat`'s constructor.

**Origin** (`HoodieHadoopFsRelationFactory.scala`):
```scala
private lazy val queryTimestamp: Option[String] =
  specifiedQueryTimestamp.orElse(toScalaOption(timeline.lastInstant()).map(_.requestedTime))
```

- If `TIME_TRAVEL_AS_OF_INSTANT` is set in read options → uses that formatted timestamp
- Otherwise → **last completed instant on the timeline** (e.g., `"20240101120000"`)

This flows through:
1. Builder: `.withLatestCommitTime(queryTimestamp)`
2. Constructor: `readerContext.setLatestCommitTime(latestCommitTime)` (`HoodieFileGroupReader.java:113`)
3. Log reader: `this.latestInstantTime = readerContext.getLatestCommitTime()`

**Effect:** The log scanner uses this as a high-watermark — it **skips log blocks whose instant time is greater than this**, so you get a point-in-time consistent read.

---

### Input 3: `instantRange` 

**Value:** `Option.empty()` for Spark snapshot reads.

**Why:** `BaseSparkInternalRowReaderContext` hardcodes it:
```java
// BaseSparkInternalRowReaderContext.java
super(storageConfiguration, tableConfig, Option.empty(), Option.empty(), recordContext);
//                                       ^^^^^^^^^^^^^^  ← instantRangeOpt
```

No code in the Spark snapshot path ever calls `setInstantRange()`. So:
- `readerContext.getInstantRange()` returns empty
- The log scanner gets an empty range → **no instant range filtering**
- Base file records also skip the `applyInstantRangeFilter` path

This parameter becomes relevant for **incremental/CDC queries** where a custom reader context provides an actual range like `[commitA, commitB]`.

---

### Input 4: `allowInflightInstants`

**Value:** `false` (default).

**Why:** The builder declares:
```java
// HoodieFileGroupReader.java, Builder
private boolean allowInflightInstants = false;
```

`HoodieFileGroupReaderBasedFileFormat` never calls `.withAllowInflightInstants(true)`, so it stays `false`.

**Effect:** The log scanner will **skip blocks from uncommitted/inflight instants**. Only blocks from completed commits (up to `latestInstantTime`) are processed. This prevents dirty reads of in-progress writes.

---

### Summary for snapshot reads

| Input | Value | Effect |
|---|---|---|
| `latestCommitTime` | Last completed instant (or time-travel target) | High-watermark: skip log blocks newer than this |
| `instantRange` | `empty` | No range filtering — read all blocks up to the watermark |
| `allowInflightInstants` | `false` | Skip uncommitted/inflight blocks |

Together these ensure a **consistent snapshot**: read everything committed up to the watermark, ignore anything newer or uncommitted.
