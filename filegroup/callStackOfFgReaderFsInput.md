# Invariants
Invariant
```
  ════════════════════════════════════════════════════════════════════════════════════
   SUMMARY: Properties of the FileSlice / InputSplit reaching the FG Reader
  ════════════════════════════════════════════════════════════════════════════════════

     P1  Committed-only filtering
         - original:  filterUncommittedFiles()  — base file AND log files
                      both checked via completionTimeQueryView.isCompleted()
         - MDT path:  filterUncommittedLogs()   — log files only
                      base file accepted if it passes P5 (isFileSliceCommitted)
                      even when its compaction is still inflight

     P2  Log files sorted by LogFileComparator              [unchanged]
         - Applied in buildFileGroups line 259, and again in InputSplit:56

     P3  Log-to-base binding via completion time            [unchanged]
         - addLogFile(completionTimeQueryView, logFile)     [ATFSV:260 / HFG:128-148]

     P4  Replaced file groups excluded
         - original:  isFileGroupReplaced()      — no time bound
         - MDT path:  isFileGroupReplacedBeforeOrOn(maxInstantTime)
                      only replaces ≤ queryInstant are considered

     P5  Base instant bounded by visible timeline           [unchanged, applied twice]
         - isFileSliceCommitted() inside HoodieFileGroup    [HFG:158-163]
         - additional bound: baseInstantTime ≤ maxInstantTime [HFG:215]

     P6  CDC log files excluded                             [unchanged]
         - InputSplit constructor line 57

     P7  Bootstrap base file mapping                        [unchanged]
         - addBootstrapBaseFileIfPresent()                  [ATFSV:1096]

     P8  Pending compaction file slice injected             [unchanged]
         - buildFileGroups addPendingCompactionFileSlice=true [ATFSV:245-254]

     NEW Compaction slice merging (no equivalent in original)
         - fetchMergedFileSlice() merges pre-compaction base
           with log files spanning the compaction boundary  [ATFSV:1595-1620]

   Legend:
     ATFSV = AbstractTableFileSystemView
     HFG   = HoodieFileGroup
```

# Call stack tree
```
  Engine (Spark SQL: SELECT * FROM hoodie_table)
    │
    ▼
  Spark3HoodiePruneFileSourcePartitions.apply()
    │
    ▼
  HoodieFileIndex.filterFileSlices()                                         [HoodieFileIndex.scala:227]
    │
    ▼
  HoodieFileIndex.prunePartitionsAndGetFileSlices()                          [HoodieFileIndex.scala:351]
    │
    ▼
  BaseHoodieTableFileIndex.getInputFileSlices(partitions...)                 [BaseHoodieTableFileIndex.java:252]
    │
    └── ensurePreloadedPartitions(partitionPaths)                            [line 259]
          │
          └── loadFileSlicesForPartitions(missingPartitions)                 [line 267]
                │
                │  // Resolve query instant:
                │  activeTimeline = getActiveTimeline()                      [line 279]
                │  // ─► metaClient.getCommitsAndCompactionTimeline()
                │  //    DATA TABLE timeline, not MDT's own timeline
                │  queryInstant = specifiedQueryInstant
                │      .or(() -> latestInstant.requestedTime)                [line 281]
                │
                ├─── STEP 1: FILE LISTING via MDT                           [line 285]
                │      listPartitionPathFiles(partitions, activeTimeline)
                │        │
                │        └── tableMetadata.getAllFilesInPartitions(          [line 489]
                │                missingPartitionPathsMap.keySet(),
                │                getPartitionPathFilter(activeTimeline))
                │              │
                │              ▼  [BaseTableMetadata]
                │            fetchAllFilesInPartitionPaths()                [line 315]
                │              └── readIndexRecordsWithKeys()               [line 331]
                │                    │   [HoodieBackedTableMetadata]
                │                    └── getLatestMergedFileSlicesBeforeOrOn
                │                          (MDT partition, queryInstant)    [line 467-468]
                │                          // reads MDT's own file slices
                │                          // to locate the avro log records
                │                          // holding file listings
                │                          │
                │                          └── HoodieMetadataPayload
                │                                .getFileList()
                │                                // returns List<StoragePathInfo>
                │                                // — raw path strings only,
                │                                // no FileSlice structure yet
                │
                │
                └─── STEP 2: COW fast-path guard                           [line 292-297]
                        if (useLatestBaseFilesPathFilterForListing
                            && isCOW || isReadOptimized)
                          → generatePartitionFileSlicesPostROTablePathFilter()
                            // skips HoodieTableFileSystemView entirely:
                            //   StoragePathInfo → HoodieBaseFile
                            //                  → FileSlice directly        [line 320-323]
                            // NO P1/P3/P4/P5/P7/P8 applied
                            // only for COW snapshot / READ_OPTIMIZED
                            //
                            // *** NOT taken in the MOR stack trace above ***
                │
                │
                └─── STEP 3: MOR / general path                            [line 298]
                        filterFiles(partitions, activeTimeline,
                                    allFiles, queryInstant)
                          │
                          │  // Construct a fresh HoodieTableFileSystemView
                          │  // from the raw StoragePathInfo list returned
                          │  // by MDT.  The DATA TABLE's timeline is passed.
                          new HoodieTableFileSystemView(                     [line 342]
                              metaClient, activeTimeline, allFiles)
                            │
                            └── addFilesToView(partitionPath, statuses)     [ATFSV:191]
                                  │
                                  └── buildFileGroups(partitionPath,        [ATFSV:193]
                                          statuses,
                                          visibleCommitsAndCompactionTimeline,
                                          addPendingCompaction=true)
                                        │
                                        │  // Group raw files into base/log maps
                                        │  baseFiles  grouped by fileId     [line 232]
                                        │  logFiles   grouped by fileId     [line 234]
                                        │
                                        │  // Per file group:
                                        │  HoodieFileGroup(partition,       [line 241]
                                        │      fileId, timeline)
                                        │    // timeline = DATA TABLE's
                                        │    // visibleCommitsAndCompactionTimeline
                                        │    // lastInstant captured here   [HFG:92]
                                        │
                                        ├── A-1: addBaseFile(baseFile)      [line 243]
                                        │
                                        ├── A-2: PROPERTY 8                 [line 245-255]
                                        │     if pendingCompaction exists
                                        │       group.addNewFileSliceAtInstant(
                                        │           compactionInstantTime)
                                        │     // injects empty slice at
                                        │     // compaction request time so
                                        │     // later log files land there
                                        │
                                        └── A-3: PROPERTY 3                 [line 259-260]
                                              logFiles.sorted(
                                                HoodieLogFile
                                                .getLogFileComparator())
                                              .forEach(logFile ->
                                                group.addLogFile(
                                                  completionTimeQueryView,
                                                  logFile))
                                                │
                                                └── getBaseInstantTime(     [HFG:129]
                                                      completionTimeQueryView,
                                                      logFile)
                                                      │
                                                      │ // completionTimeQueryView
                                                      │ // .getCompletionTime(
                                                      │ //   latestSliceKey,
                                                      │ //   logFile.deltaCommitTime)
                                                      │ //              [HFG:134]
                                                      │ // Walk slices newest→oldest
                                                      │ // find largest base instant
                                                      │ // whose completion time ≤
                                                      │ // log's completion time [HFG:138]
                                                      │ // pending → firstKey()  [HFG:147]
                                                      └──► fileSlices.get(baseInstant)
                                                            .addLogFile(logFile)
                                                                          [HFG:125]
    ▼
  (HoodieTableFileSystemView now holds fully-built HoodieFileGroup objects)
    │
    ▼
  [queryInstant.isPresent() == true in the stack trace above]
    │
    ▼
  getLatestMergedFileSlicesBeforeOrOn(                                       [ATFSV:1068]
      partitionPath, queryInstant)
    │
    └── getLatestMergedFileSliceBeforeOrOnInternal(                          [ATFSV:1077]
            partitionStr,
            maxInstantTime = queryInstant,
            currentInstantTime = queryInstant,
            includeInflight = false)
          │
          │  fetchAllStoredFileGroups(partition)                             [ATFSV:1085]
          │
          ├── PROPERTY 4 (time-scoped variant)                              [ATFSV:1086]
          │     .filter(fg ->
          │         !isFileGroupReplacedBeforeOrOn(
          │             fg.getFileGroupId(), maxInstantTime))
          │     // Only REPLACE instants ≤ maxInstantTime exclude the group.
          │     // A future replace is invisible to this query instant.
          │     // [differs from getLatestFileSlices which uses
          │     //  isFileGroupReplaced() with no time bound]
          │
          ├── PROPERTY 5 (re-applied per file-group)                        [ATFSV:1088]
          │     fileGroup.getLatestFileSliceBeforeOrOn(maxInstantTime)
          │       │
          │       └── getAllFileSlices()                          [HFG:183-188]
          │             .filter(this::isFileSliceCommitted)
          │               │
          │               │  isFileSliceCommitted(slice):        [HFG:158-163]
          │               │    (a) baseInstantTime
          │               │        ≤ lastInstant.requestedTime
          │               │    (b) timeline.containsOrBefore
          │               │         TimelineStarts(baseInstant)
          │               │
          │             .filter(slice -> baseInstantTime         [HFG:215]
          │                       ≤ maxInstantTime)
          │             .findFirst()
          │     // Two-level gate: committed (P5) AND ≤ maxInstantTime
          │
          ├── PROPERTY 1 (logs only — base file NOT re-checked)             [ATFSV:1091-1092]
          │     fileSlice = filterUncommittedLogs(fileSlice)
          │       │
          │       └── fileSlice.getLogFiles()                    [ATFSV:627]
          │             .filter(logFile ->
          │                 completionTimeQueryView
          │                   .isCompleted(logFile.getDeltaCommitTime()))
          │             // uncommitted log files dropped
          │             // base file passes through unconditionally
          │             // [contrast: getLatestFileSlices calls
          │             //  filterUncommittedFiles which checks base
          │             //  file too via completionTimeQueryView]
          │
          ├── NEW: pending-compaction slice merging                         [ATFSV:1091]
          │     fetchMergedFileSlice(fileGroup, fileSlice,
          │         currentInstantTime, includeInflight=false)
          │       │
          │       └── getPendingCompactionOperationWithInstant(             [ATFSV:1600]
          │               fileGroup.getFileGroupId())
          │             if present && fileSlice.baseInstant
          │                         == compactionInstantTime:              [ATFSV:1612]
          │               prevFileSlice =
          │                 fileGroup.getLatestFileSliceBefore(            [ATFSV:1613]
          │                     compactionInstantTime)
          │               return mergeCompactionPendingFileSlices(         [ATFSV:1615]
          │                   fileSlice, prevFileSlice)
          │               // produces virtual slice:
          │               //   baseFile  = prevFileSlice.baseFile
          │               //   logFiles  = prevFileSlice.logFiles
          │               //             + fileSlice.logFiles (current)
          │             else: return fileSlice unchanged
          │
          └── PROPERTY 7                                                    [ATFSV:1096]
                .map(this::addBootstrapBaseFileIfPresent)
                  // if baseInstantTime == METADATA_BOOTSTRAP_INSTANT_TS
                  // attach external bootstrap base file path
    ▼
  FileSlice { baseFile, logFiles } returned to filterFiles()
    │
    ▼
  (collected into List<FileSlice> per partition, cached in
    cachedAllInputFileSlices, handed back to HoodieFileIndex)
    │
    │  NOTE: PROPERTY 2 and PROPERTY 6 are applied downstream
    │  inside HoodieFileGroupReader / InputSplit constructor,
    │  identical to the original path.
    ▼
  HoodieFileGroupReader.Builder.withFileSlice(fileSlice)                           [line 396]
    │  this.baseFileOption = fileSlice.getBaseFile();                               [line 397]
    │  this.logFiles = fileSlice.getLogFiles();                                    [line 398]
    │
    └── .build()                                                                   [line 496]
          │
          └── new InputSplit(baseFileOption, Either.left(logFiles), ...)           [line 521]
                │
                │  ┌──────────────────────────────────────────────────────────────────────┐
                │  │ InputSplit constructor                                  [line 51]    │
                │  │                                                                      │
                │  │ this.logFiles = recordsToMerge.asLeft()                              │
                │  │                                                                      │
                │  │   // PROPERTY 2: Final log file sort order                           │
                │  │   .sorted(HoodieLogFile.getLogFileComparator())        [line 56]     │
                │  │   //  LogFileComparator.compare():              [HoodieLogFile:215]  │
                │  │   //    1st: deltaCommitTime (request time) ASC     [line 239]       │
                │  │   //    2nd: logVersion ASC                         [line 235]       │
                │  │   //    3rd: writeToken ASC (nulls first)           [line 223]       │
                │  │   //    4th: suffix ASC                             [line 227]       │
                │  │                                                                      │
                │  │   // PROPERTY 6: CDC log files excluded                              │
                │  │   .filter(logFile -> !logFile.getFileName()                          │
                │  │       .endsWith(HoodieCDCUtils.CDC_LOGFILE_SUFFIX))  [line 57]       │
                │  │                                                                      │
                │  │   .collect(Collectors.toList());                     [line 58]       │
                │  └──────────────────────────────────────────────────────────────────────┘
                │
                ▼
  HoodieFileGroupReader.initRecordIterators()                                      [line 128]
```

## Compaction slice merging
---
Compaction slice merging: what it does and why

Background: what the file group looks like when compaction is pending

When a compaction is requested but not yet completed, the file group holds two file slices simultaneously:

```
  fileSlices (TreeMap, reverse order):

    key = compactionInstantTime  →  FileSlice-C
      baseFile  = absent (compaction hasn't produced the parquet yet)
      logFiles  = [delta commits written AFTER the compaction was requested]
                   — these landed here via P3/P8 binding

    key = prevInstantTime        →  FileSlice-P   (the "penultimate" slice)
      baseFile  = last completed parquet file
      logFiles  = [delta commits written BEFORE the compaction was requested]
```

FileSlice-C was injected by P8 (addNewFileSliceAtInstant(compactionInstantTime)) so that new log files have somewhere to land. It has no base file because thecompaction hasn't run yet.

If a reader were handed FileSlice-C directly it would see: no base file, only the post-compaction-request log files. It would be missing all the data in FileSlice-P — the last real parquet and all prior delta commits.

  ---
What fetchMergedFileSlice does
```java
  // ATFSV:1600-1617
  Option<Pair<String, CompactionOperation>> compactionOpWithInstant =
      getPendingCompactionOperationWithInstant(fileGroup.getFileGroupId());

  if (compactionOpWithInstant.isPresent()) {
      String compactionInstantTime = compactionOpWithInstant.get().getKey();

      // Is the slice we resolved actually the compaction placeholder?
      if (fileSlice.getBaseInstantTime().equals(compactionInstantTime)) {   // [line 1612]
          Option<FileSlice> prevFileSlice =
              fileGroup.getLatestFileSliceBefore(compactionInstantTime);    // [line 1613]
          if (prevFileSlice.isPresent()) {
              return mergeCompactionPendingFileSlices(fileSlice, prevFileSlice.get());  // [line 1615]
          }
      }
  }
  return fileSlice;  // no pending compaction, or slice is not the compaction placeholder
```
The trigger condition is: the file slice that getLatestFileSliceBeforeOrOn(maxInstantTime) resolved has baseInstantTime == compactionInstantTime. That means the query
instant landed on or after the compaction request, and the file group is still mid-compaction. The slice the reader would get is incomplete. So the code fetches the
penultimate slice and merges them.

  ---
What mergeCompactionPendingFileSlices produces
```java
  // ATFSV:1573-1583
  private static FileSlice mergeCompactionPendingFileSlices(
          FileSlice lastSlice,        // FileSlice-C: compaction placeholder
          FileSlice penultimateSlice) // FileSlice-P: last real base file
  {
      // Anchor identity on the PENULTIMATE slice's base instant time
      FileSlice merged = new FileSlice(
          penultimateSlice.getPartitionPath(),
          penultimateSlice.getBaseInstantTime(),  // ← prevInstantTime, not compactionInstantTime
          penultimateSlice.getFileId());

      // Take the base file from the PENULTIMATE slice (the real parquet)
      if (penultimateSlice.getBaseFile().isPresent()) {
          merged.setBaseFile(penultimateSlice.getBaseFile().get());
      }

      // Append ALL log files: penultimate first, then last (post-compaction-request)
      penultimateSlice.getLogFiles().forEach(merged::addLogFile);  // pre-compaction deltas
      lastSlice.getLogFiles().forEach(merged::addLogFile);          // post-compaction deltas

      return merged;
  }
```
The result is a single FileSlice that spans the compaction boundary:
```
  merged FileSlice:
    baseInstantTime = prevInstantTime        ← identity of the pre-compaction slice
    baseFile        = last real parquet      ← from FileSlice-P
    logFiles        = [pre-compaction deltas ... post-compaction deltas]
                      ← penultimate's logs + lastSlice's logs, in that order
```
  ---
Net effect: before and after

BEFORE merge (what the file group contains physically):
```
    FileSlice-C  [baseInstant = compactionInstantTime]
      baseFile  = (none)
      logFiles  = [log.3, log.4]   ← written after compaction was requested

    FileSlice-P  [baseInstant = prevInstantTime]
      baseFile  = data-20231001.parquet
      logFiles  = [log.1, log.2]   ← written before compaction was requested
```

AFTER mergeCompactionPendingFileSlices:
```
    merged FileSlice  [baseInstant = prevInstantTime]
      baseFile  = data-20231001.parquet
      logFiles  = [log.1, log.2, log.3, log.4]
```
The reader gets one coherent slice — the parquet from before the compaction request, plus every delta commit written since, as if the compaction never happened. No data from either side of the compaction boundary is lost or duplicated.

# Stack traces
MDT File listing
```
"ScalaTest-run-running-TestMergeModeCommitTimeOrdering@1" prio=5 tid=0x1 nid=NA runnable
  java.lang.Thread.State: RUNNABLE
	  at org.apache.hudi.common.table.view.AbstractTableFileSystemView.getLatestMergedFileSliceBeforeOrOnInternal(AbstractTableFileSystemView.java:1082)
	  at org.apache.hudi.common.table.view.AbstractTableFileSystemView.getLatestMergedFileSlicesBeforeOrOn(AbstractTableFileSystemView.java:1069)
	  at org.apache.hudi.metadata.HoodieTableMetadataUtil.getPartitionFileSlices(HoodieTableMetadataUtil.java:1517)
	  at org.apache.hudi.metadata.HoodieTableMetadataUtil.getPartitionLatestMergedFileSlices(HoodieTableMetadataUtil.java:1457)
	  at org.apache.hudi.metadata.HoodieBackedTableMetadata.lambda$readIndexRecords$8(HoodieBackedTableMetadata.java:468)
	  at org.apache.hudi.metadata.HoodieBackedTableMetadata$$Lambda$4332.418417286.apply(Unknown Source:-1)
	  at java.util.concurrent.ConcurrentHashMap.computeIfAbsent(ConcurrentHashMap.java:1705)
	  - locked <0x6ca2> (a java.util.concurrent.ConcurrentHashMap$ReservationNode)
	  at org.apache.hudi.metadata.HoodieBackedTableMetadata.readIndexRecords(HoodieBackedTableMetadata.java:467)
	  at org.apache.hudi.metadata.HoodieBackedTableMetadata.readIndexRecordsWithKeys(HoodieBackedTableMetadata.java:434)
	  at org.apache.hudi.metadata.HoodieBackedTableMetadata.readIndexRecordsWithKeys(HoodieBackedTableMetadata.java:428)
	  at org.apache.hudi.metadata.BaseTableMetadata.fetchAllFilesInPartitionPaths(BaseTableMetadata.java:331)
	  at org.apache.hudi.metadata.BaseTableMetadata.getAllFilesInPartitions(BaseTableMetadata.java:161)
	  at org.apache.hudi.BaseHoodieTableFileIndex.listPartitionPathFiles(BaseHoodieTableFileIndex.java:489)
	  at org.apache.hudi.BaseHoodieTableFileIndex.loadFileSlicesForPartitions(BaseHoodieTableFileIndex.java:285)
	  at org.apache.hudi.BaseHoodieTableFileIndex.ensurePreloadedPartitions(BaseHoodieTableFileIndex.java:267)
	  at org.apache.hudi.BaseHoodieTableFileIndex.getInputFileSlices(BaseHoodieTableFileIndex.java:253)
	  at org.apache.hudi.HoodieFileIndex.prunePartitionsAndGetFileSlices(HoodieFileIndex.scala:351)
	  at org.apache.hudi.HoodieFileIndex.filterFileSlices(HoodieFileIndex.scala:227)
	  at org.apache.spark.sql.hudi.analysis.Spark3HoodiePruneFileSourcePartitions$$anonfun$apply$1.applyOrElse(Spark3HoodiePruneFileSourcePartitions.scala:55)
	  at org.apache.spark.sql.hudi.analysis.Spark3HoodiePruneFileSourcePartitions$$anonfun$apply$1.applyOrElse(Spark3HoodiePruneFileSourcePartitions.scala:43)
```

at org.apache.hudi.BaseHoodieTableFileIndex.listPartitionPathFiles example return value
```
org.apache.hudi.BaseHoodieTableFileIndex#listPartitionPathFiles returns list of files for the target table applying all kinds of fileters
0 = {StoragePathInfo@27959} "StoragePathInfo{path=file:/private/var/folders/sl/gfxz9xjx57ddcttsjcthbnm40000gn/T/spark-c905e48f-3cc4-400b-84d2-90b271acd3fd/.ab31b6ac-c468-4696-903e-9d2bf015a7be-0_20260406193910367.log.1_0-66-119, length=953, isDirectory=false, blockReplication=0, blockSize=33554432, modificationTime=0, locations=null}"
1 = {StoragePathInfo@27960} "StoragePathInfo{path=file:/private/var/folders/sl/gfxz9xjx57ddcttsjcthbnm40000gn/T/spark-c905e48f-3cc4-400b-84d2-90b271acd3fd/ab31b6ac-c468-4696-903e-9d2bf015a7be-0_0-26-41_20260406193834536.parquet, length=435137, isDirectory=false, blockReplication=0, blockSize=33554432, modificationTime=0, locations=null}"
2 = {StoragePathInfo@27961} "StoragePathInfo{path=file:/private/var/folders/sl/gfxz9xjx57ddcttsjcthbnm40000gn/T/spark-c905e48f-3cc4-400b-84d2-90b271acd3fd/.ab31b6ac-c468-4696-903e-9d2bf015a7be-0_20260406193838793.log.1_0-48-87, length=1144, isDirectory=false, blockReplication=0, blockSize=33554432, modificationTime=0, locations=null}"
```

Then we assemble the files into file groups and get the file slices
```
"ScalaTest-run-running-TestMergeModeCommitTimeOrdering@1" prio=5 tid=0x1 nid=NA runnable
  java.lang.Thread.State: RUNNABLE
	  at org.apache.hudi.common.table.view.AbstractTableFileSystemView.getLatestMergedFileSliceBeforeOrOnInternal(AbstractTableFileSystemView.java:1085)
	  at org.apache.hudi.common.table.view.AbstractTableFileSystemView.getLatestMergedFileSlicesBeforeOrOn(AbstractTableFileSystemView.java:1069)
	  at org.apache.hudi.BaseHoodieTableFileIndex.lambda$filterFiles$7(BaseHoodieTableFileIndex.java:355)
	  at org.apache.hudi.BaseHoodieTableFileIndex$$Lambda$4659.1351560056.apply(Unknown Source:-1)
	  at org.apache.hudi.common.util.Option.map(Option.java:112)
	  at org.apache.hudi.BaseHoodieTableFileIndex.lambda$filterFiles$9(BaseHoodieTableFileIndex.java:354)
	  at org.apache.hudi.BaseHoodieTableFileIndex$$Lambda$4658.27469226.apply(Unknown Source:-1)
	  at java.util.stream.Collectors.lambda$uniqKeysMapAccumulator$1(Collectors.java:178)
	  at java.util.stream.Collectors$$Lambda$173.798278875.accept(Unknown Source:-1)
	  at java.util.stream.ReduceOps$3ReducingSink.accept(ReduceOps.java:169)
	  at java.util.ArrayList$ArrayListSpliterator.forEachRemaining(ArrayList.java:1655)
	  at java.util.stream.AbstractPipeline.copyInto(AbstractPipeline.java:484)
	  at java.util.stream.AbstractPipeline.wrapAndCopyInto(AbstractPipeline.java:474)
	  at java.util.stream.ReduceOps$ReduceOp.evaluateSequential(ReduceOps.java:913)
	  at java.util.stream.AbstractPipeline.evaluate(AbstractPipeline.java:234)
	  at java.util.stream.ReferencePipeline.collect(ReferencePipeline.java:578)
	  at org.apache.hudi.BaseHoodieTableFileIndex.filterFiles(BaseHoodieTableFileIndex.java:350)
	  at org.apache.hudi.BaseHoodieTableFileIndex.loadFileSlicesForPartitions(BaseHoodieTableFileIndex.java:298)
	  at org.apache.hudi.BaseHoodieTableFileIndex.ensurePreloadedPartitions(BaseHoodieTableFileIndex.java:267)
	  at org.apache.hudi.BaseHoodieTableFileIndex.getInputFileSlices(BaseHoodieTableFileIndex.java:253)
	  at org.apache.hudi.HoodieFileIndex.prunePartitionsAndGetFileSlices(HoodieFileIndex.scala:351)
	  at org.apache.hudi.HoodieFileIndex.filterFileSlices(HoodieFileIndex.scala:227)
	  at org.apache.spark.sql.hudi.analysis.Spark3HoodiePruneFileSourcePartitions$$anonfun$apply$1.applyOrElse(Spark3HoodiePruneFileSourcePartitions.scala:55)
	  at org.apache.spark.sql.hudi.analysis.Spark3HoodiePruneFileSourcePartitions$$anonfun$apply$1.applyOrElse(Spark3HoodiePruneFileSourcePartitions.scala:43)
```
Example return result
```
FileSlice(fileGroupId=HoodieFileGroupId(partitionPath=, fileId=ab31b6ac-c468-4696-903e-9d2bf015a7be-0), baseInstantTime=20260406193834536, baseFile=HoodieBaseFile(fileId=ab31b6ac-c468-4696-903e-9d2bf015a7be-0, commitTime=20260406193834536, bootstrapBaseFile=Optional.empty), logFiles=[HoodieLogFile(pathStr=file:/private/var/folders/sl/gfxz9xjx57ddcttsjcthbnm40000gn/T/spark-c905e48f-3cc4-400b-84d2-90b271acd3fd/.ab31b6ac-c468-4696-903e-9d2bf015a7be-0_20260406193910367.log.1_0-66-119, fileSize=953), HoodieLogFile(pathStr=file:/private/var/folders/sl/gfxz9xjx57ddcttsjcthbnm40000gn/T/spark-c905e48f-3cc4-400b-84d2-90b271acd3fd/.ab31b6ac-c468-4696-903e-9d2bf015a7be-0_20260406193838793.log.1_0-48-87, fileSize=1144)])
```
could you trace further down on the path of how the file slices are built, compared to my original thought, any changes to the invariants