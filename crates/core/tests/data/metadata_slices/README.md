# Synthetic metadata-table HFiles

Five HFiles holding Avro-encoded `HoodieMetadataRecord` values, plus the writer schema
those values were encoded against. They exist because the real fixture tables in this
repo cannot produce the cases below: their metadata tables were never cleaned, and none
of them is non-partitioned.

`metadata-record.avsc` is the 7,969-byte writer schema, copied verbatim out of the
`files` partition of the `v8_trips_8i3u1d` fixture. Each HFile carries the same JSON in
its file-info block under the key `schema`, which is where a reader looks for it; an
HFile of metadata values without that entry cannot be decoded.

| File | Key | `type` | `filesystemMetadata` |
|---|---|---|---|
| `files-live.hfile` | `city=p00000000` | 2 | `f00000000-0_0-1-1_20250101000000000.parquet` → size 1024, `isDeleted` false |
| `files-tombstone.hfile` | `city=p00000000` | 2 | the same file name → size 0, `isDeleted` true |
| `nonpartitioned-base.hfile` | `.` | 2 | `f00000000-0_0-1-1_20250101000000000.parquet` → size 1024, `isDeleted` false |
| `nonpartitioned-log.hfile` | `.` | 2 | `f00000001-0_0-1-1_20250101000000000.parquet` → size 1024, `isDeleted` false |
| `allpartitions-dot.hfile` | `__all_partitions__` | 1 | `.` → size 0, `isDeleted` false |

`type` is the discriminator on `HoodieMetadataRecord`: 1 is the all-partitions record,
whose map keys name partitions, and 2 is a files record, whose map keys name files. A
tombstone is `size = 0, isDeleted = true`, which is what `HoodieMetadataPayload` writes
for a cleaned file.

To regenerate one, write the record above with any Avro encoder and append it to an
HFile through Hudi's own `HFileWriterImpl` (`hudi-io`), keyed as the table says, having
first appended the schema as file info:

```java
HFileContext ctx = HFileContext.builder()
    .compressionCodec(CompressionCodec.NONE).blockSize(1 << 26).build();
try (HFileWriterImpl hw = new HFileWriterImpl(ctx, out)) {
  hw.appendFileInfo("schema", schemaJson.getBytes(StandardCharsets.UTF_8));
  hw.append(key, avroEncodedRecordBytes);
}
```

The classpath needs `protobuf-java` alongside `hudi-io` and `avro`: the file-info block
is a protobuf message, so without it the write fails at close rather than at open.

The Hudi log file that wraps one of these in the tests is built in the test itself, so
the framing the tests depend on stays next to the assertions.
