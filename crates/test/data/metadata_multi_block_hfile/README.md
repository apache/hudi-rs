# `metadata_multi_block_hfile`

One `record_index` HFile from a metadata table, carrying **9 data blocks** in
563 KB.

## Why it exists

Key pushdown selects which data blocks to fetch. Every other Avro-carrying HFile
committed here has exactly **one** data block, and with one block a seek and a
full scan fetch the same bytes — so a reader that ignored the predicate entirely
would pass against all of them. This fixture is the smallest committed artifact
on which that difference is observable.

It is the file only, not the table: the test opens it through the base-file
reader by path, so the surrounding timeline and the other nine file groups would
be 40 MB of dead weight.

## How it was generated

`generate.sql`, run against Spark 3.4.1 with
`org.apache.hudi:hudi-spark3.4-bundle_2.12:1.1.1` (Spark 3.5.x bundles its own
Hudi and fails with `Multiple sources found for HUDI`). 600,000 rows over two
inserts, then two updates and a delete, with the metadata table pinned to one
file group per partition and compacted every 2 delta commits so the records land
in HFiles rather than log blocks.

The record count is what drives the block count: `hoodie.hfile.block.size` does
not reach the writer (Hudi's `getHFileBlockSize` has exactly one occurrence, its
own declaration), so block count can only be moved by writing more records.
