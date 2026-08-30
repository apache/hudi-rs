# `metadata_bloom_filters_slice`

One `bloom_filters` file slice from a metadata table: the base HFile and the log
file written after it.

## Why it exists

The corpus reaches three metadata partition types — `files`, `partition_stats`
and `secondary_index`. `bloom_filters` is the fourth, and the only one whose
payload is a raw byte buffer rather than a struct of scalars, so a decoder that
works for the others can still fail on it.

The slice is the file pair, not the table: the test opens it through the file
group reader by path.

## How it was generated

`generate.sql`, run against Spark 3.4.1 with
`org.apache.hudi:hudi-spark3.4-bundle_2.12:1.1.1` (Spark 3.5.x bundles its own
Hudi and fails with `Multiple sources found for HUDI`). 4,000 rows, one file
group per metadata partition.

Row count matters for size: the same script at 600,000 rows produces a
`bloom_filters` base of 4.55 MB against 158 KB here, because a bloom filter is
sized by the entries it holds.

## What it does not carry

`generate.sql` disables `column_stats` deliberately — see the comment in it —
which also suppresses `partition_stats`, since Hudi does not build partition
stats without column stats. Two statistics merge rules that this fixture was
originally meant to reach are therefore not reachable from any Spark workload;
`tasks/eng-47745-multi-block-metadata-fixture/log.md` records the five variants
tried and why each failed.
