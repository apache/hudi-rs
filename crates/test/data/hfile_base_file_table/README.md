# `hfile_base_file_table`

A merge-on-read table that is **not** the metadata table, whose base file and log block are both
HFile. It exists to pin that an HFile base file is readable on an ordinary table — the reader's
capability, not the table's location, decides.

## Shape

| | keys | fares | commit |
| --- | --- | --- | --- |
| base file | `uuid0001`–`uuid0004` | 11–14 | `20250101000000000` |
| log block | `uuid0003`–`uuid0006` | 102–105 | `20250102000000000` |

The two overlap on `uuid0003` and `uuid0004`. Under `COMMIT_TIME_ORDERING` the log wins on the
overlap, so a correct read returns six rows with fares 11, 12, 102, 103, 104, 105. The shape is
chosen so three different failures look different: dropping the log block gives four rows at
11–14, merging in the wrong direction gives 13 and 14 on the overlap, and losing the base gives
four rows starting at `uuid0003`.

The records carry `_hoodie_*` meta fields and an ordinary schema, so nothing about them is
metadata-table specific.

## Regenerating

The HFiles are written by Hudi's own `HFileWriterImpl` with Avro-encoded values and the writer
schema in the file info, matching what Hudi writes. The log file wraps `log.hfile` in a single
`HfileData` (type 4) block with the instant time and schema in the block header.
