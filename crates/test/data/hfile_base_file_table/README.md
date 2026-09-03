# `hfile_base_file_table`

A merge-on-read table that is **not** the metadata table, whose base file and log block are both
HFile. It exists to pin two things: that an HFile base file is readable on an ordinary table — the
reader's capability, not the table's location, decides — and that both merge modes reach the
right answer over HFile inputs.

## Shape

| key | base fare | base `ts` | log fare | log `ts` |
| --- | --- | --- | --- | --- |
| `uuid0001` | 11 | 5001 | — | — |
| `uuid0002` | 12 | 5002 | — | — |
| `uuid0003` | 13 | 5003 | 102 | **1000** |
| `uuid0004` | 14 | 5004 | 103 | **9000** |
| `uuid0005` | — | — | 104 | 5005 |
| `uuid0006` | — | — | 105 | 5006 |

The base file is committed at `20250101000000000`, the log block at `20250102000000000`, so the
log is always the later **commit**. The two overlap on `uuid0003` and `uuid0004`, and their event
times are set so the overlap disagrees between modes:

- `uuid0003` — the log's event time (1000) is **older** than the base's (5003)
- `uuid0004` — the log's event time (9000) is **newer** than the base's (5004)

## What a correct read returns

| mode | uuid0001 | uuid0002 | uuid0003 | uuid0004 | uuid0005 | uuid0006 |
| --- | --- | --- | --- | --- | --- | --- |
| `COMMIT_TIME_ORDERING` | 11 | 12 | **102** | 103 | 104 | 105 |
| `EVENT_TIME_ORDERING` | 11 | 12 | **13** | 103 | 104 | 105 |

`uuid0003` is the discriminator: commit-time takes the log's value because the log is the later
commit, event-time keeps the base's because the log's event time is older. A test that cannot tell
those two apart is not testing the merge.

The other shapes stay covered: dropping the log block gives four rows at 11–14, and losing the base
gives four rows from `uuid0003`.

Records carry `_hoodie_*` meta fields and an ordinary schema, so nothing about them is
metadata-table specific.

## Regenerating

The HFiles are written by Hudi's own `HFileWriterImpl` with Avro-encoded values and the writer
schema in the file info. The log file wraps `log.hfile` in a single `HfileData` (type 4) block with
the instant time and schema in the block header.
