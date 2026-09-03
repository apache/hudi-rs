# `mor_log_inserts`

A merge-on-read slice whose log records **insert** rather than update: one base
file of 2,000 rows at commit `20250101000000000`, one log file of 40 records at
`20250102000000000`, under keys the base file does not contain.

## Why the log records must be inserts

The instant-range filter is a whole-file gate — `apply_instant_range_filter`
keeps every batch or none, decided by the base file's single commit instant. To
observe it you need a read where excluding the base file leaves something behind.

Every other merge-on-read fixture here has log records that update base keys, so
excluding the base leaves the updates with nothing to update and the read returns
nothing whether the gate fired or not. That is why the gate can be disabled
outright with the whole suite still green.

## Why the test also turns meta fields off

With `hoodie.populate.meta.fields` at its default, `create_commit_time_filter_mask`
filters the same rows at row level *after* the merge, so both mechanisms agree
and neither can be isolated. Setting it false makes that mask a no-op — which is
also the real configuration where this gate is the only enforcement of an
instant window.

## How it was generated

```
fg-gen --out <dir> --files 1 --total-bytes 262144 \
       --log-files 1 --log-records 40 --log-key-offset 50000000 --row-group-rows 500
```

Table version 6, so the commit metadata is JSON. `--log-key-offset` shifts the log
records past the base file's key range; without it they are updates.
