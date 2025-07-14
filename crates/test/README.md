# The `test` crate

This is a crate meant for facilitating testing in `hudi-rs` - it provides utilities and test data.

The `data/` directory contains fully prepared sample Hudi tables categorized by table version and configuration options:
each zipped file contains the Hudi table and the corresponding `.sql` contains the SQL used to generate the table.

Enums in `src/lib.rs` like `QuickstartTripsTable` and `SampleTable` are for conveniently accessing these ready-made
tables in tests. They take care of the unzipping and hand over the table paths to callers.
