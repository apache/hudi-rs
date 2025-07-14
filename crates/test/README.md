# The `test` crate

This is a crate meant for facilitating testing in `hudi-rs` - it provides utilities and test data.

The `data/` directory contains fully prepared sample Hudi tables categorized by table version and configuration options.

Enums in `src/lib.rs` like `QuickstartTripsTable` and `SampleTable` are for conveniently accessing these ready-made
tables in tests.
