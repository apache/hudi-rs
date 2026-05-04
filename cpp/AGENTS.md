# C++ Conventions

`cxx` bridge in `cpp/src/lib.rs` — keep thin; push logic into `crates/core`. Don't widen the FFI
surface to expose internal Hudi types. Functions may throw `rust::Error`; document the error
semantics on the C++ side.
