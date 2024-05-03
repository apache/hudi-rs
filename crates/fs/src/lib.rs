use std::ffi::OsStr;

pub mod file_systems;
pub mod test_utils;

#[macro_export]
macro_rules! assert_approx_eq {
    ($a:expr, $b:expr, $delta:expr) => {{
        let a = $a;
        let b = $b;
        let delta = $delta;
        let diff = if a > b { a - b } else { b - a };

        assert!(
            diff <= delta,
            "assertion failed: `(left ≈ right)`\n  left: `{:?}`,\n right: `{:?}`,\n delta: `{:?}`",
            a, b, delta
        );
    }};
}

pub fn file_name_without_ext(file_name: Option<&OsStr>) -> String {
    return file_name
        .and_then(|e| e.to_str())
        .unwrap()
        .rsplit_once('.')
        .unwrap()
        .0
        .to_owned();
}
