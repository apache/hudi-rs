use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};

use tempfile::tempdir;

pub fn extract_test_table(fixture_path: &Path) -> PathBuf {
    let target_dir = tempdir().unwrap().path().to_path_buf();
    let archive = fs::read(fixture_path).unwrap();
    zip_extract::extract(Cursor::new(archive), &target_dir, true).unwrap();
    target_dir
}
