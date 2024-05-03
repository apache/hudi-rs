use std::error::Error;
use std::{fs::File, path::Path};

use parquet::file::reader::{FileReader, Length, SerializedFileReader};

use crate::assert_approx_eq;

#[derive(Debug)]
pub struct FileMetadata {
    pub path: String,
    pub name: String,
    pub size: u64,
    pub num_records: i64,
}

impl FileMetadata {
    pub fn from_path(p: &Path) -> Result<Self, Box<dyn Error>> {
        let file = File::open(p)?;
        let reader = SerializedFileReader::new(file).unwrap();
        let num_records = reader.metadata().file_metadata().num_rows();
        Ok(Self {
            path: p.to_str().unwrap().to_string(),
            name: p.file_name().unwrap().to_os_string().into_string().unwrap(),
            size: p.metadata().unwrap().len(),
            num_records,
        })
    }
}

#[test]
fn read_file_metadata() {
    let fixture_path = Path::new("fixtures/a.parquet");
    let fm = FileMetadata::from_path(fixture_path).unwrap();
    assert_eq!(fm.path, "fixtures/a.parquet");
    assert_eq!(fm.name, "a.parquet");
    assert_approx_eq!(fm.size, 866, 20);
    assert_eq!(fm.num_records, 5);
}
