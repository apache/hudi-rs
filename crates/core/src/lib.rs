use crate::table::Table;

mod error;
mod file_group;
pub mod table;
pub type HudiTable = Table;
mod timeline;

pub const BASE_FILE_EXTENSIONS: [&str; 1] = ["parquet"];

pub fn is_base_file_format_supported(ext: &str) -> bool {
    BASE_FILE_EXTENSIONS.contains(&ext)
}
