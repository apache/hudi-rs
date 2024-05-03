use std::collections::HashSet;
use std::error::Error;
use std::path::{Path, PathBuf};
use std::{fs, io};

use hashbrown::HashMap;

use hudi_fs::file_systems::FileMetadata;
use hudi_fs::test_utils::extract_test_table;

use crate::file_group::{BaseFile, FileGroup};
use crate::timeline::Timeline;

#[derive(Debug, Clone)]
pub struct MetaClient {
    pub base_path: PathBuf,
    pub timeline: Timeline,
}

impl MetaClient {
    pub fn new(base_path: &Path) -> Self {
        match Timeline::init(base_path) {
            Ok(timeline) => Self {
                base_path: base_path.to_path_buf(),
                timeline,
            },
            Err(e) => panic!("Failed to instantiate meta client: {}", e),
        }
    }

    fn get_leaf_dirs(path: &Path) -> Result<Vec<PathBuf>, io::Error> {
        let mut leaf_dirs = Vec::new();
        let mut is_leaf_dir = true;
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            if entry.path().is_dir() {
                is_leaf_dir = false;
                let curr_sub_dir = entry.path();
                let curr = Self::get_leaf_dirs(&curr_sub_dir)?;
                leaf_dirs.extend(curr);
            }
        }
        if is_leaf_dir {
            leaf_dirs.push(path.to_path_buf())
        }
        Ok(leaf_dirs)
    }

    pub fn get_partition_paths(&self) -> Result<Vec<String>, io::Error> {
        let mut first_level_partition_paths: Vec<PathBuf> = Vec::new();
        for entry in fs::read_dir(self.base_path.as_path())? {
            let p = entry?.path();
            if p.is_dir() && p.file_name().and_then(|e| e.to_str()) != Some(".hoodie") {
                first_level_partition_paths.push(p);
            }
        }
        let mut full_partition_paths: Vec<PathBuf> = Vec::new();
        for p in first_level_partition_paths {
            full_partition_paths.extend(Self::get_leaf_dirs(p.as_path())?)
        }
        let common_prefix_len = self.base_path.to_str().unwrap().len() + 1;
        let mut partition_paths = Vec::new();
        for p in full_partition_paths {
            let full_partition_path = p.to_str().unwrap();
            partition_paths.push(full_partition_path[common_prefix_len..].to_owned())
        }
        Ok(partition_paths)
    }

    pub fn get_file_groups(&self, partition_path: &str) -> Result<Vec<FileGroup>, Box<dyn Error>> {
        let mut part_path = self.base_path.to_path_buf();
        part_path.push(partition_path);
        let mut fg_id_to_base_files: HashMap<String, Vec<BaseFile>> = HashMap::new();
        for entry in fs::read_dir(part_path)? {
            let p = entry?.path();
            if p.is_file() && p.extension().and_then(|e| e.to_str()) == Some("parquet") {
                let file_metadata = FileMetadata::from_path(p.as_path())?;
                let base_file = BaseFile::from_file_metadata(file_metadata);
                let fg_id = &base_file.file_group_id;
                fg_id_to_base_files
                    .entry(fg_id.to_owned())
                    .or_insert_with(Vec::new)
                    .push(base_file);
            }
        }
        let mut file_groups: Vec<FileGroup> = Vec::new();
        for (fg_id, base_files) in fg_id_to_base_files.into_iter() {
            let mut fg = FileGroup::new(fg_id.to_owned(), Some(partition_path.to_owned()));
            for bf in base_files {
                fg.add_base_file(bf)?;
            }
            file_groups.push(fg);
        }
        Ok(file_groups)
    }
}

#[test]
fn meta_client_get_partition_paths() {
    let fixture_path = Path::new("fixtures/table/0.x_cow_partitioned.zip");
    let target_table_path = extract_test_table(fixture_path);
    let meta_client = MetaClient::new(&target_table_path);
    let partition_paths = meta_client.get_partition_paths().unwrap();
    assert_eq!(
        partition_paths,
        vec!["chennai", "sao_paulo", "san_francisco"]
    )
}

#[test]
fn meta_client_get_file_groups() {
    let fixture_path = Path::new("fixtures/table/0.x_cow_partitioned.zip");
    let target_table_path = extract_test_table(fixture_path);
    let meta_client = MetaClient::new(&target_table_path);
    let file_groups = meta_client.get_file_groups("san_francisco").unwrap();
    assert_eq!(file_groups.len(), 3);
    let fg_ids: HashSet<&str> = HashSet::from_iter(file_groups.iter().map(|fg| fg.id.as_str()));
    assert_eq!(
        fg_ids,
        HashSet::from_iter(vec![
            "5a226868-2934-4f84-a16f-55124630c68d-0",
            "780b8586-3ad0-48ef-a6a1-d2217845ce4a-0",
            "d9082ffd-2eb1-4394-aefc-deb4a61ecc57-0"
        ])
    );
}
#[test]
fn meta_client_active_timeline_init_as_expected() {
    let fixture_path = Path::new("fixtures/table/0.x_cow_partitioned.zip");
    let target_table_path = extract_test_table(fixture_path);
    let meta_client = MetaClient::new(&target_table_path);
    assert_eq!(meta_client.timeline.instants.len(), 2)
}
