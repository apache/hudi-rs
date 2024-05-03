use crate::error::HudiFileSystemViewError;
use crate::error::HudiFileSystemViewError::FailToLoadPartitions;
use crate::file_group::{FileGroup, FileSlice};
use crate::table::meta_client::MetaClient;
use hashbrown::HashMap;
use hudi_fs::test_utils::extract_test_table;
use std::collections::HashSet;
use std::path::Path;

pub struct FileSystemView {
    meta_client: MetaClient,
    partition_to_file_groups: HashMap<String, Vec<FileGroup>>,
}

impl FileSystemView {
    pub fn init(meta_client: MetaClient) -> Result<Self, HudiFileSystemViewError> {
        let mut fs_view = FileSystemView {
            meta_client,
            partition_to_file_groups: HashMap::new(),
        };
        fs_view.load_partitions()?;
        Ok(fs_view)
    }

    fn load_partitions(&mut self) -> Result<(), HudiFileSystemViewError> {
        match self.meta_client.get_partition_paths() {
            Ok(partition_paths) => {
                for p in partition_paths {
                    match self.meta_client.get_file_groups(p.as_str()) {
                        Ok(file_groups) => {
                            self.partition_to_file_groups.insert(p, file_groups);
                        }
                        Err(e) => return Err(FailToLoadPartitions(e)),
                    }
                }
            }
            Err(e) => return Err(FailToLoadPartitions(Box::new(e))),
        }
        Ok(())
    }

    pub fn get_latest_file_slices(&self) -> Vec<&FileSlice> {
        let mut file_slices = Vec::new();
        for fgs in self.partition_to_file_groups.values() {
            for fg in fgs {
                match fg.get_latest_file_slice() {
                    Some(file_slice) => file_slices.push(file_slice.clone()),
                    None => (),
                }
            }
        }
        file_slices
    }
}
#[test]
fn meta_client_get_file_groups() {
    let fixture_path = Path::new("fixtures/table/0.x_cow_partitioned.zip");
    let target_table_path = extract_test_table(fixture_path);
    let meta_client = MetaClient::new(&target_table_path);
    let fs_view = FileSystemView::init(meta_client).unwrap();
    let file_slices = fs_view.get_latest_file_slices();
    assert_eq!(file_slices.len(), 5);
    let mut fg_ids = Vec::new();
    for f in file_slices {
        let fp = f.file_group_id();
        fg_ids.push(fp);
    }
    let actual: HashSet<&str> = fg_ids.into_iter().collect();
    assert_eq!(
        actual,
        HashSet::from_iter(vec![
            "780b8586-3ad0-48ef-a6a1-d2217845ce4a-0",
            "d9082ffd-2eb1-4394-aefc-deb4a61ecc57-0",
            "ee915c68-d7f8-44f6-9759-e691add290d8-0",
            "68d3c349-f621-4cd8-9e8b-c6dd8eb20d08-0",
            "5a226868-2934-4f84-a16f-55124630c68d-0"
        ])
    );
}
