use std::collections::BTreeMap;
use std::fmt;
use std::fmt::Formatter;

use hudi_fs::file_systems::FileMetadata;

use crate::error::HudiFileGroupError;
use crate::error::HudiFileGroupError::CommitTimeAlreadyExists;

#[derive(Debug)]
pub struct BaseFile {
    pub file_group_id: String,
    pub commit_time: String,
    pub metadata: Option<FileMetadata>,
}

impl BaseFile {
    pub fn new(file_name: &str) -> Self {
        let (name, _) = file_name.rsplit_once('.').unwrap();
        let parts: Vec<&str> = name.split('_').collect();
        let file_group_id = parts[0].to_owned();
        let commit_time = parts[2].to_owned();
        Self {
            file_group_id,
            commit_time,
            metadata: None,
        }
    }

    pub fn from_file_metadata(file_metadata: FileMetadata) -> Self {
        let mut base_file = Self::new(file_metadata.name.as_str());
        base_file.metadata = Some(file_metadata);
        base_file
    }
}

#[derive(Debug)]
pub struct FileSlice {
    pub base_file: BaseFile,
    pub partition_path: Option<String>,
}

impl FileSlice {
    pub fn file_path(&self) -> Option<&str> {
        match &self.base_file.metadata {
            None => None,
            Some(file_metadata) => Some(file_metadata.path.as_str()),
        }
    }

    pub fn file_group_id(&self) -> &str {
        &self.base_file.file_group_id
    }

    pub fn base_instant_time(&self) -> &str {
        &self.base_file.commit_time
    }

    pub fn set_base_file(&mut self, base_file: BaseFile) {
        self.base_file = base_file
    }
}

#[derive(Debug)]
pub struct FileGroup {
    pub id: String,
    pub partition_path: Option<String>,
    pub file_slices: BTreeMap<String, FileSlice>,
}

impl fmt::Display for FileGroup {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(
            format!(
                "File Group: partition {:?} id {}",
                &self.partition_path, &self.id
            )
            .as_str(),
        )
    }
}

impl FileGroup {
    pub fn new(id: String, partition_path: Option<String>) -> Self {
        Self {
            id,
            partition_path,
            file_slices: BTreeMap::new(),
        }
    }

    pub fn add_base_file_from_name(
        &mut self,
        file_name: &str,
    ) -> Result<&Self, HudiFileGroupError> {
        let base_file = BaseFile::new(file_name);
        self.add_base_file(base_file)
    }

    pub fn add_base_file(&mut self, base_file: BaseFile) -> Result<&Self, HudiFileGroupError> {
        let commit_time = base_file.commit_time.as_str();
        if self.file_slices.contains_key(commit_time) {
            Err(CommitTimeAlreadyExists(
                commit_time.to_owned(),
                self.to_string(),
            ))
        } else {
            self.file_slices.insert(
                commit_time.to_owned(),
                FileSlice {
                    partition_path: self.partition_path.clone(),
                    base_file,
                },
            );
            Ok(self)
        }
    }

    pub fn get_latest_file_slice(&self) -> Option<&FileSlice> {
        return self.file_slices.values().next_back();
    }
}

#[test]
fn create_a_base_file_successfully() {
    let base_file =
        BaseFile::new("5a226868-2934-4f84-a16f-55124630c68d-0_0-7-24_20240402144910683.parquet");
    assert_eq!(
        base_file.file_group_id,
        "5a226868-2934-4f84-a16f-55124630c68d-0"
    );
    assert_eq!(base_file.commit_time, "20240402144910683");
}

#[test]
fn load_a_valid_file_group() {
    let mut fg = FileGroup::new("5a226868-2934-4f84-a16f-55124630c68d-0".to_owned(), None);
    let _ = fg.add_base_file_from_name(
        "5a226868-2934-4f84-a16f-55124630c68d-0_0-7-24_20240402144910683.parquet",
    );
    let _ = fg.add_base_file_from_name(
        "5a226868-2934-4f84-a16f-55124630c68d-0_2-10-0_20240402123035233.parquet",
    );
    assert_eq!(fg.file_slices.len(), 2);
    assert!(fg.partition_path.is_none());
    let commit_times: Vec<&str> = fg.file_slices.keys().map(|k| k.as_str()).collect();
    assert_eq!(commit_times, vec!["20240402123035233", "20240402144910683"]);
    assert_eq!(
        fg.get_latest_file_slice().unwrap().base_file.commit_time,
        "20240402144910683"
    )
}
