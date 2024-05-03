use std::error::Error;
use std::fmt::Debug;
use std::io;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum HudiFileGroupError {
    #[error("Base File {0} has unsupported format: {1}")]
    UnsupportedBaseFileFormat(String, String),
    #[error("Commit time {0} is already present in File Group {1}")]
    CommitTimeAlreadyExists(String, String),
}

#[derive(Debug, Error)]
pub enum HudiTimelineError {
    #[error("Error in reading commit metadata: {0}")]
    FailToReadCommitMetadata(io::Error),
}

#[derive(Debug, Error)]
pub enum HudiFileSystemViewError {
    #[error("Error in loading partitions: {0}")]
    FailToLoadPartitions(Box<dyn Error>),
}

#[derive(Debug, Error)]
pub enum HudiCoreError {
    #[error("Failed to load file group")]
    FailToLoadFileGroup(#[from] HudiFileGroupError),
    #[error("Failed to init timeline")]
    FailToInitTimeline(#[from] HudiTimelineError),
    #[error("Failed to build file system view")]
    FailToBuildFileSystemView(#[from] HudiFileSystemViewError),
}
