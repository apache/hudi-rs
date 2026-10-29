/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//! Mirrors `org.apache.hudi.common.table.read.InputSplit`.
//!
//! Describes the data to be read from a file group: an optional base file,
//! a list of log files, and the partition path context.

use crate::file_group::log_file::LogFile;
use std::ops::Range;
use std::str::FromStr;

/// A contiguous run of a base file's row groups, read by one of several readers
/// that together cover one file slice.
///
/// Each reader decodes only its row groups and, when the log blocks carry record
/// positions, keeps only the log records addressed at those rows, so the slice's
/// merge runs on as many cores as there are ranges. The ranges of a slice are
/// produced together by [`BaseFileRange::split`], which hands the last one an
/// unbounded position range so every position has exactly one owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaseFileRange {
    /// Row groups this reader decodes, by index in the footer.
    pub row_groups: Range<usize>,
    /// Base-row positions the row groups hold.
    pub positions: Range<u64>,
}

/// One row group of a base file as a reader that cuts the file by bytes sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowGroupSpan {
    /// Byte offset of the group's first page.
    pub start: u64,
    /// Compressed bytes of the group.
    pub bytes: u64,
    /// Rows in the group.
    pub rows: u64,
}

impl BaseFileRange {
    /// The row groups a byte-range split `[start, start + length)` selects: those
    /// whose midpoint falls inside it, the rule Spark's and Velox's parquet
    /// readers apply, so the splits of a file together select every row group
    /// exactly once. `None` when the split holds no midpoint.
    pub fn for_byte_split(spans: &[RowGroupSpan], start: u64, length: u64) -> Option<Self> {
        let end = start.saturating_add(length);
        let mut rows_before = 0u64;
        let mut selected: Option<(usize, u64, usize, u64)> = None;
        for (index, span) in spans.iter().enumerate() {
            let midpoint = span.start + span.bytes / 2;
            if (start..end).contains(&midpoint) {
                selected = Some(match selected {
                    None => (index, rows_before, index + 1, rows_before + span.rows),
                    Some((first, first_row, _, _)) => {
                        (first, first_row, index + 1, rows_before + span.rows)
                    }
                });
            }
            rows_before += span.rows;
        }
        selected.map(|(first, first_row, end_group, end_row)| BaseFileRange {
            row_groups: first..end_group,
            // The last range takes every position past the known rows, as `split` does.
            positions: first_row..if end_group == spans.len() {
                u64::MAX
            } else {
                end_row
            },
        })
    }

    /// Whether this range starts the file. The first range is the one that
    /// reads the whole file when the log blocks turn out to carry no positions.
    pub fn is_first(&self) -> bool {
        self.row_groups.start == 0
    }

    pub fn owns_position(&self, position: u64) -> bool {
        self.positions.contains(&position)
    }

    /// Cut a file's row groups, given as `(rows, bytes)` per group in file order,
    /// into at most `parts` contiguous ranges of roughly equal bytes.
    ///
    /// Returns one range when `parts` is at most one or the file has a single
    /// row group, and nothing for a file with no row groups.
    pub fn split(row_groups: &[(u64, u64)], parts: usize) -> Vec<BaseFileRange> {
        if row_groups.is_empty() {
            return Vec::new();
        }
        let parts = parts.clamp(1, row_groups.len()) as u64;
        let total_bytes: u64 = row_groups.iter().map(|(_, bytes)| bytes).sum();
        let mut ranges = Vec::with_capacity(parts as usize);
        let (mut group_start, mut position_start, mut bytes_start) = (0usize, 0u64, 0u64);
        let (mut bytes_so_far, mut rows_so_far) = (0u64, 0u64);
        for (index, (rows, bytes)) in row_groups.iter().enumerate() {
            bytes_so_far += bytes;
            rows_so_far += rows;
            let cut_so_far = ranges.len() as u64;
            let is_last_group = index + 1 == row_groups.len();
            // Cut once this range holds its share of what is left, so a fat
            // group early on does not starve the ranges after it, or when every
            // remaining group is needed to give the remaining ranges one each;
            // the last range takes everything that remains.
            let share = (total_bytes - bytes_start).div_ceil(parts - cut_so_far);
            let reached_share = bytes_so_far - bytes_start >= share;
            let groups_left = (row_groups.len() - index - 1) as u64;
            let ranges_left = parts - cut_so_far - 1;
            if is_last_group || (ranges_left > 0 && (reached_share || groups_left <= ranges_left)) {
                ranges.push(BaseFileRange {
                    row_groups: group_start..index + 1,
                    positions: position_start..rows_so_far,
                });
                group_start = index + 1;
                position_start = rows_so_far;
                bytes_start = bytes_so_far;
            }
        }
        if let Some(last) = ranges.last_mut() {
            last.positions.end = u64::MAX;
        }
        ranges
    }
}

/// Describes the input data for a file group read.
///
/// Carries the base file (if any), the list of log files to scan,
/// the partition path, and the byte range to read from the base file.
#[derive(Debug, Clone)]
pub struct InputSplit {
    /// Path to the base file (relative to table root), if present.
    pub base_file_path: Option<String>,

    /// Commit time of the base file, if present.
    pub base_file_commit_time: Option<String>,

    /// Relative paths to log files to scan.
    pub log_file_paths: Vec<String>,

    /// Partition path for this file group (e.g. "year=2024/month=01").
    pub partition_path: String,

    /// The part of the base file this reader covers; `None` reads all of it.
    pub base_file_range: Option<BaseFileRange>,
}

/// CDC log-file suffix. Mirrors Java's `HoodieCDCUtils.CDC_LOGFILE_SUFFIX` (".cdc").
/// CDC log files carry change-data-capture blocks and must be excluded from a
/// normal snapshot read — gold drops them in `InputSplit`'s constructor
/// (`InputSplit.java:57`).
const CDC_LOGFILE_SUFFIX: &str = ".cdc";

impl InputSplit {
    pub fn new(
        base_file_path: Option<String>,
        base_file_commit_time: Option<String>,
        log_file_paths: Vec<String>,
        partition_path: String,
    ) -> Self {
        // Filter out CDC log files, then sort ascending by
        // deltaCommitTime → logVersion → writeToken. This mirrors Java's
        // InputSplit constructor (InputSplit.java:56-58), which sorts via
        // HoodieLogFile.getLogFileComparator() and filters file names ending in
        // HoodieCDCUtils.CDC_LOGFILE_SUFFIX. The C++ side may send log files in
        // descending order (from FileSlice's reverse TreeSet), so we re-sort.
        let log_file_paths = Self::filter_cdc_log_files(log_file_paths);
        let log_file_paths = Self::sort_log_file_paths(log_file_paths);
        Self {
            base_file_path,
            base_file_commit_time,
            log_file_paths,
            partition_path,
            base_file_range: None,
        }
    }

    pub fn with_base_file_range(mut self, range: Option<BaseFileRange>) -> Self {
        self.base_file_range = range;
        self
    }

    /// Drop CDC log files from the scan list.
    ///
    /// Mirrors Java's `InputSplit` constructor filter
    /// (`InputSplit.java:57`): `!logFile.getFileName().endsWith(CDC_LOGFILE_SUFFIX)`.
    /// The match is on the file-name portion (after the last `/`), matching Java's
    /// `getFileName()` semantics.
    fn filter_cdc_log_files(paths: Vec<String>) -> Vec<String> {
        paths
            .into_iter()
            .filter(|p| {
                let name = p.rsplit('/').next().unwrap_or(p);
                !name.ends_with(CDC_LOGFILE_SUFFIX)
            })
            .collect()
    }

    /// Sort log file paths ascending by deltaCommitTime → logVersion → writeToken.
    ///
    /// Mirrors Java's `InputSplit` constructor which sorts via
    /// `HoodieLogFile.getLogFileComparator()`.
    fn sort_log_file_paths(mut paths: Vec<String>) -> Vec<String> {
        if paths.len() <= 1 {
            return paths;
        }
        paths.sort_by(|a, b| {
            let name_a = a.rsplit('/').next().unwrap_or(a);
            let name_b = b.rsplit('/').next().unwrap_or(b);
            match (LogFile::from_str(name_a), LogFile::from_str(name_b)) {
                (Ok(lf_a), Ok(lf_b)) => lf_a.cmp(&lf_b),
                _ => a.cmp(b), // fallback to lexicographic if parsing fails
            }
        });
        paths
    }

    /// Returns true if there are log files that need to be merged with the base file.
    pub fn has_log_files(&self) -> bool {
        !self.log_file_paths.is_empty()
    }

    /// Returns true when there are no log files to merge, so the split reduces
    /// to its base file. A base-file-only split is exactly this case.
    pub fn is_base_only(&self) -> bool {
        !self.has_log_files()
    }
}

#[cfg(test)]
mod byte_split_tests {
    use super::*;

    /// Four groups laid out back to back after the 4-byte magic.
    fn spans() -> Vec<RowGroupSpan> {
        let mut start = 4;
        [(100, 1000), (100, 3000), (50, 500), (100, 1500)]
            .into_iter()
            .map(|(rows, bytes)| {
                let span = RowGroupSpan { start, bytes, rows };
                start += bytes;
                span
            })
            .collect()
    }

    #[test]
    fn splits_that_partition_the_file_select_every_group_once() {
        let spans = spans();
        let file_size = 6004 + 800; // data plus a footer
        let half = file_size / 2;
        let first = BaseFileRange::for_byte_split(&spans, 0, half).unwrap();
        let second = BaseFileRange::for_byte_split(&spans, half, file_size - half).unwrap();
        // Midpoints: 504, 2504, 4254, 5254; the split boundary is 3402.
        assert_eq!(first.row_groups, 0..2);
        assert_eq!(first.positions, 0..200);
        assert_eq!(second.row_groups, 2..4);
        assert_eq!(second.positions.start, 200);
        assert_eq!(
            second.positions.end,
            u64::MAX,
            "the last range owns the tail"
        );
    }

    #[test]
    fn a_split_without_a_midpoint_selects_nothing() {
        let spans = spans();
        assert!(BaseFileRange::for_byte_split(&spans, 6004, 800).is_none());
        assert!(BaseFileRange::for_byte_split(&spans, 600, 100).is_none());
        assert!(BaseFileRange::for_byte_split(&[], 0, 100).is_none());
    }

    #[test]
    fn a_split_covering_the_file_selects_every_group() {
        let spans = spans();
        let all = BaseFileRange::for_byte_split(&spans, 0, u64::MAX).unwrap();
        assert_eq!(all.row_groups, 0..4);
        assert_eq!(all.positions, 0..u64::MAX);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Java's InputSplit sorts log files ascending by deltaCommitTime → logVersion → writeToken
    /// (via HoodieLogFile.getLogFileComparator()). The C++ side sends them in descending order
    /// (from FileSlice's reverse TreeSet). InputSplit must re-sort to ascending.
    #[test]
    fn test_log_file_paths_sorted_ascending() {
        // Log files in DESCENDING deltaCommitTime order (as C++ sends them)
        let log_files = vec![
            ".72d44234-0_20260408194651210.log.1_0-272-505".to_string(), // NEWER
            ".72d44234-0_20260408194649548.log.1_0-254-473".to_string(), // OLDER
        ];

        let split = InputSplit::new(
            Some("base.parquet".to_string()),
            None,
            log_files,
            String::new(),
        );

        // Must be re-sorted to ASCENDING deltaCommitTime
        assert_eq!(split.log_file_paths.len(), 2);
        assert!(
            split.log_file_paths[0].contains("20260408194649548"),
            "First log file should be the OLDER commit, got: {}",
            split.log_file_paths[0]
        );
        assert!(
            split.log_file_paths[1].contains("20260408194651210"),
            "Second log file should be the NEWER commit, got: {}",
            split.log_file_paths[1]
        );
    }

    /// Same deltaCommitTime, different logVersions — should sort by version ascending.
    #[test]
    fn test_log_file_paths_sorted_by_version() {
        let log_files = vec![
            ".fileId-0_20260408194649548.log.3_0-100-200".to_string(), // version 3
            ".fileId-0_20260408194649548.log.1_0-100-200".to_string(), // version 1
            ".fileId-0_20260408194649548.log.2_0-100-200".to_string(), // version 2
        ];

        let split = InputSplit::new(None, None, log_files, String::new());

        assert!(split.log_file_paths[0].contains(".log.1_"));
        assert!(split.log_file_paths[1].contains(".log.2_"));
        assert!(split.log_file_paths[2].contains(".log.3_"));
    }

    /// With partition path prefix — sort should work on the file name portion.
    #[test]
    fn test_log_file_paths_sorted_with_partition_prefix() {
        let log_files = vec![
            "year=2024/month=01/.fileId-0_20260408194651210.log.1_0-272-505".to_string(),
            "year=2024/month=01/.fileId-0_20260408194649548.log.1_0-254-473".to_string(),
        ];

        let split = InputSplit::new(None, None, log_files, "year=2024/month=01".to_string());

        assert!(split.log_file_paths[0].contains("20260408194649548"));
        assert!(split.log_file_paths[1].contains("20260408194651210"));
    }

    /// CDC log files (`.cdc` suffix) must be dropped from the scan list, mirroring
    /// Java's `InputSplit` constructor filter (`InputSplit.java:57`,
    /// `HoodieCDCUtils.CDC_LOGFILE_SUFFIX`). A normal snapshot read must not pull
    /// in change-data-capture blocks.
    #[test]
    fn test_cdc_log_files_filtered_out() {
        let log_files = vec![
            ".fileId-0_20260408194649548.log.1_0-100-200".to_string(), // normal
            ".fileId-0_20260408194651210.log.2_0-101-201.cdc".to_string(), // CDC — must be dropped
            ".fileId-0_20260408194652000.log.3_0-102-202".to_string(), // normal
        ];

        let split = InputSplit::new(None, None, log_files, String::new());

        assert_eq!(
            split.log_file_paths.len(),
            2,
            "CDC log file should be excluded, got: {:?}",
            split.log_file_paths
        );
        assert!(
            split.log_file_paths.iter().all(|p| !p.ends_with(".cdc")),
            "no remaining log file should be a .cdc file, got: {:?}",
            split.log_file_paths
        );
        // The two normal log files survive and are sorted ascending.
        assert!(split.log_file_paths[0].contains("20260408194649548"));
        assert!(split.log_file_paths[1].contains("20260408194652000"));
    }

    /// CDC filter matches on the file-name portion when a partition prefix is present.
    #[test]
    fn test_cdc_log_files_filtered_with_partition_prefix() {
        let log_files = vec![
            "year=2024/.fileId-0_20260408194649548.log.1_0-100-200".to_string(),
            "year=2024/.fileId-0_20260408194651210.log.2_0-101-201.cdc".to_string(),
        ];

        let split = InputSplit::new(None, None, log_files, "year=2024".to_string());

        assert_eq!(split.log_file_paths.len(), 1);
        assert!(split.log_file_paths[0].contains("20260408194649548"));
    }

    /// Empty or single log file — no sorting needed, should not crash.
    #[test]
    fn test_log_file_paths_empty_and_single() {
        let split_empty = InputSplit::new(None, None, vec![], String::new());
        assert!(split_empty.log_file_paths.is_empty());

        let split_single = InputSplit::new(
            None,
            None,
            vec![".fileId-0_20260408194649548.log.1_0-254-473".to_string()],
            String::new(),
        );
        assert_eq!(split_single.log_file_paths.len(), 1);
    }

    #[test]
    fn split_cuts_row_groups_by_bytes_and_hands_the_tail_to_the_last_range() {
        // Six groups, 100 rows each; the third is four times the size of the rest.
        let groups = [
            (100, 10),
            (100, 10),
            (100, 40),
            (100, 10),
            (100, 10),
            (100, 10),
        ];
        let ranges = BaseFileRange::split(&groups, 3);
        assert_eq!(
            ranges,
            vec![
                BaseFileRange {
                    row_groups: 0..3,
                    positions: 0..300,
                },
                BaseFileRange {
                    row_groups: 3..5,
                    positions: 300..500,
                },
                BaseFileRange {
                    row_groups: 5..6,
                    positions: 500..u64::MAX,
                },
            ]
        );
        assert!(ranges[0].is_first());
        assert!(!ranges[1].is_first());
        assert!(ranges[2].owns_position(1_000_000));
        assert!(!ranges[1].owns_position(500));
    }

    #[test]
    fn split_never_returns_more_ranges_than_row_groups() {
        let groups = [(10, 5), (10, 5)];
        let ranges = BaseFileRange::split(&groups, 8);
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0].row_groups, 0..1);
        assert_eq!(ranges[1].row_groups, 1..2);
        assert_eq!(ranges[1].positions, 10..u64::MAX);
    }

    #[test]
    fn split_into_one_part_covers_the_whole_file() {
        let groups = [(10, 5), (10, 5), (10, 5)];
        for parts in [0, 1] {
            let ranges = BaseFileRange::split(&groups, parts);
            assert_eq!(ranges.len(), 1);
            assert_eq!(ranges[0].row_groups, 0..3);
            assert_eq!(ranges[0].positions, 0..u64::MAX);
        }
        assert!(BaseFileRange::split(&[], 4).is_empty());
    }

    #[test]
    fn split_handles_groups_with_no_bytes() {
        let groups = [(10, 0), (10, 0), (10, 0)];
        let ranges = BaseFileRange::split(&groups, 2);
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0].row_groups, 0..1);
        assert_eq!(ranges[1].row_groups, 1..3);
    }
}
