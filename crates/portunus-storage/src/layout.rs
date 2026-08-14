//! Deterministic logical-to-multi-file range mapping.
//!
//! A validated manifest defines one contiguous logical byte space over ordered
//! files, including zero-length files. Construction has an inclusive file-count
//! ceiling and rejects ambiguous keys or total-length overflow. Mapping accepts
//! half-open ranges `[offset, offset + length)` and emits only nonempty segments.
//!
//! ```text
//! logical:  [ file 0 ][empty][   file 2   ]
//! request:          [-----------)
//! segments: [tail 0]          [prefix 2]
//! ```
//!
//! This module performs no path interpretation or I/O and owns no locking policy.
//! Consumers resolve stable file indexes within their own trusted storage root.

use std::collections::BTreeMap;
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileSpec {
    key: String,
    length: u64,
}

impl FileSpec {
    /// Inputs: stable opaque file key and its logical byte length.
    /// Outputs: owned manifest entry; validation occurs when constructing a layout.
    /// Logic: retain caller intent without interpreting keys as filesystem paths.
    #[must_use]
    pub fn new(key: impl Into<String>, length: u64) -> Self {
        Self {
            key: key.into(),
            length,
        }
    }

    /// Inputs: shared file specification.
    /// Outputs: borrowed opaque key.
    /// Logic: expose identity without transferring manifest ownership.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Inputs: shared file specification.
    /// Outputs: configured logical length in bytes.
    /// Logic: expose immutable layout metadata.
    #[must_use]
    pub const fn length(&self) -> u64 {
        self.length
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RangeSegment {
    pub file_index: usize,
    pub file_offset: u64,
    pub request_offset: u64,
    pub length: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Layout {
    files: Vec<FileSpec>,
    starts: Vec<u64>,
    total_length: u64,
}

impl Layout {
    /// Inputs: ordered file manifest and inclusive retained-entry ceiling.
    /// Outputs: validated layout or stable collection/key/overflow failure.
    /// Logic: validate before retention while computing each logical file start.
    /// # Errors
    /// Returns zero/count, empty/duplicate key, or total-length overflow errors.
    pub fn new(files: Vec<FileSpec>, max_files: usize) -> Result<Self, LayoutError> {
        if max_files == 0 {
            return Err(LayoutError::ZeroFileLimit);
        }
        if files.is_empty() {
            return Err(LayoutError::EmptyManifest);
        }
        if files.len() > max_files {
            return Err(LayoutError::TooManyFiles {
                actual: files.len(),
                limit: max_files,
            });
        }
        let mut keys = BTreeMap::new();
        let mut starts = Vec::with_capacity(files.len());
        let mut total_length = 0_u64;
        for (index, file) in files.iter().enumerate() {
            if file.key.is_empty() {
                return Err(LayoutError::EmptyFileKey { file_index: index });
            }
            if let Some(first_index) = keys.insert(file.key.as_str(), index) {
                return Err(LayoutError::DuplicateFileKey {
                    first_index,
                    duplicate_index: index,
                });
            }
            starts.push(total_length);
            total_length = total_length
                .checked_add(file.length)
                .ok_or(LayoutError::TotalLengthOverflow { file_index: index })?;
        }
        Ok(Self {
            files,
            starts,
            total_length,
        })
    }

    /// Inputs: shared validated layout.
    /// Outputs: total logical length in bytes.
    /// Logic: expose the checked construction-time sum.
    #[must_use]
    pub const fn total_length(&self) -> u64 {
        self.total_length
    }

    /// Inputs: zero-based logical offset and byte length forming a half-open range.
    /// Outputs: ordered nonempty physical segments or exact out-of-bounds details.
    /// Logic: checked range end, then intersect request with each ordered file span.
    /// # Errors
    /// Returns `RangeOutOfBounds` for arithmetic overflow or an end beyond total.
    pub fn map(&self, offset: u64, length: u64) -> Result<Vec<RangeSegment>, LayoutError> {
        let end = offset
            .checked_add(length)
            .filter(|end| *end <= self.total_length)
            .ok_or(LayoutError::RangeOutOfBounds {
                offset,
                length,
                total_length: self.total_length,
            })?;
        let mut segments = Vec::new();
        for (file_index, (file, file_start)) in self.files.iter().zip(&self.starts).enumerate() {
            let file_end = file_start + file.length;
            let overlap_start = offset.max(*file_start);
            let overlap_end = end.min(file_end);
            if overlap_start < overlap_end {
                segments.push(RangeSegment {
                    file_index,
                    file_offset: overlap_start - file_start,
                    request_offset: overlap_start - offset,
                    length: overlap_end - overlap_start,
                });
            }
        }
        Ok(segments)
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum LayoutError {
    #[error("file limit must be greater than zero")]
    ZeroFileLimit,
    #[error("file manifest must not be empty")]
    EmptyManifest,
    #[error("manifest has {actual} files, exceeding limit {limit}")]
    TooManyFiles { actual: usize, limit: usize },
    #[error("file key at index {file_index} must not be empty")]
    EmptyFileKey { file_index: usize },
    #[error("file key at index {duplicate_index} duplicates index {first_index}")]
    DuplicateFileKey {
        first_index: usize,
        duplicate_index: usize,
    },
    #[error("manifest total length overflows at file index {file_index}")]
    TotalLengthOverflow { file_index: usize },
    #[error("range [{offset}, {offset} + {length}) exceeds total length {total_length}")]
    RangeOutOfBounds {
        offset: u64,
        length: u64,
        total_length: u64,
    },
}
