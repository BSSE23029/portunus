//! Explicit concurrent read/write policy for logical multi-file operations.
//!
//! Each file owns one asynchronous read/write lock. Reads share access; writes are
//! exclusive. Every request is bounded by an inclusive unique-file ceiling, sorted,
//! and deduplicated before acquisition, so overlapping multi-file operations cannot
//! deadlock through caller-provided order. Partial acquisition drops atomically.
//!
//! ```text
//! indexes [2,0,2] ──validate/sort/dedup──> [0,2] ──ordered locks──> permit
//! ```
//!
//! This policy coordinates access only. It does not map byte ranges, perform I/O,
//! schedule operations, or install a runtime/global tracing subscriber.

use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock, TryLockError};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AccessMode {
    Read,
    Write,
}

#[derive(Clone, Debug)]
pub struct AccessCoordinator {
    files: Arc<[Arc<RwLock<()>>]>,
    max_files_per_request: usize,
}

impl AccessCoordinator {
    /// Inputs: nonzero file count and inclusive unique-file request ceiling.
    /// Outputs: immutable shared coordinator or stable zero-bound error.
    /// Logic: validate budgets before allocating one lock per physical file.
    /// # Errors
    /// Returns distinct errors for zero files and zero per-request capacity.
    pub fn new(file_count: usize, max_files_per_request: usize) -> Result<Self, AccessError> {
        if file_count == 0 {
            return Err(AccessError::ZeroFileCount);
        }
        if max_files_per_request == 0 {
            return Err(AccessError::ZeroFilesPerRequest);
        }
        let files = (0..file_count)
            .map(|_| Arc::new(RwLock::new(())))
            .collect::<Vec<_>>()
            .into();
        Ok(Self {
            files,
            max_files_per_request,
        })
    }

    /// Inputs: requested file indexes; duplicates count once after canonicalization.
    /// Outputs: immediate shared-read permit or stable validation/saturation error.
    /// Logic: validate/sort indexes, then try locks in ascending order atomically.
    /// # Errors
    /// Returns empty/count/index or read-saturation details.
    pub fn try_read(&self, indexes: &[usize]) -> Result<AccessPermit, AccessError> {
        self.try_acquire(indexes, AccessMode::Read)
    }

    /// Inputs: requested file indexes; duplicates count once after canonicalization.
    /// Outputs: immediate exclusive-write permit or validation/saturation error.
    /// Logic: validate/sort indexes, then try locks in ascending order atomically.
    /// # Errors
    /// Returns empty/count/index or write-saturation details.
    pub fn try_write(&self, indexes: &[usize]) -> Result<AccessPermit, AccessError> {
        self.try_acquire(indexes, AccessMode::Write)
    }

    /// Inputs: requested files and borrowed cooperative cancellation signal.
    /// Outputs: eventual shared-read permit or validation/cancellation failure.
    /// Logic: acquire canonical locks sequentially with cancellation priority;
    /// returning early drops every previously acquired read guard.
    /// # Errors
    /// Returns empty/count/index or cancellation errors.
    pub async fn read(
        &self,
        indexes: &[usize],
        cancellation: &CancellationToken,
    ) -> Result<AccessPermit, AccessError> {
        self.acquire(indexes, AccessMode::Read, cancellation).await
    }

    /// Inputs: requested files and borrowed cooperative cancellation signal.
    /// Outputs: eventual exclusive-write permit or validation/cancellation failure.
    /// Logic: acquire canonical locks sequentially with cancellation priority;
    /// returning early drops every previously acquired write guard.
    /// # Errors
    /// Returns empty/count/index or cancellation errors.
    pub async fn write(
        &self,
        indexes: &[usize],
        cancellation: &CancellationToken,
    ) -> Result<AccessPermit, AccessError> {
        self.acquire(indexes, AccessMode::Write, cancellation).await
    }

    // Inputs: caller indexes and requested access mode.
    // Outputs: immediate all-or-nothing owned guards or stable error.
    // Logic: canonicalize once, then let vector drop unwind any partial acquisition.
    fn try_acquire(
        &self,
        indexes: &[usize],
        mode: AccessMode,
    ) -> Result<AccessPermit, AccessError> {
        let indexes = self.canonical_indexes(indexes)?;
        let mut guards = Vec::with_capacity(indexes.len());
        for index in indexes {
            let lock = Arc::clone(&self.files[index]);
            let guard = match mode {
                AccessMode::Read => HeldLock::Read {
                    _guard: lock
                        .try_read_owned()
                        .map_err(|error| map_lock_error(error, mode))?,
                },
                AccessMode::Write => HeldLock::Write {
                    _guard: lock
                        .try_write_owned()
                        .map_err(|error| map_lock_error(error, mode))?,
                },
            };
            guards.push(guard);
        }
        Ok(AccessPermit { mode, guards })
    }

    // Inputs: caller indexes, mode, and cooperative cancellation signal.
    // Outputs: eventual all-or-nothing owned guards or stable error.
    // Logic: canonicalize, then cancellation-prioritized wait in global index order.
    async fn acquire(
        &self,
        indexes: &[usize],
        mode: AccessMode,
        cancellation: &CancellationToken,
    ) -> Result<AccessPermit, AccessError> {
        let indexes = self.canonical_indexes(indexes)?;
        if cancellation.is_cancelled() {
            return Err(AccessError::Cancelled);
        }
        let mut guards = Vec::with_capacity(indexes.len());
        for index in indexes {
            let lock = Arc::clone(&self.files[index]);
            let guard = match mode {
                AccessMode::Read => {
                    let wait = lock.read_owned();
                    tokio::select! {
                        biased;
                        () = cancellation.cancelled() => return Err(AccessError::Cancelled),
                        guard = wait => HeldLock::Read { _guard: guard },
                    }
                }
                AccessMode::Write => {
                    let wait = lock.write_owned();
                    tokio::select! {
                        biased;
                        () = cancellation.cancelled() => return Err(AccessError::Cancelled),
                        guard = wait => HeldLock::Write { _guard: guard },
                    }
                }
            };
            guards.push(guard);
        }
        Ok(AccessPermit { mode, guards })
    }

    // Inputs: arbitrary caller-provided indexes.
    // Outputs: ascending unique indexes or exact collection/index failure.
    // Logic: reject empty, sort/dedup owned copy, then validate limit and range.
    fn canonical_indexes(&self, indexes: &[usize]) -> Result<Vec<usize>, AccessError> {
        if indexes.is_empty() {
            return Err(AccessError::EmptyRequest);
        }
        let mut indexes = indexes.to_vec();
        indexes.sort_unstable();
        indexes.dedup();
        if indexes.len() > self.max_files_per_request {
            return Err(AccessError::TooManyFiles {
                actual: indexes.len(),
                limit: self.max_files_per_request,
            });
        }
        if let Some(index) = indexes.iter().find(|index| **index >= self.files.len()) {
            return Err(AccessError::InvalidFile {
                file_index: *index,
                file_count: self.files.len(),
            });
        }
        Ok(indexes)
    }
}

#[derive(Debug)]
enum HeldLock {
    Read { _guard: OwnedRwLockReadGuard<()> },
    Write { _guard: OwnedRwLockWriteGuard<()> },
}

#[derive(Debug)]
pub struct AccessPermit {
    mode: AccessMode,
    guards: Vec<HeldLock>,
}

impl AccessPermit {
    /// Inputs: shared access permit.
    /// Outputs: read or write mode held by every guard.
    /// Logic: expose immutable policy state without exposing lock internals.
    #[must_use]
    pub const fn mode(&self) -> AccessMode {
        self.mode
    }

    /// Inputs: shared access permit.
    /// Outputs: number of unique physical files locked.
    /// Logic: report canonical guard count for bounded-operation telemetry.
    #[must_use]
    pub const fn files(&self) -> usize {
        self.guards.len()
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum AccessError {
    #[error("file count must be greater than zero")]
    ZeroFileCount,
    #[error("files-per-request limit must be greater than zero")]
    ZeroFilesPerRequest,
    #[error("access request must contain at least one file")]
    EmptyRequest,
    #[error("request addresses {actual} files, exceeding limit {limit}")]
    TooManyFiles { actual: usize, limit: usize },
    #[error("file index {file_index} is outside file count {file_count}")]
    InvalidFile {
        file_index: usize,
        file_count: usize,
    },
    #[error("{mode:?} access is currently saturated")]
    Saturated { mode: AccessMode },
    #[error("storage access acquisition was cancelled")]
    Cancelled,
}

// Inputs: executor-specific try-lock failure and public access mode.
// Outputs: stable saturation error.
// Logic: hide Tokio lock details while retaining requested mode context.
const fn map_lock_error(_error: TryLockError, mode: AccessMode) -> AccessError {
    AccessError::Saturated { mode }
}
