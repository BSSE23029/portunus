//! Atomic content-addressed filesystem commit.
//!
//! Only [`VerifiedChunk`] values can cross this I/O boundary. Objects are written
//! and synchronized in a temporary sibling, then atomically linked into an encoded
//! algorithm/digest path without overwriting an existing identity. Concurrent or
//! repeated equal commits are idempotent; differing bytes produce a stable collision.
//!
//! ```text
//! VerifiedChunk ──write+sync temp──> hard-link digest path ──> Stored
//!                                      └─ already exists ──> compare ──> Present|Collision
//! ```
//!
//! This module does not validate payloads, assemble blocks, enforce global quotas,
//! or journal higher-level workflow state. It never interprets digest bytes as paths.

use crate::{assembly::VerifiedChunk, integrity::ContentId};
use std::{
    io,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
use thiserror::Error;
use tokio::{
    fs::{self, OpenOptions},
    io::AsyncWriteExt,
};

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitOutcome {
    Stored,
    AlreadyPresent,
}

#[derive(Debug, Error)]
pub enum CommitError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("content identity collides with existing object at {path}")]
    IdentityCollision { path: PathBuf },
}

#[derive(Clone, Debug)]
pub struct ContentStore {
    root: PathBuf,
}

impl ContentStore {
    /// Inputs: filesystem root owned by the composing application.
    /// Outputs: content store rooted at a created directory or an I/O error.
    /// Logic: establish only the root; algorithm subdirectories remain demand-driven.
    /// # Errors
    /// Returns an I/O error when the root cannot be created.
    pub async fn open(root: impl AsRef<Path>) -> Result<Self, CommitError> {
        fs::create_dir_all(root.as_ref()).await?;
        Ok(Self {
            root: root.as_ref().to_path_buf(),
        })
    }

    /// Inputs: shared store and opaque algorithm-tagged content identity.
    /// Outputs: deterministic path beneath the configured root.
    /// Logic: hex-encode both components so hostile labels/digests cannot traverse.
    #[must_use]
    pub fn path_for(&self, identity: &ContentId) -> PathBuf {
        self.root
            .join(hex(identity.algorithm().as_bytes()))
            .join(hex(identity.digest()))
    }

    /// Inputs: owned integrity-proven chunk.
    /// Outputs: stored/idempotent outcome or stable I/O/collision failure.
    /// Logic: sync a unique sibling temporary file, atomically hard-link without
    /// overwrite, compare a concurrent/existing object, and always remove the temp.
    /// # Errors
    /// Returns filesystem errors or `IdentityCollision` when equal IDs differ.
    pub async fn commit(&self, chunk: VerifiedChunk) -> Result<CommitOutcome, CommitError> {
        let (identity, bytes) = chunk.into_parts();
        let target = self.path_for(&identity);
        let parent = target.parent().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "content path has no parent")
        })?;
        fs::create_dir_all(parent).await?;
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let temporary = parent.join(format!(".tmp-{}-{sequence}", std::process::id()));
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .await?;
        if let Err(error) = file.write_all(&bytes).await {
            drop(file);
            let _ = fs::remove_file(&temporary).await;
            return Err(error.into());
        }
        if let Err(error) = file.sync_all().await {
            drop(file);
            let _ = fs::remove_file(&temporary).await;
            return Err(error.into());
        }
        drop(file);

        let link_result = fs::hard_link(&temporary, &target).await;
        let _ = fs::remove_file(&temporary).await;
        match link_result {
            Ok(()) => Ok(CommitOutcome::Stored),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                if fs::read(&target).await? == bytes {
                    Ok(CommitOutcome::AlreadyPresent)
                } else {
                    Err(CommitError::IdentityCollision { path: target })
                }
            }
            Err(error) => Err(error.into()),
        }
    }
}

// Inputs: arbitrary bytes used as one address component.
// Outputs: lowercase two-character-per-byte hexadecimal text.
// Logic: encode without treating caller bytes as Unicode or filesystem syntax.
fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}
