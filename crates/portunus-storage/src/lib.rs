//! Async, preallocated piece storage with SHA-1 integrity checks.
//!
//! Network bytes are untrusted until their complete piece hash matches expected
//! metadata. The integrity check therefore occurs before any piece is committed.
//!
//! ```text
//! bytes ──length check──> SHA-1 ──digest match──> seek(offset) ──write──> disk
//!   └──────── invalid length/hash ──────────────> reject (disk unchanged)
//! ```
use sha1::{Digest, Sha1};
use std::{io, path::Path};
use thiserror::Error;
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncSeekExt, AsyncWriteExt, SeekFrom},
    sync::Mutex,
};

pub mod assembly;
pub mod content;
pub mod integrity;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("piece {0} is outside the manifest")]
    InvalidPiece(u32),
    #[error("piece {0} failed SHA-1 verification")]
    HashMismatch(u32),
}

pub struct PieceStore {
    file: Mutex<File>,
    piece_length: u64,
    total_length: u64,
    hashes: Vec<[u8; 20]>,
}
impl PieceStore {
    /// Creates and preallocates a single-file piece store.
    ///
    /// **Inputs:** Destination path, regular piece size, total file size, and one
    /// expected 20-byte SHA-1 digest per piece.
    ///
    /// **Outputs:** An asynchronously writable [`PieceStore`], or an I/O error.
    ///
    /// **Logic:** Open a new read/write file with truncation, preallocate its final
    /// logical length, and retain immutable layout/hash metadata. A Tokio mutex
    /// serializes seek-plus-write because those two operations share one cursor.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when creation, truncation, or preallocation fails.
    pub async fn create(
        path: impl AsRef<Path>,
        piece_length: u64,
        total_length: u64,
        hashes: Vec<[u8; 20]>,
    ) -> Result<Self, Error> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(path)
            .await?;
        file.set_len(total_length).await?;
        Ok(Self {
            file: Mutex::new(file),
            piece_length,
            total_length,
            hashes,
        })
    }
    /// Verifies and atomically decides whether to write one complete piece.
    ///
    /// **Inputs:** Zero-based piece `index` and the complete candidate `data`.
    ///
    /// **Outputs:** `Ok(())` after a flushed write, or a typed invalid-index,
    /// integrity, or I/O error. Hash/length failures write nothing.
    ///
    /// **Logic:** Resolve expected hash/length (including a shorter final piece),
    /// hash bytes before locking the file, then seek to `index * piece_length`,
    /// write all bytes, and flush while holding the cursor lock.
    ///
    /// # Errors
    ///
    /// Returns invalid-piece, hash-mismatch, or filesystem I/O failures.
    pub async fn write_verified_piece(&self, index: u32, data: &[u8]) -> Result<(), Error> {
        let expected = *self
            .hashes
            .get(index as usize)
            .ok_or(Error::InvalidPiece(index))?;
        let start = u64::from(index) * self.piece_length;
        let expected_len = self
            .piece_length
            .min(self.total_length.saturating_sub(start));
        if data.len() as u64 != expected_len || Sha1::digest(data).as_slice() != expected {
            return Err(Error::HashMismatch(index));
        }
        let mut file = self.file.lock().await;
        file.seek(SeekFrom::Start(start)).await?;
        file.write_all(data).await?;
        file.flush().await?;
        drop(file);
        Ok(())
    }
}
/// Computes the protocol-compatible SHA-1 digest of an in-memory byte slice.
///
/// **Inputs:** Arbitrary `data` bytes.
///
/// **Outputs:** The deterministic 20-byte SHA-1 digest.
///
/// **Logic:** Feed the full slice into the digest implementation and convert its
/// fixed-size result into an array convenient for manifests and equality checks.
#[must_use]
pub fn sha1(data: &[u8]) -> [u8; 20] {
    Sha1::digest(data).into()
}
