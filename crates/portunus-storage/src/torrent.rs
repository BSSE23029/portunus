//! `BitTorrent` single-file piece storage compatibility adapter.
//!
//! This reference workload preallocates one destination and accepts only complete
//! pieces whose expected SHA-1 digest and protocol-derived length match. A mutex
//! serializes the shared seek/write cursor. Hash failures leave disk unchanged.
//!
//! ```text
//! piece bytes ──length + SHA-1──> seek(piece offset) ──write + flush──> file
//! ```
//!
//! This module is not the reusable storage model: it does not provide sparse block
//! assembly, content addressing, journals, multi-file mapping, or shared quotas.

use crate::integrity::sha1_digest;
use std::{io, path::Path};
use thiserror::Error;
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncSeekExt, AsyncWriteExt, SeekFrom},
    sync::Mutex,
};

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
    /// Inputs: destination, regular piece length, total length, and expected hashes.
    /// Outputs: asynchronously writable preallocated adapter or I/O error.
    /// Logic: truncate/preallocate one file and retain immutable torrent metadata;
    /// a mutex serializes seek plus write because the file has one shared cursor.
    /// # Errors
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

    /// Inputs: zero-based piece index and complete untrusted candidate bytes.
    /// Outputs: flushed write or invalid-index, integrity, or I/O error.
    /// Logic: validate final-piece length and SHA-1 before locking and writing.
    /// # Errors
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
        if data.len() as u64 != expected_len || sha1_digest(data) != expected {
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
