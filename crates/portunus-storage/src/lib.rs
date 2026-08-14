//! Async, preallocated piece storage with SHA-1 integrity checks.
use sha1::{Digest, Sha1};
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
    pub async fn write_verified_piece(&self, index: u32, data: &[u8]) -> Result<(), Error> {
        let expected = *self
            .hashes
            .get(index as usize)
            .ok_or(Error::InvalidPiece(index))?;
        let start = index as u64 * self.piece_length;
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
        Ok(())
    }
}
pub fn sha1(data: &[u8]) -> [u8; 20] {
    Sha1::digest(data).into()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn validates_before_write() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("portunus-storage-{}", std::process::id()));
        let data = b"piece";
        let s = PieceStore::create(&path, 5, 5, vec![sha1(data)])
            .await
            .unwrap();
        s.write_verified_piece(0, data).await.unwrap();
        let _ = tokio::fs::remove_file(path).await;
    }
}
