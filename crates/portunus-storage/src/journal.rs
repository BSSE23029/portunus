//! Crash-recoverable bounded journal for sparse chunk blocks.
//!
//! The journal has a 12-byte header followed by checksummed records. Its inclusive
//! byte ceiling is checked before every append and before recovery allocation.
//! Appends synchronize before success. Recovery replays complete valid records and
//! truncates only an incomplete final record; complete corruption is never hidden.
//!
//! ```text
//! header: "PTJ1" | chunk_length:u64
//! record: offset:u64 | payload_length:u32 | payload | SHA1(header + payload)
//! ```
//!
//! This module records ingestion intent. It does not validate complete content,
//! assemble chunks, publish objects, or coordinate admission across journals.

use sha1::{Digest, Sha1};
use std::{io, path::Path};
use thiserror::Error;
use tokio::{
    fs::{self, File, OpenOptions},
    io::AsyncWriteExt,
};

const MAGIC: &[u8; 4] = b"PTJ1";
const HEADER_BYTES: usize = 12;
const RECORD_OVERHEAD: usize = 32;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JournalBlock {
    pub offset: usize,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JournalSnapshot {
    pub chunk_length: usize,
    pub blocks: Vec<JournalBlock>,
}

#[derive(Debug, Error)]
pub enum JournalError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("journal byte limit must be greater than zero")]
    ZeroByteLimit,
    #[error("chunk length must be greater than zero")]
    ZeroChunkLength,
    #[error("journal requires {actual} bytes, exceeding limit {limit}")]
    ByteLimitExceeded { actual: usize, limit: usize },
    #[error("invalid journal header")]
    InvalidHeader,
    #[error("block at {offset} with length {length} exceeds chunk length {chunk_length}")]
    BlockOutOfRange {
        offset: usize,
        length: usize,
        chunk_length: usize,
    },
    #[error("journal record at byte {record_offset} failed integrity validation")]
    CorruptRecord { record_offset: usize },
    #[error("journal record length cannot be represented on this platform")]
    LengthOverflow,
}

#[derive(Debug)]
pub struct Journal {
    file: File,
    chunk_length: usize,
    max_bytes: usize,
    current_bytes: usize,
}

impl Journal {
    /// Inputs: path, nonzero chunk length, and inclusive journal byte ceiling.
    /// Outputs: new synchronized journal or typed configuration/I/O failure.
    /// Logic: validate both budgets, write the fixed header, and synchronize it.
    /// # Errors
    /// Returns zero/limit errors or filesystem failures.
    pub async fn create(
        path: impl AsRef<Path>,
        chunk_length: usize,
        max_bytes: usize,
    ) -> Result<Self, JournalError> {
        validate_limits(chunk_length, max_bytes)?;
        if HEADER_BYTES > max_bytes {
            return Err(JournalError::ByteLimitExceeded {
                actual: HEADER_BYTES,
                limit: max_bytes,
            });
        }
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(path)
            .await?;
        let chunk_length = u64::try_from(chunk_length).map_err(|_| JournalError::LengthOverflow)?;
        file.write_all(MAGIC).await?;
        file.write_all(&chunk_length.to_be_bytes()).await?;
        file.sync_all().await?;
        Ok(Self {
            file,
            chunk_length: usize::try_from(chunk_length)
                .map_err(|_| JournalError::LengthOverflow)?,
            max_bytes,
            current_bytes: HEADER_BYTES,
        })
    }

    /// Inputs: existing journal path and inclusive maximum recoverable bytes.
    /// Outputs: append-ready journal plus ordered valid block snapshot.
    /// Logic: bound read allocation, validate records, then truncate a torn suffix.
    /// # Errors
    /// Returns malformed/corrupt/limit details or filesystem failures.
    pub async fn resume(
        path: impl AsRef<Path>,
        max_bytes: usize,
    ) -> Result<(Self, JournalSnapshot), JournalError> {
        if max_bytes == 0 {
            return Err(JournalError::ZeroByteLimit);
        }
        let metadata = fs::metadata(path.as_ref()).await?;
        let actual = usize::try_from(metadata.len()).map_err(|_| JournalError::LengthOverflow)?;
        if actual > max_bytes {
            return Err(JournalError::ByteLimitExceeded {
                actual,
                limit: max_bytes,
            });
        }
        let bytes = fs::read(path.as_ref()).await?;
        let (chunk_length, blocks, valid_bytes) = recover_bytes(&bytes)?;
        let file = OpenOptions::new()
            .append(true)
            .read(true)
            .open(path)
            .await?;
        if valid_bytes != actual {
            file.set_len(u64::try_from(valid_bytes).map_err(|_| JournalError::LengthOverflow)?)
                .await?;
            file.sync_all().await?;
        }
        Ok((
            Self {
                file,
                chunk_length,
                max_bytes,
                current_bytes: valid_bytes,
            },
            JournalSnapshot {
                chunk_length,
                blocks,
            },
        ))
    }

    /// Inputs: zero-based chunk offset and borrowed block payload.
    /// Outputs: synchronized durable record or stable range/limit/I/O failure.
    /// Logic: admit the full encoded record, checksum it, then append and sync once.
    /// # Errors
    /// Returns range, representation, journal-limit, or filesystem errors.
    pub async fn append(&mut self, offset: usize, bytes: &[u8]) -> Result<(), JournalError> {
        validate_block(self.chunk_length, offset, bytes.len())?;
        let record_bytes = RECORD_OVERHEAD
            .checked_add(bytes.len())
            .ok_or(JournalError::LengthOverflow)?;
        let projected = self
            .current_bytes
            .checked_add(record_bytes)
            .ok_or(JournalError::LengthOverflow)?;
        if projected > self.max_bytes {
            return Err(JournalError::ByteLimitExceeded {
                actual: projected,
                limit: self.max_bytes,
            });
        }
        let encoded = encode_record(offset, bytes)?;
        self.file.write_all(&encoded).await?;
        self.file.sync_data().await?;
        self.current_bytes = projected;
        Ok(())
    }
}

// Inputs: chunk length and journal byte ceiling.
// Outputs: success or stable independent zero-budget error.
// Logic: reject unusable configuration before filesystem mutation.
const fn validate_limits(chunk_length: usize, max_bytes: usize) -> Result<(), JournalError> {
    if max_bytes == 0 {
        return Err(JournalError::ZeroByteLimit);
    }
    if chunk_length == 0 {
        return Err(JournalError::ZeroChunkLength);
    }
    Ok(())
}

// Inputs: chunk length plus a proposed half-open block range.
// Outputs: success or exact range error; overflow is treated as out-of-range.
// Logic: checked addition prevents hostile offsets from wrapping admission.
fn validate_block(chunk: usize, offset: usize, length: usize) -> Result<(), JournalError> {
    if offset.checked_add(length).is_none_or(|end| end > chunk) {
        return Err(JournalError::BlockOutOfRange {
            offset,
            length,
            chunk_length: chunk,
        });
    }
    Ok(())
}

// Inputs: admitted offset and payload.
// Outputs: one checksummed record buffer or representation error.
// Logic: serialize fixed-width header, payload, then SHA-1 over both.
fn encode_record(offset: usize, bytes: &[u8]) -> Result<Vec<u8>, JournalError> {
    let offset = u64::try_from(offset).map_err(|_| JournalError::LengthOverflow)?;
    let length = u32::try_from(bytes.len()).map_err(|_| JournalError::LengthOverflow)?;
    let mut record = Vec::with_capacity(RECORD_OVERHEAD + bytes.len());
    record.extend_from_slice(&offset.to_be_bytes());
    record.extend_from_slice(&length.to_be_bytes());
    record.extend_from_slice(bytes);
    let digest = Sha1::digest(&record);
    record.extend_from_slice(&digest);
    Ok(record)
}

// Inputs: bounded complete journal file bytes.
// Outputs: header length, valid records, and durable-prefix byte count.
// Logic: parse sequentially, stop only on an incomplete suffix, reject corruption.
fn recover_bytes(bytes: &[u8]) -> Result<(usize, Vec<JournalBlock>, usize), JournalError> {
    if bytes.len() < HEADER_BYTES || &bytes[..4] != MAGIC {
        return Err(JournalError::InvalidHeader);
    }
    let chunk_length = usize::try_from(u64::from_be_bytes(
        bytes[4..12]
            .try_into()
            .map_err(|_| JournalError::InvalidHeader)?,
    ))
    .map_err(|_| JournalError::LengthOverflow)?;
    if chunk_length == 0 {
        return Err(JournalError::InvalidHeader);
    }
    let mut cursor = HEADER_BYTES;
    let mut blocks = Vec::new();
    while cursor < bytes.len() {
        if bytes.len() - cursor < RECORD_OVERHEAD {
            break;
        }
        let record_offset = cursor;
        let offset = usize::try_from(u64::from_be_bytes(
            bytes[cursor..cursor + 8]
                .try_into()
                .map_err(|_| JournalError::CorruptRecord { record_offset })?,
        ))
        .map_err(|_| JournalError::LengthOverflow)?;
        let length = u32::from_be_bytes(
            bytes[cursor + 8..cursor + 12]
                .try_into()
                .map_err(|_| JournalError::CorruptRecord { record_offset })?,
        ) as usize;
        let total = RECORD_OVERHEAD
            .checked_add(length)
            .ok_or(JournalError::LengthOverflow)?;
        if bytes.len() - cursor < total {
            break;
        }
        validate_block(chunk_length, offset, length)?;
        let payload_end = cursor + 12 + length;
        let expected = &bytes[payload_end..payload_end + 20];
        if Sha1::digest(&bytes[cursor..payload_end]).as_slice() != expected {
            return Err(JournalError::CorruptRecord { record_offset });
        }
        blocks.push(JournalBlock {
            offset,
            bytes: bytes[cursor + 12..payload_end].to_vec(),
        });
        cursor += total;
    }
    Ok((chunk_length, blocks, cursor))
}
