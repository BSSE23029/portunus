//! Bounded sparse and out-of-order chunk assembly policy.
//!
//! Each assembler allocates exactly the declared chunk length for payload bytes
//! plus a compact received-bit map. Both the chunk-size and buffered-payload limits
//! are inclusive and validated before allocation. Blocks may arrive out of order;
//! identical overlap is idempotent, while conflicting overlap rejects atomically.
//!
//! ```text
//! blocks(offset, bytes) ──range/overlap checks──> bounded buffer
//! bounded complete buffer ──integrity policy──> committable bytes
//! ```
//!
//! This module owns no disk I/O, journals, global quotas, or scheduling. Those
//! orchestration policies consume only bytes returned by [`ChunkAssembler::finish`].

use crate::integrity::{ContentId, IntegrityError, IntegrityValidator};
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AssemblyConfig {
    max_chunk_bytes: usize,
    max_buffered_bytes: usize,
}

impl AssemblyConfig {
    /// Inputs: inclusive per-chunk and per-assembler payload byte ceilings.
    /// Outputs: validated independent limits or a typed zero-bound error.
    /// Logic: reject unusable budgets before any chunk allocation can occur.
    /// # Errors
    /// Returns a distinct zero-limit error for each independent budget.
    pub const fn new(
        max_chunk_bytes: usize,
        max_buffered_bytes: usize,
    ) -> Result<Self, AssemblyError> {
        if max_chunk_bytes == 0 {
            return Err(AssemblyError::ZeroChunkLimit);
        }
        if max_buffered_bytes == 0 {
            return Err(AssemblyError::ZeroBufferLimit);
        }
        Ok(Self {
            max_chunk_bytes,
            max_buffered_bytes,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AssemblyProgress {
    pub received_bytes: usize,
    pub expected_bytes: usize,
    pub complete: bool,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum AssemblyError {
    #[error("maximum chunk bytes must be greater than zero")]
    ZeroChunkLimit,
    #[error("maximum buffered bytes must be greater than zero")]
    ZeroBufferLimit,
    #[error("chunk length {actual} exceeds configured limit {limit}")]
    ChunkTooLarge { actual: usize, limit: usize },
    #[error("chunk requires {actual} buffered bytes, exceeding limit {limit}")]
    BufferLimitExceeded { actual: usize, limit: usize },
    #[error("block at {offset} with length {length} exceeds chunk length {chunk_length}")]
    BlockOutOfRange {
        offset: usize,
        length: usize,
        chunk_length: usize,
    },
    #[error("block conflicts with received byte at offset {offset}")]
    ConflictingOverlap { offset: usize },
    #[error("chunk is incomplete: received {received} of {expected} bytes")]
    Incomplete { received: usize, expected: usize },
    #[error(transparent)]
    Integrity(#[from] IntegrityError),
}

#[derive(Debug)]
pub struct ChunkAssembler<V> {
    identity: ContentId,
    validator: V,
    bytes: Vec<u8>,
    received: Vec<bool>,
    received_bytes: usize,
}

impl<V: IntegrityValidator> ChunkAssembler<V> {
    /// Inputs: declared length, expected identity, validator, and inclusive limits.
    /// Outputs: empty bounded assembler or a pre-allocation admission error.
    /// Logic: check chunk then buffer budget in stable order before allocating state.
    /// # Errors
    /// Returns [`AssemblyError::ChunkTooLarge`] or `BufferLimitExceeded`.
    pub fn new(
        chunk_length: usize,
        identity: ContentId,
        validator: V,
        config: AssemblyConfig,
    ) -> Result<Self, AssemblyError> {
        if chunk_length > config.max_chunk_bytes {
            return Err(AssemblyError::ChunkTooLarge {
                actual: chunk_length,
                limit: config.max_chunk_bytes,
            });
        }
        if chunk_length > config.max_buffered_bytes {
            return Err(AssemblyError::BufferLimitExceeded {
                actual: chunk_length,
                limit: config.max_buffered_bytes,
            });
        }
        Ok(Self {
            identity,
            validator,
            bytes: vec![0; chunk_length],
            received: vec![false; chunk_length],
            received_bytes: 0,
        })
    }

    /// Inputs: zero-based byte `offset` and borrowed block payload.
    /// Outputs: exact progress or typed range/conflicting-overlap details.
    /// Logic: validate the full block without mutation, then fill only unseen bytes.
    /// # Errors
    /// Returns `BlockOutOfRange` or `ConflictingOverlap`; state remains unchanged.
    pub fn ingest(
        &mut self,
        offset: usize,
        block: &[u8],
    ) -> Result<AssemblyProgress, AssemblyError> {
        let end = offset
            .checked_add(block.len())
            .filter(|end| *end <= self.bytes.len())
            .ok_or(AssemblyError::BlockOutOfRange {
                offset,
                length: block.len(),
                chunk_length: self.bytes.len(),
            })?;
        for (position, candidate) in (offset..end).zip(block) {
            if self.received[position] && self.bytes[position] != *candidate {
                return Err(AssemblyError::ConflictingOverlap { offset: position });
            }
        }
        for (position, candidate) in (offset..end).zip(block) {
            if !self.received[position] {
                self.bytes[position] = *candidate;
                self.received[position] = true;
                self.received_bytes += 1;
            }
        }
        Ok(self.progress())
    }

    /// Inputs: fully owned assembler state.
    /// Outputs: verified committable bytes or incomplete/integrity failure.
    /// Logic: require exact coverage, then invoke the selected validator once.
    /// # Errors
    /// Returns `Incomplete` or wraps the validator's stable integrity details.
    pub fn finish(self) -> Result<Vec<u8>, AssemblyError> {
        if self.received_bytes != self.bytes.len() {
            return Err(AssemblyError::Incomplete {
                received: self.received_bytes,
                expected: self.bytes.len(),
            });
        }
        self.validator
            .validate(&self.bytes, self.identity.digest())?;
        Ok(self.bytes)
    }

    // Inputs: shared assembler state.
    // Outputs: exact received/expected byte counts and completion flag.
    // Logic: derive completion from equality so empty chunks remain well-defined.
    const fn progress(&self) -> AssemblyProgress {
        AssemblyProgress {
            received_bytes: self.received_bytes,
            expected_bytes: self.bytes.len(),
            complete: self.received_bytes == self.bytes.len(),
        }
    }
}
