//! Content identities and pluggable in-memory integrity validation.
//!
//! An identity owns a non-empty algorithm label and non-empty opaque digest.
//! Digest widths belong to validators, allowing fixed cryptographic hashes and
//! application-defined schemes to share one storage boundary. Validators inspect
//! complete candidate chunks but perform no disk I/O, allocation, or commit.
//!
//! ```text
//! candidate bytes + opaque expected digest ──validator──> accept | typed reject
//! ```
//!
//! This module does not select algorithms, stream piece assembly, name files, or
//! install process-global policy. Storage composition chooses the validator.

use sha1::{Digest, Sha1};
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContentId {
    algorithm: String,
    digest: Vec<u8>,
}

impl ContentId {
    /// Inputs: owned/borrowed algorithm text and opaque digest bytes.
    /// Outputs: an owned identity or a typed error for either empty component.
    /// Logic: reject ambiguous identities before retaining caller-supplied data.
    /// # Errors
    /// Returns [`IntegrityError::EmptyAlgorithm`] or [`IntegrityError::EmptyDigest`].
    pub fn new(
        algorithm: impl Into<String>,
        digest: impl Into<Vec<u8>>,
    ) -> Result<Self, IntegrityError> {
        let algorithm = algorithm.into();
        if algorithm.is_empty() {
            return Err(IntegrityError::EmptyAlgorithm);
        }
        let digest = digest.into();
        if digest.is_empty() {
            return Err(IntegrityError::EmptyDigest);
        }
        Ok(Self { algorithm, digest })
    }

    /// Inputs: shared identity reference.
    /// Outputs: borrowed stable algorithm label.
    /// Logic: expose classification without transferring identity ownership.
    #[must_use]
    pub fn algorithm(&self) -> &str {
        &self.algorithm
    }

    /// Inputs: shared identity reference.
    /// Outputs: borrowed opaque digest bytes.
    /// Logic: expose validator input without copying digest storage.
    #[must_use]
    pub fn digest(&self) -> &[u8] {
        &self.digest
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum IntegrityError {
    #[error("content identity algorithm must not be empty")]
    EmptyAlgorithm,
    #[error("content identity digest must not be empty")]
    EmptyDigest,
    #[error("invalid digest length: expected {expected} bytes, received {actual}")]
    InvalidDigestLength { expected: usize, actual: usize },
    #[error("content integrity mismatch")]
    Mismatch,
}

pub trait IntegrityValidator: Send + Sync {
    /// Inputs: complete candidate bytes and opaque expected digest bytes.
    /// Outputs: success or stable malformed-digest/integrity failure details.
    /// Logic: validate without disk I/O; implementations define digest semantics.
    /// # Errors
    /// Returns an [`IntegrityError`] when the digest is malformed or does not match.
    fn validate(&self, data: &[u8], expected: &[u8]) -> Result<(), IntegrityError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Sha1Validator;

impl IntegrityValidator for Sha1Validator {
    /// Inputs: complete candidate bytes and an expected 20-byte SHA-1 digest.
    /// Outputs: success, exact length details, or an integrity mismatch.
    /// Logic: reject the digest shape before hashing, then compare fixed bytes.
    fn validate(&self, data: &[u8], expected: &[u8]) -> Result<(), IntegrityError> {
        if expected.len() != 20 {
            return Err(IntegrityError::InvalidDigestLength {
                expected: 20,
                actual: expected.len(),
            });
        }
        if Sha1::digest(data).as_slice() == expected {
            Ok(())
        } else {
            Err(IntegrityError::Mismatch)
        }
    }
}
