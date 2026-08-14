//! Shared byte and operation admission with cooperative backpressure.
//!
//! Two independent inclusive ceilings bound bytes retained by admitted work and
//! concurrent operations. Admission owns both permits or neither. Nonblocking calls
//! support load shedding; asynchronous calls wait without polling and race explicit
//! cancellation. RAII permits restore capacity on every return, error, or task drop.
//!
//! ```text
//! request(bytes) ──byte permits──> operation permit ──> admitted work
//!       └─ too large/saturated/cancelled ─────────────> typed rejection
//! ```
//!
//! This module does not allocate chunk buffers, schedule work, perform I/O, or set
//! global logging policy. It emits bounded structured diagnostics through `tracing`.

use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QuotaSnapshot {
    pub byte_limit: u32,
    pub used_bytes: u32,
    pub operation_limit: usize,
    pub active_operations: usize,
}

#[derive(Clone, Debug)]
pub struct StorageQuota {
    bytes: Arc<Semaphore>,
    operations: Arc<Semaphore>,
    byte_limit: u32,
    operation_limit: usize,
}

impl StorageQuota {
    /// Inputs: nonzero byte and concurrent-operation inclusive ceilings.
    /// Outputs: shared admission policy or distinct zero-budget error.
    /// Logic: validate independent limits before constructing semaphores.
    /// # Errors
    /// Returns [`QuotaError::ZeroByteLimit`] or `ZeroOperationLimit`.
    pub fn new(byte_limit: u32, operation_limit: usize) -> Result<Self, QuotaError> {
        if byte_limit == 0 {
            return Err(QuotaError::ZeroByteLimit);
        }
        if operation_limit == 0 {
            return Err(QuotaError::ZeroOperationLimit);
        }
        Ok(Self {
            bytes: Arc::new(Semaphore::new(byte_limit as usize)),
            operations: Arc::new(Semaphore::new(operation_limit)),
            byte_limit,
            operation_limit,
        })
    }

    /// Inputs: requested retained bytes; zero-byte work still uses one operation.
    /// Outputs: immediate RAII permit or typed too-large/saturated/closed rejection.
    /// Logic: acquire bytes then operation nonblockingly; partial admission drops.
    /// # Errors
    /// Returns stable request or current-capacity details without waiting.
    pub fn try_admit(&self, requested_bytes: u32) -> Result<QuotaPermit, QuotaError> {
        self.validate_request(requested_bytes)?;
        let bytes = Arc::clone(&self.bytes)
            .try_acquire_many_owned(requested_bytes)
            .map_err(|error| map_try_error(&error, requested_bytes))?;
        let operation = match Arc::clone(&self.operations).try_acquire_owned() {
            Ok(permit) => permit,
            Err(error) => return Err(map_try_error(&error, requested_bytes)),
        };
        tracing::trace!(requested_bytes, "storage quota admitted work");
        Ok(QuotaPermit {
            _bytes: bytes,
            _operation: operation,
            requested_bytes,
        })
    }

    /// Inputs: requested bytes and borrowed cooperative cancellation signal.
    /// Outputs: eventual RAII permit or typed request/cancellation/closed error.
    /// Logic: give cancellation priority before and during each semaphore wait;
    /// a cancelled second phase drops already acquired byte capacity immediately.
    /// # Errors
    /// Returns request-too-large, cancelled, or closed errors.
    pub async fn admit(
        &self,
        requested_bytes: u32,
        cancellation: &CancellationToken,
    ) -> Result<QuotaPermit, QuotaError> {
        self.validate_request(requested_bytes)?;
        if cancellation.is_cancelled() {
            return Err(QuotaError::Cancelled);
        }
        let byte_wait = Arc::clone(&self.bytes).acquire_many_owned(requested_bytes);
        let bytes = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(QuotaError::Cancelled),
            result = byte_wait => result.map_err(|_| QuotaError::Closed)?,
        };
        let operation_wait = Arc::clone(&self.operations).acquire_owned();
        let operation = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(QuotaError::Cancelled),
            result = operation_wait => result.map_err(|_| QuotaError::Closed)?,
        };
        tracing::trace!(requested_bytes, "storage quota admitted waiting work");
        Ok(QuotaPermit {
            _bytes: bytes,
            _operation: operation,
            requested_bytes,
        })
    }

    /// Inputs: shared quota state.
    /// Outputs: consistent per-semaphore instantaneous usage counters.
    /// Logic: subtract currently available permits from immutable configured limits.
    #[must_use]
    pub fn snapshot(&self) -> QuotaSnapshot {
        QuotaSnapshot {
            byte_limit: self.byte_limit,
            used_bytes: self
                .byte_limit
                .saturating_sub(u32::try_from(self.bytes.available_permits()).unwrap_or(u32::MAX)),
            operation_limit: self.operation_limit,
            active_operations: self
                .operation_limit
                .saturating_sub(self.operations.available_permits()),
        }
    }

    // Inputs: requested byte count.
    // Outputs: success or exact request/configured limit details.
    // Logic: reject impossible work before touching either shared semaphore.
    const fn validate_request(&self, requested: u32) -> Result<(), QuotaError> {
        if requested > self.byte_limit {
            return Err(QuotaError::RequestTooLarge {
                requested,
                limit: self.byte_limit,
            });
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct QuotaPermit {
    _bytes: OwnedSemaphorePermit,
    _operation: OwnedSemaphorePermit,
    requested_bytes: u32,
}

impl QuotaPermit {
    /// Inputs: shared admission permit.
    /// Outputs: exact byte capacity held until this permit is dropped.
    /// Logic: expose accounting without exposing underlying semaphore mutation.
    #[must_use]
    pub const fn bytes(&self) -> u32 {
        self.requested_bytes
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum QuotaError {
    #[error("storage byte limit must be greater than zero")]
    ZeroByteLimit,
    #[error("storage operation limit must be greater than zero")]
    ZeroOperationLimit,
    #[error("request requires {requested} bytes, exceeding limit {limit}")]
    RequestTooLarge { requested: u32, limit: u32 },
    #[error("storage quota is saturated for a {requested_bytes}-byte request")]
    Saturated { requested_bytes: u32 },
    #[error("storage admission was cancelled")]
    Cancelled,
    #[error("storage admission is closed")]
    Closed,
}

// Inputs: Tokio nonblocking semaphore failure and original byte request.
// Outputs: stable quota-level saturated or closed error.
// Logic: hide executor-specific error types at the reusable public boundary.
const fn map_try_error(error: &TryAcquireError, requested_bytes: u32) -> QuotaError {
    match error {
        TryAcquireError::NoPermits => QuotaError::Saturated { requested_bytes },
        TryAcquireError::Closed => QuotaError::Closed,
    }
}
