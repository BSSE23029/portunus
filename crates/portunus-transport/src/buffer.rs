//! Independent logical buffer limits and measured allocation accounting.
//!
//! Inbound retained bytes and one encoded outbound frame have separate inclusive
//! ceilings. Successful observations update monotonic peaks for both logical length
//! and allocator capacity; rejected growth never contaminates those measurements.
//!
//! This module does not allocate buffers, pool memory, encode frames, perform I/O,
//! or claim allocator capacity equals live payload bytes.

use thiserror::Error;
use tracing::{trace, warn};

/// Stable direction labels shared by buffer configuration, errors, and telemetry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BufferDirection {
    Inbound,
    Outbound,
}

/// Independent inclusive retained-byte limits for one session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BufferBudget {
    max_inbound_bytes: usize,
    max_outbound_bytes: usize,
}

impl BufferBudget {
    /// Creates positive independent logical byte limits.
    ///
    /// **Inputs:** Maximum retained inbound bytes and encoded outbound frame bytes.
    /// **Outputs:** Immutable budget or first direction-specific zero error.
    /// **Logic:** Validate inbound then outbound before runtime buffer allocation.
    ///
    /// # Errors
    /// Returns [`BufferConfigError::ZeroLimit`] naming the zero direction.
    pub const fn new(
        max_inbound_bytes: usize,
        max_outbound_bytes: usize,
    ) -> Result<Self, BufferConfigError> {
        if max_inbound_bytes == 0 {
            return Err(BufferConfigError::ZeroLimit {
                direction: BufferDirection::Inbound,
            });
        }
        if max_outbound_bytes == 0 {
            return Err(BufferConfigError::ZeroLimit {
                direction: BufferDirection::Outbound,
            });
        }
        Ok(Self {
            max_inbound_bytes,
            max_outbound_bytes,
        })
    }

    /// Returns the inclusive retained inbound byte ceiling.
    ///
    /// **Inputs:** Shared budget borrow.
    /// **Outputs:** Positive byte count.
    /// **Logic:** Expose validated policy without mutation.
    #[must_use]
    pub const fn max_inbound_bytes(&self) -> usize {
        self.max_inbound_bytes
    }

    /// Returns the inclusive encoded outbound frame byte ceiling.
    ///
    /// **Inputs:** Shared budget borrow.
    /// **Outputs:** Positive byte count.
    /// **Logic:** Expose validated policy without mutation.
    #[must_use]
    pub const fn max_outbound_bytes(&self) -> usize {
        self.max_outbound_bytes
    }
}

/// Stable buffer budget validation failures.
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
pub enum BufferConfigError {
    #[error("{direction:?} buffer limit must be greater than zero")]
    ZeroLimit { direction: BufferDirection },
}

/// Stable one-over-budget observation context.
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
#[error("{direction:?} buffer attempted {attempted} bytes above limit {limit}")]
pub struct BufferLimitError {
    pub direction: BufferDirection,
    pub attempted: usize,
    pub limit: usize,
}

/// Peak logical and allocated bytes observed during a session.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct BufferUsage {
    inbound_bytes: usize,
    inbound_capacity: usize,
    outbound_bytes: usize,
    outbound_capacity: usize,
}

impl BufferUsage {
    /// Returns the peak successfully retained inbound logical bytes.
    ///
    /// **Inputs:** Shared usage borrow.
    /// **Outputs:** Byte count within the configured inbound limit.
    /// **Logic:** Report logical data independently from allocator capacity.
    #[must_use]
    pub const fn peak_inbound_bytes(&self) -> usize {
        self.inbound_bytes
    }

    /// Returns the peak inbound buffer allocation capacity observed.
    ///
    /// **Inputs:** Shared usage borrow.
    /// **Outputs:** Allocator capacity in bytes; it may exceed logical limits.
    /// **Logic:** Measure allocation overhead instead of claiming it is payload.
    #[must_use]
    pub const fn peak_inbound_capacity(&self) -> usize {
        self.inbound_capacity
    }

    /// Returns the peak successfully encoded outbound logical bytes.
    ///
    /// **Inputs:** Shared usage borrow.
    /// **Outputs:** Byte count within the configured outbound limit.
    /// **Logic:** Report one-frame logical size separately from retained capacity.
    #[must_use]
    pub const fn peak_outbound_bytes(&self) -> usize {
        self.outbound_bytes
    }

    /// Returns the peak outbound buffer allocation capacity observed.
    ///
    /// **Inputs:** Shared usage borrow.
    /// **Outputs:** Allocator capacity in bytes.
    /// **Logic:** Make reusable-buffer retention visible to operations and benchmarks.
    #[must_use]
    pub const fn peak_outbound_capacity(&self) -> usize {
        self.outbound_capacity
    }
}

/// Mutable per-session observer enforcing one [`BufferBudget`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BufferAccountant {
    budget: BufferBudget,
    usage: BufferUsage,
}

impl BufferAccountant {
    /// Creates zeroed measurements under a validated budget.
    ///
    /// **Inputs:** Copyable validated buffer budget.
    /// **Outputs:** Accountant with all peaks at zero.
    /// **Logic:** Separate policy validation from runtime observation.
    #[must_use]
    pub const fn new(budget: BufferBudget) -> Self {
        Self {
            budget,
            usage: BufferUsage {
                inbound_bytes: 0,
                inbound_capacity: 0,
                outbound_bytes: 0,
                outbound_capacity: 0,
            },
        }
    }

    /// Enforces and records one inbound logical length/allocation observation.
    ///
    /// **Inputs:** Exclusive accountant, retained length, and allocator capacity bytes.
    /// **Outputs:** Unit or stable over-budget error without changing peaks.
    /// **Logic:** Validate first, then monotonically update independent measurements.
    ///
    /// # Errors
    /// Returns [`BufferLimitError`] when `bytes` exceeds the inbound limit.
    pub fn observe_inbound(
        &mut self,
        bytes: usize,
        capacity: usize,
    ) -> Result<(), BufferLimitError> {
        observe(
            BufferDirection::Inbound,
            bytes,
            capacity,
            self.budget.max_inbound_bytes,
            &mut self.usage.inbound_bytes,
            &mut self.usage.inbound_capacity,
        )
    }

    /// Enforces and records one outbound logical length/allocation observation.
    ///
    /// **Inputs:** Exclusive accountant, encoded length, and allocator capacity bytes.
    /// **Outputs:** Unit or stable over-budget error without changing peaks.
    /// **Logic:** Validate first, then monotonically update independent measurements.
    ///
    /// # Errors
    /// Returns [`BufferLimitError`] when `bytes` exceeds the outbound limit.
    pub fn observe_outbound(
        &mut self,
        bytes: usize,
        capacity: usize,
    ) -> Result<(), BufferLimitError> {
        observe(
            BufferDirection::Outbound,
            bytes,
            capacity,
            self.budget.max_outbound_bytes,
            &mut self.usage.outbound_bytes,
            &mut self.usage.outbound_capacity,
        )
    }

    /// Returns a copyable consistent usage snapshot.
    ///
    /// **Inputs:** Shared accountant borrow.
    /// **Outputs:** Current four peak measurements.
    /// **Logic:** Copy one small value without exposing mutable counters.
    #[must_use]
    pub const fn usage(&self) -> BufferUsage {
        self.usage
    }

    /// Returns the immutable budget enforced by this accountant.
    ///
    /// **Inputs:** Shared accountant borrow.
    /// **Outputs:** Copy of both independent logical byte ceilings.
    /// **Logic:** Allow runtime I/O to calculate remaining admission before reading.
    #[must_use]
    pub const fn budget(&self) -> BufferBudget {
        self.budget
    }
}

/// Validates one observation and updates its logical/allocation peaks.
///
/// **Inputs:** Direction, logical/capacity bytes, limit, and exclusive peak fields.
/// **Outputs:** Unit or stable error; rejected observations leave both peaks unchanged.
/// **Logic:** Enforce logical limit first, then take monotonic maxima and trace metadata.
fn observe(
    direction: BufferDirection,
    bytes: usize,
    capacity: usize,
    limit: usize,
    peak_bytes: &mut usize,
    peak_capacity: &mut usize,
) -> Result<(), BufferLimitError> {
    if bytes > limit {
        warn!(
            ?direction,
            attempted = bytes,
            limit,
            "buffer limit rejected"
        );
        return Err(BufferLimitError {
            direction,
            attempted: bytes,
            limit,
        });
    }
    *peak_bytes = (*peak_bytes).max(bytes);
    *peak_capacity = (*peak_capacity).max(capacity);
    trace!(?direction, bytes, capacity, "buffer usage observed");
    Ok(())
}
