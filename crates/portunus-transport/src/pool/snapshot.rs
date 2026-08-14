//! Immutable operational telemetry for one bounded reusable buffer pool.
//!
//! Counts are sampled consistently under the pool lock and saturate rather than
//! wrap in the mutable owner. Retained buffers are a current gauge; acquisitions,
//! reuses, and discarded returns are cumulative counters.
//!
//! This module stores no buffers, exposes no mutation, and does not install a
//! metrics exporter or process-global telemetry policy.

/// Copyable operational counters for one pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BufferPoolSnapshot {
    pub(super) retained_buffers: usize,
    pub(super) acquisitions: u64,
    pub(super) reuses: u64,
    pub(super) discarded_returns: u64,
}

impl BufferPoolSnapshot {
    /// Returns currently retained empty buffer count.
    ///
    /// **Inputs:** Shared snapshot borrow.
    /// **Outputs:** Gauge in `0..=configured retained limit`.
    /// **Logic:** Expose the consistent captured gauge without pool locking.
    #[must_use]
    pub const fn retained_buffers(&self) -> usize {
        self.retained_buffers
    }

    /// Returns successful pool acquisition count.
    ///
    /// **Inputs:** Shared snapshot borrow.
    /// **Outputs:** Saturating cumulative acquisition counter.
    /// **Logic:** Count only admitted requests, never over-capacity rejections.
    #[must_use]
    pub const fn acquisitions(&self) -> u64 {
        self.acquisitions
    }

    /// Returns acquisitions served from retained allocations.
    ///
    /// **Inputs:** Shared snapshot borrow.
    /// **Outputs:** Saturating cumulative reuse counter no greater than acquisitions.
    /// **Logic:** Distinguish allocation reuse from newly allocated buffers.
    #[must_use]
    pub const fn reuses(&self) -> u64 {
        self.reuses
    }

    /// Returns returns discarded by capacity or count bounds.
    ///
    /// **Inputs:** Shared snapshot borrow.
    /// **Outputs:** Saturating cumulative discarded-return counter.
    /// **Logic:** Make bounded-retention pressure observable without buffer details.
    #[must_use]
    pub const fn discarded_returns(&self) -> u64 {
        self.discarded_returns
    }
}
