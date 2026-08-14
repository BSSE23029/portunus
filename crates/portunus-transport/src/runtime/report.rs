//! Stable terminal reports and failures for the framed-session runtime.
//!
//! Reports count only completed application/I/O handoffs. Errors retain a bounded
//! operation label plus standard I/O category so telemetry and retry policy do not
//! parse prose. Display detail is diagnostic and is not a compatibility key.
//!
//! This module owns result contracts only. It does not perform I/O, transition
//! lifecycle state, classify retryability, log failures, or expose payload bytes.

use crate::{BufferLimitError, BufferUsage, SessionState};
use std::io;
use thiserror::Error;

/// Measured terminal state returned after an orderly session shutdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionReport {
    final_state: SessionState,
    inbound_frames: u64,
    outbound_frames: u64,
    buffer_usage: BufferUsage,
}

impl SessionReport {
    /// Creates a terminal report at the runtime ownership boundary.
    ///
    /// **Inputs:** Final lifecycle state and completed inbound/outbound frame counts.
    /// **Outputs:** Immutable report with no external side effects.
    /// **Logic:** Centralize construction while keeping counters externally read-only.
    pub(super) const fn new(
        final_state: SessionState,
        inbound_frames: u64,
        outbound_frames: u64,
        buffer_usage: BufferUsage,
    ) -> Self {
        Self {
            final_state,
            inbound_frames,
            outbound_frames,
            buffer_usage,
        }
    }

    /// Returns the lifecycle state reached before task completion.
    ///
    /// **Inputs:** Shared report borrow.
    /// **Outputs:** Copyable terminal lifecycle state.
    /// **Logic:** Expose state without mutable runtime internals.
    #[must_use]
    pub const fn final_state(&self) -> SessionState {
        self.final_state
    }

    /// Returns successfully admitted inbound frame count.
    ///
    /// **Inputs:** Shared report borrow.
    /// **Outputs:** Count incremented only after queue delivery.
    /// **Logic:** Distinguish decoded-and-delivered frames from raw reads.
    #[must_use]
    pub const fn inbound_frames(&self) -> u64 {
        self.inbound_frames
    }

    /// Returns successfully written outbound frame count.
    ///
    /// **Inputs:** Shared report borrow.
    /// **Outputs:** Count incremented only after complete frame writes.
    /// **Logic:** Measure completed runtime handoffs, not application attempts.
    #[must_use]
    pub const fn outbound_frames(&self) -> u64 {
        self.outbound_frames
    }

    /// Returns peak logical and allocated buffer measurements.
    ///
    /// **Inputs:** Shared report borrow.
    /// **Outputs:** Copyable final buffer-usage snapshot.
    /// **Logic:** Preserve the distinction between bounded bytes and retained capacity.
    #[must_use]
    pub const fn buffer_usage(&self) -> BufferUsage {
        self.buffer_usage
    }
}

/// Stable terminal operation context for codec, transport, and task failures.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
#[error("session {operation} failed ({kind:?}): {detail}")]
pub struct SessionError {
    operation: &'static str,
    kind: io::ErrorKind,
    detail: String,
}

impl SessionError {
    /// Returns the stable operation label (`decode`, `encode`, `read`, `write`, or `task`).
    ///
    /// **Inputs:** Shared error borrow.
    /// **Outputs:** Static bounded operation label.
    /// **Logic:** Support structured diagnostics without parsing display text.
    #[must_use]
    pub const fn operation(&self) -> &'static str {
        self.operation
    }

    /// Returns the underlying I/O-compatible error category.
    ///
    /// **Inputs:** Shared error borrow.
    /// **Outputs:** Copyable standard error kind.
    /// **Logic:** Preserve machine-readable failure classification.
    #[must_use]
    pub const fn kind(&self) -> io::ErrorKind {
        self.kind
    }

    /// Builds a bounded operation failure from an I/O-compatible error.
    ///
    /// **Inputs:** Static operation label and owned standard I/O error.
    /// **Outputs:** Stable session error retaining kind and display detail.
    /// **Logic:** Normalize codec and stream failures at the runtime boundary.
    pub(super) fn io(operation: &'static str, source: &io::Error) -> Self {
        Self {
            operation,
            kind: source.kind(),
            detail: source.to_string(),
        }
    }

    /// Builds an error for a panicked or cancelled runtime task.
    ///
    /// **Inputs:** Owned bounded join-failure description.
    /// **Outputs:** Task-labelled `Other` session error.
    /// **Logic:** Avoid leaking Tokio join types through the public contract.
    pub(super) const fn task(detail: String) -> Self {
        Self {
            operation: "task",
            kind: io::ErrorKind::Other,
            detail,
        }
    }

    /// Builds a stable timeout error for runtime timing termination.
    ///
    /// **Inputs:** Static `idle` or `deadline` operation label.
    /// **Outputs:** Timed-out session failure with bounded diagnostic detail.
    /// **Logic:** Preserve terminal policy reason without exposing timer internals.
    pub(super) fn timeout(operation: &'static str) -> Self {
        Self {
            operation,
            kind: io::ErrorKind::TimedOut,
            detail: format!("{operation} boundary elapsed"),
        }
    }

    /// Builds a stable invalid-data error from one logical buffer limit rejection.
    ///
    /// **Inputs:** Stable operation label and copyable limit context.
    /// **Outputs:** Session failure preserving direction, attempted bytes, and limit.
    /// **Logic:** Keep buffer enforcement machine-classifiable at the runtime boundary.
    pub(super) fn buffer(operation: &'static str, failure: BufferLimitError) -> Self {
        Self {
            operation,
            kind: io::ErrorKind::InvalidData,
            detail: failure.to_string(),
        }
    }
}
