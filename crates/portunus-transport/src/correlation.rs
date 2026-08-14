//! Bounded protocol-neutral request correlation with explicit deadline expiry.
//!
//! Each ID owns at most one pending value and absolute monotonic deadline. The
//! inclusive capacity is checked before insertion, rejected values are returned
//! to their caller, and expiry order is deterministic by correlation ID.
//!
//! This module does not generate IDs, read clocks implicitly, spawn timeout tasks,
//! retry requests, encode wire fields, or decide whether expiry is fatal.

use std::{collections::BTreeMap, time::Instant};
use thiserror::Error;
use tracing::{debug, warn};

/// Opaque protocol-neutral identifier for one correlated operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CorrelationId(u64);

impl CorrelationId {
    /// Wraps a caller-selected unsigned correlation value.
    ///
    /// **Inputs:** Any 64-bit value; a table enforces uniqueness.
    /// **Outputs:** Copyable opaque identifier.
    /// **Logic:** Avoid coupling session policy to protocol wire widths.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the wrapped value for adapters and telemetry.
    ///
    /// **Inputs:** Identifier by copy.
    /// **Outputs:** Original unsigned value.
    /// **Logic:** Provide conversion without representation mutation.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Stable reasons why correlation admission was rejected.
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
pub enum CorrelationError {
    #[error("correlation capacity must be greater than zero")]
    ZeroCapacity,
    #[error("correlation table reached its limit of {limit}")]
    AtCapacity { limit: usize },
    #[error("correlation id {id:?} is already in flight")]
    Duplicate { id: CorrelationId },
}

/// Rejected admission retaining ownership of the pending value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorrelationInsertError<T> {
    reason: CorrelationError,
    value: T,
}

impl<T> CorrelationInsertError<T> {
    /// Returns the stable copyable rejection reason.
    ///
    /// **Inputs:** Shared rejected-admission borrow.
    /// **Outputs:** Capacity or duplicate failure.
    /// **Logic:** Permit inspection before choosing ownership recovery.
    #[must_use]
    pub const fn reason(&self) -> CorrelationError {
        self.reason
    }

    /// Recovers the value rejected by admission.
    ///
    /// **Inputs:** Consumed rejection.
    /// **Outputs:** Original pending value without cloning.
    /// **Logic:** Backpressure must not silently discard application work.
    #[must_use]
    pub fn into_value(self) -> T {
        self.value
    }
}

#[derive(Debug)]
struct Pending<T> {
    deadline: Instant,
    value: T,
}

/// Deterministically ordered bounded collection of pending correlated values.
#[derive(Debug)]
pub struct CorrelationTable<T> {
    limit: usize,
    pending: BTreeMap<CorrelationId, Pending<T>>,
}

impl<T> CorrelationTable<T> {
    /// Creates an empty table with a positive inclusive ceiling.
    ///
    /// **Inputs:** Maximum simultaneously pending IDs.
    /// **Outputs:** Empty table or zero-capacity validation error.
    /// **Logic:** Reject zero before allocation or admission.
    ///
    /// # Errors
    /// Returns [`CorrelationError::ZeroCapacity`] for zero.
    pub const fn new(limit: usize) -> Result<Self, CorrelationError> {
        if limit == 0 {
            return Err(CorrelationError::ZeroCapacity);
        }
        Ok(Self {
            limit,
            pending: BTreeMap::new(),
        })
    }

    /// Admits one unique pending value and absolute deadline.
    ///
    /// **Inputs:** ID, monotonic deadline, and owned value.
    /// **Outputs:** Unit or rejection retaining the unchanged value.
    /// **Logic:** Diagnose duplicates before capacity and mutate only after validation.
    ///
    /// # Errors
    /// Returns duplicate or at-capacity context with ownership of `value`.
    pub fn insert(
        &mut self,
        id: CorrelationId,
        deadline: Instant,
        value: T,
    ) -> Result<(), CorrelationInsertError<T>> {
        let reason = if self.pending.contains_key(&id) {
            Some(CorrelationError::Duplicate { id })
        } else if self.pending.len() == self.limit {
            Some(CorrelationError::AtCapacity { limit: self.limit })
        } else {
            None
        };
        if let Some(reason) = reason {
            warn!(
                correlation_id = id.get(),
                ?reason,
                "correlation admission rejected"
            );
            return Err(CorrelationInsertError { reason, value });
        }
        self.pending.insert(id, Pending { deadline, value });
        debug!(
            correlation_id = id.get(),
            in_flight = self.pending.len(),
            "correlation admitted"
        );
        Ok(())
    }

    /// Removes and returns one response-correlated value.
    ///
    /// **Inputs:** Exclusive table borrow and response identifier.
    /// **Outputs:** Original value or `None` for an unknown/already-resolved ID.
    /// **Logic:** Remove atomically so a response resolves at most once.
    pub fn resolve(&mut self, id: CorrelationId) -> Option<T> {
        let pending = self.pending.remove(&id)?;
        debug!(
            correlation_id = id.get(),
            in_flight = self.pending.len(),
            "correlation resolved"
        );
        Some(pending.value)
    }

    /// Removes deadlines at or before an explicit instant.
    ///
    /// **Inputs:** Exclusive table borrow and caller-supplied monotonic cutoff.
    /// **Outputs:** Owned `(ID, value)` pairs sorted by ascending ID.
    /// **Logic:** Partition the ordered bounded map into retained and expired values;
    /// explicit time keeps simulation and higher-level retry policy deterministic.
    pub fn expire_at(&mut self, now: Instant) -> Vec<(CorrelationId, T)> {
        let mut expired = Vec::new();
        for (id, pending) in std::mem::take(&mut self.pending) {
            if pending.deadline <= now {
                debug!(correlation_id = id.get(), "correlation expired");
                expired.push((id, pending.value));
            } else {
                self.pending.insert(id, pending);
            }
        }
        expired
    }

    /// Returns the current pending request count.
    ///
    /// **Inputs:** Shared table borrow.
    /// **Outputs:** Count in `0..=limit`.
    /// **Logic:** Expose bounded occupancy for snapshots and telemetry.
    #[must_use]
    pub fn len(&self) -> usize {
        self.pending.len()
    }

    /// Reports whether no correlation state remains.
    ///
    /// **Inputs:** Shared table borrow.
    /// **Outputs:** `true` exactly at zero occupancy.
    /// **Logic:** Delegate to the ordered map without iteration.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
}
