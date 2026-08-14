//! Protocol-neutral framed-session configuration and lifecycle policy.
//!
//! Queue capacities and correlated in-flight work are independent inclusive
//! budgets. The lifecycle machine accepts only explicit transitions, preserves
//! its state on rejection, and emits bounded structured diagnostics.
//!
//! ```text
//! Connecting ──connected──> Active ──drain──> Draining ──closed──> Closed
//!      └──────────────── transport closed ────────────────────────>┘
//! ```
//!
//! This module defines policy and state only. It does not perform socket I/O,
//! encode protocol messages, spawn tasks, reconnect, or install tracing output.

use thiserror::Error;
use tracing::{debug, warn};

/// Independent inclusive resource ceilings for one framed session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionConfig {
    inbound_capacity: usize,
    outbound_capacity: usize,
    max_in_flight: usize,
}

impl SessionConfig {
    /// Creates validated resource budgets for one connection session.
    ///
    /// **Inputs:** Positive inbound queue, outbound queue, and in-flight ceilings.
    ///
    /// **Outputs:** Immutable configuration or the first resource-specific error.
    ///
    /// **Logic:** Validate independent budgets in pipeline order before allocation.
    ///
    /// # Errors
    ///
    /// Returns [`SessionConfigError::ZeroCapacity`] naming the first zero budget.
    pub const fn new(
        inbound_capacity: usize,
        outbound_capacity: usize,
        max_in_flight: usize,
    ) -> Result<Self, SessionConfigError> {
        if inbound_capacity == 0 {
            return Err(SessionConfigError::ZeroCapacity {
                resource: "inbound",
            });
        }
        if outbound_capacity == 0 {
            return Err(SessionConfigError::ZeroCapacity {
                resource: "outbound",
            });
        }
        if max_in_flight == 0 {
            return Err(SessionConfigError::ZeroCapacity {
                resource: "in_flight",
            });
        }
        Ok(Self {
            inbound_capacity,
            outbound_capacity,
            max_in_flight,
        })
    }

    /// Returns the maximum decoded frames waiting for consumers.
    ///
    /// **Inputs:** Shared configuration borrow.
    ///
    /// **Outputs:** Positive inclusive inbound queue capacity in frame count.
    ///
    /// **Logic:** Expose the validated budget without permitting mutation.
    #[must_use]
    pub const fn inbound_capacity(&self) -> usize {
        self.inbound_capacity
    }

    /// Returns the maximum outbound frames waiting for transport I/O.
    ///
    /// **Inputs:** Shared configuration borrow.
    ///
    /// **Outputs:** Positive inclusive outbound queue capacity in frame count.
    ///
    /// **Logic:** Expose the validated budget without permitting mutation.
    #[must_use]
    pub const fn outbound_capacity(&self) -> usize {
        self.outbound_capacity
    }

    /// Returns the maximum requests awaiting correlated responses.
    ///
    /// **Inputs:** Shared configuration borrow.
    ///
    /// **Outputs:** Positive inclusive in-flight request count.
    ///
    /// **Logic:** Keep correlation admission independent from queue capacities.
    #[must_use]
    pub const fn max_in_flight(&self) -> usize {
        self.max_in_flight
    }
}

/// Stable session configuration validation failures.
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
pub enum SessionConfigError {
    #[error("session {resource} capacity must be greater than zero")]
    ZeroCapacity { resource: &'static str },
}

/// Externally observable connection lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionState {
    Connecting,
    Active,
    Draining,
    Closed,
}

/// Events accepted by the protocol-neutral lifecycle machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleEvent {
    Connected,
    DrainRequested,
    TransportClosed,
}

/// A rejected lifecycle transition with stable typed context.
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
#[error("cannot apply {event:?} while session is {state:?}")]
pub struct TransitionError {
    pub state: SessionState,
    pub event: LifecycleEvent,
}

/// Deterministic lifecycle state owned by one connection task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionMachine {
    state: SessionState,
}

impl SessionMachine {
    /// Creates a session awaiting transport establishment.
    ///
    /// **Inputs:** No external state or allocation.
    ///
    /// **Outputs:** A machine in [`SessionState::Connecting`].
    ///
    /// **Logic:** Every connection begins before protocol traffic is admitted.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            state: SessionState::Connecting,
        }
    }

    /// Returns the current externally observable lifecycle state.
    ///
    /// **Inputs:** Shared machine borrow.
    ///
    /// **Outputs:** Copy of the current state.
    ///
    /// **Logic:** Permit snapshots without exposing mutable state storage.
    #[must_use]
    pub const fn state(&self) -> SessionState {
        self.state
    }

    /// Applies one lifecycle event when valid for the current state.
    ///
    /// **Inputs:** Exclusive machine borrow and one explicit event.
    ///
    /// **Outputs:** New state on success; typed source/event error on rejection.
    ///
    /// **Logic:** Compute before mutation, log bounded enum fields, and preserve
    /// state when the pair is absent from the transition table.
    ///
    /// # Errors
    ///
    /// Returns [`TransitionError`] for every undefined state/event pair.
    pub fn apply(&mut self, event: LifecycleEvent) -> Result<SessionState, TransitionError> {
        let next = match (self.state, event) {
            (SessionState::Connecting, LifecycleEvent::Connected) => SessionState::Active,
            (
                SessionState::Connecting | SessionState::Active | SessionState::Draining,
                LifecycleEvent::TransportClosed,
            ) => SessionState::Closed,
            (SessionState::Active, LifecycleEvent::DrainRequested) => SessionState::Draining,
            _ => {
                warn!(state = ?self.state, event = ?event, "rejected session transition");
                return Err(TransitionError {
                    state: self.state,
                    event,
                });
            }
        };
        debug!(from = ?self.state, to = ?next, event = ?event, "session state changed");
        self.state = next;
        Ok(next)
    }
}

impl Default for SessionMachine {
    /// Creates the default pre-connection lifecycle state.
    ///
    /// **Inputs:** No parameters or environmental state.
    ///
    /// **Outputs:** Equivalent to [`SessionMachine::new`].
    ///
    /// **Logic:** Keep derived/default construction aligned with lifecycle policy.
    fn default() -> Self {
        Self::new()
    }
}
