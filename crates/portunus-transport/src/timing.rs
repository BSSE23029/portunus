//! Deterministic connection deadline, heartbeat, and idle-eviction policy.
//!
//! The caller supplies every monotonic instant. Heartbeat and idle durations are
//! positive inclusive thresholds, idle cannot precede heartbeat opportunity, and
//! terminal connection deadlines take precedence over all liveness work.
//!
//! ```text
//! deadline elapsed? ─yes─> terminate
//!        │ no
//! idle elapsed? ─────yes─> evict
//!        │ no
//! heartbeat elapsed? yes─> emit heartbeat
//! ```
//!
//! This module does not sleep, read a clock, create heartbeat messages, perform
//! I/O, reconnect, or decide retryability after a terminal action.

use std::time::{Duration, Instant};
use thiserror::Error;
use tracing::{debug, trace};

/// Validated heartbeat and idle thresholds for one connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimingConfig {
    heartbeat: Duration,
    idle: Duration,
}

impl TimingConfig {
    /// Creates positive liveness thresholds with heartbeat no later than idle.
    ///
    /// **Inputs:** Heartbeat interval and inbound-idle eviction duration.
    /// **Outputs:** Immutable config or stable boundary error.
    /// **Logic:** Validate independently, then reject unreachable heartbeat policy.
    ///
    /// # Errors
    /// Returns zero-duration or idle-before-heartbeat context.
    pub fn new(heartbeat: Duration, idle: Duration) -> Result<Self, TimingConfigError> {
        if heartbeat.is_zero() {
            return Err(TimingConfigError::ZeroHeartbeat);
        }
        if idle.is_zero() {
            return Err(TimingConfigError::ZeroIdle);
        }
        if idle < heartbeat {
            return Err(TimingConfigError::IdleBeforeHeartbeat { heartbeat, idle });
        }
        Ok(Self { heartbeat, idle })
    }

    /// Returns the inclusive outbound-inactivity heartbeat threshold.
    ///
    /// **Inputs:** Shared config borrow.
    /// **Outputs:** Positive duration.
    /// **Logic:** Expose validated policy without mutation.
    #[must_use]
    pub const fn heartbeat(&self) -> Duration {
        self.heartbeat
    }

    /// Returns the inclusive inbound-inactivity eviction threshold.
    ///
    /// **Inputs:** Shared config borrow.
    /// **Outputs:** Positive duration at least the heartbeat interval.
    /// **Logic:** Expose validated policy without mutation.
    #[must_use]
    pub const fn idle(&self) -> Duration {
        self.idle
    }
}

/// Stable validation failures for connection timing policy.
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
pub enum TimingConfigError {
    #[error("heartbeat interval must be greater than zero")]
    ZeroHeartbeat,
    #[error("idle timeout must be greater than zero")]
    ZeroIdle,
    #[error("idle timeout {idle:?} is shorter than heartbeat interval {heartbeat:?}")]
    IdleBeforeHeartbeat { heartbeat: Duration, idle: Duration },
    #[error("connection deadline {deadline:?} is not after start {started_at:?}")]
    DeadlineElapsed {
        started_at: Instant,
        deadline: Instant,
    },
}

/// Mutually exclusive policy result at one explicit observation instant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimingAction {
    Wait,
    HeartbeatDue,
    IdleEviction,
    DeadlineElapsed,
}

/// Per-connection activity state evaluated against validated timing policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectionTimer {
    config: TimingConfig,
    deadline: Instant,
    last_inbound: Instant,
    last_outbound: Instant,
}

impl ConnectionTimer {
    /// Starts deterministic timing state for an established connection.
    ///
    /// **Inputs:** Config, explicit start instant, and strictly future deadline.
    /// **Outputs:** Timer initialized with inbound/outbound activity at start.
    /// **Logic:** Validate the absolute execution window before accepting activity.
    ///
    /// # Errors
    /// Returns [`TimingConfigError::DeadlineElapsed`] unless deadline is after start.
    pub fn new(
        config: TimingConfig,
        started_at: Instant,
        deadline: Instant,
    ) -> Result<Self, TimingConfigError> {
        if deadline <= started_at {
            return Err(TimingConfigError::DeadlineElapsed {
                started_at,
                deadline,
            });
        }
        Ok(Self {
            config,
            deadline,
            last_inbound: started_at,
            last_outbound: started_at,
        })
    }

    /// Records successful inbound frame activity without permitting clock rollback.
    ///
    /// **Inputs:** Exclusive timer borrow and explicit observed instant.
    /// **Outputs:** Updates last-inbound only when `at` is later.
    /// **Logic:** Maximum preserves monotonic state under reordered instrumentation.
    pub fn record_inbound(&mut self, at: Instant) {
        self.last_inbound = self.last_inbound.max(at);
        trace!(?at, "recorded inbound connection activity");
    }

    /// Records successful outbound frame activity without permitting clock rollback.
    ///
    /// **Inputs:** Exclusive timer borrow and explicit observed instant.
    /// **Outputs:** Updates last-outbound only when `at` is later.
    /// **Logic:** Maximum preserves monotonic state under reordered instrumentation.
    pub fn record_outbound(&mut self, at: Instant) {
        self.last_outbound = self.last_outbound.max(at);
        trace!(?at, "recorded outbound connection activity");
    }

    /// Evaluates one instant using terminal-first deterministic precedence.
    ///
    /// **Inputs:** Shared timer and caller-supplied monotonic observation instant.
    /// **Outputs:** Exactly one wait, heartbeat, idle, or deadline action.
    /// **Logic:** Compare elapsed durations without adding instants, avoiding overflow;
    /// deadline wins over idle, which wins over heartbeat at coincident boundaries.
    #[must_use]
    pub fn evaluate(&self, now: Instant) -> TimingAction {
        let action = if now >= self.deadline {
            TimingAction::DeadlineElapsed
        } else if now.saturating_duration_since(self.last_inbound) >= self.config.idle {
            TimingAction::IdleEviction
        } else if now.saturating_duration_since(self.last_outbound) >= self.config.heartbeat {
            TimingAction::HeartbeatDue
        } else {
            TimingAction::Wait
        };
        if action != TimingAction::Wait {
            debug!(?action, "connection timing action due");
        }
        action
    }
}
