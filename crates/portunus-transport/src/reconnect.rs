//! Bounded deterministic exponential reconnection policy.
//!
//! Attempt numbers are one-based: attempt one uses the initial delay, each later
//! admitted attempt doubles it, and the configured maximum is inclusive. Attempt
//! zero and numbers above the retry budget are rejected as `None`.
//!
//! This module computes policy only. It does not sleep, add random jitter, connect
//! sockets, classify failures, own cancellation, or spawn retry tasks.

use std::time::Duration;
use thiserror::Error;
use tracing::debug;

/// Validated bounded exponential reconnection policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReconnectPolicy {
    max_attempts: u32,
    initial_delay: Duration,
    maximum_delay: Duration,
}

impl ReconnectPolicy {
    /// Creates a finite non-spinning capped reconnection policy.
    ///
    /// **Inputs:** Positive attempt count, initial delay, and inclusive maximum delay.
    /// **Outputs:** Immutable policy or the first stable validation failure.
    /// **Logic:** Validate independent zero boundaries before comparing delay order.
    ///
    /// # Errors
    /// Returns a typed zero or contradictory-delay configuration error.
    pub fn new(
        max_attempts: u32,
        initial_delay: Duration,
        maximum_delay: Duration,
    ) -> Result<Self, ReconnectConfigError> {
        if max_attempts == 0 {
            return Err(ReconnectConfigError::ZeroAttempts);
        }
        if initial_delay.is_zero() {
            return Err(ReconnectConfigError::ZeroInitialDelay);
        }
        if maximum_delay.is_zero() {
            return Err(ReconnectConfigError::ZeroMaximumDelay);
        }
        if initial_delay > maximum_delay {
            return Err(ReconnectConfigError::InitialExceedsMaximum {
                initial: initial_delay,
                maximum: maximum_delay,
            });
        }
        Ok(Self {
            max_attempts,
            initial_delay,
            maximum_delay,
        })
    }

    /// Returns the inclusive number of reconnect attempts admitted.
    ///
    /// **Inputs:** Shared policy borrow.
    /// **Outputs:** Positive one-based attempt ceiling.
    /// **Logic:** Expose validated budget for orchestration snapshots.
    #[must_use]
    pub const fn max_attempts(&self) -> u32 {
        self.max_attempts
    }

    /// Computes one admitted attempt's deterministic capped delay.
    ///
    /// **Inputs:** One-based attempt number; zero is invalid.
    /// **Outputs:** Delay for `1..=max_attempts`, otherwise `None`.
    /// **Logic:** Compute the power of two with checked constant-work arithmetic,
    /// saturate duration multiplication, then apply the inclusive configured cap.
    #[must_use]
    pub fn delay_for(&self, attempt: u32) -> Option<Duration> {
        if attempt == 0 || attempt > self.max_attempts {
            return None;
        }
        let factor = 2_u32.checked_pow(attempt - 1).unwrap_or(u32::MAX);
        let delay = self
            .initial_delay
            .saturating_mul(factor)
            .min(self.maximum_delay);
        debug!(attempt, ?delay, "reconnection delay selected");
        Some(delay)
    }
}

/// Stable reconnection policy validation failures.
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
pub enum ReconnectConfigError {
    #[error("reconnection attempts must be greater than zero")]
    ZeroAttempts,
    #[error("initial reconnection delay must be greater than zero")]
    ZeroInitialDelay,
    #[error("maximum reconnection delay must be greater than zero")]
    ZeroMaximumDelay,
    #[error("initial delay {initial:?} exceeds maximum {maximum:?}")]
    InitialExceedsMaximum {
        initial: Duration,
        maximum: Duration,
    },
}
