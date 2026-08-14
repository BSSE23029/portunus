//! Deterministic retry and terminal-failure decisions.
//!
//! `attempt` counts completed failures before a proposed next try. A transient
//! failure retries while `attempt < max_attempts`; the inclusive delay cap bounds
//! exponential growth. Permanent failures bypass retry, and hostile large attempt
//! counters remain constant-work and overflow-safe.
//!
//! This module chooses an action only. It does not sleep, own clocks, spawn tasks,
//! classify protocol errors, add randomness, or emit global diagnostics.

use std::time::Duration;
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FailureClass {
    Transient,
    Permanent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetryContext {
    pub attempt: u32,
    pub failure: FailureClass,
}

impl RetryContext {
    /// Inputs: completed failure count and caller-classified failure stability.
    /// Outputs: immutable explicit retry decision context.
    /// Logic: keep policy deterministic and independent from ambient time/errors.
    #[must_use]
    pub const fn new(attempt: u32, failure: FailureClass) -> Self {
        Self { attempt, failure }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryDecision {
    RetryAfter(Duration),
    Exhausted,
    PermanentFailure,
}

pub trait RetryPolicy: Send + Sync {
    /// Inputs: explicit completed-attempt count and classified failure.
    /// Outputs: retry delay or stable terminal reason.
    /// Logic: implementations are pure and do not sleep or mutate task state.
    fn decide(&self, context: RetryContext) -> RetryDecision;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExponentialRetry {
    max_attempts: u32,
    base_delay: Duration,
    max_delay: Duration,
}

impl ExponentialRetry {
    /// Inputs: retry-count ceiling, nonzero base delay, and nonzero inclusive cap.
    /// Outputs: validated deterministic policy or independent zero-bound error.
    /// Logic: validate before retaining timing policy; cap may be below base.
    /// # Errors
    /// Returns a distinct error for each unusable zero configuration field.
    pub const fn new(
        max_attempts: u32,
        base_delay: Duration,
        max_delay: Duration,
    ) -> Result<Self, RetryPolicyError> {
        if max_attempts == 0 {
            return Err(RetryPolicyError::ZeroAttempts);
        }
        if base_delay.is_zero() {
            return Err(RetryPolicyError::ZeroBaseDelay);
        }
        if max_delay.is_zero() {
            return Err(RetryPolicyError::ZeroMaximumDelay);
        }
        Ok(Self {
            max_attempts,
            base_delay,
            max_delay,
        })
    }
}

impl RetryPolicy for ExponentialRetry {
    /// Inputs: completed attempt count and transient/permanent classification.
    /// Outputs: capped exponential delay or stable terminal outcome.
    /// Logic: terminal checks precede one checked shift and duration multiply.
    fn decide(&self, context: RetryContext) -> RetryDecision {
        if context.failure == FailureClass::Permanent {
            return RetryDecision::PermanentFailure;
        }
        if context.attempt >= self.max_attempts {
            return RetryDecision::Exhausted;
        }
        let multiplier = 1_u32.checked_shl(context.attempt).unwrap_or(u32::MAX);
        let delay = self
            .base_delay
            .checked_mul(multiplier)
            .unwrap_or(self.max_delay)
            .min(self.max_delay);
        RetryDecision::RetryAfter(delay)
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum RetryPolicyError {
    #[error("maximum retry attempts must be greater than zero")]
    ZeroAttempts,
    #[error("retry base delay must be greater than zero")]
    ZeroBaseDelay,
    #[error("retry maximum delay must be greater than zero")]
    ZeroMaximumDelay,
}
