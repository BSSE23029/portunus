//! Protocol-neutral scheduling and retry policy contracts.
//!
//! Scheduling consumes bounded job metadata and returns an index into the caller's
//! slice; retry consumes explicit failure context and returns a deterministic action.
//! Both traits are object-safe so composition roots can select policy at runtime.
//!
//! This module defines policy only. It does not own queues, tasks, clocks, sleeps,
//! protocol identifiers, payloads, I/O, or process-global observability.

mod retry;
mod schedule;

pub use retry::{
    ExponentialRetry, FailureClass, RetryContext, RetryDecision, RetryPolicy, RetryPolicyError,
};
pub use schedule::{JobCandidate, PriorityScheduler, SchedulingStrategy};
