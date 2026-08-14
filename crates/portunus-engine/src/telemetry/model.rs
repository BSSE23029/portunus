//! Stable orchestration state, event, snapshot, and configuration values.
//!
//! Jobs follow explicit queued/running/retry/terminal transitions. Snapshots and
//! events carry monotonic revisions assigned by the owning hub. Capacity values are
//! nonzero and inclusive; IDs are ordered opaque `u64` values.
//!
//! This module contains values and pure transition policy only. It does not allocate
//! channels, lock state, publish events, admit resources, or execute jobs.

use crate::budget::BudgetSnapshot;
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct JobId(u64);

impl JobId {
    /// Inputs: caller-owned stable unsigned identifier.
    /// Outputs: typed job identity suitable for ordering and correlation.
    /// Logic: prevent accidental mixing with unrelated numeric identifiers.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobState {
    Queued,
    Running,
    RetryWaiting,
    Completed,
    Failed,
    Cancelled,
}

impl JobState {
    // Inputs: one immutable job state.
    // Outputs: whether no further execution transition is allowed.
    // Logic: centralize terminal membership for transition/removal policy.
    pub(super) const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JobSnapshot {
    pub id: JobId,
    pub state: JobState,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EngineSnapshot {
    pub revision: u64,
    pub jobs: Vec<JobSnapshot>,
    pub budget: BudgetSnapshot,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EngineEventKind {
    Admitted,
    Transitioned { from: JobState, to: JobState },
    Removed { terminal: JobState },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EngineEvent {
    pub revision: u64,
    pub job_id: JobId,
    pub kind: EngineEventKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StateHubConfig {
    pub(super) max_jobs: usize,
    pub(super) event_capacity: usize,
}

impl StateHubConfig {
    /// Inputs: nonzero retained-job ceiling and bounded event channel capacity.
    /// Outputs: validated hub configuration or independent zero-bound error.
    /// Logic: reject disabled/unbounded topology before channel allocation.
    /// # Errors
    /// Returns distinct job-limit or event-capacity errors.
    pub const fn new(max_jobs: usize, event_capacity: usize) -> Result<Self, StateError> {
        if max_jobs == 0 {
            return Err(StateError::ZeroJobLimit);
        }
        if event_capacity == 0 {
            return Err(StateError::ZeroEventCapacity);
        }
        Ok(Self {
            max_jobs,
            event_capacity,
        })
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum StateError {
    #[error("retained job limit must be greater than zero")]
    ZeroJobLimit,
    #[error("event capacity must be greater than zero")]
    ZeroEventCapacity,
    #[error("duplicate job {0:?}")]
    DuplicateJob(JobId),
    #[error("retained job limit {limit} is exhausted")]
    JobLimitExceeded { limit: usize },
    #[error("unknown job {0:?}")]
    UnknownJob(JobId),
    #[error("invalid job {id:?} transition from {from:?} to {to:?}")]
    InvalidTransition {
        id: JobId,
        from: JobState,
        to: JobState,
    },
    #[error("job {id:?} in state {state:?} is not terminal")]
    NotTerminal { id: JobId, state: JobState },
    #[error("state revision space is exhausted")]
    RevisionExhausted,
}

// Inputs: existing and proposed job states.
// Outputs: whether the directed lifecycle edge is allowed.
// Logic: enumerate structured queue/run/retry/terminal transitions explicitly.
pub(super) const fn valid_transition(from: JobState, to: JobState) -> bool {
    matches!(
        (from, to),
        (JobState::Queued, JobState::Running | JobState::Cancelled)
            | (
                JobState::Running,
                JobState::Completed
                    | JobState::Failed
                    | JobState::RetryWaiting
                    | JobState::Cancelled
            )
            | (
                JobState::RetryWaiting,
                JobState::Queued | JobState::Cancelled
            )
    )
}
