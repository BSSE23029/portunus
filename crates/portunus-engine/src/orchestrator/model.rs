//! Stable job, completion, configuration, and orchestration error values.
//!
//! Job work is a reusable thread-safe factory so transient failures can create a
//! fresh future without retaining a prior future. Ready times are monotonic
//! `Duration` values supplied by callers; no wall clock is read here.
//!
//! This module owns values only. It does not queue, schedule, spawn, sleep, admit
//! resources, publish state, or classify application-specific failures.

use crate::{
    budget::{BudgetError, ResourceRequest},
    policy::FailureClass,
    runtime::{TaskError, TaskId},
    telemetry::{JobId, StateError},
};
use std::{future::Future, pin::Pin, sync::Arc, time::Duration};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

pub type JobFuture = Pin<Box<dyn Future<Output = Result<(), JobFailure>> + Send + 'static>>;
pub type JobFactory = Arc<dyn Fn(u32, CancellationToken) -> JobFuture + Send + Sync + 'static>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JobFailure {
    pub code: u32,
    pub class: FailureClass,
}

impl JobFailure {
    /// Inputs: stable application error code and caller-defined failure class.
    /// Outputs: retry-policy-neutral failure description.
    /// Logic: retain classification beside the code without exposing error payloads.
    #[must_use]
    pub const fn new(code: u32, class: FailureClass) -> Self {
        Self { code, class }
    }
}

#[derive(Clone)]
pub struct JobSpec {
    pub(super) priority: i64,
    pub(super) cost: u32,
    pub(super) resources: ResourceRequest,
    pub(super) work: JobFactory,
}

impl JobSpec {
    /// Inputs: scheduling priority/cost, complete resource request, and retryable factory.
    /// Outputs: owned protocol-neutral job specification.
    /// Logic: keep payload execution opaque while exposing bounded policy metadata.
    #[must_use]
    pub fn new(priority: i64, cost: u32, resources: ResourceRequest, work: JobFactory) -> Self {
        Self {
            priority,
            cost,
            resources,
            work,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Dispatch {
    pub job_id: JobId,
    pub task_id: TaskId,
    pub attempt: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobCompletion {
    Completed,
    RetryScheduled { delay: Duration },
    Failed { code: Option<u32> },
    Cancelled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OrchestratorConfig {
    pub(super) max_jobs: usize,
    pub(super) event_capacity: usize,
}

impl OrchestratorConfig {
    /// Inputs: nonzero retained-job and bounded event capacities.
    /// Outputs: validated orchestration topology or independent stable error.
    /// Logic: reject zero topology before allocating queues or channels.
    /// # Errors
    /// Returns a distinct error for each zero capacity.
    pub const fn new(max_jobs: usize, event_capacity: usize) -> Result<Self, OrchestratorError> {
        if max_jobs == 0 {
            return Err(OrchestratorError::ZeroJobLimit);
        }
        if event_capacity == 0 {
            return Err(OrchestratorError::ZeroEventCapacity);
        }
        Ok(Self {
            max_jobs,
            event_capacity,
        })
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum OrchestratorError {
    #[error("retained job limit must be greater than zero")]
    ZeroJobLimit,
    #[error("event capacity must be greater than zero")]
    ZeroEventCapacity,
    #[error("job identifier space is exhausted")]
    IdExhausted,
    #[error("scheduler returned invalid candidate index {index} for length {len}")]
    InvalidSelection { index: usize, len: usize },
    #[error("selected job {0:?} disappeared from the owned queue")]
    QueueInvariant(JobId),
    #[error("retry ready time overflowed monotonic duration space")]
    ReadyTimeOverflow,
    #[error("task result channel closed before reporting completion")]
    ResultChannelClosed,
    #[error(transparent)]
    Budget(#[from] BudgetError),
    #[error(transparent)]
    Task(#[from] TaskError),
    #[error(transparent)]
    State(#[from] StateError),
}
