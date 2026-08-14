//! Structured ownership for bounded cooperative asynchronous tasks.
//!
//! One [`TaskGroup`] owns every child cancellation token, budget permit, and join
//! handle. IDs increase monotonically and tasks remain registered until joined.
//! Cooperative shutdown cancels then joins in ID order; dropping a live group first
//! cancels and then aborts every remaining child so no task silently detaches.
//!
//! ```text
//! TaskGroup ──owns──> child token + budget permit + JoinHandle
//!    ├─ cancel(id) ──> cooperative signal
//!    ├─ join(id) ────> terminal outcome + resource release
//!    └─ shutdown ────> cancel all + ordered join all
//! ```
//!
//! This module does not decide work priority, retry failures, perform I/O, or install
//! process-global telemetry. Task application failures are stable caller-defined codes.

use crate::budget::{BudgetError, BudgetPool, ResourceRequest};
use std::{collections::BTreeMap, future::Future};
use thiserror::Error;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct TaskId(u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskTermination {
    Completed,
    Failed { code: u32 },
    Cancelled,
    Aborted,
    Panicked,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TaskOutcome {
    pub id: TaskId,
    pub termination: TaskTermination,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum TaskError {
    #[error(transparent)]
    Budget(#[from] BudgetError),
    #[error("unknown task {0:?}")]
    UnknownTask(TaskId),
    #[error("task identifier space is exhausted")]
    IdExhausted,
}

#[derive(Debug)]
struct TaskEntry {
    cancellation: CancellationToken,
    handle: JoinHandle<Result<(), u32>>,
}

#[derive(Debug)]
pub struct TaskGroup {
    budget: BudgetPool,
    root: CancellationToken,
    tasks: BTreeMap<TaskId, TaskEntry>,
    next_id: u64,
}

impl TaskGroup {
    /// Inputs: shared multi-dimensional budget pool.
    /// Outputs: empty task owner with independent root cancellation scope.
    /// Logic: retain admission policy and initialize deterministic ID ownership.
    #[must_use]
    pub fn new(budget: BudgetPool) -> Self {
        Self {
            budget,
            root: CancellationToken::new(),
            tasks: BTreeMap::new(),
            next_id: 0,
        }
    }

    /// Inputs: resource request and one-use factory receiving a child token.
    /// Outputs: immediate task ID or budget/identifier admission error.
    /// Logic: reserve an ID and resources before invoking/spawning work; the task
    /// future owns its permit until every normal or unwind termination path ends.
    /// # Errors
    /// Returns identifier exhaustion or nonblocking budget rejection.
    pub fn try_spawn<F, Fut>(
        &mut self,
        request: ResourceRequest,
        work: F,
    ) -> Result<TaskId, TaskError>
    where
        F: FnOnce(CancellationToken) -> Fut,
        Fut: Future<Output = Result<(), u32>> + Send + 'static,
    {
        let id = self.reserve_id()?;
        self.spawn_admitted(id, self.budget.try_admit(request)?, work);
        Ok(id)
    }

    /// Inputs: resource request and one-use factory receiving a child token.
    /// Outputs: eventually admitted task ID or cancellation/budget/ID error.
    /// Logic: reserve ID, wait against group cancellation for all resource permits,
    /// then spawn with the same structured ownership used by nonblocking admission.
    /// # Errors
    /// Returns identifier exhaustion, cancellation, or closed budget errors.
    pub async fn spawn<F, Fut>(
        &mut self,
        request: ResourceRequest,
        work: F,
    ) -> Result<TaskId, TaskError>
    where
        F: FnOnce(CancellationToken) -> Fut,
        Fut: Future<Output = Result<(), u32>> + Send + 'static,
    {
        let id = self.reserve_id()?;
        self.spawn_admitted(id, self.budget.admit(request, &self.root).await?, work);
        Ok(id)
    }

    /// Inputs: registered task ID.
    /// Outputs: unit after delivering its cooperative cancellation signal.
    /// Logic: look up owned metadata without removing join ownership.
    /// # Errors
    /// Returns `UnknownTask` when the ID is not currently owned.
    pub fn cancel(&self, id: TaskId) -> Result<(), TaskError> {
        let task = self.tasks.get(&id).ok_or(TaskError::UnknownTask(id))?;
        task.cancellation.cancel();
        Ok(())
    }

    /// Inputs: registered task ID.
    /// Outputs: terminal outcome after removing and joining exactly that task.
    /// Logic: preserve whether cancellation was requested, await the owned handle,
    /// classify application failure separately from abort and panic, then release it.
    /// # Errors
    /// Returns `UnknownTask` when the ID is not currently owned.
    pub async fn join(&mut self, id: TaskId) -> Result<TaskOutcome, TaskError> {
        let task = self.tasks.remove(&id).ok_or(TaskError::UnknownTask(id))?;
        let cancelled = task.cancellation.is_cancelled();
        let termination = match task.handle.await {
            Ok(_) if cancelled => TaskTermination::Cancelled,
            Ok(Ok(())) => TaskTermination::Completed,
            Ok(Err(code)) => TaskTermination::Failed { code },
            Err(error) if error.is_cancelled() => TaskTermination::Aborted,
            Err(_) => TaskTermination::Panicked,
        };
        Ok(TaskOutcome { id, termination })
    }

    /// Inputs: owned group with any number of cooperative children.
    /// Outputs: ID-ordered terminal outcomes after all children finish.
    /// Logic: cancel the root, snapshot ordered IDs, and join each owned child.
    pub async fn shutdown(mut self) -> Vec<TaskOutcome> {
        self.root.cancel();
        let ids = self.tasks.keys().copied().collect::<Vec<_>>();
        let mut outcomes = Vec::with_capacity(ids.len());
        for id in ids {
            if let Ok(outcome) = self.join(id).await {
                outcomes.push(outcome);
            }
        }
        outcomes
    }

    // Inputs: mutable group sequence state.
    // Outputs: next monotonically increasing public ID or exhaustion error.
    // Logic: checked increment keeps wraparound from reusing an owned identifier.
    fn reserve_id(&mut self) -> Result<TaskId, TaskError> {
        self.next_id = self.next_id.checked_add(1).ok_or(TaskError::IdExhausted)?;
        Ok(TaskId(self.next_id))
    }

    // Inputs: reserved ID, owned permit, and one-use task factory.
    // Outputs: registered child task; no fallible operation remains.
    // Logic: derive child token, invoke factory, move permit into spawned future,
    // and insert all cancellation/join ownership under the reserved ID.
    fn spawn_admitted<F, Fut>(&mut self, id: TaskId, permit: crate::budget::BudgetPermit, work: F)
    where
        F: FnOnce(CancellationToken) -> Fut,
        Fut: Future<Output = Result<(), u32>> + Send + 'static,
    {
        let cancellation = self.root.child_token();
        let future = work(cancellation.clone());
        let handle = tokio::spawn(async move {
            let _permit = permit;
            future.await
        });
        self.tasks.insert(
            id,
            TaskEntry {
                cancellation,
                handle,
            },
        );
    }
}

impl Drop for TaskGroup {
    // Inputs: group being dropped with possibly live child tasks.
    // Outputs: cooperative cancellation followed by immediate abort of every handle.
    // Logic: guarantee loss of the owner cannot detach background work indefinitely.
    fn drop(&mut self) {
        self.root.cancel();
        for task in self.tasks.values() {
            task.handle.abort();
        }
    }
}
