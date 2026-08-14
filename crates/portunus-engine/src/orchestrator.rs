//! Bounded protocol-neutral orchestration composition.
//!
//! One owner composes a retained-job ceiling, scheduling strategy, retry policy,
//! multi-dimensional admission, structured task group, and revisioned state hub.
//! Jobs use caller-clocked ready boundaries. Admission failure preserves queue
//! ownership; retry factories create fresh futures and running work stays owned.
//!
//! `submit -> bounded queue -> scheduler -> budget -> owned task -> join`, with
//! failed transient work returning to the queue behind its retry-ready boundary.
//!
//! It does not sleep, read clocks, classify errors, perform I/O, persist jobs,
//! install logging, or contain torrent-specific scheduling.

use crate::{
    budget::{BudgetConfig, BudgetPool},
    policy::{JobCandidate, RetryContext, RetryDecision, RetryPolicy, SchedulingStrategy},
    runtime::{TaskGroup, TaskId, TaskTermination},
    telemetry::{EngineSnapshot, JobId, JobState, StateHub, StateHubConfig},
};
use std::{collections::BTreeMap, time::Duration};
use tokio::sync::oneshot;

pub mod model;
pub use model::{
    Dispatch, JobCompletion, JobFactory, JobFailure, JobFuture, JobSpec, OrchestratorConfig,
    OrchestratorError,
};

struct QueuedJob {
    spec: JobSpec,
    attempts: u32,
    ready_at: Duration,
    waiting: bool,
}

struct RunningJob {
    job_id: JobId,
    queued: QueuedJob,
    result: oneshot::Receiver<Result<(), JobFailure>>,
}

pub struct Orchestrator {
    scheduler: Box<dyn SchedulingStrategy>,
    retry: Box<dyn RetryPolicy>,
    tasks: TaskGroup,
    state: StateHub,
    queued: BTreeMap<JobId, QueuedJob>,
    running: BTreeMap<TaskId, RunningJob>,
    next_job_id: u64,
}

impl Orchestrator {
    /// Inputs: validated topology/budgets and object-safe scheduling/retry policies.
    /// Outputs: empty bounded orchestration owner with shared state publication.
    /// Logic: share one budget pool between task admission and observable snapshots.
    #[must_use]
    pub fn new(
        config: OrchestratorConfig,
        budget: BudgetConfig,
        scheduler: Box<dyn SchedulingStrategy>,
        retry: Box<dyn RetryPolicy>,
    ) -> Self {
        let pool = BudgetPool::new(budget);
        let hub_config = StateHubConfig {
            max_jobs: config.max_jobs,
            event_capacity: config.event_capacity,
        };
        Self {
            scheduler,
            retry,
            tasks: TaskGroup::new(pool.clone()),
            state: StateHub::new(hub_config, pool),
            queued: BTreeMap::new(),
            running: BTreeMap::new(),
            next_job_id: 0,
        }
    }

    /// Inputs: owned reusable job specification.
    /// Outputs: stable job ID or retained-capacity/identifier failure.
    /// Logic: reserve monotonically increasing identity, publish admission, then queue.
    /// # Errors
    /// Returns identifier exhaustion or state-hub capacity errors.
    pub fn try_submit(&mut self, spec: JobSpec) -> Result<JobId, OrchestratorError> {
        let value = self
            .next_job_id
            .checked_add(1)
            .ok_or(OrchestratorError::IdExhausted)?;
        let id = JobId::new(value);
        self.state.admit(id)?;
        self.next_job_id = value;
        self.queued.insert(
            id,
            QueuedJob {
                spec,
                attempts: 0,
                ready_at: Duration::ZERO,
                waiting: false,
            },
        );
        Ok(id)
    }

    /// Inputs: explicit monotonic time used to release due retries.
    /// Outputs: selected running task, absence, or policy/admission/state error.
    /// Logic: promote due retries, schedule bounded metadata, admit/spawn atomically,
    /// and restore queue ownership unchanged when resource admission rejects work.
    /// # Errors
    /// Returns invalid strategy selection, budget/task, or state-transition errors.
    pub fn try_dispatch_next(
        &mut self,
        now: Duration,
    ) -> Result<Option<Dispatch>, OrchestratorError> {
        self.promote_ready(now)?;
        let ids = self
            .queued
            .iter()
            .filter_map(|(id, job)| (!job.waiting).then_some(*id))
            .collect::<Vec<_>>();
        let candidates = ids
            .iter()
            .enumerate()
            .map(|(index, id)| {
                let job = &self.queued[id];
                JobCandidate::new(index as u64, job.spec.priority, job.spec.cost, job.attempts)
            })
            .collect::<Vec<_>>();
        let Some(index) = self.scheduler.select(&candidates) else {
            return Ok(None);
        };
        let id = *ids.get(index).ok_or(OrchestratorError::InvalidSelection {
            index,
            len: ids.len(),
        })?;
        let queued = self
            .queued
            .remove(&id)
            .ok_or(OrchestratorError::QueueInvariant(id))?;
        let attempt = queued.attempts.saturating_add(1);
        let factory = queued.spec.work.clone();
        let (result_tx, result) = oneshot::channel();
        let spawned = self
            .tasks
            .try_spawn(queued.spec.resources, move |cancellation| async move {
                let outcome = factory(attempt, cancellation).await;
                let code = outcome.as_ref().err().map_or(0, |failure| failure.code);
                let failed = outcome.is_err();
                let _ = result_tx.send(outcome);
                if failed {
                    Err(code)
                } else {
                    Ok(())
                }
            });
        let task_id = match spawned {
            Ok(task_id) => task_id,
            Err(error) => {
                self.queued.insert(id, queued);
                return Err(error.into());
            }
        };
        self.state.transition(id, JobState::Running)?;
        self.running.insert(
            task_id,
            RunningJob {
                job_id: id,
                queued,
                result,
            },
        );
        Ok(Some(Dispatch {
            job_id: id,
            task_id,
            attempt,
        }))
    }

    /// Inputs: owned running task ID and explicit monotonic completion time.
    /// Outputs: terminal/retry outcome after the task is joined exactly once.
    /// Logic: combine structured termination with application failure details, then
    /// publish terminal state or retain the same job behind a retry-ready boundary.
    /// # Errors
    /// Returns unknown task, closed result channel, timing overflow, or state errors.
    pub async fn join(
        &mut self,
        task_id: TaskId,
        now: Duration,
    ) -> Result<JobCompletion, OrchestratorError> {
        let mut running = self
            .running
            .remove(&task_id)
            .ok_or(crate::runtime::TaskError::UnknownTask(task_id))?;
        let outcome = self.tasks.join(task_id).await?;
        match outcome.termination {
            TaskTermination::Completed => {
                let result = running
                    .result
                    .await
                    .map_err(|_| OrchestratorError::ResultChannelClosed)?;
                if result.is_ok() {
                    self.state.transition(running.job_id, JobState::Completed)?;
                    return Ok(JobCompletion::Completed);
                }
                unreachable!("successful task termination reports successful work")
            }
            TaskTermination::Failed { .. } => {
                let failure = running
                    .result
                    .await
                    .map_err(|_| OrchestratorError::ResultChannelClosed)?
                    .expect_err("failed task termination reports failed work");
                match self
                    .retry
                    .decide(RetryContext::new(running.queued.attempts, failure.class))
                {
                    RetryDecision::RetryAfter(delay) => {
                        running.queued.attempts = running.queued.attempts.saturating_add(1);
                        running.queued.ready_at = now
                            .checked_add(delay)
                            .ok_or(OrchestratorError::ReadyTimeOverflow)?;
                        running.queued.waiting = true;
                        self.state
                            .transition(running.job_id, JobState::RetryWaiting)?;
                        self.queued.insert(running.job_id, running.queued);
                        Ok(JobCompletion::RetryScheduled { delay })
                    }
                    RetryDecision::Exhausted | RetryDecision::PermanentFailure => {
                        self.state.transition(running.job_id, JobState::Failed)?;
                        Ok(JobCompletion::Failed {
                            code: Some(failure.code),
                        })
                    }
                }
            }
            TaskTermination::Cancelled | TaskTermination::Aborted => {
                self.state.transition(running.job_id, JobState::Cancelled)?;
                Ok(JobCompletion::Cancelled)
            }
            TaskTermination::Panicked => {
                self.state.transition(running.job_id, JobState::Failed)?;
                Ok(JobCompletion::Failed { code: None })
            }
        }
    }

    /// Inputs: retained queued, retry-waiting, or running job ID.
    /// Outputs: cancellation request applied to queue state or owned child task.
    /// Logic: queued work is removed immediately; running work receives its
    /// cooperative token and remains owned until the caller joins it.
    /// # Errors
    /// Returns task/state lookup or invalid-terminal-transition errors.
    pub fn cancel(&mut self, job_id: JobId) -> Result<(), OrchestratorError> {
        if self.queued.remove(&job_id).is_some() {
            self.state.transition(job_id, JobState::Cancelled)?;
            return Ok(());
        }
        if let Some(task_id) = self
            .running
            .iter()
            .find_map(|(task_id, job)| (job.job_id == job_id).then_some(*task_id))
        {
            self.tasks.cancel(task_id)?;
            return Ok(());
        }
        self.state.transition(job_id, JobState::Cancelled)?;
        Ok(())
    }

    /// Inputs: retained job ID already in a terminal state.
    /// Outputs: released retained-job capacity and revisioned removal event.
    /// Logic: delegate validation/removal after queue and task ownership ends.
    /// # Errors
    /// Returns unknown, nonterminal, or revision-exhaustion state errors.
    pub fn remove_terminal(&self, job_id: JobId) -> Result<(), OrchestratorError> {
        self.state.remove_terminal(job_id)?;
        Ok(())
    }

    /// Inputs: shared orchestrator state.
    /// Outputs: owned latest consistent jobs/resource snapshot.
    /// Logic: delegate to the revisioned hub without exposing mutable queue state.
    #[must_use]
    pub fn snapshot(&self) -> EngineSnapshot {
        self.state.snapshot()
    }

    // Inputs: explicit monotonic now.
    // Outputs: all due retry jobs transitioned back to schedulable queued state.
    // Logic: scan bounded retained jobs in stable ID order and publish each promotion.
    fn promote_ready(&mut self, now: Duration) -> Result<(), OrchestratorError> {
        for (id, job) in &mut self.queued {
            if job.waiting && job.ready_at <= now {
                self.state.transition(*id, JobState::Queued)?;
                job.waiting = false;
            }
        }
        Ok(())
    }
}
