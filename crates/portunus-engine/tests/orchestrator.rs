//! Integration coverage for bounded protocol-neutral orchestration composition.

use portunus_engine::{
    budget::{BudgetConfig, ResourceRequest},
    orchestrator::{JobCompletion, JobFailure, JobSpec, Orchestrator, OrchestratorConfig},
    policy::{ExponentialRetry, FailureClass, PriorityScheduler},
    telemetry::JobState,
};
use std::{sync::Arc, time::Duration};

#[path = "orchestrator/model.rs"]
mod model;

// Inputs: two jobs, one worker, deterministic priorities, and successful factories.
// Outputs: higher-priority dispatch first, bounded overload, and completed snapshots.
// Logic: prove queue, scheduler, budget, task owner, and state hub compose end to end.
#[tokio::test]
async fn schedules_and_completes_bounded_jobs() {
    let mut engine = test_engine(2);
    let low = engine.try_submit(spec(1, succeed())).unwrap();
    let high = engine.try_submit(spec(9, succeed())).unwrap();
    assert!(engine.try_submit(spec(5, succeed())).is_err());

    let dispatched = engine.try_dispatch_next(Duration::ZERO).unwrap().unwrap();
    assert_eq!(dispatched.job_id, high);
    assert!(engine.try_dispatch_next(Duration::ZERO).is_err());
    assert_eq!(
        engine
            .join(dispatched.task_id, Duration::ZERO)
            .await
            .unwrap(),
        JobCompletion::Completed
    );

    let second = engine.try_dispatch_next(Duration::ZERO).unwrap().unwrap();
    assert_eq!(second.job_id, low);
    assert_eq!(
        engine.join(second.task_id, Duration::ZERO).await.unwrap(),
        JobCompletion::Completed
    );
    assert!(engine
        .snapshot()
        .jobs
        .iter()
        .all(|job| job.state == JobState::Completed));
}

// Inputs: transient first failure, explicit monotonic time, and retry policy.
// Outputs: delayed retry is ineligible before its boundary and succeeds at it.
// Logic: prove retry timing is caller-driven and deterministic without wall-clock sleeps.
#[tokio::test]
async fn retries_only_at_the_explicit_ready_boundary() {
    let attempts = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let observed = Arc::clone(&attempts);
    let work = Arc::new(move |_attempt, _cancellation| {
        let attempts = Arc::clone(&observed);
        Box::pin(async move {
            if attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                Err(JobFailure::new(7, FailureClass::Transient))
            } else {
                Ok(())
            }
        }) as _
    });
    let mut engine = test_engine(1);
    let job = engine.try_submit(spec(1, work)).unwrap();
    let first = engine.try_dispatch_next(Duration::ZERO).unwrap().unwrap();
    assert_eq!(
        engine.join(first.task_id, Duration::ZERO).await.unwrap(),
        JobCompletion::RetryScheduled {
            delay: Duration::from_millis(5)
        }
    );
    assert!(engine
        .try_dispatch_next(Duration::from_millis(4))
        .unwrap()
        .is_none());
    let retry = engine
        .try_dispatch_next(Duration::from_millis(5))
        .unwrap()
        .unwrap();
    assert_eq!(retry.job_id, job);
    assert_eq!(
        engine
            .join(retry.task_id, Duration::from_millis(5))
            .await
            .unwrap(),
        JobCompletion::Completed
    );
}

// Inputs: one running cancellation-aware job and its retained terminal record.
// Outputs: cooperative cancellation, cancelled snapshot, then released job capacity.
// Logic: prove shutdown signals flow through task ownership and cleanup is explicit.
#[tokio::test]
async fn cancels_running_work_and_releases_terminal_capacity() {
    let work = Arc::new(|_, cancellation: tokio_util::sync::CancellationToken| {
        Box::pin(async move {
            cancellation.cancelled().await;
            Ok(())
        }) as _
    });
    let mut engine = test_engine(1);
    let job = engine.try_submit(spec(1, work)).unwrap();
    let dispatched = engine.try_dispatch_next(Duration::ZERO).unwrap().unwrap();
    engine.cancel(job).unwrap();
    assert_eq!(
        engine
            .join(dispatched.task_id, Duration::ZERO)
            .await
            .unwrap(),
        JobCompletion::Cancelled
    );
    assert_eq!(engine.snapshot().jobs[0].state, JobState::Cancelled);
    engine.remove_terminal(job).unwrap();
    assert!(engine.snapshot().jobs.is_empty());
    assert!(engine.try_submit(spec(1, succeed())).is_ok());
}

// Inputs: priority and a reusable asynchronous work factory.
// Outputs: generic job specification with one unit in every byte stage.
// Logic: keep tests focused on orchestration rather than fixture construction.
fn spec(priority: i64, work: portunus_engine::orchestrator::JobFactory) -> JobSpec {
    JobSpec::new(priority, 1, ResourceRequest::new(1, 1, 1), work)
}

// Inputs: no external state.
// Outputs: reusable factory that immediately succeeds.
// Logic: provide deterministic work without clocks, I/O, or public services.
fn succeed() -> portunus_engine::orchestrator::JobFactory {
    Arc::new(|_, _| Box::pin(async { Ok(()) }))
}

// Inputs: retained-job ceiling.
// Outputs: orchestrator with one worker and deterministic policies.
// Logic: share exact bounded configuration across behavioral tests.
fn test_engine(max_jobs: usize) -> Orchestrator {
    Orchestrator::new(
        OrchestratorConfig::new(max_jobs, 8).unwrap(),
        BudgetConfig::new(1, 1, 1, 1).unwrap(),
        Box::new(PriorityScheduler),
        Box::new(
            ExponentialRetry::new(2, Duration::from_millis(5), Duration::from_millis(10)).unwrap(),
        ),
    )
}
