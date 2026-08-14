use portunus_engine::{
    budget::{BudgetConfig, BudgetPool},
    telemetry::{JobId, JobState, StateError, StateHub, StateHubConfig},
};
use tokio::sync::broadcast::error::TryRecvError;

#[path = "telemetry/model.rs"]
mod model;

// Inputs: exact two-job capacity, duplicate ID, and one-over admission.
// Outputs: ordered consistent snapshot and stable admission failures.
// Logic: prove retained orchestration state is bounded independently from channels.
#[test]
fn maintains_bounded_consistent_snapshots() {
    let budget = BudgetPool::new(BudgetConfig::new(1, 1, 1, 1).unwrap());
    let hub = StateHub::new(StateHubConfig::new(2, 4).unwrap(), budget);
    hub.admit(JobId::new(2)).unwrap();
    hub.admit(JobId::new(1)).unwrap();
    assert_eq!(
        hub.admit(JobId::new(1)).unwrap_err(),
        StateError::DuplicateJob(JobId::new(1))
    );
    assert_eq!(
        hub.admit(JobId::new(3)).unwrap_err(),
        StateError::JobLimitExceeded { limit: 2 }
    );
    let snapshot = hub.snapshot();
    assert_eq!(snapshot.revision, 2);
    assert_eq!(snapshot.jobs[0].id, JobId::new(1));
    assert_eq!(snapshot.jobs[1].id, JobId::new(2));
    assert!(snapshot
        .jobs
        .iter()
        .all(|job| job.state == JobState::Queued));
}

// Inputs: valid lifecycle, invalid terminal transition, and bounded event receiver.
// Outputs: exact state snapshots, transition context, and observable lag.
// Logic: couple state mutation and event revision under one serialized operation.
#[test]
fn publishes_revisioned_transitions_and_bounded_lag() {
    let budget = BudgetPool::new(BudgetConfig::new(1, 1, 1, 1).unwrap());
    let hub = StateHub::new(StateHubConfig::new(1, 1).unwrap(), budget);
    let mut events = hub.subscribe_events();
    let mut snapshots = hub.subscribe_snapshots();
    let id = JobId::new(9);
    hub.admit(id).unwrap();
    hub.transition(id, JobState::Running).unwrap();
    assert!(matches!(events.try_recv(), Err(TryRecvError::Lagged(1))));
    let event = events.try_recv().unwrap();
    assert_eq!(event.revision, 2);
    assert_eq!(snapshots.borrow_and_update().revision, 2);
    hub.transition(id, JobState::Completed).unwrap();
    assert_eq!(
        hub.transition(id, JobState::Running).unwrap_err(),
        StateError::InvalidTransition {
            id,
            from: JobState::Completed,
            to: JobState::Running,
        }
    );
    hub.remove_terminal(id).unwrap();
    assert!(hub.snapshot().jobs.is_empty());
}
