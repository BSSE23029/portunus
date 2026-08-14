//! Bounded revisioned orchestration state, snapshots, and event streams.
//!
//! One mutex serializes job-map mutation, monotonically increasing revisions, latest
//! snapshot publication, and bounded event publication. Retained jobs have an
//! inclusive ceiling and stable IDs; snapshots use deterministic ID order. `watch`
//! serves latest state while `broadcast` makes slow-consumer lag explicit.
//!
//! ```text
//! transition ──lock──> validate + mutate + revision
//!                       ├─ watch latest consistent snapshot
//!                       └─ bounded broadcast event (lag observable)
//! ```
//!
//! This module observes resource budgets but does not admit/spawn/schedule/retry work,
//! persist state, execute I/O, or install a process-global telemetry exporter.

use crate::budget::BudgetPool;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, MutexGuard},
};
use tokio::sync::{broadcast, watch};

pub mod model;
use model::valid_transition;
pub use model::{
    EngineEvent, EngineEventKind, EngineSnapshot, JobId, JobSnapshot, JobState, StateError,
    StateHubConfig,
};

#[derive(Debug)]
struct State {
    revision: u64,
    jobs: BTreeMap<JobId, JobState>,
}

#[derive(Debug)]
struct HubInner {
    config: StateHubConfig,
    state: Mutex<State>,
    budget: BudgetPool,
    snapshots: watch::Sender<EngineSnapshot>,
    events: broadcast::Sender<EngineEvent>,
}

#[derive(Clone, Debug)]
pub struct StateHub {
    inner: Arc<HubInner>,
}

impl StateHub {
    /// Inputs: validated capacities and shared engine resource pool.
    /// Outputs: cloneable state hub initialized at revision zero.
    /// Logic: construct bounded event/latest snapshot channels around one state owner.
    #[must_use]
    pub fn new(config: StateHubConfig, budget: BudgetPool) -> Self {
        let initial = EngineSnapshot {
            revision: 0,
            jobs: Vec::new(),
            budget: budget.snapshot(),
        };
        let (snapshots, _) = watch::channel(initial);
        let (events, _) = broadcast::channel(config.event_capacity);
        Self {
            inner: Arc::new(HubInner {
                config,
                state: Mutex::new(State {
                    revision: 0,
                    jobs: BTreeMap::new(),
                }),
                budget,
                snapshots,
                events,
            }),
        }
    }

    /// Inputs: stable job ID not currently retained.
    /// Outputs: queued job at next revision or duplicate/capacity failure.
    /// Logic: validate and mutate under one lock, then publish matching snapshot/event.
    /// # Errors
    /// Returns duplicate job, capacity, or revision-exhaustion errors.
    pub fn admit(&self, id: JobId) -> Result<(), StateError> {
        let mut state = lock_state(&self.inner.state);
        if state.jobs.contains_key(&id) {
            return Err(StateError::DuplicateJob(id));
        }
        if state.jobs.len() == self.inner.config.max_jobs {
            return Err(StateError::JobLimitExceeded {
                limit: self.inner.config.max_jobs,
            });
        }
        state.jobs.insert(id, JobState::Queued);
        let result = self.publish(&mut state, id, EngineEventKind::Admitted);
        drop(state);
        result
    }

    /// Inputs: retained job ID and requested next state.
    /// Outputs: published transition or stable lookup/state/revision failure.
    /// Logic: validate the state-machine edge before mutation and publication.
    /// # Errors
    /// Returns unknown job, invalid edge, or revision exhaustion.
    pub fn transition(&self, id: JobId, to: JobState) -> Result<(), StateError> {
        let mut state = lock_state(&self.inner.state);
        let from = *state.jobs.get(&id).ok_or(StateError::UnknownJob(id))?;
        if !valid_transition(from, to) {
            return Err(StateError::InvalidTransition { id, from, to });
        }
        state.jobs.insert(id, to);
        let result = self.publish(&mut state, id, EngineEventKind::Transitioned { from, to });
        drop(state);
        result
    }

    /// Inputs: retained terminal job ID.
    /// Outputs: removed record at next revision or lookup/nonterminal failure.
    /// Logic: preserve terminal state in the event before releasing retained capacity.
    /// # Errors
    /// Returns unknown, nonterminal, or revision exhaustion errors.
    pub fn remove_terminal(&self, id: JobId) -> Result<(), StateError> {
        let mut state = lock_state(&self.inner.state);
        let terminal = *state.jobs.get(&id).ok_or(StateError::UnknownJob(id))?;
        if !terminal.is_terminal() {
            return Err(StateError::NotTerminal {
                id,
                state: terminal,
            });
        }
        state.jobs.remove(&id);
        let result = self.publish(&mut state, id, EngineEventKind::Removed { terminal });
        drop(state);
        result
    }

    /// Inputs: shared hub.
    /// Outputs: owned latest consistent state/resource snapshot.
    /// Logic: clone the watch value; mutation publishes only complete snapshots.
    #[must_use]
    pub fn snapshot(&self) -> EngineSnapshot {
        self.inner.snapshots.borrow().clone()
    }

    /// Inputs: shared hub.
    /// Outputs: independent latest-state watch receiver.
    /// Logic: subscribe after construction while preserving current snapshot.
    #[must_use]
    pub fn subscribe_snapshots(&self) -> watch::Receiver<EngineSnapshot> {
        self.inner.snapshots.subscribe()
    }

    /// Inputs: shared hub.
    /// Outputs: bounded event receiver whose lag errors quantify dropped history.
    /// Logic: create a new broadcast cursor without expanding configured capacity.
    #[must_use]
    pub fn subscribe_events(&self) -> broadcast::Receiver<EngineEvent> {
        self.inner.events.subscribe()
    }

    // Inputs: locked mutable state and already-applied event description.
    // Outputs: matching next-revision snapshot/event or exhaustion error.
    // Logic: increment once, build ordered snapshot, replace latest, then broadcast.
    fn publish(
        &self,
        state: &mut State,
        job_id: JobId,
        kind: EngineEventKind,
    ) -> Result<(), StateError> {
        state.revision = state
            .revision
            .checked_add(1)
            .ok_or(StateError::RevisionExhausted)?;
        let snapshot = EngineSnapshot {
            revision: state.revision,
            jobs: state
                .jobs
                .iter()
                .map(|(id, state)| JobSnapshot {
                    id: *id,
                    state: *state,
                })
                .collect(),
            budget: self.inner.budget.snapshot(),
        };
        self.inner.snapshots.send_replace(snapshot);
        let _ = self.inner.events.send(EngineEvent {
            revision: state.revision,
            job_id,
            kind,
        });
        Ok(())
    }
}

// Inputs: possibly poisoned state mutex.
// Outputs: mutable guard, recovering inner state after a prior panic.
// Logic: avoid making all later telemetry operations panic because one holder did.
fn lock_state(mutex: &Mutex<State>) -> MutexGuard<'_, State> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
