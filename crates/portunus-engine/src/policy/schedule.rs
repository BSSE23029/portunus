//! Deterministic scheduling over generic bounded job metadata.
//!
//! Candidates carry only stable ID, signed priority, estimated resource cost, and
//! completed-attempt count. A strategy returns a borrowed-slice index and cannot
//! retain or mutate caller jobs. Empty input has no selection.
//!
//! This module does not define protocol rarity, queue ownership, admission, retry,
//! task spawning, or execution. Adapters translate domain signals into priorities.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JobCandidate {
    pub id: u64,
    pub priority: i64,
    pub cost: u32,
    pub attempts: u32,
}

impl JobCandidate {
    /// Inputs: stable ID, signed priority, estimated cost, and completed attempts.
    /// Outputs: immutable scheduler metadata without payload ownership.
    /// Logic: package generic comparable dimensions for strategy evaluation.
    #[must_use]
    pub const fn new(id: u64, priority: i64, cost: u32, attempts: u32) -> Self {
        Self {
            id,
            priority,
            cost,
            attempts,
        }
    }
}

pub trait SchedulingStrategy: Send {
    /// Inputs: bounded borrowed candidate slice in caller-defined queue order.
    /// Outputs: a valid selected index or `None` for empty/no eligible input.
    /// Logic: implementations inspect metadata only and retain no candidate borrow.
    fn select(&mut self, candidates: &[JobCandidate]) -> Option<usize>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct PriorityScheduler;

impl SchedulingStrategy for PriorityScheduler {
    /// Inputs: generic candidates; all supplied entries are considered eligible.
    /// Outputs: highest priority index, tying by lowest cost then lowest stable ID.
    /// Logic: one linear scan preserves bounded memory and deterministic selection.
    fn select(&mut self, candidates: &[JobCandidate]) -> Option<usize> {
        candidates
            .iter()
            .enumerate()
            .max_by(|(_, left), (_, right)| {
                left.priority
                    .cmp(&right.priority)
                    .then_with(|| right.cost.cmp(&left.cost))
                    .then_with(|| right.id.cmp(&left.id))
            })
            .map(|(index, _)| index)
    }
}
