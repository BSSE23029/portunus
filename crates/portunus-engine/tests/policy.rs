use portunus_engine::policy::{JobCandidate, PriorityScheduler, SchedulingStrategy};

#[path = "policy/retry.rs"]
mod retry;
#[path = "policy/schedule.rs"]
mod schedule;

// Inputs: priority scheduler behind the public strategy trait object.
// Outputs: selected candidate through dynamic dispatch.
// Logic: prove composition roots can choose policies without generic propagation.
#[test]
fn scheduling_contract_is_object_safe() {
    let mut policy: Box<dyn SchedulingStrategy> = Box::new(PriorityScheduler);
    let candidates = [JobCandidate::new(7, 1, 1, 0)];
    assert_eq!(policy.select(&candidates), Some(0));
}
