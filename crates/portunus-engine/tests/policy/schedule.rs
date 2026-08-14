use portunus_engine::policy::{JobCandidate, PriorityScheduler, SchedulingStrategy};

// Inputs: eligible candidates differing in priority, cost, and stable ID.
// Outputs: highest priority, then lowest cost, then lowest ID.
// Logic: verify deterministic generic scheduling without protocol concepts.
#[test]
fn prioritizes_jobs_with_stable_ties() {
    let mut scheduler = PriorityScheduler;
    let candidates = [
        JobCandidate::new(3, 5, 2, 0),
        JobCandidate::new(2, 5, 1, 0),
        JobCandidate::new(1, 5, 1, 0),
        JobCandidate::new(0, 4, 0, 0),
    ];
    assert_eq!(scheduler.select(&candidates), Some(2));
    assert_eq!(scheduler.select(&[]), None);
}
