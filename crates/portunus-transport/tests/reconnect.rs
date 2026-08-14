use portunus_transport::{ReconnectConfigError, ReconnectPolicy};
use std::time::Duration;

// Inputs: zero attempts/timeouts and an initial delay above its cap.
// Outputs: stable validation failure for each independent policy boundary.
// Logic: prevent infinite retry, zero-delay spin, and contradictory caps.
#[test]
fn validates_reconnection_policy_boundaries() {
    assert_eq!(
        ReconnectPolicy::new(0, Duration::from_secs(1), Duration::from_secs(2)),
        Err(ReconnectConfigError::ZeroAttempts)
    );
    assert_eq!(
        ReconnectPolicy::new(1, Duration::ZERO, Duration::from_secs(2)),
        Err(ReconnectConfigError::ZeroInitialDelay)
    );
    assert_eq!(
        ReconnectPolicy::new(1, Duration::from_secs(1), Duration::ZERO),
        Err(ReconnectConfigError::ZeroMaximumDelay)
    );
    assert_eq!(
        ReconnectPolicy::new(1, Duration::from_secs(3), Duration::from_secs(2)),
        Err(ReconnectConfigError::InitialExceedsMaximum {
            initial: Duration::from_secs(3),
            maximum: Duration::from_secs(2),
        })
    );
}

// Inputs: five one-based attempts with a one-second initial and five-second cap.
// Outputs: exponential delays capped deterministically at five seconds.
// Logic: establish retry-number units and inclusive maximum-attempt boundary.
#[test]
fn computes_capped_exponential_reconnection_delays() {
    let policy = ReconnectPolicy::new(5, Duration::from_secs(1), Duration::from_secs(5)).unwrap();

    assert_eq!(policy.delay_for(0), None);
    assert_eq!(policy.delay_for(1), Some(Duration::from_secs(1)));
    assert_eq!(policy.delay_for(2), Some(Duration::from_secs(2)));
    assert_eq!(policy.delay_for(3), Some(Duration::from_secs(4)));
    assert_eq!(policy.delay_for(4), Some(Duration::from_secs(5)));
    assert_eq!(policy.delay_for(5), Some(Duration::from_secs(5)));
    assert_eq!(policy.delay_for(6), None);
}

// Inputs: a very large valid attempt number and a tiny maximum delay.
// Outputs: capped result without looping proportional to the attempt number.
// Logic: ensure hostile/high counters cannot turn delay calculation into CPU work.
#[test]
fn caps_large_attempt_numbers_in_constant_work() {
    let policy =
        ReconnectPolicy::new(u32::MAX, Duration::from_nanos(1), Duration::from_nanos(3)).unwrap();

    assert_eq!(policy.delay_for(u32::MAX), Some(Duration::from_nanos(3)));
}
