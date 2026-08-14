use portunus_engine::policy::{
    ExponentialRetry, FailureClass, RetryContext, RetryDecision, RetryPolicy, RetryPolicyError,
};
use std::time::Duration;

// Inputs: zero attempts/delays and exact-minimum valid configuration.
// Outputs: independent stable configuration errors and success.
// Logic: reject unusable retry policy before failure handling begins.
#[test]
fn validates_retry_policy_boundaries() {
    assert_eq!(
        ExponentialRetry::new(0, Duration::from_millis(1), Duration::from_millis(1)).unwrap_err(),
        RetryPolicyError::ZeroAttempts
    );
    assert_eq!(
        ExponentialRetry::new(1, Duration::ZERO, Duration::from_millis(1)).unwrap_err(),
        RetryPolicyError::ZeroBaseDelay
    );
    assert_eq!(
        ExponentialRetry::new(1, Duration::from_millis(1), Duration::ZERO).unwrap_err(),
        RetryPolicyError::ZeroMaximumDelay
    );
    assert!(ExponentialRetry::new(1, Duration::from_millis(1), Duration::from_millis(1)).is_ok());
}

// Inputs: transient attempts at zero, exact last retry, and one-over boundary.
// Outputs: capped exponential delays then terminal exhaustion.
// Logic: define `attempt` as completed failures before the proposed next try.
#[test]
fn computes_bounded_retry_decisions() {
    let policy =
        ExponentialRetry::new(3, Duration::from_millis(10), Duration::from_millis(25)).unwrap();
    assert_eq!(
        policy.decide(RetryContext::new(0, FailureClass::Transient)),
        RetryDecision::RetryAfter(Duration::from_millis(10))
    );
    assert_eq!(
        policy.decide(RetryContext::new(2, FailureClass::Transient)),
        RetryDecision::RetryAfter(Duration::from_millis(25))
    );
    assert_eq!(
        policy.decide(RetryContext::new(3, FailureClass::Transient)),
        RetryDecision::Exhausted
    );
    assert_eq!(
        policy.decide(RetryContext::new(u32::MAX, FailureClass::Transient)),
        RetryDecision::Exhausted
    );
    assert_eq!(
        policy.decide(RetryContext::new(0, FailureClass::Permanent)),
        RetryDecision::PermanentFailure
    );
}
