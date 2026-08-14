use portunus_transport::{ConnectionTimer, TimingAction, TimingConfig, TimingConfigError};
use std::time::{Duration, Instant};

// Inputs: zero heartbeat/idle durations and idle shorter than heartbeat.
// Outputs: stable independent validation failures with configured durations.
// Logic: reject policies that spin or evict before a heartbeat can be attempted.
#[test]
fn validates_connection_timing_boundaries() {
    assert_eq!(
        TimingConfig::new(Duration::ZERO, Duration::from_secs(1)),
        Err(TimingConfigError::ZeroHeartbeat)
    );
    assert_eq!(
        TimingConfig::new(Duration::from_secs(1), Duration::ZERO),
        Err(TimingConfigError::ZeroIdle)
    );
    assert_eq!(
        TimingConfig::new(Duration::from_secs(2), Duration::from_secs(1)),
        Err(TimingConfigError::IdleBeforeHeartbeat {
            heartbeat: Duration::from_secs(2),
            idle: Duration::from_secs(1),
        })
    );
    assert!(TimingConfig::new(Duration::from_secs(1), Duration::from_secs(1)).is_ok());
}

// Inputs: explicit instants immediately before and exactly at heartbeat/idle limits.
// Outputs: no early action, inclusive heartbeat, then inclusive idle eviction.
// Logic: prove boundary ordering without sleeping or reading an ambient clock.
#[test]
fn evaluates_heartbeat_and_idle_boundaries_deterministically() {
    let origin = Instant::now();
    let config = TimingConfig::new(Duration::from_secs(2), Duration::from_secs(5)).unwrap();
    let mut timer = ConnectionTimer::new(config, origin, origin + Duration::from_secs(20)).unwrap();

    assert_eq!(
        timer.evaluate(
            (origin + Duration::from_secs(2))
                .checked_sub(Duration::from_nanos(1))
                .unwrap(),
        ),
        TimingAction::Wait
    );
    assert_eq!(
        timer.evaluate(origin + Duration::from_secs(2)),
        TimingAction::HeartbeatDue
    );
    timer.record_outbound(origin + Duration::from_secs(2));
    assert_eq!(
        timer.evaluate(origin + Duration::from_secs(5)),
        TimingAction::IdleEviction
    );
}

// Inputs: inbound activity, a connection deadline, and coincident terminal limits.
// Outputs: activity postpones idle eviction while deadline wins at equality.
// Logic: absolute connection deadline has deterministic precedence over liveness policy.
#[test]
fn records_activity_and_prioritizes_connection_deadlines() {
    let origin = Instant::now();
    let config = TimingConfig::new(Duration::from_secs(2), Duration::from_secs(5)).unwrap();
    let deadline = origin + Duration::from_secs(6);
    let mut timer = ConnectionTimer::new(config, origin, deadline).unwrap();
    timer.record_inbound(origin + Duration::from_secs(4));

    assert_ne!(
        timer.evaluate(origin + Duration::from_secs(5)),
        TimingAction::IdleEviction
    );
    assert_eq!(timer.evaluate(deadline), TimingAction::DeadlineElapsed);
}

// Inputs: deadline equal to session start.
// Outputs: stable invalid-deadline error preserving both instants.
// Logic: a connection must have a strictly future execution window.
#[test]
fn rejects_an_elapsed_initial_connection_deadline() {
    let origin = Instant::now();
    let config = TimingConfig::new(Duration::from_secs(1), Duration::from_secs(2)).unwrap();

    assert_eq!(
        ConnectionTimer::new(config, origin, origin),
        Err(TimingConfigError::DeadlineElapsed {
            started_at: origin,
            deadline: origin,
        })
    );
}
