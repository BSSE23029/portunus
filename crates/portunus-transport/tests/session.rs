use portunus_transport::{
    LifecycleEvent, SessionConfig, SessionConfigError, SessionMachine, SessionState,
    TransitionError,
};

// Inputs: exact minimum queue and in-flight capacities.
// Outputs: accepted immutable configuration preserving each independent budget.
// Logic: establish one as the inclusive lower boundary for every resource ceiling.
#[test]
fn accepts_exact_minimum_session_capacities() {
    let config = SessionConfig::new(1, 1, 1).unwrap();

    assert_eq!(config.inbound_capacity(), 1);
    assert_eq!(config.outbound_capacity(), 1);
    assert_eq!(config.max_in_flight(), 1);
}

// Inputs: zero for each capacity while the other independent budgets remain valid.
// Outputs: stable resource-specific validation errors.
// Logic: prove no unbounded/default queue semantics can hide behind a zero value.
#[test]
fn rejects_each_zero_session_capacity() {
    assert_eq!(
        SessionConfig::new(0, 1, 1),
        Err(SessionConfigError::ZeroCapacity {
            resource: "inbound"
        })
    );
    assert_eq!(
        SessionConfig::new(1, 0, 1),
        Err(SessionConfigError::ZeroCapacity {
            resource: "outbound"
        })
    );
    assert_eq!(
        SessionConfig::new(1, 1, 0),
        Err(SessionConfigError::ZeroCapacity {
            resource: "in_flight"
        })
    );
}

// Inputs: the complete valid connection lifecycle event sequence.
// Outputs: explicit connecting, active, draining, and closed states in order.
// Logic: keep lifecycle transitions protocol-neutral and externally observable.
#[test]
fn advances_through_the_session_lifecycle() {
    let mut machine = SessionMachine::new();
    assert_eq!(machine.state(), SessionState::Connecting);

    machine.apply(LifecycleEvent::Connected).unwrap();
    assert_eq!(machine.state(), SessionState::Active);
    machine.apply(LifecycleEvent::DrainRequested).unwrap();
    assert_eq!(machine.state(), SessionState::Draining);
    machine.apply(LifecycleEvent::TransportClosed).unwrap();
    assert_eq!(machine.state(), SessionState::Closed);
}

// Inputs: a drain request before connection establishment.
// Outputs: stable error retaining the source state and rejected event.
// Logic: invalid transitions must not mutate lifecycle state.
#[test]
fn rejects_invalid_lifecycle_transitions_without_mutation() {
    let mut machine = SessionMachine::new();
    assert_eq!(
        machine.apply(LifecycleEvent::DrainRequested),
        Err(TransitionError {
            state: SessionState::Connecting,
            event: LifecycleEvent::DrainRequested,
        })
    );
    assert_eq!(machine.state(), SessionState::Connecting);
}
