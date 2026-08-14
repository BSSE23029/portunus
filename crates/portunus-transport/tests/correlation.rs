use portunus_transport::{CorrelationError, CorrelationId, CorrelationTable};
use std::time::{Duration, Instant};

// Inputs: exact one-entry capacity followed by one additional distinct request.
// Outputs: first admission succeeds and the over-boundary error retains its limit.
// Logic: prove correlation cannot create hidden unbounded in-flight state.
#[test]
fn enforces_the_exact_in_flight_boundary() {
    let now = Instant::now();
    let mut table = CorrelationTable::new(1).unwrap();
    table
        .insert(CorrelationId::new(7), now + Duration::from_secs(1), "first")
        .unwrap();

    let rejected = table
        .insert(
            CorrelationId::new(8),
            now + Duration::from_secs(1),
            "second",
        )
        .unwrap_err();
    assert_eq!(rejected.reason(), CorrelationError::AtCapacity { limit: 1 });
    assert_eq!(rejected.into_value(), "second");
    assert_eq!(table.len(), 1);
}

// Inputs: zero capacity and a duplicate ID while unused capacity remains.
// Outputs: distinct stable validation and duplicate-correlation failures.
// Logic: reject ambiguous response ownership before mutating existing requests.
#[test]
fn rejects_zero_capacity_and_duplicate_ids() {
    assert!(matches!(
        CorrelationTable::<()>::new(0),
        Err(CorrelationError::ZeroCapacity)
    ));
    let now = Instant::now();
    let id = CorrelationId::new(11);
    let mut table = CorrelationTable::new(2).unwrap();
    table.insert(id, now, "original").unwrap();

    let rejected = table.insert(id, now, "replacement").unwrap_err();
    assert_eq!(rejected.reason(), CorrelationError::Duplicate { id });
    assert_eq!(rejected.into_value(), "replacement");
    assert_eq!(table.resolve(id), Some("original"));
}

// Inputs: entries before, exactly at, and after an explicit expiry instant.
// Outputs: inclusive expiry in correlation order while the future entry remains.
// Logic: make timeout behavior deterministic and independent of sleeping or Tokio.
#[test]
fn expires_deadlines_inclusively_and_resolves_remaining_requests() {
    let now = Instant::now();
    let mut table = CorrelationTable::new(3).unwrap();
    table
        .insert(
            CorrelationId::new(3),
            now.checked_sub(Duration::from_nanos(1)).unwrap(),
            "past",
        )
        .unwrap();
    table.insert(CorrelationId::new(1), now, "exact").unwrap();
    table
        .insert(
            CorrelationId::new(2),
            now + Duration::from_nanos(1),
            "future",
        )
        .unwrap();

    assert_eq!(
        table.expire_at(now),
        vec![
            (CorrelationId::new(1), "exact"),
            (CorrelationId::new(3), "past"),
        ]
    );
    assert_eq!(table.resolve(CorrelationId::new(2)), Some("future"));
    assert!(table.is_empty());
}
