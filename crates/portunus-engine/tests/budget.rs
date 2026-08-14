use portunus_engine::budget::{
    BudgetConfig, BudgetError, BudgetPool, ResourceKind, ResourceRequest,
};

#[path = "budget/config.rs"]
mod config;

// Inputs: exact four-dimensional request, saturated retry, and one-over requests.
// Outputs: all-or-nothing permit, stable details, snapshot, and RAII release.
// Logic: prove bounded worker admission and pipeline bytes cannot leak independently.
#[test]
fn enforces_exact_and_rejected_resource_requests() {
    let pool = BudgetPool::new(BudgetConfig::new(1, 4, 3, 2).unwrap());
    let exact = ResourceRequest::new(4, 3, 2);
    let permit = pool.try_admit(exact).unwrap();
    let snapshot = pool.snapshot();
    assert_eq!(snapshot.active_tasks, 1);
    assert_eq!(snapshot.network_bytes, 4);
    assert_eq!(snapshot.verification_bytes, 3);
    assert_eq!(snapshot.disk_bytes, 2);
    assert!(matches!(
        pool.try_admit(ResourceRequest::new(0, 0, 0)),
        Err(BudgetError::Saturated)
    ));
    assert_eq!(
        pool.try_admit(ResourceRequest::new(5, 0, 0)).unwrap_err(),
        BudgetError::RequestTooLarge {
            resource: ResourceKind::Network,
            requested: 5,
            limit: 4,
        }
    );
    drop(permit);
    assert_eq!(pool.snapshot().active_tasks, 0);
    assert!(pool.try_admit(exact).is_ok());
}
