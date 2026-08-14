use portunus_engine::budget::{BudgetConfig, BudgetError, ResourceKind};

// Inputs: zero values for every independently configured engine resource.
// Outputs: distinct stable configuration errors and exact-minimum success.
// Logic: reject unusable orchestration policy before semaphore allocation.
#[test]
fn validates_independent_budget_boundaries() {
    assert_eq!(
        BudgetConfig::new(0, 1, 1, 1).unwrap_err(),
        BudgetError::ZeroLimit(ResourceKind::Tasks)
    );
    assert_eq!(
        BudgetConfig::new(1, 0, 1, 1).unwrap_err(),
        BudgetError::ZeroLimit(ResourceKind::Network)
    );
    assert_eq!(
        BudgetConfig::new(1, 1, 0, 1).unwrap_err(),
        BudgetError::ZeroLimit(ResourceKind::Verification)
    );
    assert_eq!(
        BudgetConfig::new(1, 1, 1, 0).unwrap_err(),
        BudgetError::ZeroLimit(ResourceKind::Disk)
    );
    assert!(BudgetConfig::new(1, 1, 1, 1).is_ok());
}
