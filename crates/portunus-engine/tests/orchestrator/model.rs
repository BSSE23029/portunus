//! Boundary coverage for generic orchestrator configuration values.

use portunus_engine::orchestrator::{OrchestratorConfig, OrchestratorError};

// Inputs: zero and exact-minimum queue/event capacities.
// Outputs: stable independent errors and successful minimum configuration.
// Logic: reject disabled topology before allocating shared runtime state.
#[test]
fn validates_orchestrator_boundaries() {
    assert_eq!(
        OrchestratorConfig::new(0, 1),
        Err(OrchestratorError::ZeroJobLimit)
    );
    assert_eq!(
        OrchestratorConfig::new(1, 0),
        Err(OrchestratorError::ZeroEventCapacity)
    );
    assert!(OrchestratorConfig::new(1, 1).is_ok());
}
