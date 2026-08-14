use portunus_engine::telemetry::{StateError, StateHubConfig};

// Inputs: zero values for independent retained-job and event capacities.
// Outputs: distinct stable configuration errors and exact minimum success.
// Logic: reject unbounded/disabled telemetry topology before channel allocation.
#[test]
fn validates_state_hub_boundaries() {
    assert_eq!(
        StateHubConfig::new(0, 1).unwrap_err(),
        StateError::ZeroJobLimit
    );
    assert_eq!(
        StateHubConfig::new(1, 0).unwrap_err(),
        StateError::ZeroEventCapacity
    );
    assert!(StateHubConfig::new(1, 1).is_ok());
}
