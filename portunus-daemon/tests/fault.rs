//! Integration coverage for deterministic bounded fault injection.

use portunus_daemon::fault::{
    DisabledFaults, FaultInjector, FaultPoint, FaultScript, FaultScriptError,
};

// Inputs: disabled injector and every stable control operation point.
// Outputs: no injected failures.
// Logic: make production default explicit and independent from environment state.
#[test]
fn disabled_faults_never_change_behavior() {
    let faults = DisabledFaults;
    for point in FaultPoint::ALL {
        assert!(faults.check(point).is_ok());
    }
}

// Inputs: two-rule ceiling, exact rules, one-over rule, and fixed failure counts.
// Outputs: bounded configuration and deterministic consume-on-check failures.
// Logic: exercise failure paths without randomness, sleeping, or external services.
#[test]
fn consumes_bounded_scripted_failures() {
    assert_eq!(
        FaultScript::new(0).unwrap_err(),
        FaultScriptError::ZeroRuleLimit
    );
    let faults = FaultScript::new(2).unwrap();
    assert_eq!(
        faults.arm(FaultPoint::AddTransfer, 0),
        Err(FaultScriptError::ZeroFailures)
    );
    faults.arm(FaultPoint::AddTransfer, 2).unwrap();
    faults.arm(FaultPoint::StopTransfer, 1).unwrap();
    assert_eq!(
        faults.arm(FaultPoint::UpdateConfig, 1).unwrap_err(),
        FaultScriptError::RuleLimitExceeded { limit: 2 }
    );
    assert!(faults.check(FaultPoint::AddTransfer).is_err());
    assert!(faults.check(FaultPoint::AddTransfer).is_err());
    assert!(faults.check(FaultPoint::AddTransfer).is_ok());
    assert!(faults.check(FaultPoint::StopTransfer).is_err());
    assert!(faults.check(FaultPoint::StopTransfer).is_ok());
}
