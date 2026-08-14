//! Integration coverage for stable structured control-plane error details.

use portunus_daemon::{
    errors::{engine_status, fault_status},
    fault::{FaultInjector, FaultPoint, FaultScript},
};
use portunus_engine::torrent::Error;
use portunus_proto::ErrorDetail;
use prost::Message;
use tonic::Code;

// Inputs: validation, lookup, and overload-adjacent engine failures.
// Outputs: stable gRPC codes plus decodable versioned reason/resource details.
// Logic: preserve machine-readable failure policy independently from display text.
#[test]
fn maps_engine_failures_to_structured_statuses() {
    let cases = [
        (
            Error::EmptySource,
            Code::InvalidArgument,
            "INVALID_SOURCE",
            false,
            "transfer.source",
        ),
        (
            Error::UnknownTransfer("transfer-9".into()),
            Code::NotFound,
            "TRANSFER_NOT_FOUND",
            false,
            "transfer",
        ),
        (
            Error::Closed,
            Code::Unavailable,
            "ENGINE_UNAVAILABLE",
            true,
            "engine.command_queue",
        ),
    ];
    for (error, code, reason, retryable, resource) in cases {
        let status = engine_status(error);
        assert_eq!(status.code(), code);
        let detail = ErrorDetail::decode(status.details()).unwrap();
        assert_eq!(detail.reason, reason);
        assert_eq!(detail.retryable, retryable);
        assert_eq!(detail.resource, resource);
    }
}

// Inputs: deterministic injected control-operation failure.
// Outputs: retryable unavailable status with payload-free stable detail.
// Logic: keep test-only failure injection indistinguishable from safe service outage.
#[test]
fn maps_injected_faults_without_request_data() {
    let faults = FaultScript::new(1).unwrap();
    faults.arm(FaultPoint::UpdateConfig, 1).unwrap();
    let status = fault_status(faults.check(FaultPoint::UpdateConfig).unwrap_err());
    assert_eq!(status.code(), Code::Unavailable);
    let detail = ErrorDetail::decode(status.details()).unwrap();
    assert_eq!(detail.reason, "INJECTED_FAULT");
    assert!(detail.retryable);
    assert_eq!(detail.resource, "control.operation");
}
