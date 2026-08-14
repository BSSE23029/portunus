//! Integration coverage for daemon operational service construction.

use portunus_daemon::operations::{mark_draining, mark_serving};
use portunus_proto::FILE_DESCRIPTOR_SET;

// Inputs: the exact embedded v1 compiler descriptor used by the daemon.
// Outputs: constructible standards-compatible server reflection service.
// Logic: reject descriptor/build drift before the composition root starts listening.
#[test]
fn builds_reflection_from_the_versioned_descriptor() {
    assert!(tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
        .build_v1()
        .is_ok());
}

// Inputs: fresh tonic health reporter/service pair and v1 service identity.
// Outputs: successful transition to serving without network or wall-clock access.
// Logic: prove health state is explicitly controlled rather than inferred from bind.
#[tokio::test]
async fn publishes_explicit_control_service_health() {
    let (mut reporter, _service) = tonic_health::server::health_reporter();
    mark_serving(&mut reporter).await;
    mark_draining(&mut reporter).await;
}
