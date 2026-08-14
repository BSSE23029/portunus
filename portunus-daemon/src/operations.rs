//! Operational health lifecycle for the versioned control service.
//!
//! The daemon publishes one stable fully qualified service name. Startup marks it
//! serving only after construction succeeds; shutdown marks it not serving before
//! tonic drains accepted requests. Health state is explicit rather than inferred.
//!
//! This module does not bind sockets, handle signals, authorize calls, reflect
//! descriptors, choose timeouts, or install process-global telemetry exporters.

use tonic_health::{server::HealthReporter, ServingStatus};

pub const CONTROL_SERVICE_NAME: &str = "portunus.v1.PortunusControl";

/// Inputs: mutable health reporter owned by the daemon composition root.
/// Outputs: control service published as serving.
/// Logic: update the stable fully qualified v1 service name after startup succeeds.
pub async fn mark_serving(reporter: &mut HealthReporter) {
    reporter
        .set_service_status(CONTROL_SERVICE_NAME, ServingStatus::Serving)
        .await;
}

/// Inputs: mutable health reporter before graceful server shutdown begins.
///
/// Outputs: control service published as not serving.
/// Logic: reject readiness probes before tonic waits for accepted requests to drain.
pub async fn mark_draining(reporter: &mut HealthReporter) {
    reporter
        .set_service_status(CONTROL_SERVICE_NAME, ServingStatus::NotServing)
        .await;
}
