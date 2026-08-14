//! Coverage for fixed-cardinality daemon OpenTelemetry instruments.

use portunus_daemon::telemetry::{AdmissionOutcome, OperationalMetrics};

// Inputs: global no-op meter and every bounded admission outcome label.
// Outputs: non-panicking counter recording without payload-derived attributes.
// Logic: keep metric cardinality fixed even when telemetry export is disabled.
#[test]
fn records_stable_admission_outcomes() {
    let metrics = OperationalMetrics::global();
    metrics.record_admission(AdmissionOutcome::Accepted);
    metrics.record_admission(AdmissionOutcome::Unauthenticated);
    metrics.record_admission(AdmissionOutcome::Overloaded);
}
