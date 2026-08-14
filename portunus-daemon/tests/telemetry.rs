//! Integration coverage for bounded OpenTelemetry export configuration.

use portunus_daemon::telemetry::{TelemetryConfig, TelemetryError};
use std::time::Duration;

#[path = "telemetry/instruments.rs"]
mod instruments;
#[path = "telemetry/runtime.rs"]
mod runtime;

// Inputs: absent endpoint and a complete valid OTLP/HTTP configuration.
// Outputs: explicit disabled mode and enabled bounded export policy.
// Logic: keep telemetry opt-in while making every retained/export cadence bound clear.
#[test]
fn resolves_disabled_and_enabled_telemetry() {
    assert_eq!(
        TelemetryConfig::from_sources(None, None, None, None).unwrap(),
        TelemetryConfig::disabled()
    );
    let config = TelemetryConfig::from_sources(
        Some("http://127.0.0.1:4318"),
        Some("8"),
        Some("4"),
        Some("250"),
    )
    .unwrap();
    assert!(config.is_enabled());
    assert_eq!(config.max_trace_queue(), 8);
    assert_eq!(config.max_export_batch(), 4);
    assert_eq!(config.metric_interval(), Duration::from_millis(250));
}

// Inputs: malformed endpoint and zero/excess independent exporter limits.
// Outputs: stable validation errors before exporters or worker threads are created.
// Logic: cover zero, exact, and first rejected boundaries for retained telemetry.
#[test]
fn rejects_malformed_and_unbounded_telemetry() {
    assert_eq!(
        TelemetryConfig::otlp("collector", 1, 1, Duration::from_millis(1)),
        Err(TelemetryError::InvalidEndpoint)
    );
    assert_eq!(
        TelemetryConfig::otlp("http://localhost:4318", 0, 1, Duration::from_millis(1)),
        Err(TelemetryError::ZeroTraceQueue)
    );
    assert_eq!(
        TelemetryConfig::otlp("http://localhost:4318", 1, 0, Duration::from_millis(1)),
        Err(TelemetryError::ZeroExportBatch)
    );
    assert_eq!(
        TelemetryConfig::otlp("http://localhost:4318", 1, 2, Duration::from_millis(1)),
        Err(TelemetryError::ExportBatchExceedsQueue { batch: 2, queue: 1 })
    );
    assert_eq!(
        TelemetryConfig::otlp("http://localhost:4318", 1, 1, Duration::ZERO),
        Err(TelemetryError::ZeroMetricInterval)
    );
    assert!(
        TelemetryConfig::otlp("https://localhost:4318", 1, 1, Duration::from_millis(1)).is_ok()
    );
}

// Inputs: malformed decimal environment-source values with and without endpoint.
// Outputs: stable parse/disabled-option errors instead of silently chosen defaults.
// Logic: make daemon startup configuration deterministic and typo-intolerant.
#[test]
fn rejects_inconsistent_telemetry_sources() {
    assert_eq!(
        TelemetryConfig::from_sources(None, Some("8"), None, None),
        Err(TelemetryError::OptionsWithoutEndpoint)
    );
    assert_eq!(
        TelemetryConfig::from_sources(Some("http://localhost:4318"), Some("bad"), None, None),
        Err(TelemetryError::InvalidTraceQueue)
    );
}
