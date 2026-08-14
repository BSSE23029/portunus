//! Lifecycle coverage for process-owned OpenTelemetry providers.

use async_trait::async_trait;
use opentelemetry::trace::{Span as _, Tracer as _};
use opentelemetry_http::{Bytes, HttpClient, HttpError, Request, Response};
use portunus_daemon::{
    logging::{init_global_logging_with_tracer, LoggingConfig},
    telemetry::{TelemetryConfig, TelemetryRuntime},
};
use std::sync::{Arc, Mutex};

// Inputs: explicitly disabled telemetry configuration.
// Outputs: inert runtime with successful idempotent shutdown and no worker/exporter.
// Logic: prove the default path performs no network access or global installation.
#[test]
fn keeps_disabled_telemetry_inert() {
    let runtime = TelemetryRuntime::start(&TelemetryConfig::disabled()).unwrap();
    assert!(!runtime.is_enabled());
    runtime.shutdown().unwrap();
}

// Inputs: disabled runtime tracer and valid error-only logging filter.
// Outputs: successful single global subscriber installation without OTLP layer.
// Logic: prove one composition API handles optional tracing export ownership.
#[test]
fn composes_logging_with_optional_tracing_export() {
    let runtime = TelemetryRuntime::start(&TelemetryConfig::disabled()).unwrap();
    let logging = LoggingConfig::new("error").unwrap();
    init_global_logging_with_tracer(&logging, runtime.tracer()).unwrap();
    runtime.shutdown().unwrap();
}

#[derive(Clone, Debug, Default)]
struct RecordingClient {
    paths: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl HttpClient for RecordingClient {
    // Inputs: OTLP HTTP request with bounded protobuf body.
    // Outputs: immediate empty success response without network access.
    // Logic: retain only the bounded URI path needed to prove signal routing.
    async fn send_bytes(&self, request: Request<Bytes>) -> Result<Response<Bytes>, HttpError> {
        self.paths.lock().unwrap().push(request.uri().path().into());
        Ok(Response::new(Bytes::new()))
    }
}

// Inputs: custom in-memory HTTP client and enabled bounded OTLP policy.
// Outputs: one flushed trace and metric request at their standard signal paths.
// Logic: prove real SDK/exporter composition without sockets, clocks, or collectors.
#[test]
fn exports_traces_and_metrics_through_injected_http_client() {
    let client = RecordingClient::default();
    let observed = Arc::clone(&client.paths);
    let config = TelemetryConfig::otlp(
        "http://collector.example",
        2,
        1,
        std::time::Duration::from_secs(10),
    )
    .unwrap();
    let runtime = TelemetryRuntime::start_with_http_client(&config, client).unwrap();
    let tracer = runtime.tracer().unwrap();
    let mut span = tracer.start("reference-operation");
    span.end();
    portunus_daemon::telemetry::OperationalMetrics::global()
        .record_admission(portunus_daemon::telemetry::AdmissionOutcome::Accepted);
    runtime.shutdown().unwrap();
    let mut paths = observed.lock().unwrap().clone();
    paths.sort();
    assert_eq!(paths, ["/v1/metrics", "/v1/traces"]);
}
