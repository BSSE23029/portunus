//! Process-owned bounded OTLP trace and metric provider lifecycle.
//!
//! Enabled mode uses OTLP/HTTP protobuf exporters, a dedicated bounded batch span
//! processor, and a periodic metric reader. Providers share a stable service resource
//! and remain owned until explicit shutdown. Disabled mode allocates no SDK workers.
//!
//! This module does not read configuration sources, install the tracing subscriber,
//! define application instruments, retry shutdown, or expose endpoint credentials.

use super::TelemetryConfig;
use opentelemetry::{global, trace::TracerProvider as _};
use opentelemetry_http::HttpClient;
use opentelemetry_otlp::{WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::{
    error::OTelSdkError,
    metrics::{PeriodicReader, SdkMeterProvider},
    trace::{BatchConfigBuilder, BatchSpanProcessor, SdkTracer, SdkTracerProvider},
    Resource,
};
use thiserror::Error;

#[derive(Debug)]
enum Providers {
    Disabled,
    Enabled {
        trace: SdkTracerProvider,
        metrics: SdkMeterProvider,
    },
}

#[derive(Debug)]
pub struct TelemetryRuntime {
    providers: Providers,
}

impl TelemetryRuntime {
    /// Inputs: validated disabled or bounded OTLP export configuration.
    /// Outputs: owned provider lifecycle; enabled mode also installs global metrics.
    /// Logic: build both exporters before publishing providers, use one resource, and
    /// configure the trace queue/batch and metric interval from explicit bounds.
    /// # Errors
    /// Returns signal-specific exporter construction failures without partial install.
    pub fn start(config: &TelemetryConfig) -> Result<Self, TelemetryRuntimeError> {
        let Some(endpoint) = config.endpoint() else {
            return Ok(Self {
                providers: Providers::Disabled,
            });
        };
        let trace_exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_endpoint(signal_endpoint(endpoint, "/v1/traces"))
            .build()
            .map_err(TelemetryRuntimeError::TraceExporter)?;
        let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_http()
            .with_endpoint(signal_endpoint(endpoint, "/v1/metrics"))
            .build()
            .map_err(TelemetryRuntimeError::MetricExporter)?;
        Ok(Self::from_exporters(
            config,
            trace_exporter,
            metric_exporter,
        ))
    }

    /// Inputs: enabled bounded configuration and cloneable caller-owned HTTP client.
    /// Outputs: provider lifecycle exporting both OTLP signals through that client.
    /// Logic: append standard signal paths, build exporters without ambient transport,
    /// then reuse the exact production provider/budget construction path.
    /// # Errors
    /// Returns disabled-config or signal-specific exporter construction failures.
    pub fn start_with_http_client<C>(
        config: &TelemetryConfig,
        client: C,
    ) -> Result<Self, TelemetryRuntimeError>
    where
        C: HttpClient + Clone + 'static,
    {
        let endpoint = config
            .endpoint()
            .ok_or(TelemetryRuntimeError::CustomClientDisabled)?;
        let trace_exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_endpoint(signal_endpoint(endpoint, "/v1/traces"))
            .with_http_client(client.clone())
            .build()
            .map_err(TelemetryRuntimeError::TraceExporter)?;
        let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_http()
            .with_endpoint(signal_endpoint(endpoint, "/v1/metrics"))
            .with_http_client(client)
            .build()
            .map_err(TelemetryRuntimeError::MetricExporter)?;
        Ok(Self::from_exporters(
            config,
            trace_exporter,
            metric_exporter,
        ))
    }

    // Inputs: validated config plus fully constructed trace and metric exporters.
    // Outputs: enabled runtime with global metric provider installed.
    // Logic: share resource identity and apply explicit queue/batch/interval policy.
    fn from_exporters(
        config: &TelemetryConfig,
        trace_exporter: opentelemetry_otlp::SpanExporter,
        metric_exporter: opentelemetry_otlp::MetricExporter,
    ) -> Self {
        let resource = Resource::builder_empty()
            .with_service_name("portunus-daemon")
            .build();
        let batch = BatchConfigBuilder::default()
            .with_max_queue_size(config.max_trace_queue())
            .with_max_export_batch_size(config.max_export_batch())
            .build();
        let processor = BatchSpanProcessor::builder(trace_exporter)
            .with_batch_config(batch)
            .build();
        let trace = SdkTracerProvider::builder()
            .with_resource(resource.clone())
            .with_span_processor(processor)
            .build();
        let reader = PeriodicReader::builder(metric_exporter)
            .with_interval(config.metric_interval())
            .build();
        let metrics = SdkMeterProvider::builder()
            .with_resource(resource)
            .with_reader(reader)
            .build();
        global::set_meter_provider(metrics.clone());
        Self {
            providers: Providers::Enabled { trace, metrics },
        }
    }

    /// Inputs: shared runtime.
    /// Outputs: whether provider workers and exporters exist.
    /// Logic: inspect lifecycle variant without exposing exporter configuration.
    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        matches!(self.providers, Providers::Enabled { .. })
    }

    /// Inputs: shared runtime before subscriber installation.
    /// Outputs: daemon tracer tied to the owned provider, or absence when disabled.
    /// Logic: create one instrumentation scope without changing global trace policy.
    #[must_use]
    pub fn tracer(&self) -> Option<SdkTracer> {
        match &self.providers {
            Providers::Disabled => None,
            Providers::Enabled { trace, .. } => Some(trace.tracer("portunus-daemon")),
        }
    }

    /// Inputs: owned runtime after server draining finishes.
    /// Outputs: successful flush/shutdown or first trace/metric SDK failure.
    /// Logic: stop traces first, then metrics; disabled mode is an inert success.
    /// # Errors
    /// Returns a signal-specific provider shutdown error.
    pub fn shutdown(self) -> Result<(), TelemetryRuntimeError> {
        match self.providers {
            Providers::Disabled => Ok(()),
            Providers::Enabled { trace, metrics } => {
                trace
                    .shutdown()
                    .map_err(TelemetryRuntimeError::TraceShutdown)?;
                metrics
                    .shutdown()
                    .map_err(TelemetryRuntimeError::MetricShutdown)
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum TelemetryRuntimeError {
    #[error("custom OTLP client requires enabled telemetry configuration")]
    CustomClientDisabled,
    #[error("OTLP trace exporter could not be built: {0}")]
    TraceExporter(opentelemetry_otlp::ExporterBuildError),
    #[error("OTLP metric exporter could not be built: {0}")]
    MetricExporter(opentelemetry_otlp::ExporterBuildError),
    #[error("trace provider shutdown failed: {0}")]
    TraceShutdown(OTelSdkError),
    #[error("metric provider shutdown failed: {0}")]
    MetricShutdown(OTelSdkError),
}

// Inputs: validated bounded base endpoint and standard absolute signal path.
// Outputs: signal-specific OTLP/HTTP endpoint.
// Logic: strip trailing slashes once so programmatic builder configuration routes
// traces and metrics separately rather than sending both to the base URI.
fn signal_endpoint(base: &str, path: &str) -> String {
    format!("{}{path}", base.trim_end_matches('/'))
}
