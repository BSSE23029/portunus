//! Validated bounded OpenTelemetry export configuration.
//!
//! Export is explicitly disabled or targets one OTLP/HTTP endpoint no longer than
//! 2,048 bytes. Trace queue and batch counts are nonzero, batch never exceeds queue,
//! and metric interval is positive milliseconds. Debug output redacts the endpoint.
//!
//! This module currently owns configuration only. It does not install subscribers,
//! construct exporters, read environment variables, emit instruments, or perform I/O.

use std::{fmt, time::Duration};
use thiserror::Error;

mod instruments;
mod runtime;
pub use instruments::{AdmissionOutcome, OperationalMetrics};
pub use runtime::{TelemetryRuntime, TelemetryRuntimeError};

const DEFAULT_TRACE_QUEUE: usize = 2_048;
const DEFAULT_EXPORT_BATCH: usize = 512;
const DEFAULT_METRIC_INTERVAL_MS: u64 = 10_000;
const MAX_ENDPOINT_BYTES: usize = 2_048;

#[derive(Clone, Eq, PartialEq)]
pub struct TelemetryConfig {
    endpoint: Option<String>,
    max_trace_queue: usize,
    max_export_batch: usize,
    metric_interval: Duration,
}

impl TelemetryConfig {
    /// Inputs: no endpoint or exporter resources.
    /// Outputs: explicit disabled telemetry configuration.
    /// Logic: retain safe default bounds even though disabled mode allocates nothing.
    #[must_use]
    pub const fn disabled() -> Self {
        Self {
            endpoint: None,
            max_trace_queue: DEFAULT_TRACE_QUEUE,
            max_export_batch: DEFAULT_EXPORT_BATCH,
            metric_interval: Duration::from_millis(DEFAULT_METRIC_INTERVAL_MS),
        }
    }

    /// Inputs: OTLP/HTTP endpoint, trace queue/batch counts, and metric interval.
    /// Outputs: validated enabled exporter configuration.
    /// Logic: bound retained strings and queues, then validate cross-field capacity.
    /// # Errors
    /// Returns stable endpoint, zero-bound, or batch-over-queue errors.
    pub fn otlp(
        endpoint: &str,
        max_trace_queue: usize,
        max_export_batch: usize,
        metric_interval: Duration,
    ) -> Result<Self, TelemetryError> {
        validate_endpoint(endpoint)?;
        if max_trace_queue == 0 {
            return Err(TelemetryError::ZeroTraceQueue);
        }
        if max_export_batch == 0 {
            return Err(TelemetryError::ZeroExportBatch);
        }
        if max_export_batch > max_trace_queue {
            return Err(TelemetryError::ExportBatchExceedsQueue {
                batch: max_export_batch,
                queue: max_trace_queue,
            });
        }
        if metric_interval.is_zero() {
            return Err(TelemetryError::ZeroMetricInterval);
        }
        Ok(Self {
            endpoint: Some(endpoint.into()),
            max_trace_queue,
            max_export_batch,
            metric_interval,
        })
    }

    /// Inputs: optional endpoint, queue, batch, and millisecond interval strings.
    /// Outputs: disabled mode or validated enabled policy using documented defaults.
    /// Logic: reject orphan options, parse each decimal independently, then delegate
    /// all boundary/cross-field validation to [`Self::otlp`].
    /// # Errors
    /// Returns stable source parsing, orphan-option, or policy validation errors.
    pub fn from_sources(
        endpoint: Option<&str>,
        max_trace_queue: Option<&str>,
        max_export_batch: Option<&str>,
        metric_interval_ms: Option<&str>,
    ) -> Result<Self, TelemetryError> {
        let Some(endpoint) = endpoint else {
            if max_trace_queue.is_some()
                || max_export_batch.is_some()
                || metric_interval_ms.is_some()
            {
                return Err(TelemetryError::OptionsWithoutEndpoint);
            }
            return Ok(Self::disabled());
        };
        let queue = parse_usize(
            max_trace_queue,
            DEFAULT_TRACE_QUEUE,
            TelemetryError::InvalidTraceQueue,
        )?;
        let batch = parse_usize(
            max_export_batch,
            DEFAULT_EXPORT_BATCH,
            TelemetryError::InvalidExportBatch,
        )?;
        let interval = parse_u64(
            metric_interval_ms,
            DEFAULT_METRIC_INTERVAL_MS,
            TelemetryError::InvalidMetricInterval,
        )?;
        Self::otlp(endpoint, queue, batch, Duration::from_millis(interval))
    }

    /// Inputs: shared configuration.
    /// Outputs: whether exporters should be constructed.
    /// Logic: endpoint presence is the sole enabled-mode discriminator.
    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        self.endpoint.is_some()
    }

    /// Inputs: shared enabled or disabled configuration.
    /// Outputs: inclusive maximum queued spans.
    /// Logic: expose resource policy without endpoint or mutation access.
    #[must_use]
    pub const fn max_trace_queue(&self) -> usize {
        self.max_trace_queue
    }

    /// Inputs: shared enabled or disabled configuration.
    /// Outputs: inclusive maximum spans per export batch.
    /// Logic: expose validated batching policy without exporter ownership.
    #[must_use]
    pub const fn max_export_batch(&self) -> usize {
        self.max_export_batch
    }

    /// Inputs: shared enabled or disabled configuration.
    /// Outputs: positive periodic metric export interval.
    /// Logic: expose cadence without ambient clock access.
    #[must_use]
    pub const fn metric_interval(&self) -> Duration {
        self.metric_interval
    }

    // Inputs: shared validated configuration.
    // Outputs: borrowed enabled endpoint or absence for disabled mode.
    // Logic: expose credential-bearing data only to the sibling runtime builder.
    pub(super) fn endpoint(&self) -> Option<&str> {
        self.endpoint.as_deref()
    }
}

impl fmt::Debug for TelemetryConfig {
    // Inputs: formatter and possibly credential-bearing endpoint configuration.
    // Outputs: mode and bounds only; endpoint bytes are always redacted.
    // Logic: make routine startup diagnostics safe while retaining resource policy.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TelemetryConfig")
            .field("enabled", &self.is_enabled())
            .field("max_trace_queue", &self.max_trace_queue)
            .field("max_export_batch", &self.max_export_batch)
            .field("metric_interval", &self.metric_interval)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum TelemetryError {
    #[error("OTLP endpoint must be an http:// or https:// URI without whitespace")]
    InvalidEndpoint,
    #[error("OTLP endpoint exceeds {limit} bytes")]
    EndpointTooLong { limit: usize },
    #[error("trace queue must be an unsigned decimal integer")]
    InvalidTraceQueue,
    #[error("export batch must be an unsigned decimal integer")]
    InvalidExportBatch,
    #[error("metric interval must be unsigned decimal milliseconds")]
    InvalidMetricInterval,
    #[error("telemetry bounds require an OTLP endpoint")]
    OptionsWithoutEndpoint,
    #[error("trace queue must be greater than zero")]
    ZeroTraceQueue,
    #[error("export batch must be greater than zero")]
    ZeroExportBatch,
    #[error("export batch {batch} exceeds trace queue {queue}")]
    ExportBatchExceedsQueue { batch: usize, queue: usize },
    #[error("metric export interval must be greater than zero")]
    ZeroMetricInterval,
}

// Inputs: borrowed endpoint string.
// Outputs: success for bounded HTTP(S) endpoint or stable validation error.
// Logic: reject empty suffixes, whitespace/control bytes, and excess retained length.
fn validate_endpoint(endpoint: &str) -> Result<(), TelemetryError> {
    if endpoint.len() > MAX_ENDPOINT_BYTES {
        return Err(TelemetryError::EndpointTooLong {
            limit: MAX_ENDPOINT_BYTES,
        });
    }
    let suffix = endpoint
        .strip_prefix("http://")
        .or_else(|| endpoint.strip_prefix("https://"));
    if suffix.is_none_or(str::is_empty) || endpoint.bytes().any(|byte| byte.is_ascii_whitespace()) {
        return Err(TelemetryError::InvalidEndpoint);
    }
    Ok(())
}

// Inputs: optional decimal usize source, default, and stable parse error.
// Outputs: parsed value or default when absent.
// Logic: preserve field-specific diagnostics without retaining source strings.
fn parse_usize(
    source: Option<&str>,
    default: usize,
    error: TelemetryError,
) -> Result<usize, TelemetryError> {
    source.map_or(Ok(default), |value| value.parse().map_err(|_| error))
}

// Inputs: optional decimal u64 source, default, and stable parse error.
// Outputs: parsed value or default when absent.
// Logic: preserve field-specific diagnostics without retaining source strings.
fn parse_u64(
    source: Option<&str>,
    default: u64,
    error: TelemetryError,
) -> Result<u64, TelemetryError> {
    source.map_or(Ok(default), |value| value.parse().map_err(|_| error))
}
