//! Fixed-cardinality operational metric instruments for the daemon boundary.
//!
//! One monotonic counter records control request admission outcomes using a closed
//! three-value attribute. Instruments come from the global meter provider, which is
//! no-op when export is disabled and OTLP-backed when the runtime installs metrics.
//!
//! This module does not include request IDs, paths, credentials, error messages,
//! protocol payloads, dynamic method names, histograms, or exporter configuration.

use opentelemetry::{global, metrics::Counter, KeyValue};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionOutcome {
    Accepted,
    Unauthenticated,
    Overloaded,
}

impl AdmissionOutcome {
    // Inputs: closed admission outcome value.
    // Outputs: stable low-cardinality OpenTelemetry attribute value.
    // Logic: centralize labels so callers cannot inject unbounded dimensions.
    const fn label(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Unauthenticated => "unauthenticated",
            Self::Overloaded => "overloaded",
        }
    }
}

#[derive(Clone, Debug)]
pub struct OperationalMetrics {
    admissions: Counter<u64>,
}

impl OperationalMetrics {
    /// Inputs: currently installed global meter provider.
    /// Outputs: cloneable fixed-cardinality daemon instruments.
    /// Logic: build the same instrument in disabled/no-op and enabled/exported modes.
    #[must_use]
    pub fn global() -> Self {
        let meter = global::meter("portunus-daemon");
        Self {
            admissions: meter
                .u64_counter("portunus.control.admissions")
                .with_description("Control-plane requests by admission outcome")
                .build(),
        }
    }

    /// Inputs: one closed admission outcome.
    /// Outputs: monotonic counter increment with one stable attribute.
    /// Logic: record exactly one request result without payload-derived dimensions.
    pub fn record_admission(&self, outcome: AdmissionOutcome) {
        self.admissions
            .add(1, &[KeyValue::new("outcome", outcome.label())]);
    }
}
