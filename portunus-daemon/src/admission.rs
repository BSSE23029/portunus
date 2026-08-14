//! Bounded fail-fast admission for control-plane requests.
//!
//! A shared nonzero semaphore defines the inclusive concurrent-call ceiling. Each
//! accepted request carries an owned permit in its extensions for the complete RPC
//! lifetime; the first request above the ceiling receives `ResourceExhausted`.
//!
//! This module does not queue callers, authorize identities, rate-limit bytes,
//! cover health/reflection, choose retry delays, or install global observability.

use crate::auth::{AuthConfig, AuthInterceptor};
use crate::telemetry::{AdmissionOutcome, OperationalMetrics};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::{service::Interceptor, Request, Status};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdmissionConfig {
    limit: usize,
}

impl AdmissionConfig {
    /// Inputs: inclusive maximum number of concurrent accepted control requests.
    /// Outputs: validated admission configuration or stable zero-limit error.
    /// Logic: reject disabled topology before allocating shared semaphore state.
    /// # Errors
    /// Returns [`AdmissionError::ZeroLimit`] when `limit` is zero.
    pub const fn new(limit: usize) -> Result<Self, AdmissionError> {
        if limit == 0 {
            return Err(AdmissionError::ZeroLimit);
        }
        Ok(Self { limit })
    }

    /// Inputs: optional borrowed decimal limit from a selected configuration source.
    /// Outputs: validated override or deterministic default of 128 concurrent calls.
    /// Logic: parse strictly and pass every value through the same nonzero validator.
    /// # Errors
    /// Returns invalid-decimal or zero-limit errors without silently using defaults.
    pub fn from_source(source: Option<&str>) -> Result<Self, AdmissionError> {
        let Some(source) = source else {
            return Self::new(128);
        };
        let limit = source
            .parse::<usize>()
            .map_err(|_| AdmissionError::InvalidLimit)?;
        Self::new(limit)
    }
}

#[derive(Clone, Debug)]
pub struct AdmissionInterceptor {
    permits: Arc<Semaphore>,
    metrics: OperationalMetrics,
}

impl AdmissionInterceptor {
    /// Inputs: validated concurrent request ceiling.
    /// Outputs: cloneable interceptor sharing one process-local admission pool.
    /// Logic: allocate exactly the configured count of owned semaphore permits.
    #[must_use]
    pub fn new(config: AdmissionConfig) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(config.limit)),
            metrics: OperationalMetrics::global(),
        }
    }

    // Inputs: authenticated request rejected before capacity admission.
    // Outputs: one fixed-cardinality unauthenticated metric increment.
    // Logic: share the same counter instance owned by the admission boundary.
    fn record_unauthenticated(&self) {
        self.metrics
            .record_admission(AdmissionOutcome::Unauthenticated);
    }
}

impl Interceptor for AdmissionInterceptor {
    // Inputs: metadata request before body dispatch.
    // Outputs: request retaining one permit or immediate resource-exhausted status.
    // Logic: never wait; attach RAII ownership to extensions for the RPC lifetime.
    #[allow(clippy::result_large_err)]
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        let Ok(permit) = Arc::clone(&self.permits).try_acquire_owned() else {
            self.metrics.record_admission(AdmissionOutcome::Overloaded);
            return Err(Status::resource_exhausted("control plane is overloaded"));
        };
        request
            .extensions_mut()
            .insert(RequestPermit(Arc::new(permit)));
        self.metrics.record_admission(AdmissionOutcome::Accepted);
        Ok(request)
    }
}

#[derive(Clone)]
struct RequestPermit(#[allow(dead_code)] Arc<OwnedSemaphorePermit>);

#[derive(Clone, Debug)]
pub struct ControlInterceptor {
    auth: AuthInterceptor,
    admission: AdmissionInterceptor,
}

impl ControlInterceptor {
    /// Inputs: validated authentication and concurrent admission policies.
    /// Outputs: combined interceptor applying authentication before capacity use.
    /// Logic: rejected credentials never consume scarce in-flight request permits.
    #[must_use]
    pub fn new(auth: AuthConfig, admission: AdmissionConfig) -> Self {
        Self {
            auth: AuthInterceptor::new(auth),
            admission: AdmissionInterceptor::new(admission),
        }
    }
}

impl Interceptor for ControlInterceptor {
    // Inputs: metadata request before control service dispatch.
    // Outputs: authenticated admitted request or first policy rejection.
    // Logic: apply authentication then attach one shared admission permit.
    #[allow(clippy::result_large_err)]
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        match self.auth.call(request) {
            Ok(request) => self.admission.call(request),
            Err(error) => {
                self.admission.record_unauthenticated();
                Err(error)
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum AdmissionError {
    #[error("concurrent request limit must be greater than zero")]
    ZeroLimit,
    #[error("concurrent request limit must be an unsigned decimal integer")]
    InvalidLimit,
}
