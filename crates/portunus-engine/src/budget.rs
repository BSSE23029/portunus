//! Multi-dimensional orchestration admission and resource ownership.
//!
//! A pool independently bounds active task slots and bytes retained in network,
//! verification, and disk stages. Every request acquires all dimensions or none;
//! one RAII permit releases them across success, failure, panic unwind, or task drop.
//! Limits are inclusive and requests are checked before shared state is touched.
//!
//! ```text
//! request ──network──> verification ──disk──> task slot ──> BudgetPermit
//!          partial acquisition on error/cancellation ─────> automatic release
//! ```
//!
//! This module owns admission only. It does not spawn work, schedule jobs, retry,
//! perform pipeline I/O, or install process-global observability policy.

use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tokio_util::sync::CancellationToken;

pub mod config;
pub use config::{BudgetConfig, BudgetError, ResourceKind, ResourceRequest};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BudgetSnapshot {
    pub active_tasks: u32,
    pub network_bytes: u32,
    pub verification_bytes: u32,
    pub disk_bytes: u32,
}

#[derive(Clone, Debug)]
pub struct BudgetPool {
    config: BudgetConfig,
    tasks: Arc<Semaphore>,
    network: Arc<Semaphore>,
    verification: Arc<Semaphore>,
    disk: Arc<Semaphore>,
}

impl BudgetPool {
    /// Inputs: validated immutable resource configuration.
    /// Outputs: cloneable pool with one semaphore per independent dimension.
    /// Logic: translate configured `u32` limits into bounded executor permits.
    #[must_use]
    pub fn new(config: BudgetConfig) -> Self {
        Self {
            tasks: Arc::new(Semaphore::new(config.max_tasks as usize)),
            network: Arc::new(Semaphore::new(config.network_bytes as usize)),
            verification: Arc::new(Semaphore::new(config.verification_bytes as usize)),
            disk: Arc::new(Semaphore::new(config.disk_bytes as usize)),
            config,
        }
    }

    /// Inputs: one task's three pipeline byte requirements.
    /// Outputs: immediate all-dimensional permit or stable rejection.
    /// Logic: validate first, then try dimensions in fixed order; local guard drops
    /// unwind every partial acquisition when a later dimension is saturated.
    /// # Errors
    /// Returns resource-specific too-large, saturated, or closed errors.
    pub fn try_admit(&self, request: ResourceRequest) -> Result<BudgetPermit, BudgetError> {
        self.validate_request(request)?;
        let network = try_many(&self.network, request.network_bytes)?;
        let verification = try_many(&self.verification, request.verification_bytes)?;
        let disk = try_many(&self.disk, request.disk_bytes)?;
        let task = Arc::clone(&self.tasks)
            .try_acquire_owned()
            .map_err(|error| map_try_error(&error))?;
        tracing::trace!(
            network_bytes = request.network_bytes,
            verification_bytes = request.verification_bytes,
            disk_bytes = request.disk_bytes,
            "engine budget admitted task"
        );
        Ok(BudgetPermit {
            _network: network,
            _verification: verification,
            _disk: disk,
            _task: task,
            request,
        })
    }

    /// Inputs: resource request and borrowed cooperative cancellation signal.
    /// Outputs: eventual all-dimensional permit or validation/cancellation failure.
    /// Logic: validate then wait in fixed order with cancellation priority; every
    /// previously acquired dimension is released when a later wait exits early.
    /// # Errors
    /// Returns too-large, cancelled, or closed errors.
    pub async fn admit(
        &self,
        request: ResourceRequest,
        cancellation: &CancellationToken,
    ) -> Result<BudgetPermit, BudgetError> {
        self.validate_request(request)?;
        let network = acquire_many(&self.network, request.network_bytes, cancellation).await?;
        let verification =
            acquire_many(&self.verification, request.verification_bytes, cancellation).await?;
        let disk = acquire_many(&self.disk, request.disk_bytes, cancellation).await?;
        let task = acquire_many(&self.tasks, 1, cancellation).await?;
        Ok(BudgetPermit {
            _network: network,
            _verification: verification,
            _disk: disk,
            _task: task,
            request,
        })
    }

    /// Inputs: shared pool state.
    /// Outputs: instantaneous used permits for all dimensions.
    /// Logic: subtract available capacity from immutable configured limits.
    #[must_use]
    pub fn snapshot(&self) -> BudgetSnapshot {
        BudgetSnapshot {
            active_tasks: used(self.config.max_tasks, &self.tasks),
            network_bytes: used(self.config.network_bytes, &self.network),
            verification_bytes: used(self.config.verification_bytes, &self.verification),
            disk_bytes: used(self.config.disk_bytes, &self.disk),
        }
    }

    // Inputs: one immutable resource request.
    // Outputs: success or first resource-specific request/limit details.
    // Logic: check in pipeline order before acquiring shared capacity.
    fn validate_request(&self, request: ResourceRequest) -> Result<(), BudgetError> {
        validate_dimension(
            ResourceKind::Network,
            request.network_bytes,
            self.config.network_bytes,
        )?;
        validate_dimension(
            ResourceKind::Verification,
            request.verification_bytes,
            self.config.verification_bytes,
        )?;
        validate_dimension(
            ResourceKind::Disk,
            request.disk_bytes,
            self.config.disk_bytes,
        )
    }
}

#[derive(Debug)]
pub struct BudgetPermit {
    _network: OwnedSemaphorePermit,
    _verification: OwnedSemaphorePermit,
    _disk: OwnedSemaphorePermit,
    _task: OwnedSemaphorePermit,
    request: ResourceRequest,
}

impl BudgetPermit {
    /// Inputs: shared admitted-task permit.
    /// Outputs: exact immutable resource request held by this permit.
    /// Logic: expose accounting without exposing semaphore mutation.
    #[must_use]
    pub const fn request(&self) -> ResourceRequest {
        self.request
    }
}

// Inputs: one resource kind, requested permits, and inclusive limit.
// Outputs: success or exact stable over-limit details.
// Logic: compare without touching shared admission state.
const fn validate_dimension(
    resource: ResourceKind,
    requested: u32,
    limit: u32,
) -> Result<(), BudgetError> {
    if requested > limit {
        return Err(BudgetError::RequestTooLarge {
            resource,
            requested,
            limit,
        });
    }
    Ok(())
}

// Inputs: shared semaphore and requested permits.
// Outputs: owned immediate permit or stable saturation/closed error.
// Logic: clone semaphore ownership so the returned guard is lifetime-independent.
fn try_many(semaphore: &Arc<Semaphore>, permits: u32) -> Result<OwnedSemaphorePermit, BudgetError> {
    Arc::clone(semaphore)
        .try_acquire_many_owned(permits)
        .map_err(|error| map_try_error(&error))
}

// Inputs: shared semaphore, permit count, and cooperative cancellation signal.
// Outputs: owned permit or cancellation/closed error.
// Logic: precheck then prioritize cancellation against executor wakeup.
async fn acquire_many(
    semaphore: &Arc<Semaphore>,
    permits: u32,
    cancellation: &CancellationToken,
) -> Result<OwnedSemaphorePermit, BudgetError> {
    if cancellation.is_cancelled() {
        return Err(BudgetError::Cancelled);
    }
    let wait = Arc::clone(semaphore).acquire_many_owned(permits);
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(BudgetError::Cancelled),
        result = wait => result.map_err(|_| BudgetError::Closed),
    }
}

// Inputs: executor-specific nonblocking semaphore failure.
// Outputs: stable engine saturation or closed error.
// Logic: keep Tokio error representation private to the budget boundary.
const fn map_try_error(error: &TryAcquireError) -> BudgetError {
    match error {
        TryAcquireError::NoPermits => BudgetError::Saturated,
        TryAcquireError::Closed => BudgetError::Closed,
    }
}

// Inputs: configured `u32` limit and corresponding semaphore.
// Outputs: currently used permits, conservatively saturated on conversion anomaly.
// Logic: convert available `usize` safely before saturating subtraction.
fn used(limit: u32, semaphore: &Semaphore) -> u32 {
    limit.saturating_sub(u32::try_from(semaphore.available_permits()).unwrap_or(u32::MAX))
}
