//! Validated resource-budget values and stable admission errors.
//!
//! Limits use inclusive `u32` units so they map directly to Tokio semaphore permits.
//! Task, network, verification, and disk limits are independent and nonzero. Per-task
//! stage requests may be zero but never exceed their corresponding configured limit.
//!
//! This module contains values only; it does not allocate semaphores, wait, spawn,
//! schedule, retry, or install observability policy.

use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourceKind {
    Tasks,
    Network,
    Verification,
    Disk,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BudgetConfig {
    pub(super) max_tasks: u32,
    pub(super) network_bytes: u32,
    pub(super) verification_bytes: u32,
    pub(super) disk_bytes: u32,
}

impl BudgetConfig {
    /// Inputs: inclusive nonzero limits for tasks and three pipeline byte stages.
    /// Outputs: validated independent budgets or first stable zero-limit error.
    /// Logic: validate in pipeline ownership order before constructing shared state.
    /// # Errors
    /// Returns `ZeroLimit` with the first invalid resource kind.
    pub const fn new(
        max_tasks: u32,
        network_bytes: u32,
        verification_bytes: u32,
        disk_bytes: u32,
    ) -> Result<Self, BudgetError> {
        if max_tasks == 0 {
            return Err(BudgetError::ZeroLimit(ResourceKind::Tasks));
        }
        if network_bytes == 0 {
            return Err(BudgetError::ZeroLimit(ResourceKind::Network));
        }
        if verification_bytes == 0 {
            return Err(BudgetError::ZeroLimit(ResourceKind::Verification));
        }
        if disk_bytes == 0 {
            return Err(BudgetError::ZeroLimit(ResourceKind::Disk));
        }
        Ok(Self {
            max_tasks,
            network_bytes,
            verification_bytes,
            disk_bytes,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResourceRequest {
    pub network_bytes: u32,
    pub verification_bytes: u32,
    pub disk_bytes: u32,
}

impl ResourceRequest {
    /// Inputs: bytes retained by one task in each pipeline stage.
    /// Outputs: immutable request; zero means that stage retains no bytes.
    /// Logic: package independent dimensions for atomic pool admission.
    #[must_use]
    pub const fn new(network_bytes: u32, verification_bytes: u32, disk_bytes: u32) -> Self {
        Self {
            network_bytes,
            verification_bytes,
            disk_bytes,
        }
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum BudgetError {
    #[error("{0:?} resource limit must be greater than zero")]
    ZeroLimit(ResourceKind),
    #[error("{resource:?} request {requested} exceeds configured limit {limit}")]
    RequestTooLarge {
        resource: ResourceKind,
        requested: u32,
        limit: u32,
    },
    #[error("engine resource budget is saturated")]
    Saturated,
    #[error("engine resource admission was cancelled")]
    Cancelled,
    #[error("engine resource admission is closed")]
    Closed,
}
