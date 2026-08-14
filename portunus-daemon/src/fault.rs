//! Deterministic bounded fault injection at daemon operation boundaries.
//!
//! Stable fault points identify control operations without request payloads. A script
//! retains at most its configured inclusive rule count and consumes an exact failure
//! count per point. Checks are serialized and deterministic; disabled mode is inert.
//!
//! This module does not use randomness, clocks, environment variables, panic, network
//! faults, disk corruption, or global mutable state. The daemon chooses the injector.

use std::{collections::BTreeMap, sync::Mutex};
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum FaultPoint {
    AddTransfer,
    StopTransfer,
    StreamMetrics,
    UpdateConfig,
}

impl FaultPoint {
    pub const ALL: [Self; 4] = [
        Self::AddTransfer,
        Self::StopTransfer,
        Self::StreamMetrics,
        Self::UpdateConfig,
    ];
}

pub trait FaultInjector: Send + Sync {
    /// Inputs: stable operation boundary without payload or credential context.
    /// Outputs: success or one deterministic injected failure.
    /// Logic: implementations decide locally and must not panic or perform I/O.
    /// # Errors
    /// Returns [`InjectedFault`] only when the named boundary is deliberately armed.
    fn check(&self, point: FaultPoint) -> Result<(), InjectedFault>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct DisabledFaults;

impl FaultInjector for DisabledFaults {
    /// Inputs: any stable fault point.
    /// Outputs: unconditional success.
    /// Logic: provide the explicit production default with zero shared state.
    fn check(&self, _point: FaultPoint) -> Result<(), InjectedFault> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct FaultScript {
    max_rules: usize,
    remaining: Mutex<BTreeMap<FaultPoint, u32>>,
}

impl FaultScript {
    /// Inputs: nonzero inclusive number of distinct simultaneously armed points.
    /// Outputs: empty deterministic script or stable zero-limit error.
    /// Logic: validate retained-state capacity before allocating the rule map.
    /// # Errors
    /// Returns [`FaultScriptError::ZeroRuleLimit`] for a zero ceiling.
    pub const fn new(max_rules: usize) -> Result<Self, FaultScriptError> {
        if max_rules == 0 {
            return Err(FaultScriptError::ZeroRuleLimit);
        }
        Ok(Self {
            max_rules,
            remaining: Mutex::new(BTreeMap::new()),
        })
    }

    /// Inputs: stable fault point and nonzero exact number of future failures.
    /// Outputs: inserted/replaced rule or stable count/capacity error.
    /// Logic: replacing an existing point preserves rule capacity; new points enforce
    /// the inclusive distinct-rule ceiling before mutation.
    /// # Errors
    /// Returns zero-failure or rule-limit errors without changing existing rules.
    pub fn arm(&self, point: FaultPoint, failures: u32) -> Result<(), FaultScriptError> {
        if failures == 0 {
            return Err(FaultScriptError::ZeroFailures);
        }
        let mut rules = self
            .remaining
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !rules.contains_key(&point) && rules.len() == self.max_rules {
            return Err(FaultScriptError::RuleLimitExceeded {
                limit: self.max_rules,
            });
        }
        rules.insert(point, failures);
        drop(rules);
        Ok(())
    }
}

impl FaultInjector for FaultScript {
    /// Inputs: stable operation boundary.
    /// Outputs: one consumed injected failure while armed, otherwise success.
    /// Logic: decrement atomically under the bounded map lock and remove exhausted
    /// rules so their retained-state slot can be reused.
    fn check(&self, point: FaultPoint) -> Result<(), InjectedFault> {
        let mut rules = self
            .remaining
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(remaining) = rules.get_mut(&point) else {
            return Ok(());
        };
        *remaining -= 1;
        if *remaining == 0 {
            rules.remove(&point);
        }
        drop(rules);
        tracing::warn!(fault_point = ?point, "daemon fault injected");
        Err(InjectedFault { point })
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[error("fault injected at {point:?}")]
pub struct InjectedFault {
    pub point: FaultPoint,
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum FaultScriptError {
    #[error("fault rule limit must be greater than zero")]
    ZeroRuleLimit,
    #[error("injected failure count must be greater than zero")]
    ZeroFailures,
    #[error("fault rule limit {limit} is exhausted")]
    RuleLimitExceeded { limit: usize },
}
