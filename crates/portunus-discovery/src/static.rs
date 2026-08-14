//! Deterministic in-memory discovery for configuration and simulation.
//!
//! [`StaticProvider`] maps opaque namespaces to configured endpoints while using
//! the same cancellation, deadline, deduplication, admission, TTL, and telemetry
//! contracts as unreliable transports. Results are sorted by socket address so
//! configuration order cannot leak nondeterminism into scheduling tests.
//!
//! This adapter performs no I/O, background refresh, health checking, retry,
//! global logging setup, or configuration-file parsing.

use crate::{DiscoverOptions, DiscoveryError, DiscoveryProvider, DiscoverySnapshot, Endpoint};
use async_trait::async_trait;
use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, Instant},
};

/// An immutable configured namespace-to-endpoint discovery provider.
#[derive(Debug, Clone)]
pub struct StaticProvider {
    ttl: Duration,
    namespaces: BTreeMap<Vec<u8>, Vec<Endpoint>>,
}

impl StaticProvider {
    /// Creates an empty provider with a configured snapshot lifetime.
    ///
    /// **Inputs:** TTL applied from each successful lookup's observation instant.
    ///
    /// **Outputs:** An empty provider ready for builder-style namespace insertion.
    ///
    /// **Logic:** Store duration independently from entries for deterministic reuse.
    #[must_use]
    pub const fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            namespaces: BTreeMap::new(),
        }
    }

    /// Adds or replaces endpoints for one opaque namespace.
    ///
    /// **Inputs:** Owned namespace bytes and any finite endpoint iterator.
    ///
    /// **Outputs:** The updated provider for declarative composition.
    ///
    /// **Logic:** Retain configured order initially; lookup policy sorts and deduplicates.
    #[must_use]
    pub fn with_namespace(
        mut self,
        namespace: Vec<u8>,
        endpoints: impl IntoIterator<Item = Endpoint>,
    ) -> Self {
        self.namespaces
            .insert(namespace, endpoints.into_iter().collect());
        self
    }
}

#[async_trait]
impl DiscoveryProvider for StaticProvider {
    /// Resolves configured endpoints under common discovery controls.
    ///
    /// **Inputs:** Opaque namespace and deadline/cancellation/admission options.
    ///
    /// **Outputs:** Deterministic unique endpoint snapshot, including an empty one
    /// for unknown namespaces, or a common validation/control error.
    ///
    /// **Logic:** Reject invalid, cancelled, or elapsed requests before lookup;
    /// sort through a set, truncate to budget, compute TTL, and emit bounded fields.
    async fn discover(
        &self,
        namespace: &[u8],
        options: DiscoverOptions,
    ) -> Result<DiscoverySnapshot, DiscoveryError> {
        options.validate()?;
        if options.cancellation().is_cancelled() {
            return Err(DiscoveryError::Cancelled);
        }
        let observed_at = Instant::now();
        if observed_at >= options.deadline() {
            return Err(DiscoveryError::DeadlineExceeded);
        }

        let candidates = self.namespaces.get(namespace).map_or(0, Vec::len);
        let mut endpoints: Vec<_> = self
            .namespaces
            .get(namespace)
            .into_iter()
            .flatten()
            .copied()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        endpoints.truncate(options.max_endpoints());
        let valid_until = observed_at.checked_add(self.ttl).ok_or_else(|| {
            DiscoveryError::Provider("static TTL exceeds monotonic clock range".to_owned())
        })?;

        tracing::debug!(
            provider = "static",
            namespace_bytes = namespace.len(),
            candidates,
            admitted = endpoints.len(),
            ttl_millis = self.ttl.as_millis(),
            "discovery snapshot resolved"
        );
        Ok(DiscoverySnapshot::new(
            namespace.to_vec(),
            endpoints,
            valid_until,
            "static",
        ))
    }
}
