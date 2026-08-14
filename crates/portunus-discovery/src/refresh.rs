//! TTL-aware caching and single-flight refresh for arbitrary discovery providers.
//!
//! One bounded snapshot is retained per namespace. A short global lock resolves
//! the namespace cell, while a separate asynchronous lock serializes only that
//! namespace's refresh. Unrelated namespaces therefore continue independently.
//!
//! ```text
//! caller limit ──> cached full snapshot ──> projected snapshot
//!                         ^
//! concurrent misses ── per-key lock ──> one provider refresh
//! ```
//!
//! This module owns cache and refresh policy only. It does not perform transport
//! I/O, retry failed providers, interpret namespaces, or install a log subscriber.

use crate::{DiscoverOptions, DiscoveryError, DiscoveryProvider, DiscoverySnapshot};
use async_trait::async_trait;
use std::{collections::BTreeMap, sync::Arc, time::Instant};
use tokio::sync::{Mutex, MutexGuard};
use tracing::{debug, trace};

type SnapshotCell = Arc<Mutex<Option<DiscoverySnapshot>>>;

/// A provider decorator that caches TTL-bound results and coalesces refreshes.
pub struct RefreshingProvider {
    inner: Arc<dyn DiscoveryProvider>,
    refresh_max_endpoints: usize,
    entries: Mutex<BTreeMap<Vec<u8>, SnapshotCell>>,
}

impl RefreshingProvider {
    /// Creates a bounded refresh decorator around any provider implementation.
    ///
    /// **Inputs:** Shared provider and positive maximum endpoints retained per key.
    ///
    /// **Outputs:** An empty cache, or a stable error for zero retained capacity.
    ///
    /// **Logic:** Refresh at the configured ceiling so a low-limit caller cannot
    /// poison later consumers; each response is projected to its caller's limit.
    ///
    /// # Errors
    ///
    /// Returns [`DiscoveryError::InvalidEndpointLimit`] when capacity is zero.
    pub fn new(
        inner: Arc<dyn DiscoveryProvider>,
        refresh_max_endpoints: usize,
    ) -> Result<Self, DiscoveryError> {
        if refresh_max_endpoints == 0 {
            return Err(DiscoveryError::InvalidEndpointLimit);
        }
        Ok(Self {
            inner,
            refresh_max_endpoints,
            entries: Mutex::new(BTreeMap::new()),
        })
    }

    /// Finds or allocates the synchronization cell for one namespace.
    ///
    /// **Inputs:** Borrowed opaque namespace bytes.
    ///
    /// **Outputs:** Shared per-key cell; allocation occurs only on the first lookup.
    ///
    /// **Logic:** Hold the global map lock only for lookup/insertion, never provider I/O.
    async fn cell(&self, namespace: &[u8]) -> SnapshotCell {
        let mut entries = self.entries.lock().await;
        entries
            .entry(namespace.to_vec())
            .or_insert_with(|| Arc::new(Mutex::new(None)))
            .clone()
    }
}

#[async_trait]
impl DiscoveryProvider for RefreshingProvider {
    /// Resolves a namespace from a fresh cache entry or one coalesced refresh.
    ///
    /// **Inputs:** Borrowed namespace and caller-owned controls for this wait.
    ///
    /// **Outputs:** A caller-limited snapshot, or common/provider error unchanged.
    ///
    /// **Logic:** Validate controls, race the per-key lock against cancellation and
    /// deadline, recheck TTL after acquiring it, then refresh at cache capacity.
    async fn discover(
        &self,
        namespace: &[u8],
        options: DiscoverOptions,
    ) -> Result<DiscoverySnapshot, DiscoveryError> {
        options.validate()?;
        check_controls(&options)?;
        let cell = self.cell(namespace).await;
        let mut entry = lock_until(&cell, &options).await?;
        check_controls(&options)?;

        if let Some(snapshot) = entry
            .as_ref()
            .filter(|item| item.valid_until() > Instant::now())
        {
            trace!(namespace_bytes = namespace.len(), "discovery cache hit");
            return Ok(project(snapshot, options.max_endpoints()));
        }

        debug!(
            namespace_bytes = namespace.len(),
            "refreshing discovery cache"
        );
        let refresh_options = DiscoverOptions::new(
            options.deadline(),
            self.refresh_max_endpoints,
            options.cancellation().clone(),
        );
        let snapshot = self.inner.discover(namespace, refresh_options).await?;
        let projected = project(&snapshot, options.max_endpoints());
        *entry = Some(snapshot);
        drop(entry);
        Ok(projected)
    }
}

/// Acquires one namespace lock while honoring caller termination controls.
///
/// **Inputs:** Shared cache cell and request options with absolute deadline/token.
///
/// **Outputs:** Exclusive cell guard, cancellation, or deadline error.
///
/// **Logic:** Race lock admission against both terminal signals; no polling occurs.
async fn lock_until<'a>(
    cell: &'a SnapshotCell,
    options: &DiscoverOptions,
) -> Result<MutexGuard<'a, Option<DiscoverySnapshot>>, DiscoveryError> {
    tokio::select! {
        guard = cell.lock() => Ok(guard),
        () = options.cancellation().cancelled() => Err(DiscoveryError::Cancelled),
        () = tokio::time::sleep_until(options.deadline().into()) => {
            Err(DiscoveryError::DeadlineExceeded)
        }
    }
}

/// Rejects requests already terminated before cache or provider work.
///
/// **Inputs:** Validated request controls and the current monotonic time.
///
/// **Outputs:** Unit or stable cancellation/deadline error with cancellation priority.
///
/// **Logic:** Match adapter behavior and prevent stale cache hits after termination.
fn check_controls(options: &DiscoverOptions) -> Result<(), DiscoveryError> {
    if options.cancellation().is_cancelled() {
        Err(DiscoveryError::Cancelled)
    } else if Instant::now() >= options.deadline() {
        Err(DiscoveryError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

/// Copies a cached snapshot under one caller's endpoint admission ceiling.
///
/// **Inputs:** Shared full-capacity snapshot and positive maximum endpoint count.
///
/// **Outputs:** Owned snapshot retaining namespace, TTL, provenance, and prefix order.
///
/// **Logic:** Copy only admitted endpoint metadata; payloads and transports are absent.
fn project(snapshot: &DiscoverySnapshot, max_endpoints: usize) -> DiscoverySnapshot {
    let admitted = snapshot.endpoints().len().min(max_endpoints);
    DiscoverySnapshot::new(
        snapshot.namespace().to_vec(),
        snapshot.endpoints()[..admitted].to_vec(),
        snapshot.valid_until(),
        snapshot.source(),
    )
}
