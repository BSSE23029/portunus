use async_trait::async_trait;
use portunus_discovery::{
    DiscoverOptions, DiscoveryError, DiscoveryProvider, DiscoverySnapshot, Endpoint,
    RefreshingProvider,
};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
struct ControlledProvider {
    calls: AtomicUsize,
    endpoints: Vec<Endpoint>,
    ttl: Duration,
    started: Notify,
    release: Notify,
    blocking: bool,
}

impl ControlledProvider {
    // Inputs: deterministic endpoint corpus, result lifetime, and blocking mode.
    // Outputs: a provider whose call count and completion are test-controlled.
    // Logic: isolate cache behavior from transports and wall-clock sleeps.
    fn new(endpoints: Vec<Endpoint>, ttl: Duration, blocking: bool) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            endpoints,
            ttl,
            started: Notify::new(),
            release: Notify::new(),
            blocking,
        }
    }
}

#[async_trait]
impl DiscoveryProvider for ControlledProvider {
    // Inputs: opaque namespace and wrapper-supplied discovery controls.
    // Outputs: a deterministic snapshot, optionally after explicit test release.
    // Logic: count refreshes and expose their start without relying on timing races.
    async fn discover(
        &self,
        namespace: &[u8],
        options: DiscoverOptions,
    ) -> Result<DiscoverySnapshot, DiscoveryError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.started.notify_one();
        if self.blocking {
            self.release.notified().await;
        }
        Ok(DiscoverySnapshot::new(
            namespace.to_vec(),
            self.endpoints[..self.endpoints.len().min(options.max_endpoints())].to_vec(),
            Instant::now() + self.ttl,
            "controlled",
        ))
    }
}

// Inputs: two result limits against one fresh cached namespace.
// Outputs: one upstream lookup and independently truncated consumer snapshots.
// Logic: prove a small caller does not poison the cache's configured capacity.
#[tokio::test]
async fn caches_until_ttl_and_projects_each_result_limit() {
    let inner = Arc::new(ControlledProvider::new(
        vec![
            Endpoint::new("127.0.0.1:7001".parse().unwrap()),
            Endpoint::new("127.0.0.1:7002".parse().unwrap()),
            Endpoint::new("127.0.0.1:7003".parse().unwrap()),
        ],
        Duration::from_secs(30),
        false,
    ));
    let provider = RefreshingProvider::new(inner.clone(), 3).unwrap();

    let first = provider.discover(b"orders", options(1)).await.unwrap();
    let second = provider.discover(b"orders", options(2)).await.unwrap();

    assert_eq!(first.endpoints().len(), 1);
    assert_eq!(second.endpoints().len(), 2);
    assert_eq!(second.source(), "controlled");
    assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
}

// Inputs: two concurrent misses for one namespace and an explicitly gated source.
// Outputs: both callers share one refresh and receive equivalent snapshots.
// Logic: verify per-namespace single-flight behavior without scheduler assumptions.
#[tokio::test]
async fn coalesces_concurrent_refreshes_for_one_namespace() {
    let inner = Arc::new(ControlledProvider::new(
        vec![Endpoint::new("127.0.0.1:7001".parse().unwrap())],
        Duration::from_secs(30),
        true,
    ));
    let provider = Arc::new(RefreshingProvider::new(inner.clone(), 4).unwrap());
    let first_provider = provider.clone();
    let first = tokio::spawn(async move { first_provider.discover(b"orders", options(4)).await });
    inner.started.notified().await;
    let second_provider = provider.clone();
    let second = tokio::spawn(async move { second_provider.discover(b"orders", options(4)).await });

    inner.release.notify_one();
    assert_eq!(
        first.await.unwrap().unwrap(),
        second.await.unwrap().unwrap()
    );
    assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
}

// Inputs: sequential lookups whose source snapshots expire immediately.
// Outputs: one upstream refresh per lookup rather than a stale cache hit.
// Logic: zero TTL makes the exclusive expiry boundary deterministic without sleep.
#[tokio::test]
async fn refreshes_at_the_ttl_boundary() {
    let inner = Arc::new(ControlledProvider::new(vec![], Duration::ZERO, false));
    let provider = RefreshingProvider::new(inner.clone(), 2).unwrap();

    provider.discover(b"orders", options(2)).await.unwrap();
    provider.discover(b"orders", options(2)).await.unwrap();

    assert_eq!(inner.calls.load(Ordering::SeqCst), 2);
}

// Inputs: one active refresh and a second caller cancelled while awaiting its key.
// Outputs: immediate cancellation for the waiter without starting another refresh.
// Logic: prove coalescing never hides request-scoped cooperative cancellation.
#[tokio::test]
async fn cancellation_interrupts_a_coalesced_waiter() {
    let inner = Arc::new(ControlledProvider::new(
        vec![],
        Duration::from_secs(30),
        true,
    ));
    let provider = Arc::new(RefreshingProvider::new(inner.clone(), 2).unwrap());
    let owner_provider = provider.clone();
    let owner = tokio::spawn(async move { owner_provider.discover(b"orders", options(2)).await });
    inner.started.notified().await;

    let token = CancellationToken::new();
    let waiter_options =
        DiscoverOptions::new(Instant::now() + Duration::from_secs(30), 2, token.clone());
    let waiter_provider = provider.clone();
    let waiter =
        tokio::spawn(async move { waiter_provider.discover(b"orders", waiter_options).await });
    token.cancel();

    assert_eq!(waiter.await.unwrap(), Err(DiscoveryError::Cancelled));
    assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
    inner.release.notify_one();
    owner.await.unwrap().unwrap();
}

// Inputs: zero cache capacity and an already-cancelled cache-hit request.
// Outputs: stable validation and cancellation errors without upstream work.
// Logic: common controls remain authoritative even when cached data exists.
#[tokio::test]
async fn rejects_invalid_capacity_and_cancelled_requests() {
    let inner = Arc::new(ControlledProvider::new(
        vec![],
        Duration::from_secs(30),
        false,
    ));
    assert!(matches!(
        RefreshingProvider::new(inner.clone(), 0),
        Err(DiscoveryError::InvalidEndpointLimit)
    ));
    let provider = RefreshingProvider::new(inner.clone(), 2).unwrap();
    provider.discover(b"orders", options(2)).await.unwrap();

    let token = CancellationToken::new();
    token.cancel();
    let cancelled = DiscoverOptions::new(Instant::now() + Duration::from_secs(10), 2, token);
    assert_eq!(
        provider.discover(b"orders", cancelled).await,
        Err(DiscoveryError::Cancelled)
    );
    assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
}

// Inputs: positive endpoint ceiling and a fresh cancellation token.
// Outputs: reusable request controls with a distant deterministic deadline.
// Logic: centralize boilerplate so each test emphasizes cache behavior.
fn options(max_endpoints: usize) -> DiscoverOptions {
    DiscoverOptions::new(
        Instant::now() + Duration::from_secs(30),
        max_endpoints,
        CancellationToken::new(),
    )
}
