use async_trait::async_trait;
use portunus_discovery::{
    DiscoverOptions, DiscoveryError, DiscoveryProvider, DiscoverySnapshot, Endpoint,
};
use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

struct RecordingProvider;

#[async_trait]
impl DiscoveryProvider for RecordingProvider {
    // Inputs: opaque namespace bytes and bounded discovery options.
    // Outputs: one deterministic endpoint or cancellation/deadline failure.
    // Logic: exercise the object-safe public provider contract without transport I/O.
    async fn discover(
        &self,
        namespace: &[u8],
        options: DiscoverOptions,
    ) -> Result<DiscoverySnapshot, DiscoveryError> {
        options.validate()?;
        if options.cancellation().is_cancelled() {
            return Err(DiscoveryError::Cancelled);
        }
        if Instant::now() >= options.deadline() {
            return Err(DiscoveryError::DeadlineExceeded);
        }
        Ok(DiscoverySnapshot::new(
            namespace.to_vec(),
            vec![Endpoint::new("127.0.0.1:7000".parse().unwrap())],
            Instant::now() + Duration::from_secs(30),
            "recording",
        ))
    }
}

// Inputs: a provider behind a trait object, opaque namespace, and valid budgets.
// Outputs: a source-attributed snapshot containing the provider endpoint.
// Logic: prove transport implementations are substitutable at the public boundary.
#[tokio::test]
async fn dispatches_through_an_object_safe_provider_contract() {
    let provider: Box<dyn DiscoveryProvider> = Box::new(RecordingProvider);
    let options = DiscoverOptions::new(
        Instant::now() + Duration::from_secs(1),
        16,
        CancellationToken::new(),
    );
    let snapshot = provider.discover(b"service/orders", options).await.unwrap();

    assert_eq!(snapshot.namespace(), b"service/orders");
    assert_eq!(snapshot.source(), "recording");
    assert_eq!(
        snapshot.endpoints()[0].address(),
        "127.0.0.1:7000".parse::<SocketAddr>().unwrap()
    );
}

// Inputs: zero endpoint budget, elapsed deadline, and pre-cancelled token.
// Outputs: stable errors before provider-specific work can begin.
// Logic: make admission, deadline, and cancellation semantics uniform across adapters.
#[tokio::test]
async fn validates_common_discovery_controls() {
    let provider = RecordingProvider;
    let token = CancellationToken::new();
    let invalid = DiscoverOptions::new(Instant::now() + Duration::from_secs(1), 0, token);
    assert_eq!(
        provider.discover(b"service", invalid).await,
        Err(DiscoveryError::InvalidEndpointLimit)
    );

    let elapsed = DiscoverOptions::new(Instant::now(), 1, CancellationToken::new());
    assert_eq!(
        provider.discover(b"service", elapsed).await,
        Err(DiscoveryError::DeadlineExceeded)
    );

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let options = DiscoverOptions::new(Instant::now() + Duration::from_secs(1), 1, cancelled);
    assert_eq!(
        provider.discover(b"service", options).await,
        Err(DiscoveryError::Cancelled)
    );
}
