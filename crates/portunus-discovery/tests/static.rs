use portunus_discovery::{
    DiscoverOptions, DiscoveryError, DiscoveryProvider, Endpoint, StaticProvider,
};
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

// Inputs: duplicate, unsorted static endpoints and a two-result admission ceiling.
// Outputs: deterministic unique endpoints, source attribution, and future expiry.
// Logic: prove reusable policy behavior without DNS, UDP, or ambient configuration.
#[tokio::test]
async fn deduplicates_orders_and_limits_static_endpoints() {
    let before = Instant::now();
    let provider = StaticProvider::new(Duration::from_secs(30)).with_namespace(
        b"service/orders".to_vec(),
        [
            Endpoint::new("127.0.0.1:7002".parse().unwrap()),
            Endpoint::new("127.0.0.1:7001".parse().unwrap()),
            Endpoint::new("127.0.0.1:7002".parse().unwrap()),
            Endpoint::new("127.0.0.1:7003".parse().unwrap()),
        ],
    );
    let options = DiscoverOptions::new(
        Instant::now() + Duration::from_secs(1),
        2,
        CancellationToken::new(),
    );
    let snapshot = provider.discover(b"service/orders", options).await.unwrap();

    assert_eq!(
        snapshot.endpoints(),
        &[
            Endpoint::new("127.0.0.1:7001".parse().unwrap()),
            Endpoint::new("127.0.0.1:7002".parse().unwrap()),
        ]
    );
    assert_eq!(snapshot.source(), "static");
    assert!(snapshot.valid_until() >= before + Duration::from_secs(30));
}

// Inputs: unknown namespace, elapsed deadline, and cancelled request.
// Outputs: empty successful snapshot or common control errors before lookup work.
// Logic: make static behavior representative of every provider implementation.
#[tokio::test]
async fn handles_absence_deadlines_and_cancellation() {
    let provider = StaticProvider::new(Duration::from_secs(10));
    let valid = DiscoverOptions::new(
        Instant::now() + Duration::from_secs(1),
        1,
        CancellationToken::new(),
    );
    assert!(provider
        .discover(b"missing", valid)
        .await
        .unwrap()
        .endpoints()
        .is_empty());

    let elapsed = DiscoverOptions::new(Instant::now(), 1, CancellationToken::new());
    assert_eq!(
        provider.discover(b"missing", elapsed).await,
        Err(DiscoveryError::DeadlineExceeded)
    );

    let token = CancellationToken::new();
    token.cancel();
    let cancelled = DiscoverOptions::new(Instant::now() + Duration::from_secs(1), 1, token);
    assert_eq!(
        provider.discover(b"missing", cancelled).await,
        Err(DiscoveryError::Cancelled)
    );
}
