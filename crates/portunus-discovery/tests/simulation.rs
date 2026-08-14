use portunus_discovery::{
    DiscoverOptions, DiscoveryProvider, Endpoint, RefreshingProvider, StaticProvider,
};
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

// Inputs: 8,192 deterministic records representing 4,096 unique endpoints.
// Outputs: stable deduplication and bounded cached projections without network I/O.
// Logic: exercise discovery scale through static transport, refresh, and admission layers.
#[tokio::test]
async fn simulates_thousands_of_endpoints_with_bounded_admission() {
    let endpoints = (0..4_096).flat_map(|index| {
        let endpoint = endpoint(index);
        [endpoint, endpoint]
    });
    let inner = Arc::new(
        StaticProvider::new(Duration::from_mins(1))
            .with_namespace(b"simulation/cluster".to_vec(), endpoints),
    );
    let provider = RefreshingProvider::new(inner, 128).unwrap();

    let narrow = provider
        .discover(b"simulation/cluster", options(32))
        .await
        .unwrap();
    let wide = provider
        .discover(b"simulation/cluster", options(128))
        .await
        .unwrap();

    assert_eq!(narrow.endpoints().len(), 32);
    assert_eq!(wide.endpoints().len(), 128);
    assert!(wide.endpoints().windows(2).all(|pair| pair[0] < pair[1]));
}

// Inputs: deterministic endpoint ordinal below 4,096.
// Outputs: one unique IPv4 socket endpoint.
// Logic: split the ordinal across address octets while retaining a fixed service port.
const fn endpoint(index: u16) -> Endpoint {
    let [high, low] = index.to_be_bytes();
    Endpoint::new(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(10, 0, high, low)),
        7_000,
    ))
}

// Inputs: positive endpoint limit for an offline simulation lookup.
// Outputs: bounded controls with a future monotonic deadline.
// Logic: keep the scale test independent of ambient configuration and cancellation.
fn options(max_endpoints: usize) -> DiscoverOptions {
    DiscoverOptions::new(
        Instant::now() + Duration::from_secs(30),
        max_endpoints,
        CancellationToken::new(),
    )
}
