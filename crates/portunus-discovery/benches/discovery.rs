//! Deterministic large-population discovery admission latency benchmark.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use portunus_discovery::{DiscoverOptions, DiscoveryProvider, Endpoint, StaticProvider};
use std::{
    net::{Ipv6Addr, SocketAddr},
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

// Inputs: Criterion context and 8,192 configured records collapsing to 4,096 peers.
// Outputs: latency samples for deterministic deduplication and bounded admission.
// Logic: reuse one provider/runtime while rebuilding request-scoped controls per sample.
fn benchmark_static_discovery(criterion: &mut Criterion) {
    let endpoints = (0_u16..4_096).flat_map(|port| {
        let endpoint = Endpoint::new(SocketAddr::new(Ipv6Addr::LOCALHOST.into(), port));
        [endpoint, endpoint]
    });
    let provider = StaticProvider::new(Duration::from_secs(30))
        .with_namespace(b"benchmark".to_vec(), endpoints);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    criterion.bench_function("discovery/static/8192_to_4096", |bencher| {
        bencher.iter(|| {
            let options = DiscoverOptions::new(
                Instant::now() + Duration::from_secs(1),
                4_096,
                CancellationToken::new(),
            );
            black_box(
                runtime
                    .block_on(provider.discover(b"benchmark", options))
                    .unwrap(),
            );
        });
    });
}

criterion_group!(benches, benchmark_static_discovery);
criterion_main!(benches);
