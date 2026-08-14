use portunus_discovery::{
    DiscoverOptions, DiscoveryProvider, Endpoint, RetryPolicy, UdpTrackerProvider,
};
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio_util::sync::CancellationToken;

// Inputs: a loopback tracker that sends one miscorrelated packet before valid replies.
// Outputs: deduplicated admitted endpoints and tracker-provided refresh lifetime.
// Logic: exercise the complete UDP connect/announce adapter without public network I/O.
#[tokio::test]
async fn discovers_through_a_correlated_udp_tracker_exchange() {
    let tracker = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let tracker_address = tracker.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let mut buffer = [0_u8; 128];
        let (length, client) = tracker.recv_from(&mut buffer).await.unwrap();
        assert_eq!(length, 16);
        let transaction = u32::from_be_bytes(buffer[12..16].try_into().unwrap());
        let mut wrong = connect_response(transaction.wrapping_add(1), 77);
        tracker.send_to(&wrong, client).await.unwrap();
        wrong[4..8].copy_from_slice(&transaction.to_be_bytes());
        tracker.send_to(&wrong, client).await.unwrap();

        let (length, client) = tracker.recv_from(&mut buffer).await.unwrap();
        assert_eq!(length, 98);
        let transaction = u32::from_be_bytes(buffer[12..16].try_into().unwrap());
        let response = announce_response(
            transaction,
            45,
            &[
                Endpoint::new("127.0.0.1:7002".parse().unwrap()),
                Endpoint::new("127.0.0.1:7001".parse().unwrap()),
                Endpoint::new("127.0.0.1:7002".parse().unwrap()),
            ],
        );
        tracker.send_to(&response, client).await.unwrap();
    });

    let provider = UdpTrackerProvider::new(
        tracker_address,
        [7; 20],
        6881,
        RetryPolicy::new(2, Duration::from_millis(50), Duration::from_millis(100)).unwrap(),
    );
    let options = DiscoverOptions::new(
        Instant::now() + Duration::from_secs(2),
        2,
        CancellationToken::new(),
    );
    let snapshot = provider.discover(&[3; 20], options).await.unwrap();
    server.await.unwrap();

    assert_eq!(snapshot.source(), "udp-tracker");
    assert_eq!(
        snapshot.endpoints(),
        &[
            Endpoint::new("127.0.0.1:7001".parse().unwrap()),
            Endpoint::new("127.0.0.1:7002".parse().unwrap()),
        ]
    );
    assert!(snapshot.valid_until() >= Instant::now() + Duration::from_secs(40));
}

// Inputs: a loopback tracker that drops the first connection request.
// Outputs: success on the second attempt within a two-attempt retry budget.
// Logic: prove retry policy is bounded and retransmits with fresh correlation state.
#[tokio::test]
async fn retries_dropped_udp_requests_within_budget() {
    let tracker = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let tracker_address = tracker.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let mut buffer = [0_u8; 128];
        let _dropped = tracker.recv_from(&mut buffer).await.unwrap();
        let (_, client) = tracker.recv_from(&mut buffer).await.unwrap();
        let transaction = u32::from_be_bytes(buffer[12..16].try_into().unwrap());
        tracker
            .send_to(&connect_response(transaction, 9), client)
            .await
            .unwrap();
        let (_, client) = tracker.recv_from(&mut buffer).await.unwrap();
        let transaction = u32::from_be_bytes(buffer[12..16].try_into().unwrap());
        tracker
            .send_to(&announce_response(transaction, 30, &[]), client)
            .await
            .unwrap();
    });
    let provider = UdpTrackerProvider::new(
        tracker_address,
        [1; 20],
        6881,
        RetryPolicy::new(2, Duration::from_millis(10), Duration::from_millis(20)).unwrap(),
    );
    let options = DiscoverOptions::new(
        Instant::now() + Duration::from_secs(1),
        4,
        CancellationToken::new(),
    );
    assert!(provider.discover(&[2; 20], options).await.is_ok());
    server.await.unwrap();
}

// Inputs: transaction ID and temporary connection ID.
// Outputs: one valid 16-byte tracker connect response.
// Logic: encode the server fixture in explicit network byte order.
fn connect_response(transaction: u32, connection: u64) -> [u8; 16] {
    let mut response = [0_u8; 16];
    response[4..8].copy_from_slice(&transaction.to_be_bytes());
    response[8..].copy_from_slice(&connection.to_be_bytes());
    response
}

// Inputs: transaction, TTL seconds, and IPv4 endpoint fixtures.
// Outputs: one tracker announce response with compact peer records.
// Logic: encode only the protocol fields consumed by the adapter under test.
fn announce_response(transaction: u32, ttl: u32, endpoints: &[Endpoint]) -> Vec<u8> {
    let mut response = Vec::with_capacity(20 + endpoints.len() * 6);
    response.extend_from_slice(&1_u32.to_be_bytes());
    response.extend_from_slice(&transaction.to_be_bytes());
    response.extend_from_slice(&ttl.to_be_bytes());
    response.extend_from_slice(&0_u32.to_be_bytes());
    response.extend_from_slice(&0_u32.to_be_bytes());
    for endpoint in endpoints {
        let std::net::SocketAddr::V4(address) = endpoint.address() else {
            panic!("fixture endpoint must be IPv4");
        };
        response.extend_from_slice(&address.ip().octets());
        response.extend_from_slice(&address.port().to_be_bytes());
    }
    response
}
