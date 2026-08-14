use portunus_transport::{start_session, PeerCodec, SessionConfig};
use tokio::io::duplex;

// Inputs: compatibility constructor over an idle in-memory duplex stream.
// Outputs: cooperatively closed session using bounded default buffers.
// Logic: verify startup composition through its public compatibility boundary.
#[tokio::test]
async fn starts_with_bounded_compatibility_buffers() {
    let (client, _remote) = duplex(16);
    let session = start_session(
        client,
        PeerCodec::new(8),
        SessionConfig::new(1, 1, 1).unwrap(),
    );
    session.cancel();

    let report = session.join().await.unwrap();
    assert!(report.buffer_usage().peak_inbound_capacity() <= 1024 * 1024);
}
