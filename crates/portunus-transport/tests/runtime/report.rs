use portunus_transport::peer::PeerCodec;
use portunus_transport::{start_session, SessionConfig, SessionState};
use tokio::io::duplex;

// Inputs: an idle in-memory session cancelled before any frame handoff.
// Outputs: closed report with exact zero inbound and outbound counters.
// Logic: verify the nested report contract through the public runtime boundary.
#[tokio::test]
async fn reports_zero_boundaries_for_an_idle_session() {
    let (client, _remote) = duplex(16);
    let session = start_session(
        client,
        PeerCodec::new(8),
        SessionConfig::new(1, 1, 1).unwrap(),
    );
    session.cancel();

    let report = session.join().await.unwrap();
    assert_eq!(report.final_state(), SessionState::Closed);
    assert_eq!(report.inbound_frames(), 0);
    assert_eq!(report.outbound_frames(), 0);
    assert_eq!(report.buffer_usage().peak_inbound_bytes(), 0);
    assert_eq!(report.buffer_usage().peak_outbound_bytes(), 0);
}
