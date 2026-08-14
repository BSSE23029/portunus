use portunus_transport::{start_timed_session, Message, PeerCodec, SessionConfig, TimingConfig};
use std::time::Duration;
use tokio::io::duplex;

// Inputs: compatibility timed constructor with future deadline under paused time.
// Outputs: valid session that cooperatively closes before any activity.
// Logic: verify extracted timed startup through its public compatibility boundary.
#[tokio::test]
async fn starts_timed_session_with_bounded_defaults() {
    let (client, _remote) = duplex(16);
    let timing = TimingConfig::new(Duration::from_secs(1), Duration::from_secs(2)).unwrap();
    let deadline = tokio::time::Instant::now().into_std() + Duration::from_secs(3);
    let session = start_timed_session(
        client,
        PeerCodec::new(8),
        SessionConfig::new(1, 1, 1).unwrap(),
        timing,
        deadline,
        || Message::KeepAlive,
    )
    .unwrap();
    session.cancel();

    assert_eq!(session.join().await.unwrap().inbound_frames(), 0);
}
