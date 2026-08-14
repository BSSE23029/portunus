use bytes::BytesMut;
use portunus_transport::{start_session, Message, PeerCodec, SessionConfig, SessionState};
use std::io;
use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc::error::TrySendError;
use tokio_util::codec::Encoder;

#[path = "runtime/report.rs"]
mod report;

// Inputs: one-slot queues over an in-memory full-duplex transport.
// Outputs: exact admission at capacity, explicit overflow, and framed delivery.
// Logic: enqueue before yielding so the spawned runtime cannot hide queue bounds.
#[tokio::test]
async fn bounds_outbound_admission_and_writes_frames() {
    let (client, mut remote) = duplex(64);
    let config = SessionConfig::new(1, 1, 1).unwrap();
    let session = start_session(client, PeerCodec::new(64), config);

    session.try_send(Message::Have(7)).unwrap();
    assert!(matches!(
        session.try_send(Message::Have(8)),
        Err(TrySendError::Full(Message::Have(8)))
    ));

    let mut frame = [0; 9];
    remote.read_exact(&mut frame).await.unwrap();
    assert_eq!(frame, [0, 0, 0, 5, 4, 0, 0, 0, 7]);
    session.cancel();
    let report = session.join().await.unwrap();
    assert_eq!(report.final_state(), SessionState::Closed);
    assert_eq!(report.outbound_frames(), 1);
}

// Inputs: two peer-wire frames delivered in one stream write to a one-slot queue.
// Outputs: ordered delivery with both frames counted and no dropped message.
// Logic: exercise codec, stream fragmentation policy, and bounded inbound backpressure.
#[tokio::test]
async fn delivers_multiple_inbound_frames_through_a_bounded_queue() {
    let (client, mut remote) = duplex(64);
    let config = SessionConfig::new(1, 1, 1).unwrap();
    let mut session = start_session(client, PeerCodec::new(64), config);
    let mut encoded = BytesMut::new();
    let mut codec = PeerCodec::new(64);
    codec.encode(Message::Interested, &mut encoded).unwrap();
    codec.encode(Message::Have(9), &mut encoded).unwrap();

    remote.write_all(&encoded).await.unwrap();
    assert_eq!(session.recv().await, Some(Message::Interested));
    assert_eq!(session.recv().await, Some(Message::Have(9)));
    session.cancel();
    let report = session.join().await.unwrap();
    assert_eq!(report.inbound_frames(), 2);
}

// Inputs: an oversized declared peer frame followed by no payload.
// Outputs: terminal decode error retaining operation and I/O error category.
// Logic: malformed protocol input closes the session without exposing raw payloads.
#[tokio::test]
async fn reports_terminal_codec_failures() {
    let (client, mut remote) = duplex(64);
    let config = SessionConfig::new(1, 1, 1).unwrap();
    let session = start_session(client, PeerCodec::new(4), config);
    remote.write_all(&[0, 0, 0, 5]).await.unwrap();

    let error = session.join().await.unwrap_err();
    assert_eq!(error.operation(), "decode");
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
}
