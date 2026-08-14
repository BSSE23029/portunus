use bytes::BytesMut;
use portunus_transport::peer::{Message, PeerCodec};
use portunus_transport::{
    pool::{BufferPool, BufferPoolConfig},
    start_session_with_buffers, start_session_with_pool, BufferBudget, SessionConfig,
};
use std::io;
use tokio::io::{duplex, AsyncWriteExt};
use tokio_util::codec::Encoder;

// Inputs: outbound frame exactly equal to its nine-byte logical budget.
// Outputs: successful write plus logical and allocator-capacity report peaks.
// Logic: cross application queue, codec, budget enforcement, I/O, and reporting.
#[tokio::test]
async fn admits_exact_outbound_buffer_limit_and_reports_usage() {
    let (client, mut remote) = duplex(64);
    let session = start_session_with_buffers(
        client,
        PeerCodec::new(64),
        SessionConfig::new(1, 1, 1).unwrap(),
        BufferBudget::new(64, 9).unwrap(),
    );
    session.try_send(Message::Have(7)).unwrap();
    let mut received = [0; 9];
    tokio::io::AsyncReadExt::read_exact(&mut remote, &mut received)
        .await
        .unwrap();
    session.cancel();

    let usage = session.join().await.unwrap().buffer_usage();
    assert_eq!(usage.peak_outbound_bytes(), 9);
    assert!(usage.peak_outbound_capacity() >= 9);
}

// Inputs: nine-byte encoded frame against an eight-byte outbound budget.
// Outputs: terminal typed operation before any frame reaches the transport.
// Logic: trusted codec allocation is measured, but over-budget bytes are never written.
#[tokio::test]
async fn rejects_one_over_the_outbound_buffer_limit() {
    let (client, _remote) = duplex(64);
    let session = start_session_with_buffers(
        client,
        PeerCodec::new(64),
        SessionConfig::new(1, 1, 1).unwrap(),
        BufferBudget::new(64, 8).unwrap(),
    );
    session.try_send(Message::Have(7)).unwrap();

    let failure = session.join().await.unwrap_err();
    assert_eq!(failure.operation(), "outbound_buffer");
    assert_eq!(failure.kind(), io::ErrorKind::InvalidData);
}

// Inputs: complete five-byte frame under an exact inbound budget.
// Outputs: decoded message and measured exact logical receive peak.
// Logic: prove bounded reads still permit the inclusive successful boundary.
#[tokio::test]
async fn admits_exact_inbound_buffer_limit() {
    let (client, mut remote) = duplex(64);
    let mut session = start_session_with_buffers(
        client,
        PeerCodec::new(64),
        SessionConfig::new(1, 1, 1).unwrap(),
        BufferBudget::new(5, 64).unwrap(),
    );
    let mut encoded = BytesMut::new();
    PeerCodec::new(64)
        .encode(Message::Interested, &mut encoded)
        .unwrap();
    remote.write_all(&encoded).await.unwrap();

    assert_eq!(session.recv().await, Some(Message::Interested));
    session.cancel();
    assert_eq!(
        session
            .join()
            .await
            .unwrap()
            .buffer_usage()
            .peak_inbound_bytes(),
        5
    );
}

// Inputs: incomplete four-byte header at a four-byte retained-input ceiling.
// Outputs: deterministic attempted-five error rather than further stream allocation.
// Logic: a codec asking for more bytes at the ceiling proves the first rejected boundary.
#[tokio::test]
async fn rejects_one_over_the_inbound_buffer_limit() {
    let (client, mut remote) = duplex(64);
    let session = start_session_with_buffers(
        client,
        PeerCodec::new(64),
        SessionConfig::new(1, 1, 1).unwrap(),
        BufferBudget::new(4, 64).unwrap(),
    );
    remote.write_all(&[0, 0, 0, 1]).await.unwrap();

    let failure = session.join().await.unwrap_err();
    assert_eq!(failure.operation(), "inbound_buffer");
    assert_eq!(failure.kind(), io::ErrorKind::InvalidData);
}

// Inputs: two sequential sessions sharing a two-entry explicitly bounded pool.
// Outputs: first join returns both allocations and second startup reuses both.
// Logic: prove runtime ownership integrates RAII pooling across session lifetimes.
#[tokio::test]
async fn reuses_pooled_allocations_across_sessions() {
    let pool = BufferPool::new(BufferPoolConfig::new(2, 64).unwrap());
    let budget = BufferBudget::new(64, 64).unwrap();

    let (first_io, _first_remote) = duplex(64);
    let first = start_session_with_pool(
        first_io,
        PeerCodec::new(64),
        SessionConfig::new(1, 1, 1).unwrap(),
        budget,
        &pool,
    )
    .unwrap();
    first.cancel();
    first.join().await.unwrap();
    assert_eq!(pool.snapshot().retained_buffers(), 2);

    let (second_io, _second_remote) = duplex(64);
    let second = start_session_with_pool(
        second_io,
        PeerCodec::new(64),
        SessionConfig::new(1, 1, 1).unwrap(),
        budget,
        &pool,
    )
    .unwrap();
    assert_eq!(pool.snapshot().reuses(), 2);
    second.cancel();
    second.join().await.unwrap();
}
