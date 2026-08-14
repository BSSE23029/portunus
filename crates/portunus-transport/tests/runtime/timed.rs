use bytes::BytesMut;
use portunus_transport::pool::{BufferPool, BufferPoolConfig};
use portunus_transport::{
    start_timed_session, start_timed_session_with_buffers, start_timed_session_with_pool,
    BufferBudget, HeartbeatFactory, Message, PeerCodec, SessionConfig, TimingConfig,
};
use std::{io, time::Duration};
use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt};
use tokio_util::codec::Encoder;

#[path = "timed/start.rs"]
mod start;

// Inputs: a stateful zero-I/O closure used as a heartbeat adapter.
// Outputs: successive owned values produced through the public factory trait.
// Logic: prove protocol adapters need no concrete runtime-specific factory type.
#[test]
fn adapts_stateful_closures_as_heartbeat_factories() {
    let mut sequence = 0_u8;
    let mut factory = move || {
        sequence += 1;
        sequence
    };

    assert_eq!(factory.heartbeat(), 1);
    assert_eq!(factory.heartbeat(), 2);
}

// Inputs: four-byte keepalive against a three-byte timed-session outbound budget.
// Outputs: terminal outbound-buffer failure exactly when heartbeat becomes due.
// Logic: prove timed liveness output traverses the same enforced accounting boundary.
#[tokio::test(start_paused = true)]
async fn enforces_buffer_limits_for_timed_heartbeats() {
    let (client, _remote) = duplex(64);
    let timing = TimingConfig::new(Duration::from_secs(1), Duration::from_secs(2)).unwrap();
    let deadline = tokio::time::Instant::now().into_std() + Duration::from_mins(1);
    let session = start_timed_session_with_buffers(
        client,
        PeerCodec::new(64),
        SessionConfig::new(1, 1, 1).unwrap(),
        BufferBudget::new(64, 3).unwrap(),
        timing,
        deadline,
        || Message::KeepAlive,
    )
    .unwrap();

    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(
        session.join().await.unwrap_err().operation(),
        "outbound_buffer"
    );
}

// Inputs: paused time, idle connection, and peer-wire keepalive factory.
// Outputs: one encoded heartbeat exactly at its inclusive due boundary.
// Logic: verify timing policy crosses the runtime/codec/I/O boundary without wall time.
#[tokio::test(start_paused = true)]
async fn emits_protocol_heartbeats_at_the_due_boundary() {
    let (client, mut remote) = duplex(64);
    let timing = TimingConfig::new(Duration::from_secs(5), Duration::from_secs(20)).unwrap();
    let deadline = tokio::time::Instant::now().into_std() + Duration::from_mins(1);
    let session = start_timed_session(
        client,
        PeerCodec::new(64),
        SessionConfig::new(1, 1, 1).unwrap(),
        timing,
        deadline,
        || Message::KeepAlive,
    )
    .unwrap();

    tokio::time::advance(Duration::from_secs(5)).await;
    let mut heartbeat = [0; 4];
    remote.read_exact(&mut heartbeat).await.unwrap();
    assert_eq!(heartbeat, [0; 4]);
    session.cancel();
    assert_eq!(session.join().await.unwrap().outbound_frames(), 1);
}

// Inputs: paused time advanced exactly to the inbound-idle threshold.
// Outputs: terminal idle operation with standard timed-out classification.
// Logic: prove idle eviction is active runtime behavior, not only a pure policy type.
#[tokio::test(start_paused = true)]
async fn evicts_connections_at_the_idle_boundary() {
    let (client, _remote) = duplex(64);
    let timing = TimingConfig::new(Duration::from_secs(5), Duration::from_secs(10)).unwrap();
    let deadline = tokio::time::Instant::now().into_std() + Duration::from_mins(1);
    let session = start_timed_session(
        client,
        PeerCodec::new(64),
        SessionConfig::new(1, 1, 1).unwrap(),
        timing,
        deadline,
        || Message::KeepAlive,
    )
    .unwrap();

    tokio::time::advance(Duration::from_secs(10)).await;
    let failure = session.join().await.unwrap_err();
    assert_eq!(failure.operation(), "idle");
    assert_eq!(failure.kind(), io::ErrorKind::TimedOut);
}

// Inputs: deadline coincident with idle eviction under paused time.
// Outputs: deadline terminal reason wins according to documented precedence.
// Logic: preserve deterministic terminal classification across runtime scheduling.
#[tokio::test(start_paused = true)]
async fn prioritizes_connection_deadlines_over_idle_eviction() {
    let (client, _remote) = duplex(64);
    let timing = TimingConfig::new(Duration::from_secs(5), Duration::from_secs(10)).unwrap();
    let deadline = tokio::time::Instant::now().into_std() + Duration::from_secs(10);
    let session = start_timed_session(
        client,
        PeerCodec::new(64),
        SessionConfig::new(1, 1, 1).unwrap(),
        timing,
        deadline,
        || Message::KeepAlive,
    )
    .unwrap();

    tokio::time::advance(Duration::from_secs(10)).await;
    assert_eq!(session.join().await.unwrap_err().operation(), "deadline");
}

// Inputs: inbound frame one second before idle, then paused time to the revised limit.
// Outputs: connection survives the original limit and evicts at the refreshed boundary.
// Logic: prove successful transport activity updates the timer used by runtime policy.
#[tokio::test(start_paused = true)]
async fn inbound_activity_postpones_idle_eviction() {
    let (client, mut remote) = duplex(64);
    let timing = TimingConfig::new(Duration::from_secs(10), Duration::from_secs(10)).unwrap();
    let deadline = tokio::time::Instant::now().into_std() + Duration::from_mins(1);
    let mut session = start_timed_session(
        client,
        PeerCodec::new(64),
        SessionConfig::new(1, 1, 1).unwrap(),
        timing,
        deadline,
        || Message::KeepAlive,
    )
    .unwrap();
    let mut frame = BytesMut::new();
    PeerCodec::new(64)
        .encode(Message::Interested, &mut frame)
        .unwrap();

    tokio::time::advance(Duration::from_secs(9)).await;
    remote.write_all(&frame).await.unwrap();
    assert_eq!(session.recv().await, Some(Message::Interested));
    tokio::time::advance(Duration::from_secs(1)).await;
    let mut heartbeat = [0; 4];
    remote.read_exact(&mut heartbeat).await.unwrap();
    assert_eq!(heartbeat, [0; 4]);

    tokio::time::advance(Duration::from_secs(9)).await;
    assert_eq!(session.join().await.unwrap_err().operation(), "idle");
}

// Inputs: timed session with explicit shared two-entry pool and immediate cancellation.
// Outputs: both runtime allocations return to the pool after join.
// Logic: ensure timed execution uses the same RAII allocation lifecycle as base sessions.
#[tokio::test(start_paused = true)]
async fn returns_timed_session_allocations_to_the_pool() {
    let pool = BufferPool::new(BufferPoolConfig::new(2, 64).unwrap());
    let (client, _remote) = duplex(64);
    let timing = TimingConfig::new(Duration::from_secs(1), Duration::from_secs(2)).unwrap();
    let deadline = tokio::time::Instant::now().into_std() + Duration::from_mins(1);
    let session = start_timed_session_with_pool(
        client,
        PeerCodec::new(64),
        SessionConfig::new(1, 1, 1).unwrap(),
        BufferBudget::new(64, 64).unwrap(),
        &pool,
        timing,
        deadline,
        || Message::KeepAlive,
    )
    .unwrap();

    session.cancel();
    session.join().await.unwrap();
    assert_eq!(pool.snapshot().retained_buffers(), 2);
}
