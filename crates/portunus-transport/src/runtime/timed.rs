//! Timed framed-session execution with heartbeats and terminal liveness policy.
//!
//! This runtime extends the bounded session with one [`ConnectionTimer`]. Reads
//! refresh inbound activity, complete writes refresh outbound activity, and paused
//! or real Tokio time drives heartbeat, idle, and absolute deadline boundaries.
//!
//! A full inbound queue may defer heartbeat output but never terminal timing or
//! cancellation. This module does not reconnect, correlate messages, or select a
//! protocol heartbeat representation; the caller supplies [`HeartbeatFactory`].

use super::{
    buffer::{close, fail, read_bounded, write_bounded},
    default_buffer_budget, FrameCodec, Session, SessionError, SessionReport,
};
use crate::{
    BufferAccountant, BufferBudget, ConnectionTimer, LifecycleEvent, SessionConfig, SessionMachine,
    TimingAction, TimingConfig, TimingConfigError,
};
use bytes::BytesMut;
use std::time::Instant;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::mpsc,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

/// Protocol adapter that constructs one outbound heartbeat message on demand.
pub trait HeartbeatFactory<O>: Send + 'static {
    /// Creates a fresh heartbeat without performing I/O.
    ///
    /// **Inputs:** Exclusive factory borrow for protocol-local sequence state.
    /// **Outputs:** One owned outbound message.
    /// **Logic:** Keep heartbeat representation outside generic runtime policy.
    fn heartbeat(&mut self) -> O;
}

impl<O, F> HeartbeatFactory<O> for F
where
    F: FnMut() -> O + Send + 'static,
{
    // Inputs: exclusive closure borrow.
    // Outputs: one closure-produced heartbeat.
    // Logic: make zero-state closures ergonomic protocol adapters.
    fn heartbeat(&mut self) -> O {
        self()
    }
}

/// Spawns a bounded session with heartbeat, idle, and absolute deadline enforcement.
///
/// **Inputs:** Owned duplex I/O, codec, queue config, timing policy, future absolute
/// standard monotonic deadline, and heartbeat factory.
/// **Outputs:** Session handle or synchronous invalid-deadline error.
/// **Logic:** Sample Tokio's clock once for initialization, allocate bounded queues,
/// and give one task exclusive ownership of all mutable connection machinery.
///
/// # Errors
/// Returns [`TimingConfigError::DeadlineElapsed`] unless deadline is strictly future.
pub fn start_timed_session<T, C, H>(
    io: T,
    codec: C,
    config: SessionConfig,
    timing: TimingConfig,
    deadline: Instant,
    heartbeat: H,
) -> Result<Session<C::Inbound, C::Outbound>, TimingConfigError>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    C: FrameCodec,
    H: HeartbeatFactory<C::Outbound>,
{
    start_timed_session_with_buffers(
        io,
        codec,
        config,
        default_buffer_budget(),
        timing,
        deadline,
        heartbeat,
    )
}

/// Spawns a timed session with explicit independent logical buffer ceilings.
///
/// **Inputs:** Owned I/O/codec, queue and buffer policies, timing/deadline, and heartbeat.
/// **Outputs:** Session handle or synchronous invalid-deadline error.
/// **Logic:** Validate timing, allocate bounded queues, and move one accountant into
/// the exclusive session task alongside all mutable connection machinery.
///
/// # Errors
/// Returns [`TimingConfigError::DeadlineElapsed`] unless deadline is strictly future.
#[allow(clippy::too_many_arguments)]
pub fn start_timed_session_with_buffers<T, C, H>(
    io: T,
    codec: C,
    config: SessionConfig,
    buffer_budget: BufferBudget,
    timing: TimingConfig,
    deadline: Instant,
    heartbeat: H,
) -> Result<Session<C::Inbound, C::Outbound>, TimingConfigError>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    C: FrameCodec,
    H: HeartbeatFactory<C::Outbound>,
{
    let timer = ConnectionTimer::new(timing, tokio::time::Instant::now().into_std(), deadline)?;
    let (inbound_tx, inbound) = mpsc::channel(config.inbound_capacity());
    let (outbound, outbound_rx) = mpsc::channel(config.outbound_capacity());
    let cancellation = CancellationToken::new();
    let task_cancellation = cancellation.clone();
    let task = tokio::spawn(run(
        io,
        codec,
        heartbeat,
        timer,
        BufferAccountant::new(buffer_budget),
        inbound_tx,
        outbound_rx,
        task_cancellation,
    ));
    Ok(Session {
        inbound,
        outbound,
        cancellation,
        task,
    })
}

/// Owns timed codec/stream execution until cancellation, closure, or terminal policy.
///
/// **Inputs:** Owned runtime machinery, bounded channel halves, and cancellation.
/// **Outputs:** Measured closed report or normalized terminal failure.
/// **Logic:** Deliver buffered frames first; every blocking point races required
/// terminal signals, while the general event loop also admits heartbeat work.
#[allow(clippy::too_many_arguments)]
async fn run<T, C, H>(
    mut io: T,
    mut codec: C,
    mut heartbeat: H,
    mut timer: ConnectionTimer,
    mut accountant: BufferAccountant,
    inbound: mpsc::Sender<C::Inbound>,
    mut outbound: mpsc::Receiver<C::Outbound>,
    cancellation: CancellationToken,
) -> Result<SessionReport, SessionError>
where
    T: AsyncRead + AsyncWrite + Unpin,
    C: FrameCodec,
    H: HeartbeatFactory<C::Outbound>,
{
    let mut machine = SessionMachine::new();
    machine
        .apply(LifecycleEvent::Connected)
        .expect("valid start");
    let initial = accountant.budget().max_inbound_bytes().min(8 * 1024);
    let mut input = BytesMut::with_capacity(initial);
    let mut output = BytesMut::new();
    let mut inbound_frames = 0_u64;
    let mut outbound_frames = 0_u64;

    loop {
        match codec.decode_frame(&mut input) {
            Ok(Some(frame)) => {
                let terminal = tokio::time::Instant::from_std(timer.terminal_wakeup());
                tokio::select! {
                    result = inbound.send(frame) => {
                        if result.is_err() { return close(io, machine, inbound_frames, outbound_frames, accountant).await; }
                        inbound_frames += 1;
                    }
                    () = cancellation.cancelled() => break,
                    () = tokio::time::sleep_until(terminal) => {
                        return Err(terminal_error(&timer));
                    }
                }
                continue;
            }
            Ok(None) => {}
            Err(failure) => return Err(fail("decode", &failure)),
        }

        let wakeup = tokio::time::Instant::from_std(timer.next_wakeup());
        tokio::select! {
            () = cancellation.cancelled() => break,
            () = tokio::time::sleep_until(wakeup) => {
                match timer.evaluate(tokio::time::Instant::now().into_std()) {
                    TimingAction::HeartbeatDue => {
                        write_bounded(&mut io, &mut codec, heartbeat.heartbeat(), &mut output, &mut accountant).await?;
                        outbound_frames += 1;
                        timer.record_outbound(tokio::time::Instant::now().into_std());
                    }
                    TimingAction::IdleEviction | TimingAction::DeadlineElapsed => {
                        return Err(terminal_error(&timer));
                    }
                    TimingAction::Wait => {}
                }
            }
            read = read_bounded(&mut io, &mut input, &mut accountant) => {
                match read? {
                    0 => return close(io, machine, inbound_frames, outbound_frames, accountant).await,
                    _ => timer.record_inbound(tokio::time::Instant::now().into_std()),
                }
            }
            item = outbound.recv() => {
                let Some(item) = item else {
                    return close(io, machine, inbound_frames, outbound_frames, accountant).await;
                };
                write_bounded(&mut io, &mut codec, item, &mut output, &mut accountant).await?;
                outbound_frames += 1;
                timer.record_outbound(tokio::time::Instant::now().into_std());
            }
        }
    }

    machine
        .apply(LifecycleEvent::DrainRequested)
        .expect("active session");
    outbound.close();
    while let Some(item) = outbound.recv().await {
        write_bounded(&mut io, &mut codec, item, &mut output, &mut accountant).await?;
        outbound_frames += 1;
    }
    close(io, machine, inbound_frames, outbound_frames, accountant).await
}

/// Maps the timer's current terminal action into a stable timeout failure.
///
/// **Inputs:** Shared timer evaluated against Tokio's current monotonic instant.
/// **Outputs:** Deadline or idle session timeout; falls back to deadline defensively.
/// **Logic:** Re-evaluate after wakeup so coincident boundaries preserve precedence.
fn terminal_error(timer: &ConnectionTimer) -> SessionError {
    match timer.evaluate(tokio::time::Instant::now().into_std()) {
        TimingAction::IdleEviction => {
            warn!("session evicted after inbound idle timeout");
            SessionError::timeout("idle")
        }
        TimingAction::DeadlineElapsed => {
            warn!("session connection deadline elapsed");
            SessionError::timeout("deadline")
        }
        TimingAction::Wait | TimingAction::HeartbeatDue => {
            debug!("terminal wakeup evaluated without terminal action");
            SessionError::timeout("deadline")
        }
    }
}
