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
    BufferHandle, FrameCodec, SessionError, SessionReport,
};
use crate::{BufferAccountant, ConnectionTimer, LifecycleEvent, SessionMachine, TimingAction};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::mpsc,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

mod start;

pub use start::{
    start_timed_session, start_timed_session_with_buffers, start_timed_session_with_pool,
    TimedSessionStartError,
};

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

/// Owns timed codec/stream execution until cancellation, closure, or terminal policy.
///
/// **Inputs:** Owned runtime machinery, bounded channel halves, and cancellation.
/// **Outputs:** Measured closed report or normalized terminal failure.
/// **Logic:** Deliver buffered frames first; every blocking point races required
/// terminal signals, while the general event loop also admits heartbeat work.
#[allow(clippy::too_many_arguments)]
pub(super) async fn run<T, C, H, I, O>(
    mut io: T,
    mut codec: C,
    mut heartbeat: H,
    mut timer: ConnectionTimer,
    mut accountant: BufferAccountant,
    mut input: I,
    mut output: O,
    inbound: mpsc::Sender<C::Inbound>,
    mut outbound: mpsc::Receiver<C::Outbound>,
    cancellation: CancellationToken,
) -> Result<SessionReport, SessionError>
where
    T: AsyncRead + AsyncWrite + Unpin,
    C: FrameCodec,
    H: HeartbeatFactory<C::Outbound>,
    I: BufferHandle,
    O: BufferHandle,
{
    let mut machine = SessionMachine::new();
    machine
        .apply(LifecycleEvent::Connected)
        .expect("valid start");
    let mut inbound_frames = 0_u64;
    let mut outbound_frames = 0_u64;

    loop {
        match codec.decode_frame(input.bytes_mut()) {
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
                        write_bounded(&mut io, &mut codec, heartbeat.heartbeat(), output.bytes_mut(), &mut accountant).await?;
                        outbound_frames += 1;
                        timer.record_outbound(tokio::time::Instant::now().into_std());
                    }
                    TimingAction::IdleEviction | TimingAction::DeadlineElapsed => {
                        return Err(terminal_error(&timer));
                    }
                    TimingAction::Wait => {}
                }
            }
            read = read_bounded(&mut io, input.bytes_mut(), &mut accountant) => {
                match read? {
                    0 => return close(io, machine, inbound_frames, outbound_frames, accountant).await,
                    _ => timer.record_inbound(tokio::time::Instant::now().into_std()),
                }
            }
            item = outbound.recv() => {
                let Some(item) = item else {
                    return close(io, machine, inbound_frames, outbound_frames, accountant).await;
                };
                write_bounded(&mut io, &mut codec, item, output.bytes_mut(), &mut accountant).await?;
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
        write_bounded(
            &mut io,
            &mut codec,
            item,
            output.bytes_mut(),
            &mut accountant,
        )
        .await?;
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
