//! Bounded protocol-neutral runtime for one established framed connection.
//!
//! The caller supplies an asynchronous duplex transport and a [`FrameCodec`].
//! Exactly one task owns both, and bounded MPSC queues form the application/I/O
//! handoff. Cancellation stops new outbound admission, drains accepted frames,
//! shuts down the writer, and returns a measured terminal report.
//!
//! ```text
//! application ──bounded outbound──> codec ──> async stream
//! application <──bounded inbound─── codec <── async stream
//!                  cancellation ──> drain ──> shutdown
//! ```
//!
//! This module does not connect sockets, select a protocol, correlate requests,
//! schedule reconnection, install tracing output, or promise transport delivery.

use crate::{LifecycleEvent, SessionConfig, SessionMachine};
use bytes::BytesMut;
use std::io;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::mpsc,
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, trace};

mod report;
mod timed;

pub use report::{SessionError, SessionReport};
pub use timed::{start_timed_session, HeartbeatFactory};

/// Codec boundary between protocol messages and persistent byte buffers.
pub trait FrameCodec: Send + 'static {
    type Inbound: Send + 'static;
    type Outbound: Send + 'static;

    /// Decodes at most one frame from a persistent receive buffer.
    ///
    /// **Inputs:** Exclusive codec and buffer borrows; incomplete bytes remain owned
    /// by the caller.
    ///
    /// **Outputs:** One message, incomplete state, or an I/O-compatible protocol error.
    ///
    /// **Logic:** Consume bytes only for a complete frame and enforce codec limits
    /// before requesting further buffer growth.
    ///
    /// # Errors
    /// Returns an I/O-compatible error for malformed or over-budget framing.
    fn decode_frame(&mut self, source: &mut BytesMut) -> io::Result<Option<Self::Inbound>>;

    /// Appends one outbound message to a caller-owned transmit buffer.
    ///
    /// **Inputs:** Exclusive codec borrow, owned message, and mutable empty buffer.
    ///
    /// **Outputs:** One appended frame or an I/O-compatible protocol error.
    ///
    /// **Logic:** Encode without performing I/O or choosing queue/global policy.
    ///
    /// # Errors
    /// Returns an I/O-compatible error when the message cannot be represented.
    fn encode_frame(&mut self, item: Self::Outbound, destination: &mut BytesMut) -> io::Result<()>;
}

/// Application-facing ownership handle for one spawned session task.
pub struct Session<I, O> {
    inbound: mpsc::Receiver<I>,
    outbound: mpsc::Sender<O>,
    cancellation: CancellationToken,
    task: JoinHandle<Result<SessionReport, SessionError>>,
}

impl<I, O> Session<I, O> {
    /// Attempts immediate outbound admission without waiting for queue capacity.
    ///
    /// **Inputs:** Shared session handle and owned outbound message.
    ///
    /// **Outputs:** Unit, or Tokio's error retaining the rejected message and reason.
    ///
    /// **Logic:** Delegate to the bounded sender; never allocate an overflow queue.
    ///
    /// # Errors
    /// Returns `Full` at capacity or `Closed` after runtime admission ends.
    pub fn try_send(&self, item: O) -> Result<(), mpsc::error::TrySendError<O>> {
        self.outbound.try_send(item)
    }

    /// Receives the next decoded inbound message under queue backpressure.
    ///
    /// **Inputs:** Exclusive session handle borrow.
    ///
    /// **Outputs:** Next message, or `None` after the runtime closes its producer.
    ///
    /// **Logic:** Consume exactly one slot from the bounded inbound queue.
    pub async fn recv(&mut self) -> Option<I> {
        self.inbound.recv().await
    }

    /// Requests cooperative graceful cancellation.
    ///
    /// **Inputs:** Shared handle; calls are idempotent.
    ///
    /// **Outputs:** Signals the runtime and any clones of the same token.
    ///
    /// **Logic:** Wake the task without aborting it so accepted output can drain.
    pub fn cancel(&self) {
        self.cancellation.cancel();
    }

    /// Waits for runtime completion while retaining channel ownership during the wait.
    ///
    /// **Inputs:** Consumed session handle and its owned task/channels.
    ///
    /// **Outputs:** Terminal report, structured session error, or task-failure error.
    ///
    /// **Logic:** Keep channels alive until the task resolves, then translate join
    /// failure into the same stable operational error surface.
    ///
    /// # Errors
    /// Returns the terminal codec, I/O, or task operation failure.
    pub async fn join(self) -> Result<SessionReport, SessionError> {
        let Self {
            inbound,
            outbound,
            cancellation,
            task,
        } = self;
        let result = task
            .await
            .map_err(|failure| SessionError::task(failure.to_string()))?;
        drop((inbound, outbound, cancellation));
        result
    }
}

/// Spawns one bounded framed runtime over an already established duplex stream.
///
/// **Inputs:** Owned async stream, codec, and validated independent capacities.
///
/// **Outputs:** Application handle owning bounded channels, cancellation, and join.
///
/// **Logic:** Allocate exactly the configured queue slots and one ownership task.
pub fn start_session<T, C>(
    io: T,
    codec: C,
    config: SessionConfig,
) -> Session<C::Inbound, C::Outbound>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    C: FrameCodec,
{
    let (inbound_tx, inbound) = mpsc::channel(config.inbound_capacity());
    let (outbound, outbound_rx) = mpsc::channel(config.outbound_capacity());
    let cancellation = CancellationToken::new();
    let task_cancellation = cancellation.clone();
    let task = tokio::spawn(run(io, codec, inbound_tx, outbound_rx, task_cancellation));
    Session {
        inbound,
        outbound,
        cancellation,
        task,
    }
}

/// Owns the codec/stream event loop until cancellation, EOF, or terminal failure.
///
/// **Inputs:** Owned I/O, codec, bounded channel halves, and cancellation token.
/// **Outputs:** Measured closed report or normalized operation failure.
/// **Logic:** Prefer already-buffered frames, otherwise select between one read,
/// one outbound message, and cancellation; every successful handoff is counted.
async fn run<T, C>(
    mut io: T,
    mut codec: C,
    inbound: mpsc::Sender<C::Inbound>,
    mut outbound: mpsc::Receiver<C::Outbound>,
    cancellation: CancellationToken,
) -> Result<SessionReport, SessionError>
where
    T: AsyncRead + AsyncWrite + Unpin,
    C: FrameCodec,
{
    let mut machine = SessionMachine::new();
    machine
        .apply(LifecycleEvent::Connected)
        .expect("valid start");
    let mut input = BytesMut::with_capacity(8 * 1024);
    let mut output = BytesMut::new();
    let mut inbound_frames = 0_u64;
    let mut outbound_frames = 0_u64;

    loop {
        match codec.decode_frame(&mut input) {
            Ok(Some(frame)) => {
                tokio::select! {
                    result = inbound.send(frame) => {
                        if result.is_err() { return close(io, machine, inbound_frames, outbound_frames).await; }
                        inbound_frames += 1;
                    }
                    () = cancellation.cancelled() => break,
                }
                continue;
            }
            Ok(None) => {}
            Err(failure) => return Err(fail("decode", &failure)),
        }

        tokio::select! {
            () = cancellation.cancelled() => break,
            read = io.read_buf(&mut input) => {
                match read.map_err(|failure| fail("read", &failure))? {
                    0 => return close(io, machine, inbound_frames, outbound_frames).await,
                    bytes => trace!(bytes, "session transport read"),
                }
            }
            item = outbound.recv() => {
                let Some(item) = item else {
                    return close(io, machine, inbound_frames, outbound_frames).await;
                };
                write_frame(&mut io, &mut codec, item, &mut output).await?;
                outbound_frames += 1;
            }
        }
    }

    machine
        .apply(LifecycleEvent::DrainRequested)
        .expect("active session");
    outbound.close();
    while let Some(item) = outbound.recv().await {
        write_frame(&mut io, &mut codec, item, &mut output).await?;
        outbound_frames += 1;
    }
    close(io, machine, inbound_frames, outbound_frames).await
}

/// Encodes and completely writes one admitted outbound frame.
///
/// **Inputs:** Exclusive stream/codec/buffer borrows and one owned message.
/// **Outputs:** Empty reusable buffer on success or operation-labelled failure.
/// **Logic:** Clear retained capacity, encode once, write all bytes, then count upstream.
async fn write_frame<T, C>(
    io: &mut T,
    codec: &mut C,
    item: C::Outbound,
    output: &mut BytesMut,
) -> Result<(), SessionError>
where
    T: AsyncWrite + Unpin,
    C: FrameCodec,
{
    output.clear();
    codec
        .encode_frame(item, output)
        .map_err(|failure| fail("encode", &failure))?;
    io.write_all(output)
        .await
        .map_err(|failure| fail("write", &failure))?;
    trace!(bytes = output.len(), "session transport wrote frame");
    Ok(())
}

/// Shuts down transport output and returns a terminal lifecycle report.
///
/// **Inputs:** Owned stream/machine and accumulated delivered frame counters.
/// **Outputs:** Closed report or shutdown write error.
/// **Logic:** Normalize active/connecting state through closure, invoke graceful
/// writer shutdown, and publish bounded terminal telemetry.
async fn close<T: AsyncWrite + Unpin>(
    mut io: T,
    mut machine: SessionMachine,
    inbound_frames: u64,
    outbound_frames: u64,
) -> Result<SessionReport, SessionError> {
    machine
        .apply(LifecycleEvent::TransportClosed)
        .expect("transport close is valid");
    io.shutdown()
        .await
        .map_err(|failure| fail("write", &failure))?;
    debug!(inbound_frames, outbound_frames, "session closed");
    Ok(SessionReport::new(
        machine.state(),
        inbound_frames,
        outbound_frames,
    ))
}

/// Converts and logs one terminal I/O-compatible operation failure.
///
/// **Inputs:** Stable operation label and owned source error.
/// **Outputs:** Normalized session error.
/// **Logic:** Emit only bounded category/detail fields; never payload buffers.
fn fail(operation: &'static str, source: &io::Error) -> SessionError {
    let failure = SessionError::io(operation, source);
    error!(operation, kind = ?failure.kind(), "session operation failed");
    failure
}
