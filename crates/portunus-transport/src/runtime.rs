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

use crate::{pool::PooledBuffer, BufferAccountant, LifecycleEvent, SessionMachine};
use bytes::BytesMut;
use std::io;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::mpsc,
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

mod buffer;
mod report;
mod start;
mod timed;

use buffer::{close, fail, read_bounded, write_bounded};
pub use report::{SessionError, SessionReport};
pub use start::{start_session, start_session_with_buffers, start_session_with_pool};
pub use timed::{
    start_timed_session, start_timed_session_with_buffers, start_timed_session_with_pool,
    HeartbeatFactory, TimedSessionStartError,
};

/// Private ownership adapter shared by owned and RAII-pooled byte buffers.
trait BufferHandle: Send + 'static {
    /// Borrows the underlying mutable byte buffer without transferring ownership.
    ///
    /// **Inputs:** Exclusive buffer-handle borrow.
    /// **Outputs:** Exclusive `BytesMut` borrow for one codec/I/O operation.
    /// **Logic:** Keep event-loop code generic while drop policy remains handle-specific.
    fn bytes_mut(&mut self) -> &mut BytesMut;
}

impl BufferHandle for BytesMut {
    // Inputs: exclusive owned buffer borrow.
    // Outputs: the same buffer borrow.
    // Logic: owned buffers require no ownership adaptation.
    fn bytes_mut(&mut self) -> &mut BytesMut {
        self
    }
}

impl BufferHandle for PooledBuffer {
    // Inputs: exclusive RAII pooled-handle borrow.
    // Outputs: exclusive underlying buffer borrow.
    // Logic: delegate without allowing the allocation to escape its return guard.
    fn bytes_mut(&mut self) -> &mut BytesMut {
        self.bytes_mut()
    }
}

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

/// Owns the codec/stream event loop until cancellation, EOF, or terminal failure.
///
/// **Inputs:** Owned I/O, codec, bounded channel halves, and cancellation token.
/// **Outputs:** Measured closed report or normalized operation failure.
/// **Logic:** Prefer already-buffered frames, otherwise select between one read,
/// one outbound message, and cancellation; every successful handoff is counted.
#[allow(clippy::too_many_arguments)]
async fn run<T, C, I, O>(
    mut io: T,
    mut codec: C,
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
                tokio::select! {
                    result = inbound.send(frame) => {
                        if result.is_err() { return close(io, machine, inbound_frames, outbound_frames, accountant).await; }
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
            read = read_bounded(&mut io, input.bytes_mut(), &mut accountant) => {
                if read? == 0 {
                    return close(io, machine, inbound_frames, outbound_frames, accountant).await;
                }
            }
            item = outbound.recv() => {
                let Some(item) = item else {
                    return close(io, machine, inbound_frames, outbound_frames, accountant).await;
                };
                write_bounded(&mut io, &mut codec, item, output.bytes_mut(), &mut accountant).await?;
                outbound_frames += 1;
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
