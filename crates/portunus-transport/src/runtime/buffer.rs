//! Bounded stream reads, encoded writes, and terminal buffer usage reporting.
//!
//! Reads expose at most the remaining inbound logical budget to the transport.
//! Writes encode into one reusable buffer, validate its logical length before I/O,
//! and retain measured allocator capacity for the final session report.
//!
//! This module trusts codec implementations as local code: outbound codecs may
//! allocate before validation. It does not pool buffers across sessions or choose limits.

use super::{FrameCodec, SessionError, SessionReport};
use crate::{BufferAccountant, BufferDirection, BufferLimitError, LifecycleEvent, SessionMachine};
use bytes::BytesMut;
use std::io;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::{debug, error, trace};

/// Reads only within remaining inbound logical capacity and records usage.
///
/// **Inputs:** Exclusive stream/buffer/accountant borrows.
/// **Outputs:** Bytes read, EOF zero, or normalized read/buffer error.
/// **Logic:** Reject an incomplete buffer already at its ceiling; otherwise wrap the
/// stream in a one-read byte ceiling and account length/capacity after successful I/O.
pub(super) async fn read_bounded<T: AsyncRead + Unpin>(
    io: &mut T,
    input: &mut BytesMut,
    accountant: &mut BufferAccountant,
) -> Result<usize, SessionError> {
    let limit = accountant.budget().max_inbound_bytes();
    if input.len() == limit {
        return Err(SessionError::buffer(
            "inbound_buffer",
            BufferLimitError {
                direction: BufferDirection::Inbound,
                attempted: limit.saturating_add(1),
                limit,
            },
        ));
    }
    let remaining = limit - input.len();
    let read_limit = u64::try_from(remaining).unwrap_or(u64::MAX);
    let mut bounded = (&mut *io).take(read_limit);
    let read = bounded
        .read_buf(input)
        .await
        .map_err(|failure| fail("read", &failure))?;
    accountant
        .observe_inbound(input.len(), input.capacity())
        .map_err(|failure| SessionError::buffer("inbound_buffer", failure))?;
    trace!(bytes = read, retained = input.len(), "bounded session read");
    Ok(read)
}

/// Encodes, validates, accounts, and completely writes one outbound frame.
///
/// **Inputs:** Exclusive stream/codec/reusable-buffer/accountant and owned message.
/// **Outputs:** Unit or normalized encode, limit, or write failure.
/// **Logic:** Clear logical bytes while retaining allocation, encode once, reject
/// over-budget output before transport I/O, then write the complete accepted frame.
pub(super) async fn write_bounded<T, C>(
    io: &mut T,
    codec: &mut C,
    item: C::Outbound,
    output: &mut BytesMut,
    accountant: &mut BufferAccountant,
) -> Result<(), SessionError>
where
    T: AsyncWrite + Unpin,
    C: FrameCodec,
{
    output.clear();
    codec
        .encode_frame(item, output)
        .map_err(|failure| fail("encode", &failure))?;
    accountant
        .observe_outbound(output.len(), output.capacity())
        .map_err(|failure| SessionError::buffer("outbound_buffer", failure))?;
    io.write_all(output)
        .await
        .map_err(|failure| fail("write", &failure))?;
    trace!(bytes = output.len(), "bounded session wrote frame");
    Ok(())
}

/// Shuts down output and returns lifecycle, frame, and buffer measurements.
///
/// **Inputs:** Owned stream/machine, delivered frame counters, and accountant snapshot.
/// **Outputs:** Closed report or shutdown write error.
/// **Logic:** Apply the terminal state, gracefully shut down, then snapshot usage.
pub(super) async fn close<T: AsyncWrite + Unpin>(
    mut io: T,
    mut machine: SessionMachine,
    inbound_frames: u64,
    outbound_frames: u64,
    accountant: BufferAccountant,
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
        accountant.usage(),
    ))
}

/// Converts and logs one terminal I/O-compatible operation failure.
///
/// **Inputs:** Stable operation label and borrowed source error.
/// **Outputs:** Normalized session error.
/// **Logic:** Emit only bounded category fields; never payload buffers.
pub(super) fn fail(operation: &'static str, source: &io::Error) -> SessionError {
    let failure = SessionError::io(operation, source);
    error!(operation, kind = ?failure.kind(), "session operation failed");
    failure
}
