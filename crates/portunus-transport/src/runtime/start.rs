//! Session startup composition for owned and explicitly pooled byte buffers.
//!
//! Constructors allocate bounded application queues, create one cancellation scope,
//! and spawn the exclusive runtime owner. Compatibility startup uses one-mebibyte
//! logical limits; explicit variants accept budgets and shared buffer pools.
//!
//! This module composes resources only. It does not execute codec/I/O loops, choose
//! timing policy, install logging, or create process-global pools.

use super::{run, BufferHandle, FrameCodec, Session};
use crate::{
    pool::{BufferPool, BufferPoolError},
    BufferAccountant, BufferBudget, SessionConfig,
};
use bytes::BytesMut;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::mpsc,
};
use tokio_util::sync::CancellationToken;

const DEFAULT_BUFFER_BYTES: usize = 1024 * 1024;

/// Spawns one bounded framed runtime with compatibility buffer ceilings.
///
/// **Inputs:** Owned established duplex stream, codec, and queue/correlation config.
/// **Outputs:** Application handle owning bounded channels, cancellation, and join.
/// **Logic:** Delegate to explicit budgets using one mebibyte per direction.
pub fn start_session<T, C>(
    io: T,
    codec: C,
    config: SessionConfig,
) -> Session<C::Inbound, C::Outbound>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    C: FrameCodec,
{
    start_session_with_buffers(io, codec, config, default_buffer_budget())
}

/// Spawns one bounded framed runtime with owned buffers and explicit byte ceilings.
///
/// **Inputs:** Owned stream/codec, queue configuration, and logical buffer budget.
/// **Outputs:** Application handle with one exclusive runtime task.
/// **Logic:** Allocate modest inbound and empty outbound buffers before task ownership.
pub fn start_session_with_buffers<T, C>(
    io: T,
    codec: C,
    config: SessionConfig,
    budget: BufferBudget,
) -> Session<C::Inbound, C::Outbound>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    C: FrameCodec,
{
    let initial = initial_inbound_capacity(budget);
    spawn(
        io,
        codec,
        config,
        budget,
        BytesMut::with_capacity(initial),
        BytesMut::new(),
    )
}

/// Spawns one bounded framed runtime using allocations from an explicit shared pool.
///
/// **Inputs:** Owned stream/codec, queue/buffer policy, and shared pool borrow.
/// **Outputs:** Session whose input/output allocations return on task completion.
/// **Logic:** Acquire both buffers synchronously before spawning so pool failure cannot
/// create a partially owned task; inbound requests only the modest initial capacity.
///
/// # Errors
/// Returns [`BufferPoolError`] if initial inbound acquisition exceeds pool capacity.
pub fn start_session_with_pool<T, C>(
    io: T,
    codec: C,
    config: SessionConfig,
    budget: BufferBudget,
    pool: &BufferPool,
) -> Result<Session<C::Inbound, C::Outbound>, BufferPoolError>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    C: FrameCodec,
{
    let input = pool.acquire(initial_inbound_capacity(budget))?;
    let output = pool.acquire(0)?;
    Ok(spawn(io, codec, config, budget, input, output))
}

/// Allocates queues/cancellation and spawns one generic buffer-owning event loop.
///
/// **Inputs:** Owned I/O/codec/config/budget and inbound/outbound buffer handles.
/// **Outputs:** Application session handle.
/// **Logic:** All mutable machinery crosses into exactly one spawned task.
fn spawn<T, C, I, O>(
    io: T,
    codec: C,
    config: SessionConfig,
    budget: BufferBudget,
    input: I,
    output: O,
) -> Session<C::Inbound, C::Outbound>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    C: FrameCodec,
    I: BufferHandle,
    O: BufferHandle,
{
    let (inbound_tx, inbound) = mpsc::channel(config.inbound_capacity());
    let (outbound, outbound_rx) = mpsc::channel(config.outbound_capacity());
    let cancellation = CancellationToken::new();
    let task_cancellation = cancellation.clone();
    let task = tokio::spawn(run(
        io,
        codec,
        BufferAccountant::new(budget),
        input,
        output,
        inbound_tx,
        outbound_rx,
        task_cancellation,
    ));
    Session {
        inbound,
        outbound,
        cancellation,
        task,
    }
}

/// Returns the modest initial receive allocation within the logical budget.
///
/// **Inputs:** Validated independent buffer budget.
/// **Outputs:** Positive capacity no greater than eight kibibytes or inbound limit.
/// **Logic:** Avoid preallocating the entire hostile-input ceiling at session startup.
pub(super) const fn initial_inbound_capacity(budget: BufferBudget) -> usize {
    let limit = budget.max_inbound_bytes();
    if limit < 8 * 1024 {
        limit
    } else {
        8 * 1024
    }
}

/// Returns compatibility default one-mebibyte logical limits per direction.
///
/// **Inputs:** No environmental state.
/// **Outputs:** Validated default buffer budget.
/// **Logic:** Keep legacy construction bounded while explicit variants remain tunable.
pub(super) fn default_buffer_budget() -> BufferBudget {
    BufferBudget::new(DEFAULT_BUFFER_BYTES, DEFAULT_BUFFER_BYTES).expect("positive defaults")
}
