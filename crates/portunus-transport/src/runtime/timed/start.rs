//! Timed session startup for owned and explicitly pooled byte buffers.
//!
//! Startup validates the absolute deadline, acquires both buffers before task spawn,
//! creates bounded application queues, and transfers all mutable state to one timed
//! event loop. Existing constructors retain their focused timing-error contracts.
//!
//! This module does not run timers, perform I/O, reconnect, or create global pools.

use super::{run, HeartbeatFactory};
use crate::{
    pool::{BufferPool, BufferPoolError},
    runtime::{
        start::{default_buffer_budget, initial_inbound_capacity},
        BufferHandle, FrameCodec, Session,
    },
    BufferAccountant, BufferBudget, ConnectionTimer, SessionConfig, TimingConfig,
    TimingConfigError,
};
use bytes::BytesMut;
use std::time::Instant;
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::mpsc,
};
use tokio_util::sync::CancellationToken;

/// Stable combined startup failure for timed sessions using a shared pool.
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
pub enum TimedSessionStartError {
    #[error(transparent)]
    Timing(#[from] TimingConfigError),
    #[error(transparent)]
    Pool(#[from] BufferPoolError),
}

/// Spawns a timed bounded session with compatibility buffer ceilings.
///
/// **Inputs:** Owned duplex I/O/codec, queue config, timing/deadline, and heartbeat.
/// **Outputs:** Session handle or synchronous invalid-deadline error.
/// **Logic:** Delegate to explicit owned buffers under one-mebibyte logical limits.
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

/// Spawns a timed session with owned buffers and explicit logical byte ceilings.
///
/// **Inputs:** Owned I/O/codec, queue/buffer policies, timing/deadline, and heartbeat.
/// **Outputs:** Session handle or synchronous invalid-deadline error.
/// **Logic:** Allocate modest inbound and empty outbound buffers before task ownership.
///
/// # Errors
/// Returns [`TimingConfigError::DeadlineElapsed`] unless deadline is strictly future.
#[allow(clippy::too_many_arguments)]
pub fn start_timed_session_with_buffers<T, C, H>(
    io: T,
    codec: C,
    config: SessionConfig,
    budget: BufferBudget,
    timing: TimingConfig,
    deadline: Instant,
    heartbeat: H,
) -> Result<Session<C::Inbound, C::Outbound>, TimingConfigError>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    C: FrameCodec,
    H: HeartbeatFactory<C::Outbound>,
{
    let input = BytesMut::with_capacity(initial_inbound_capacity(budget));
    spawn(
        io,
        codec,
        config,
        budget,
        timing,
        deadline,
        heartbeat,
        input,
        BytesMut::new(),
    )
}

/// Spawns a timed session whose allocations return to an explicit shared pool.
///
/// **Inputs:** Owned runtime inputs plus logical budget and shared pool borrow.
/// **Outputs:** Timed session or typed timing/pool startup failure.
/// **Logic:** Acquire both buffers before spawning; failed timing validation returns
/// acquired RAII buffers automatically, leaving no partial task or allocation leak.
///
/// # Errors
/// Returns [`TimedSessionStartError`] for invalid deadline or pool capacity rejection.
#[allow(clippy::too_many_arguments)]
pub fn start_timed_session_with_pool<T, C, H>(
    io: T,
    codec: C,
    config: SessionConfig,
    budget: BufferBudget,
    pool: &BufferPool,
    timing: TimingConfig,
    deadline: Instant,
    heartbeat: H,
) -> Result<Session<C::Inbound, C::Outbound>, TimedSessionStartError>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    C: FrameCodec,
    H: HeartbeatFactory<C::Outbound>,
{
    let input = pool.acquire(initial_inbound_capacity(budget))?;
    let output = pool.acquire(0)?;
    Ok(spawn(
        io, codec, config, budget, timing, deadline, heartbeat, input, output,
    )?)
}

/// Validates timing, allocates bounded channels, and spawns the timed event loop.
///
/// **Inputs:** Owned runtime policies/machinery and two buffer ownership handles.
/// **Outputs:** Session handle or invalid-deadline error before task creation.
/// **Logic:** Sample Tokio time once, validate, then move all state into one task.
#[allow(clippy::too_many_arguments)]
fn spawn<T, C, H, I, O>(
    io: T,
    codec: C,
    config: SessionConfig,
    budget: BufferBudget,
    timing: TimingConfig,
    deadline: Instant,
    heartbeat: H,
    input: I,
    output: O,
) -> Result<Session<C::Inbound, C::Outbound>, TimingConfigError>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    C: FrameCodec,
    H: HeartbeatFactory<C::Outbound>,
    I: BufferHandle,
    O: BufferHandle,
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
        BufferAccountant::new(budget),
        input,
        output,
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
