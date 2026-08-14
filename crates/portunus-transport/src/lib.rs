//! Protocol-neutral bounded framed-session infrastructure.
//!
//! This crate owns connection lifecycle, queues, timing, correlation, buffer
//! accounting, and execution. Concrete wire formats belong in adapter modules;
//! [`peer`] is the `BitTorrent` reference workload used to validate the boundary.

mod buffer;
mod correlation;
pub mod peer;
pub mod pool;
mod reconnect;
mod runtime;
mod session;
mod timing;

pub use buffer::{
    BufferAccountant, BufferBudget, BufferConfigError, BufferDirection, BufferLimitError,
    BufferUsage,
};
pub use correlation::{CorrelationError, CorrelationId, CorrelationInsertError, CorrelationTable};
pub use reconnect::{ReconnectConfigError, ReconnectPolicy};
pub use runtime::{
    start_session, start_session_with_buffers, start_session_with_pool, start_timed_session,
    start_timed_session_with_buffers, start_timed_session_with_pool, FrameCodec, HeartbeatFactory,
    Session, SessionError, SessionReport, TimedSessionStartError,
};
pub use session::{
    LifecycleEvent, SessionConfig, SessionConfigError, SessionMachine, SessionState,
    TransitionError,
};
pub use timing::{ConnectionTimer, TimingAction, TimingConfig, TimingConfigError};
