//! Bounded reusable byte-buffer pool with RAII ownership and measured reuse.
//!
//! A pool retains at most a configured number of empty [`BytesMut`] allocations,
//! each no larger than an inclusive capacity ceiling. Acquired buffers have one
//! owner and return automatically on drop; excess or oversized returns are discarded.
//!
//! The pool is explicit shared state, never global. It does not size protocol frames,
//! enforce live logical-byte budgets, clear secret memory, or perform asynchronous I/O.

use bytes::BytesMut;
use std::{
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex},
};
use thiserror::Error;
use tracing::{debug, trace};

mod snapshot;

pub use snapshot::BufferPoolSnapshot;

/// Positive retained-count and per-allocation capacity ceilings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BufferPoolConfig {
    max_retained_buffers: usize,
    max_buffer_capacity: usize,
}

impl BufferPoolConfig {
    /// Creates independent positive pool resource ceilings.
    ///
    /// **Inputs:** Maximum empty buffers retained and maximum capacity per buffer.
    /// **Outputs:** Immutable config or first stable zero-boundary error.
    /// **Logic:** Validate retained count before byte capacity; allocate nothing.
    ///
    /// # Errors
    /// Returns the direction-specific zero configuration failure.
    pub const fn new(
        max_retained_buffers: usize,
        max_buffer_capacity: usize,
    ) -> Result<Self, BufferPoolError> {
        if max_retained_buffers == 0 {
            return Err(BufferPoolError::ZeroRetainedBuffers);
        }
        if max_buffer_capacity == 0 {
            return Err(BufferPoolError::ZeroBufferCapacity);
        }
        Ok(Self {
            max_retained_buffers,
            max_buffer_capacity,
        })
    }
}

/// Stable pool configuration and acquisition failures.
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
pub enum BufferPoolError {
    #[error("buffer pool must retain at least one buffer")]
    ZeroRetainedBuffers,
    #[error("buffer pool capacity must be greater than zero")]
    ZeroBufferCapacity,
    #[error("requested buffer capacity {requested} exceeds pool limit {limit}")]
    RequestExceedsCapacity { requested: usize, limit: usize },
}

#[derive(Debug, Default)]
struct PoolState {
    buffers: Vec<BytesMut>,
    acquisitions: u64,
    reuses: u64,
    discarded_returns: u64,
}

#[derive(Debug)]
struct PoolInner {
    config: BufferPoolConfig,
    state: Mutex<PoolState>,
}

/// Cloneable handle to one explicitly configured shared buffer pool.
#[derive(Debug, Clone)]
pub struct BufferPool {
    inner: Arc<PoolInner>,
}

impl BufferPool {
    /// Creates an empty pool without preallocating byte storage.
    ///
    /// **Inputs:** Validated retained-count and capacity configuration.
    /// **Outputs:** Cloneable pool handle with zero telemetry.
    /// **Logic:** Allocate synchronization/state only; buffers remain demand-driven.
    #[must_use]
    pub fn new(config: BufferPoolConfig) -> Self {
        Self {
            inner: Arc::new(PoolInner {
                config,
                state: Mutex::new(PoolState::default()),
            }),
        }
    }

    /// Acquires one uniquely owned empty buffer with at least requested capacity.
    ///
    /// **Inputs:** Shared pool and requested initial capacity in bytes.
    /// **Outputs:** RAII buffer or over-capacity error before allocation.
    /// **Logic:** Reuse the last sufficient retained allocation, otherwise allocate;
    /// counters saturate and a poisoned telemetry lock recovers its contained state.
    ///
    /// # Errors
    /// Returns [`BufferPoolError::RequestExceedsCapacity`] above the pool ceiling.
    pub fn acquire(&self, requested: usize) -> Result<PooledBuffer, BufferPoolError> {
        if requested > self.inner.config.max_buffer_capacity {
            return Err(BufferPoolError::RequestExceedsCapacity {
                requested,
                limit: self.inner.config.max_buffer_capacity,
            });
        }
        let mut state = self.lock_state();
        state.acquisitions = state.acquisitions.saturating_add(1);
        let position = state
            .buffers
            .iter()
            .rposition(|buffer| buffer.capacity() >= requested);
        let buffer = position.map_or_else(
            || BytesMut::with_capacity(requested),
            |position| {
                state.reuses = state.reuses.saturating_add(1);
                state.buffers.swap_remove(position)
            },
        );
        drop(state);
        trace!(requested, capacity = buffer.capacity(), "buffer acquired");
        Ok(PooledBuffer {
            buffer,
            pool: self.clone(),
        })
    }

    /// Returns a consistent copyable pool telemetry snapshot.
    ///
    /// **Inputs:** Shared pool handle.
    /// **Outputs:** Counts for retained buffers, acquisitions, reuses, and discards.
    /// **Logic:** Read all counters under the same poison-tolerant lock.
    #[must_use]
    pub fn snapshot(&self) -> BufferPoolSnapshot {
        let state = self.lock_state();
        BufferPoolSnapshot {
            retained_buffers: state.buffers.len(),
            acquisitions: state.acquisitions,
            reuses: state.reuses,
            discarded_returns: state.discarded_returns,
        }
    }

    /// Locks mutable state while preserving recoverability after an unrelated panic.
    ///
    /// **Inputs:** Shared pool handle.
    /// **Outputs:** Mutex guard, including poisoned inner state when necessary.
    /// **Logic:** Pool accounting remains usable rather than propagating lock poisoning.
    fn lock_state(&self) -> std::sync::MutexGuard<'_, PoolState> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Returns one cleared allocation or discards it under configured retention bounds.
    ///
    /// **Inputs:** Shared pool and uniquely owned buffer from an RAII handle.
    /// **Outputs:** Mutates retained buffers or discard telemetry.
    /// **Logic:** Clear logical bytes, then require both capacity and count ceilings.
    fn release(&self, mut buffer: BytesMut) {
        buffer.clear();
        let mut state = self.lock_state();
        let retained = if buffer.capacity() <= self.inner.config.max_buffer_capacity
            && state.buffers.len() < self.inner.config.max_retained_buffers
        {
            state.buffers.push(buffer);
            Some(state.buffers.len())
        } else {
            state.discarded_returns = state.discarded_returns.saturating_add(1);
            None
        };
        drop(state);
        if let Some(retained) = retained {
            debug!(retained, "buffer returned to pool");
        } else {
            debug!("buffer return discarded by pool bounds");
        }
    }
}

/// Uniquely owned buffer that returns its allocation to the source pool on drop.
#[derive(Debug)]
pub struct PooledBuffer {
    buffer: BytesMut,
    pool: BufferPool,
}

impl PooledBuffer {
    /// Borrows the mutable byte buffer for codecs and I/O.
    ///
    /// **Inputs:** Exclusive pooled-handle borrow.
    /// **Outputs:** Exclusive buffer borrow valid for the caller scope.
    /// **Logic:** The option is populated for the full public handle lifetime.
    pub const fn bytes_mut(&mut self) -> &mut BytesMut {
        &mut self.buffer
    }

    /// Returns current allocator capacity in bytes.
    ///
    /// **Inputs:** Shared pooled-handle borrow.
    /// **Outputs:** Underlying `BytesMut` capacity.
    /// **Logic:** Expose allocation accounting without transferring ownership.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.buffer.capacity()
    }
}

impl Deref for PooledBuffer {
    type Target = BytesMut;

    // Inputs: shared pooled-handle borrow.
    // Outputs: shared underlying buffer borrow.
    // Logic: preserve common BytesMut read APIs without ownership escape.
    fn deref(&self) -> &Self::Target {
        &self.buffer
    }
}

impl DerefMut for PooledBuffer {
    // Inputs: exclusive pooled-handle borrow.
    // Outputs: exclusive underlying buffer borrow.
    // Logic: preserve common BytesMut mutation APIs without ownership escape.
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buffer
    }
}

impl Drop for PooledBuffer {
    // Inputs: exclusive handle during deterministic or unwinding destruction.
    // Outputs: returns the allocation to its pool when still present.
    // Logic: take once so the buffer cannot be returned twice.
    fn drop(&mut self) {
        self.pool.release(std::mem::take(&mut self.buffer));
    }
}
