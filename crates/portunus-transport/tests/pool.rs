use portunus_transport::pool::{BufferPool, BufferPoolConfig, BufferPoolError};

#[path = "pool/snapshot.rs"]
mod snapshot;

// Inputs: zero retained count, zero byte ceiling, and exact minimum configuration.
// Outputs: independent stable errors plus accepted one-buffer/one-byte pool.
// Logic: establish zero and exact resource boundaries before allocation.
#[test]
fn validates_buffer_pool_boundaries() {
    assert_eq!(
        BufferPoolConfig::new(0, 1),
        Err(BufferPoolError::ZeroRetainedBuffers)
    );
    assert_eq!(
        BufferPoolConfig::new(1, 0),
        Err(BufferPoolError::ZeroBufferCapacity)
    );
    assert!(BufferPoolConfig::new(1, 1).is_ok());
}

// Inputs: exact-capacity acquisition, RAII return, then another acquisition.
// Outputs: same allocation capacity is reused and pool telemetry reflects one reuse.
// Logic: prove buffers cross session-like ownership boundaries without global state.
#[test]
fn returns_and_reuses_buffers_within_capacity() {
    let pool = BufferPool::new(BufferPoolConfig::new(1, 8).unwrap());
    let first_capacity = {
        let mut buffer = pool.acquire(8).unwrap();
        buffer.bytes_mut().extend_from_slice(b"12345678");
        buffer.capacity()
    };
    assert_eq!(pool.snapshot().retained_buffers(), 1);

    let second = pool.acquire(8).unwrap();
    assert_eq!(second.capacity(), first_capacity);
    let snapshot = pool.snapshot();
    assert_eq!(snapshot.acquisitions(), 2);
    assert_eq!(snapshot.reuses(), 1);
}

// Inputs: request one byte over the per-buffer limit.
// Outputs: stable error retaining requested bytes and configured ceiling.
// Logic: reject before allocation so hostile sizing cannot bypass pool policy.
#[test]
fn rejects_one_over_the_pool_buffer_limit() {
    let pool = BufferPool::new(BufferPoolConfig::new(1, 8).unwrap());

    assert_eq!(
        pool.acquire(9).unwrap_err(),
        BufferPoolError::RequestExceedsCapacity {
            requested: 9,
            limit: 8,
        }
    );
    assert_eq!(pool.snapshot().acquisitions(), 0);
}

// Inputs: two simultaneous buffers returned to a one-entry pool.
// Outputs: one retained allocation and one measured discard.
// Logic: retained memory remains bounded even when concurrent ownership exceeds it.
#[test]
fn discards_returns_above_the_retained_count() {
    let pool = BufferPool::new(BufferPoolConfig::new(1, 8).unwrap());
    let first = pool.acquire(8).unwrap();
    let second = pool.acquire(8).unwrap();
    drop(first);
    drop(second);

    let snapshot = pool.snapshot();
    assert_eq!(snapshot.retained_buffers(), 1);
    assert_eq!(snapshot.discarded_returns(), 1);
}
