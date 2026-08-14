use portunus_transport::pool::{BufferPool, BufferPoolConfig};

// Inputs: newly constructed pool before acquisition or return.
// Outputs: exact zero gauge and cumulative counter boundaries.
// Logic: verify snapshot initialization through the public pool boundary.
#[test]
fn starts_with_zero_operational_counters() {
    let pool = BufferPool::new(BufferPoolConfig::new(1, 8).unwrap());
    let snapshot = pool.snapshot();

    assert_eq!(snapshot.retained_buffers(), 0);
    assert_eq!(snapshot.acquisitions(), 0);
    assert_eq!(snapshot.reuses(), 0);
    assert_eq!(snapshot.discarded_returns(), 0);
}
