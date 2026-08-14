use portunus_transport::{
    BufferAccountant, BufferBudget, BufferConfigError, BufferDirection, BufferLimitError,
};

// Inputs: zero read/write limits and exact positive minimum limits.
// Outputs: independent stable validation failures and accepted one-byte budgets.
// Logic: establish zero, exact, and independent resource boundaries.
#[test]
fn validates_independent_buffer_budget_boundaries() {
    assert_eq!(
        BufferBudget::new(0, 1),
        Err(BufferConfigError::ZeroLimit {
            direction: BufferDirection::Inbound,
        })
    );
    assert_eq!(
        BufferBudget::new(1, 0),
        Err(BufferConfigError::ZeroLimit {
            direction: BufferDirection::Outbound,
        })
    );
    let budget = BufferBudget::new(1, 1).unwrap();
    assert_eq!(budget.max_inbound_bytes(), 1);
    assert_eq!(budget.max_outbound_bytes(), 1);
}

// Inputs: exact inbound limit then one byte above it with allocator capacity metadata.
// Outputs: exact observation succeeds, over-limit error is stable, and peaks do not lie.
// Logic: rejected logical growth must not be incorporated into successful usage metrics.
#[test]
fn accounts_exact_and_rejected_inbound_growth() {
    let mut accountant = BufferAccountant::new(BufferBudget::new(8, 16).unwrap());
    accountant.observe_inbound(8, 16).unwrap();
    assert_eq!(
        accountant.observe_inbound(9, 32),
        Err(BufferLimitError {
            direction: BufferDirection::Inbound,
            attempted: 9,
            limit: 8,
        })
    );

    let usage = accountant.usage();
    assert_eq!(usage.peak_inbound_bytes(), 8);
    assert_eq!(usage.peak_inbound_capacity(), 16);
}

// Inputs: multiple outbound observations including exact limit and one over.
// Outputs: monotonic logical/allocation peaks and independent outbound rejection.
// Logic: allocator capacity is measured rather than misrepresented as zero-copy usage.
#[test]
fn accounts_outbound_allocation_independently() {
    let mut accountant = BufferAccountant::new(BufferBudget::new(4, 8).unwrap());
    accountant.observe_outbound(3, 4).unwrap();
    accountant.observe_outbound(8, 16).unwrap();
    assert_eq!(
        accountant.observe_outbound(9, 32),
        Err(BufferLimitError {
            direction: BufferDirection::Outbound,
            attempted: 9,
            limit: 8,
        })
    );

    let usage = accountant.usage();
    assert_eq!(usage.peak_outbound_bytes(), 8);
    assert_eq!(usage.peak_outbound_capacity(), 16);
}
