use portunus_storage::quota::{QuotaError, StorageQuota};
use tokio_util::sync::CancellationToken;

// Inputs: zero values for each independent quota dimension.
// Outputs: distinct stable configuration failures.
// Logic: reject unusable backpressure policy before semaphore construction.
#[test]
fn validates_independent_quota_boundaries() {
    assert_eq!(
        StorageQuota::new(0, 1).unwrap_err(),
        QuotaError::ZeroByteLimit
    );
    assert_eq!(
        StorageQuota::new(1, 0).unwrap_err(),
        QuotaError::ZeroOperationLimit
    );
    assert!(StorageQuota::new(1, 1).is_ok());
}

// Inputs: an exact-capacity admission followed by saturated and one-over requests.
// Outputs: stable rejection details, measured snapshot, and RAII capacity release.
// Logic: prove byte and operation permits move together without quota leakage.
#[test]
fn enforces_exact_and_rejected_admission_boundaries() {
    let quota = StorageQuota::new(4, 1).unwrap();
    let permit = quota.try_admit(4).unwrap();
    assert_eq!(permit.bytes(), 4);
    assert_eq!(quota.snapshot().used_bytes, 4);
    assert!(matches!(
        quota.try_admit(1),
        Err(QuotaError::Saturated { requested_bytes: 1 })
    ));
    assert_eq!(
        quota.try_admit(5).unwrap_err(),
        QuotaError::RequestTooLarge {
            requested: 5,
            limit: 4,
        }
    );
    drop(permit);
    assert_eq!(quota.snapshot().used_bytes, 0);
    assert!(quota.try_admit(4).is_ok());
}

// Inputs: saturated quota and a pre-cancelled cooperative cancellation token.
// Outputs: prompt typed cancellation without consuming later capacity.
// Logic: exercise waiting backpressure deterministically without wall-clock sleeps.
#[tokio::test]
async fn cancels_waiting_admission_without_leaking_capacity() {
    let quota = StorageQuota::new(4, 1).unwrap();
    let held = quota.try_admit(4).unwrap();
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    assert!(matches!(
        quota.admit(4, &cancellation).await,
        Err(QuotaError::Cancelled)
    ));
    drop(held);
    assert!(quota.try_admit(4).is_ok());
}
