use portunus_storage::access::{AccessCoordinator, AccessError, AccessMode};
use tokio_util::sync::CancellationToken;

// Inputs: zero file count, zero request ceiling, exact values, and one-over request.
// Outputs: stable independent validation/admission errors.
// Logic: ensure lock metadata and per-operation lock sets are bounded before waits.
#[test]
fn validates_access_boundaries() {
    assert_eq!(
        AccessCoordinator::new(0, 1).unwrap_err(),
        AccessError::ZeroFileCount
    );
    assert_eq!(
        AccessCoordinator::new(1, 0).unwrap_err(),
        AccessError::ZeroFilesPerRequest
    );
    let coordinator = AccessCoordinator::new(2, 1).unwrap();
    assert_eq!(
        coordinator.try_read(&[0, 1]).unwrap_err(),
        AccessError::TooManyFiles {
            actual: 2,
            limit: 1,
        }
    );
    assert_eq!(
        coordinator.try_read(&[2]).unwrap_err(),
        AccessError::InvalidFile {
            file_index: 2,
            file_count: 2,
        }
    );
}

// Inputs: overlapping reads and writes on one file.
// Outputs: shared read admission, exclusive write rejection, and RAII release.
// Logic: make concurrent read/write behavior explicit and deterministic.
#[test]
fn enforces_shared_read_and_exclusive_write_policy() {
    let coordinator = AccessCoordinator::new(1, 1).unwrap();
    let first = coordinator.try_read(&[0]).unwrap();
    let second = coordinator.try_read(&[0]).unwrap();
    assert_eq!(first.mode(), AccessMode::Read);
    assert!(matches!(
        coordinator.try_write(&[0]),
        Err(AccessError::Saturated {
            mode: AccessMode::Write
        })
    ));
    drop(first);
    drop(second);
    let writer = coordinator.try_write(&[0]).unwrap();
    assert!(matches!(
        coordinator.try_read(&[0]),
        Err(AccessError::Saturated {
            mode: AccessMode::Read
        })
    ));
    drop(writer);
    assert!(coordinator.try_read(&[0]).is_ok());
}

// Inputs: unordered duplicate file set blocked by a writer and pre-cancellation.
// Outputs: canonical unique lock count and cancellation without retained locks.
// Logic: prevent deadlocks by fixed ordering and prove cooperative wait teardown.
#[tokio::test]
async fn canonicalizes_lock_sets_and_cancels_waiters() {
    let coordinator = AccessCoordinator::new(3, 3).unwrap();
    let permit = coordinator.try_read(&[2, 0, 2]).unwrap();
    assert_eq!(permit.files(), 2);
    drop(permit);
    let held = coordinator.try_write(&[0]).unwrap();
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    assert!(matches!(
        coordinator.read(&[0], &cancellation).await,
        Err(AccessError::Cancelled)
    ));
    drop(held);
    assert!(coordinator.try_write(&[0]).is_ok());
}
