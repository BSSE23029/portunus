use portunus_storage::{
    assembly::{AssemblyConfig, ChunkAssembler},
    content::{CommitError, CommitOutcome, ContentStore},
    integrity::{ContentId, Sha1Validator},
};
use std::path::PathBuf;

// Inputs: a test-specific directory suffix.
// Outputs: isolated process-local path beneath the operating-system temporary root.
// Logic: combine process identity with suffix so parallel fixtures do not collide.
fn test_root(suffix: &str) -> PathBuf {
    std::env::temp_dir().join(format!("portunus-content-{}-{suffix}", std::process::id()))
}

// Inputs: fixed bytes and their SHA-1 content identity.
// Outputs: integrity-proven chunk that is eligible for transactional commit.
// Logic: cross the public assembly/validation boundary without bypass constructors.
fn verified(bytes: &[u8]) -> portunus_storage::assembly::VerifiedChunk {
    let identity = ContentId::new("sha1", portunus_storage::sha1(bytes)).unwrap();
    let mut assembler = ChunkAssembler::new(
        bytes.len(),
        identity,
        Sha1Validator,
        AssemblyConfig::new(bytes.len(), bytes.len()).unwrap(),
    )
    .unwrap();
    assembler.ingest(0, bytes).unwrap();
    assembler.finish().unwrap()
}

// Inputs: one verified chunk committed twice to an empty content root.
// Outputs: atomic stored/existing outcomes and exact bytes at a stable digest path.
// Logic: prove idempotent content addressing across validation and filesystem I/O.
#[tokio::test]
async fn atomically_commits_verified_content() {
    let root = test_root("commit");
    let store = ContentStore::open(&root).await.unwrap();
    let first = verified(b"content");
    let identity = first.identity().clone();
    assert_eq!(store.commit(first).await.unwrap(), CommitOutcome::Stored);
    assert_eq!(
        tokio::fs::read(store.path_for(&identity)).await.unwrap(),
        b"content"
    );
    assert_eq!(
        store.commit(verified(b"content")).await.unwrap(),
        CommitOutcome::AlreadyPresent
    );
    tokio::fs::remove_dir_all(root).await.unwrap();
}

// Inputs: a pre-existing corrupt object at the requested identity path.
// Outputs: collision error and preservation of the previously committed bytes.
// Logic: ensure idempotency never converts into silent overwrite on identity reuse.
#[tokio::test]
async fn rejects_content_identity_collisions() {
    let root = test_root("collision");
    let store = ContentStore::open(&root).await.unwrap();
    let chunk = verified(b"right");
    let target = store.path_for(chunk.identity());
    tokio::fs::create_dir_all(target.parent().unwrap())
        .await
        .unwrap();
    tokio::fs::write(&target, b"wrong").await.unwrap();

    assert!(matches!(
        store.commit(chunk).await,
        Err(CommitError::IdentityCollision { .. })
    ));
    assert_eq!(tokio::fs::read(target).await.unwrap(), b"wrong");
    tokio::fs::remove_dir_all(root).await.unwrap();
}
