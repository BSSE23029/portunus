use portunus_storage::{sha1, Error, PieceStore};
use std::path::PathBuf;

// Inputs: a test-specific filename suffix.
// Outputs: an isolated path in the operating system temporary directory.
// Logic: combine process and suffix identities to avoid parallel collisions.
fn test_path(suffix: &str) -> PathBuf {
    std::env::temp_dir().join(format!("portunus-{}-{suffix}", std::process::id()))
}

// Inputs: two valid pieces, including a shorter final piece.
// Outputs: a preallocated file containing both verified pieces.
// Logic: exercise layout, final-length calculation, hashing, seek, and commit.
#[tokio::test]
async fn writes_verified_pieces() {
    let path = test_path("valid");
    let store = PieceStore::create(&path, 5, 8, vec![sha1(b"first"), sha1(b"end")])
        .await
        .unwrap();
    store.write_verified_piece(1, b"end").await.unwrap();
    store.write_verified_piece(0, b"first").await.unwrap();
    assert_eq!(tokio::fs::read(&path).await.unwrap(), b"firstend");
    tokio::fs::remove_file(path).await.unwrap();
}

// Inputs: incorrect bytes and an out-of-range piece index.
// Outputs: hash mismatch and invalid-piece errors with an unchanged file.
// Logic: verify validation happens before acquiring the disk commit path.
#[tokio::test]
async fn rejects_untrusted_pieces() {
    let path = test_path("invalid");
    let store = PieceStore::create(&path, 5, 5, vec![sha1(b"right")])
        .await
        .unwrap();
    assert!(matches!(
        store.write_verified_piece(0, b"wrong").await,
        Err(Error::HashMismatch(0))
    ));
    assert!(matches!(
        store.write_verified_piece(1, b"right").await,
        Err(Error::InvalidPiece(1))
    ));
    assert_eq!(tokio::fs::read(&path).await.unwrap(), [0; 5]);
    tokio::fs::remove_file(path).await.unwrap();
}
