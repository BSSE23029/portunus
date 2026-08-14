use portunus_storage::{
    assembly::{AssemblyConfig, ChunkAssembler},
    integrity::{ContentId, Sha1Validator},
    journal::{Journal, JournalError},
};
use std::path::PathBuf;
use tokio::io::AsyncWriteExt;

// Inputs: a test-specific filename suffix.
// Outputs: isolated process-local journal path.
// Logic: combine process identity and suffix for deterministic parallel isolation.
fn test_path(suffix: &str) -> PathBuf {
    std::env::temp_dir().join(format!("portunus-journal-{}-{suffix}", std::process::id()))
}

// Inputs: two out-of-order records followed by a simulated torn record prefix.
// Outputs: deterministic replay and physical truncation to the last valid record.
// Logic: model process failure without timing, signals, or filesystem fault injection.
#[tokio::test]
async fn recovers_records_and_discards_torn_tail() {
    let path = test_path("resume");
    let mut journal = Journal::create(&path, 4, 256).await.unwrap();
    journal.append(2, b"ta").await.unwrap();
    journal.append(0, b"da").await.unwrap();
    drop(journal);
    let valid_length = tokio::fs::metadata(&path).await.unwrap().len();

    let mut file = tokio::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .await
        .unwrap();
    file.write_all(&[0, 0, 0]).await.unwrap();
    file.sync_all().await.unwrap();
    drop(file);

    let (_journal, snapshot) = Journal::resume(&path, 256).await.unwrap();
    assert_eq!(snapshot.chunk_length, 4);
    assert_eq!(snapshot.blocks.len(), 2);
    assert_eq!(
        (snapshot.blocks[0].offset, &*snapshot.blocks[0].bytes),
        (2, &b"ta"[..])
    );
    assert_eq!(
        (snapshot.blocks[1].offset, &*snapshot.blocks[1].bytes),
        (0, &b"da"[..])
    );
    let identity =
        ContentId::new("sha1", portunus_storage::integrity::sha1_digest(b"data")).unwrap();
    let mut assembler = ChunkAssembler::new(
        snapshot.chunk_length,
        identity,
        Sha1Validator,
        AssemblyConfig::new(4, 4).unwrap(),
    )
    .unwrap();
    for block in snapshot.blocks {
        assembler.ingest(block.offset, &block.bytes).unwrap();
    }
    assert_eq!(assembler.finish().unwrap().bytes(), b"data");
    assert_eq!(
        tokio::fs::metadata(&path).await.unwrap().len(),
        valid_length
    );
    tokio::fs::remove_file(path).await.unwrap();
}

// Inputs: zero budget, exact one-record budget, one-over append, and bad range.
// Outputs: stable errors with actual/configured bytes and block coordinates.
// Logic: prove journal disk growth is admitted before a record is written.
#[tokio::test]
async fn enforces_journal_and_chunk_boundaries() {
    let zero_path = test_path("zero");
    assert!(matches!(
        Journal::create(&zero_path, 4, 0).await,
        Err(JournalError::ZeroByteLimit)
    ));

    let path = test_path("limit");
    let mut journal = Journal::create(&path, 4, 46).await.unwrap();
    journal.append(0, b"da").await.unwrap();
    assert!(matches!(
        journal.append(2, b"t").await,
        Err(JournalError::ByteLimitExceeded {
            actual: 79,
            limit: 46,
        })
    ));
    assert!(matches!(
        journal.append(3, b"ta").await,
        Err(JournalError::BlockOutOfRange {
            offset: 3,
            length: 2,
            chunk_length: 4,
        })
    ));
    tokio::fs::remove_file(path).await.unwrap();
}

// Inputs: valid record whose payload byte is corrupted after synchronization.
// Outputs: stable corruption location instead of replaying untrusted recovery data.
// Logic: distinguish a torn suffix from a complete but integrity-invalid record.
#[tokio::test]
async fn rejects_corrupt_complete_records() {
    let path = test_path("corrupt");
    let mut journal = Journal::create(&path, 4, 128).await.unwrap();
    journal.append(0, b"data").await.unwrap();
    drop(journal);
    let mut bytes = tokio::fs::read(&path).await.unwrap();
    bytes[24] ^= 1;
    tokio::fs::write(&path, bytes).await.unwrap();

    assert!(matches!(
        Journal::resume(&path, 128).await,
        Err(JournalError::CorruptRecord { record_offset: 12 })
    ));
    tokio::fs::remove_file(path).await.unwrap();
}
