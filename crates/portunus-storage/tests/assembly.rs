use portunus_storage::{
    assembly::{AssemblyConfig, AssemblyError, ChunkAssembler},
    integrity::{ContentId, Sha1Validator},
};

// Inputs: four-byte chunk delivered tail-first in two non-overlapping blocks.
// Outputs: exact progress accounting and verified bytes in logical order.
// Logic: prove arrival order is independent from bounded assembly and validation.
#[test]
fn assembles_sparse_out_of_order_blocks() {
    let identity = ContentId::new("sha1", portunus_storage::sha1(b"data")).unwrap();
    let mut assembler = ChunkAssembler::new(
        4,
        identity,
        Sha1Validator,
        AssemblyConfig::new(4, 4).unwrap(),
    )
    .unwrap();

    assert_eq!(assembler.ingest(2, b"ta").unwrap().received_bytes, 2);
    let progress = assembler.ingest(0, b"da").unwrap();
    assert_eq!(progress.received_bytes, 4);
    assert!(progress.complete);
    assert_eq!(assembler.finish().unwrap().bytes(), b"data");
}

// Inputs: zero, exact, and one-over independent configuration boundaries.
// Outputs: stable configuration and admission errors with configured values.
// Logic: ensure no allocation occurs for invalid or over-budget chunk declarations.
#[test]
fn enforces_chunk_and_buffer_boundaries() {
    assert_eq!(
        AssemblyConfig::new(0, 1).unwrap_err(),
        AssemblyError::ZeroChunkLimit
    );
    assert_eq!(
        AssemblyConfig::new(1, 0).unwrap_err(),
        AssemblyError::ZeroBufferLimit
    );
    let identity = || ContentId::new("sha1", [1]).unwrap();
    assert!(ChunkAssembler::new(
        4,
        identity(),
        Sha1Validator,
        AssemblyConfig::new(4, 4).unwrap()
    )
    .is_ok());
    assert_eq!(
        ChunkAssembler::new(
            5,
            identity(),
            Sha1Validator,
            AssemblyConfig::new(4, 5).unwrap()
        )
        .unwrap_err(),
        AssemblyError::ChunkTooLarge {
            actual: 5,
            limit: 4,
        }
    );
    assert_eq!(
        ChunkAssembler::new(
            5,
            identity(),
            Sha1Validator,
            AssemblyConfig::new(5, 4).unwrap()
        )
        .unwrap_err(),
        AssemblyError::BufferLimitExceeded {
            actual: 5,
            limit: 4,
        }
    );
}

// Inputs: out-of-range block, conflicting overlap, incomplete and corrupt chunks.
// Outputs: typed errors that preserve offsets and byte-count details.
// Logic: isolate malformed ingestion and integrity failure before commit eligibility.
#[test]
fn rejects_malformed_or_unverified_assembly() {
    let identity = ContentId::new("sha1", portunus_storage::sha1(b"good")).unwrap();
    let mut assembler = ChunkAssembler::new(
        4,
        identity,
        Sha1Validator,
        AssemblyConfig::new(4, 4).unwrap(),
    )
    .unwrap();
    assert_eq!(
        assembler.ingest(3, b"xx").unwrap_err(),
        AssemblyError::BlockOutOfRange {
            offset: 3,
            length: 2,
            chunk_length: 4,
        }
    );
    assembler.ingest(0, b"ba").unwrap();
    assert_eq!(
        assembler.ingest(1, b"d").unwrap_err(),
        AssemblyError::ConflictingOverlap { offset: 1 }
    );
    assert_eq!(
        assembler.finish().unwrap_err(),
        AssemblyError::Incomplete {
            received: 2,
            expected: 4,
        }
    );

    let identity = ContentId::new("sha1", portunus_storage::sha1(b"good")).unwrap();
    let mut corrupt = ChunkAssembler::new(
        4,
        identity,
        Sha1Validator,
        AssemblyConfig::new(4, 4).unwrap(),
    )
    .unwrap();
    corrupt.ingest(0, b"evil").unwrap();
    assert_eq!(
        corrupt.finish().unwrap_err(),
        AssemblyError::Integrity(portunus_storage::integrity::IntegrityError::Mismatch)
    );
}
