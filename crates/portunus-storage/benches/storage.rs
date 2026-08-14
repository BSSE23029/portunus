use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use portunus_storage::{
    assembly::{AssemblyConfig, ChunkAssembler},
    integrity::{sha1_digest, ContentId, Sha1Validator},
    layout::{FileSpec, Layout},
};

// Inputs: Criterion runner and deterministic 64-KiB chunk split into 4-KiB blocks.
// Outputs: measured assembly/validation throughput with bounded allocations.
// Logic: rebuild an assembler per iteration and ingest blocks in reverse order.
fn benchmark_assembly(criterion: &mut Criterion) {
    const CHUNK_BYTES: usize = 64 * 1024;
    const BLOCK_BYTES: usize = 4 * 1024;
    let bytes = vec![0x5a; CHUNK_BYTES];
    let digest = sha1_digest(&bytes);
    let mut group = criterion.benchmark_group("storage/assembly");
    group.throughput(Throughput::Bytes(CHUNK_BYTES as u64));
    group.bench_function("64k_reverse_blocks", |bencher| {
        bencher.iter(|| {
            let identity = ContentId::new("sha1", digest).unwrap();
            let mut assembler = ChunkAssembler::new(
                CHUNK_BYTES,
                identity,
                Sha1Validator,
                AssemblyConfig::new(CHUNK_BYTES, CHUNK_BYTES).unwrap(),
            )
            .unwrap();
            for offset in (0..CHUNK_BYTES).step_by(BLOCK_BYTES).rev() {
                assembler
                    .ingest(offset, black_box(&bytes[offset..offset + BLOCK_BYTES]))
                    .unwrap();
            }
            black_box(assembler.finish().unwrap());
        });
    });
    group.finish();
}

// Inputs: Criterion runner and a 1,024-file deterministic logical layout.
// Outputs: measured cross-file range-mapping latency.
// Logic: map a range spanning the middle 128 files and retain emitted segments.
fn benchmark_layout(criterion: &mut Criterion) {
    let files = (0..1_024)
        .map(|index| FileSpec::new(format!("file-{index}"), 4_096))
        .collect();
    let layout = Layout::new(files, 1_024).unwrap();
    criterion.bench_function("storage/layout/1024_files", |bencher| {
        bencher.iter(|| {
            black_box(
                layout
                    .map(black_box(448 * 4_096), black_box(128 * 4_096))
                    .unwrap(),
            );
        });
    });
}

criterion_group!(benches, benchmark_assembly, benchmark_layout);
criterion_main!(benches);
