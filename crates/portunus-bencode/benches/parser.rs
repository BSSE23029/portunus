use criterion::{black_box, criterion_group, criterion_main, Criterion};
use portunus_bencode::{parse, parse_spanned, IncrementalParser, Limits};

// Inputs: Criterion runtime and representative scalar/metadata encodings.
// Outputs: throughput samples for ordinary and exact-span parsing.
// Logic: benchmark borrowed parsing separately from the opt-in per-node span tree.
fn parsing(criterion: &mut Criterion) {
    let fixture = include_bytes!("../tests/fixtures/torrent_metadata.bencode");
    let metadata = fixture.strip_suffix(b"\n").unwrap_or(fixture);

    criterion.bench_function("parse/scalar", |bencher| {
        bencher.iter(|| parse(black_box(b"20:12345678901234567890")));
    });
    criterion.bench_function("parse/metadata", |bencher| {
        bencher.iter(|| parse(black_box(metadata)));
    });
    criterion.bench_function("parse_spanned/metadata", |bencher| {
        bencher.iter(|| parse_spanned(black_box(metadata)));
    });
}

// Inputs: Criterion runtime and deterministic metadata divided into small chunks.
// Outputs: end-to-end incremental recognition throughput samples.
// Logic: include reset and buffering costs while avoiding transport or wall-clock I/O.
fn incremental(criterion: &mut Criterion) {
    let fixture = include_bytes!("../tests/fixtures/torrent_metadata.bencode");
    let metadata = fixture.strip_suffix(b"\n").unwrap_or(fixture);
    criterion.bench_function("incremental/metadata_16_byte_chunks", |bencher| {
        bencher.iter(|| {
            let mut parser = IncrementalParser::new(Limits::default());
            for chunk in black_box(metadata).chunks(16) {
                parser.push(chunk).unwrap();
            }
            black_box(parser.document());
        });
    });
}

criterion_group!(benches, parsing, incremental);
criterion_main!(benches);
