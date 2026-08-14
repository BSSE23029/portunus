//! Measured peer-adapter framing costs at the generic codec boundary.

use bytes::{Bytes, BytesMut};
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use portunus_transport::{
    peer::{Message, PeerCodec},
    FrameCodec,
};

// Inputs: Criterion measurement context and a deterministic 16 KiB reference block.
// Outputs: throughput samples for encode and decode through the generic codec trait.
// Logic: reuse caller-owned buffers so results measure framing rather than session I/O.
fn benchmark_peer_codec(criterion: &mut Criterion) {
    let block = Bytes::from(vec![0x5a; 16 * 1024]);
    let message = Message::Piece {
        index: 7,
        begin: 0,
        block,
    };
    let mut encoded = BytesMut::new();
    PeerCodec::new(17 * 1024)
        .encode_frame(message.clone(), &mut encoded)
        .unwrap();
    let mut group = criterion.benchmark_group("transport/peer_codec");
    group.throughput(Throughput::Bytes(16 * 1024));
    group.bench_function("encode_16k_piece", |bencher| {
        let mut output = BytesMut::with_capacity(encoded.len());
        let mut codec = PeerCodec::new(17 * 1024);
        bencher.iter(|| {
            output.clear();
            codec
                .encode_frame(black_box(message.clone()), &mut output)
                .unwrap();
            black_box(&output);
        });
    });
    group.bench_function("decode_16k_piece", |bencher| {
        let mut codec = PeerCodec::new(17 * 1024);
        bencher.iter_batched(
            || encoded.clone(),
            |mut input| black_box(codec.decode_frame(&mut input).unwrap()),
            criterion::BatchSize::SmallInput,
        );
    });
    group.finish();
}

criterion_group!(benches, benchmark_peer_codec);
criterion_main!(benches);
