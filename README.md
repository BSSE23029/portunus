# Portunus

Portunus is a modular Rust P2P data orchestrator with a gRPC control plane and a BitTorrent-compatible data plane.

Its product is the **reusable systems infrastructure**, not a consumer torrent client. BitTorrent is the reference workload used to exercise hostile binary input, unreliable discovery, stateful framed connections, out-of-order chunks, integrity verification, and bounded concurrency.

Start with the illustrated [Systems Engineering Guide](docs/THEORY.md) for the theory behind the code. Every authored function also documents its inputs, outputs, and internal logic directly beside its implementation.

## Workspace

- `portunus-proto`: generated gRPC contracts
- `portunus-bencode`: zero-copy bencode values
- `portunus-discovery`: UDP tracker protocol primitives
- `portunus-transport`: peer handshake and framed codec
- `portunus-storage`: async preallocated, SHA-1-verified piece storage
- `portunus-engine`: bounded command actor and rarest-first scheduling
- `portunus-daemon`: gRPC server binary

## Run

```sh
cargo test --workspace
PORTUNUS_LOG=info cargo run -p portunus-daemon
```

The daemon listens on `127.0.0.1:50051` by default. Override it with `PORTUNUS_ADDR`.
Logging defaults to `info`. Set `PORTUNUS_LOG=debug` for global diagnostics or
use targeted filters such as `PORTUNUS_LOG=portunus_engine=trace,tonic=warn`.
`RUST_LOG` remains a lower-precedence ecosystem-compatible fallback.

Portunus is protocol infrastructure. Only distribute content you have the right to share, and expose user controls for bandwidth, metered networks, and seeding.
