# Portunus

Portunus is a modular Rust P2P data orchestrator with a gRPC control plane and a BitTorrent-compatible data plane.

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
RUST_LOG=info cargo run -p portunus-daemon
```

The daemon listens on `127.0.0.1:50051` by default. Override it with `PORTUNUS_ADDR`.

Portunus is protocol infrastructure. Only distribute content you have the right to share, and expose user controls for bandwidth, metered networks, and seeding.
