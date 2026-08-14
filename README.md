# Portunus

Portunus is a modular Rust P2P data orchestrator with a gRPC control plane and a BitTorrent-compatible data plane.

Its product is the **reusable systems infrastructure**, not a consumer torrent client. BitTorrent is the reference workload used to exercise hostile binary input, unreliable discovery, stateful framed connections, out-of-order chunks, integrity verification, and bounded concurrency.

Start with the illustrated [Systems Engineering Guide](docs/THEORY.md) for the theory behind the code. Every authored function also documents its inputs, outputs, and internal logic directly beside its implementation.

## Workspace

- `portunus-bencode`: borrowed binary parsing with configurable resource limits,
  precise offsets, typed Serde access, canonical encoding, incremental recognition,
  and exact source spans
- `portunus-discovery`: pluggable bounded discovery, deterministic static fixtures,
  TTL refresh coalescing, and a retrying UDP tracker adapter
- `portunus-transport`: framed-session state machines, bounded queues and buffers,
  request correlation, timing, reconnection policy, and explicit buffer pools
- `portunus-storage`: pluggable integrity, sparse chunk assembly, atomic
  content-addressed commits, crash-recoverable journals, quotas, range mapping,
  and concurrent access policy
- `portunus-engine`: structured task ownership, multi-stage admission, scheduling
  and retry traits, cancellation, consistent snapshots, and bounded event streams
- `portunus-proto`: versioned `portunus.v1` gRPC contracts and reflection metadata
- `portunus-daemon`: the thin composition root, operational policy owner, and
  deterministic BitTorrent-compatible reference harness

## Run

```sh
cargo test --workspace
PORTUNUS_LOG=info cargo run -p portunus-daemon
cargo bench --workspace
```

The daemon listens on `127.0.0.1:50051` by default. Override it with `PORTUNUS_ADDR`.
Logging defaults to `info`. Set `PORTUNUS_LOG=debug` for global diagnostics or
use targeted filters such as `PORTUNUS_LOG=portunus_engine=trace,tonic=warn`.
`RUST_LOG` remains a lower-precedence ecosystem-compatible fallback.

Optional operational controls:

- `PORTUNUS_BEARER_TOKEN` enables the control-plane authentication hook.
- `PORTUNUS_MAX_IN_FLIGHT` sets fail-fast gRPC admission capacity.
- `PORTUNUS_OTLP_ENDPOINT` enables OTLP/HTTP traces and metrics. Related
  `PORTUNUS_TRACE_QUEUE`, `PORTUNUS_EXPORT_BATCH`, and
  `PORTUNUS_METRIC_INTERVAL_MS` variables bound export resources.

The daemon publishes standard gRPC health and reflection services, marks the
versioned control service not-serving before graceful drain, and returns
versioned structured error details. Use `grpcurl` or a generated v1 client as a
test driver; no GUI is part of the project scope.

## Verification

Repository-owned tests use loopback or in-memory transports only. The full-stack
reference test composes all data-plane crates across 8,192 synthetic endpoints,
an in-memory peer-wire transfer, verified storage, engine-owned completion, and
deterministic injected failure. No public tracker or peer is contacted.

Benchmarks cover parser modes, large-manifest storage mapping, sparse assembly,
large-population discovery, framed codec throughput, and orchestration latency.
Benchmark values are machine-specific; compare runs from the same environment.

Portunus is protocol infrastructure. Only distribute content you have the right to share, and expose user controls for bandwidth, metered networks, and seeding.
