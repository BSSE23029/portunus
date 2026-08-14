# Portunus Systems Engineering Guide

Portunus uses BitTorrent-compatible data structures as a demanding systems workload. The goal is not a consumer torrent product. The goal is to learn and build reusable components for binary parsing, service discovery, framed networking, verified storage, bounded concurrency, and gRPC orchestration.

## How to read and extend the code

Every authored function carries the same three-part contract immediately above it:

```text
Inputs:  What enters the function, including important ownership or lifetime rules.
Outputs: Every meaningful success, absence, and error outcome.
Logic:   The algorithm and why its ordering or resource policy matters.
```

Public functions use `///` Rustdoc so the explanation appears in IDE help and generated API documentation. Private helpers, trait implementations, build scripts, and tests use normal `//` comments with the same structure. Generated Protobuf functions are documented in `proto/portunus_api.proto`, their human-maintained source of truth.

## The system as two planes

```text
                    CONTROL PLANE
  external client ──gRPC──> daemon ──bounded command──> engine actor
                                 <──metrics stream───────┘

                       DATA PLANE
  discovery ──endpoints──> transport ──blocks──> storage
      UDP/TCP retries       framed TCP           hash + disk
```

The control plane answers **what should happen**. The data plane performs the high-volume work. Keeping them separate prevents slow clients or telemetry consumers from becoming part of the hot data path.

## One request through the current scaffold

```text
AddTransfer RPC
      │
      ▼
validate request and create a one-shot reply channel
      │
      ▼
send Command::Add through a bounded MPSC queue
      │
      ▼
single engine actor mutates its transfer map
      │
      ├──returns transfer ID through one-shot channel
      └──publishes a new metrics snapshot through watch channel
```

Three channel types serve three different communication shapes:

| Channel | Shape | Why Portunus uses it |
|---|---|---|
| `mpsc` | many producers, one consumer | Serialize commands and impose backpressure. |
| `oneshot` | exactly one reply | Match an asynchronous command with its result. |
| `watch` | latest value | Metrics consumers generally need the newest snapshot, not every historical update. |

## Borrowing instead of copying

`portunus-bencode` returns byte strings that point into the caller's original input:

```text
input allocation:  [ d 4 : n a m e 4 : d a t a e ]
                         ▲───────▲
Value::Bytes(&input[...])┘       │ no second string allocation
```

This is called zero-copy parsing. Rust's lifetime system ensures the parsed value cannot outlive the input buffer it references.

## Framing a byte stream

TCP delivers an ordered stream, not application messages. A codec converts arbitrary arriving chunks into complete frames:

```text
TCP chunks:       [00 00] [00 05 04 00] [00 00 09]
                         buffering
                             │
                             ▼
decoded frame:    length=5, message=Have(piece 9)
```

The decoder first waits for four length bytes, validates the declared size, then waits until the complete frame is buffered. A frame-size limit prevents a peer from forcing unbounded allocation.

## Verify before commit

Storage follows a simple integrity boundary:

```text
untrusted block bytes ──assemble──> complete piece ──SHA-1──> expected digest?
                                                               │
                                                no ──discard───┴──yes──write
```

SHA-1 is retained only for BitTorrent compatibility. The reusable storage design will eventually accept a pluggable integrity algorithm.

## Backpressure

A fast producer and slow consumer can otherwise grow memory without limit:

```text
network producer ──> [ bounded queue ] ──> disk consumer
                           full
                            │
                            └──producer waits instead of allocating forever
```

Portunus treats every boundary—gRPC commands, network frames, verification work, and disk writes—as a resource budget. Throughput matters, but bounded and predictable behavior matters first.

## What comes next

Each crate will be hardened as a reusable library:

1. Binary parser limits, canonical encoding, fuzzing, and benchmarks.
2. A transport-independent discovery trait with timeout and retry policies.
3. A generic framed-session runtime with cancellation and bounded queues.
4. Content-addressed chunk storage with atomic commits and recovery.
5. A policy-driven orchestration runtime with scheduling and admission control.
6. An observable gRPC reference daemon used as a composition example.
