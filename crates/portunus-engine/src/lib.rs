//! Protocol-neutral bounded orchestration infrastructure.
//!
//! Public modules separate multi-dimensional resource admission, scheduling and
//! retry policy, structured asynchronous task ownership, and revisioned state
//! streams. The `BitTorrent` actor remains available only as a compatibility workload.
//!
//! This crate does not provide process-global logging, storage, wire codecs,
//! discovery transports, or a consumer download product.

pub mod budget;
pub mod policy;
pub mod runtime;
pub mod telemetry;
pub mod torrent;
