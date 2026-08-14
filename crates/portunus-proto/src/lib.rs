//! Generated Portunus control-plane contracts.
//!
//! Protobuf is the source of truth: external programs can implement the same
//! contract in any supported language. `tonic::include_proto!` includes Rust
//! code produced by `build.rs`; generated code is never edited by hand.
#![allow(
    clippy::default_trait_access,
    clippy::derive_partial_eq_without_eq,
    clippy::doc_markdown,
    clippy::missing_const_for_fn,
    clippy::missing_errors_doc,
    clippy::too_many_lines
)]
pub const API_VERSION: &str = "v1";
pub const API_PACKAGE: &str = "portunus.v1";
pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("portunus_descriptor");

tonic::include_proto!("portunus.v1");
