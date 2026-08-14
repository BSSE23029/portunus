//! Integration coverage for public control-plane compatibility metadata.

use portunus_proto::{API_PACKAGE, API_VERSION, FILE_DESCRIPTOR_SET};

// Inputs: generated contract metadata embedded in the protocol crate.
// Outputs: stable v1 package identity and nonempty reflection descriptor bytes.
// Logic: prove versioning is part of the wire contract rather than daemon convention.
#[test]
fn exposes_versioned_reflection_contract() {
    assert_eq!(API_VERSION, "v1");
    assert_eq!(API_PACKAGE, "portunus.v1");
    assert!(!FILE_DESCRIPTOR_SET.is_empty());
}
