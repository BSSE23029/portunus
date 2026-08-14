// Inputs:
// - Cargo's build environment and `../../proto/portunus_api.proto`.
// Outputs:
// - Generated Rust client/server types in Cargo's `OUT_DIR`, or a build error.
// Logic:
// - Tell Cargo when to rerun this script, then ask `tonic-build` to compile both
//   sides of the contract. Generated files are included by the library crate.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=../../proto/portunus_api.proto");
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["../../proto/portunus_api.proto"], &["../../proto"])?;
    Ok(())
}
