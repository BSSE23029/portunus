// Inputs:
// - Cargo's build environment and `../../proto/portunus_api.proto`.
// Outputs:
// - Generated Rust client/server types in Cargo's `OUT_DIR`, or a build error.
// Logic:
// - Tell Cargo when to rerun this script, then ask `tonic-build` to compile both
//   sides of the contract. Generated files are included by the library crate.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=../../proto/portunus_api.proto");
    let descriptor =
        std::path::PathBuf::from(std::env::var("OUT_DIR")?).join("portunus_descriptor.bin");
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(descriptor)
        .compile_protos(&["../../proto/portunus_api.proto"], &["../../proto"])?;
    Ok(())
}
