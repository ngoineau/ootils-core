//! build.rs — compile the .proto into Rust at build time via tonic-build.
//!
//! ADR-017 §2.1: gRPC is the IPC between Python (FastAPI) and the Rust
//! engine service. The protobuf schema is the contract; both sides
//! regenerate their bindings from this single source of truth.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use the vendored protoc binary — no system install needed.
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);

    tonic_build::configure()
        // Generate both server (for the Rust service) and client (for Rust
        // integration tests; Python uses its own grpc-tools generator).
        .build_server(true)
        .build_client(true)
        // Keep proto source path explicit so cargo re-runs on .proto edits.
        .compile_protos(
            &["proto/engine.proto"],
            &["proto"],
        )?;
    Ok(())
}
