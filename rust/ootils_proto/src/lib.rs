//! ootils_proto — generated gRPC bindings for the ootils engine v1 API.
//!
//! The actual code is generated at build time by `tonic-build` from
//! `proto/engine.proto`. This file just re-exports the generated module
//! tree under a stable path: `ootils_proto::engine::v1::*`.
//!
//! See `ADR-017 §2.1` for the rationale of choosing gRPC + tonic as the
//! Python ↔ Rust IPC.

pub mod engine {
    pub mod v1 {
        tonic::include_proto!("ootils.engine.v1");
    }
}
