"""
ootils_core._grpc — generated Python gRPC bindings for the ootils engine.

DO NOT edit `engine_pb2.py` or `engine_pb2_grpc.py` by hand. They are
regenerated from `rust/ootils_proto/proto/engine.proto` via:

    python -m grpc_tools.protoc \\
        --proto_path=rust/ootils_proto/proto \\
        --python_out=src/ootils_core/_grpc \\
        --grpc_python_out=src/ootils_core/_grpc \\
        --pyi_out=src/ootils_core/_grpc \\
        rust/ootils_proto/proto/engine.proto

See `scripts/regenerate_grpc_stubs.sh` (Phase 6).
"""

from . import engine_pb2, engine_pb2_grpc  # noqa: F401
