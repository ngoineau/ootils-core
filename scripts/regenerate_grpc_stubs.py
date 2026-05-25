"""
regenerate_grpc_stubs.py — regenerate Python gRPC bindings from
`rust/ootils_proto/proto/engine.proto`.

Run this any time the .proto changes:

    python scripts/regenerate_grpc_stubs.py

The generated files land in `src/ootils_core/_grpc/`. The script also
patches the import in `engine_pb2_grpc.py` to be relative
(`from . import engine_pb2 ...`) — protoc's default output uses an
absolute import that breaks under a package.
"""
from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PROTO_DIR = ROOT / "rust" / "ootils_proto" / "proto"
OUT_DIR = ROOT / "src" / "ootils_core" / "_grpc"
PROTO_FILE = PROTO_DIR / "engine.proto"


def main() -> int:
    if not PROTO_FILE.exists():
        print(f"ERROR: {PROTO_FILE} not found", file=sys.stderr)
        return 1
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable,
        "-m",
        "grpc_tools.protoc",
        f"--proto_path={PROTO_DIR}",
        f"--python_out={OUT_DIR}",
        f"--grpc_python_out={OUT_DIR}",
        f"--pyi_out={OUT_DIR}",
        str(PROTO_FILE),
    ]
    print("$", " ".join(cmd))
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        print("STDOUT:", res.stdout, file=sys.stderr)
        print("STDERR:", res.stderr, file=sys.stderr)
        return res.returncode

    # Patch the gRPC stub to use a relative import — protoc emits
    # `import engine_pb2 as engine__pb2` which breaks when the file
    # lives inside a package.
    grpc_file = OUT_DIR / "engine_pb2_grpc.py"
    content = grpc_file.read_text()
    patched = re.sub(
        r"^import engine_pb2 as ",
        "from . import engine_pb2 as ",
        content,
        flags=re.MULTILINE,
    )
    if patched != content:
        grpc_file.write_text(patched)
        print(f"  patched relative import in {grpc_file.name}")

    # Make sure there's an __init__.py.
    init_py = OUT_DIR / "__init__.py"
    if not init_py.exists():
        init_py.write_text(
            '"""Auto-generated gRPC bindings — do not edit."""\n'
            "from . import engine_pb2, engine_pb2_grpc  # noqa: F401\n"
        )

    print("OK — stubs regenerated in", OUT_DIR)
    return 0


if __name__ == "__main__":
    sys.exit(main())
