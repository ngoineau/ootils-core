# syntax=docker/dockerfile:1
# ============================================================================
# Dockerfile — API image, with an OPT-IN Rust kernel wheel.
#
# ARG WITH_RUST (default "0") gates whether the `ootils_kernel` PyO3
# extension (Architecture A, ADR-016) is compiled and installed at all. It
# does NOT change the runtime default engine — that stays 'sql'
# (OOTILS_ENGINE is unset here on purpose; see
# src/ootils_core/engine/orchestration). WITH_RUST only controls whether the
# 'rust' engine choice is AVAILABLE in this image.
#
# WITH_RUST=0 (default): byte-for-byte the same build as before this file
# gained a Rust option — no `rust:` base image is ever pulled, no
# cargo/maturin toolchain is ever downloaded, no extra layers.
#
# WITH_RUST=1: adds a `rust-builder` stage (rust:1.82-slim, matching the
# MSRV pinned in rust/Cargo.toml's `[workspace.package] rust-version`) that
# compiles the ootils_kernel wheel via maturin, then installs it into the
# final image.
#
# How the gating actually works (read before "simplifying" this):
# Dockerfile syntax has no if/else. A naive `RUN if [ "$WITH_RUST" = 1 ];
# then ...` guard around the rust build steps would NOT stop the
# `FROM rust:...` line itself from being pulled — a `FROM` is unconditional
# the moment its stage is reached by the builder. Instead this file uses
# ARG-based STAGE SELECTION: two candidate final stages are defined
# (`selected-0` = plain, `selected-1` = +wheel), and
# `FROM selected-${WITH_RUST} AS selected` picks one by name. BuildKit
# resolves `${WITH_RUST}` before constructing its build DAG and only
# builds the ANCESTORS of the stage actually reached — so when WITH_RUST=0,
# `selected-1` (and everything under it: `py-with-rust`, `rust-builder`,
# and the `rust:1.82-slim` base image) is never touched, never pulled,
# never built. This requires the BuildKit builder, which is the default for
# `docker build` / `docker compose build` on any current Docker Engine (the
# same assumption this repo already makes for Dockerfile.engine).
#
# Build with the Rust wheel:
#     docker build --build-arg WITH_RUST=1 .
#     docker compose build --build-arg WITH_RUST=1   (or set WITH_RUST=1 in .env)
# WITH_RUST accepts only "0" or "1" — anything else fails stage resolution
# ("stage not found") rather than silently doing the wrong thing.
# ============================================================================

ARG WITH_RUST=0

# ---- Stage: plain Python image (today's build, unchanged) -----------------
FROM python:3.14-slim AS py-base

WORKDIR /app

# Copy source before install (editable install requires src/ to exist)
COPY pyproject.toml .
COPY src/ /app/src/
COPY scripts/ /app/scripts/

RUN pip install --no-cache-dir .

# ---- Stage: Rust wheel builder (ONLY reached when WITH_RUST=1, see above) -
FROM rust:1.82-slim AS rust-builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-dev python3-venv \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build/rust

# Cargo needs every workspace member's manifest + a valid entry-point file
# to resolve rust/Cargo.toml's [workspace], even though only ootils_kernel
# is actually compiled here. ootils_engine/ootils_proto are stubbed with
# minimal placeholder sources — this mirrors Dockerfile.engine's own stub
# trick for the same reason, with the roles reversed (there ootils_kernel
# is the stub; here it's the real crate).
COPY rust/Cargo.toml rust/Cargo.lock /build/rust/
COPY rust/ootils_kernel /build/rust/ootils_kernel/
COPY rust/ootils_engine/Cargo.toml /build/rust/ootils_engine/Cargo.toml
COPY rust/ootils_proto/Cargo.toml /build/rust/ootils_proto/Cargo.toml
RUN mkdir -p /build/rust/ootils_engine/src /build/rust/ootils_proto/src \
    && echo "fn main() {}" > /build/rust/ootils_engine/src/main.rs \
    && echo "" > /build/rust/ootils_proto/src/lib.rs

# Isolated venv for maturin — rust:*-slim's Debian base marks the system
# Python as externally-managed (PEP 668); a venv sidesteps that cleanly
# instead of forcing --break-system-packages on a build-only container.
RUN python3 -m venv /opt/maturin-venv \
    && /opt/maturin-venv/bin/pip install --no-cache-dir "maturin>=1.7,<2.0"
ENV PATH="/opt/maturin-venv/bin:${PATH}"

WORKDIR /build/rust/ootils_kernel
RUN maturin build --release --out /wheel

# ---- Stage: Python image + Rust wheel installed ----------------------------
FROM py-base AS py-with-rust

COPY --from=rust-builder /wheel /wheel
RUN pip install --no-cache-dir /wheel/*.whl \
    && rm -rf /wheel

# ---- Stage selection: pick py-base (0) or py-with-rust (1) via ARG --------
FROM py-base AS selected-0
FROM py-with-rust AS selected-1
FROM selected-${WITH_RUST} AS selected

# ---- Final: identical steps regardless of WITH_RUST ------------------------
FROM selected AS final

RUN addgroup --system ootils \
    && adduser --system --ingroup ootils --home /app ootils \
    && chown -R ootils:ootils /app

USER ootils

CMD ["uvicorn", "ootils_core.api.app:app", "--host", "0.0.0.0", "--port", "8000"]
