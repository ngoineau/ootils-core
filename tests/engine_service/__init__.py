"""
tests/engine_service/ — battery for the standalone Rust engine service
(ADR-017 Architecture B).

Tests require:
  - the ootils-engine binary built in rust/target/release/
  - a reachable Postgres with a seeded baseline (DATABASE_URL env)
  - grpcio installed in the venv

If any of those is missing, tests in this directory are skipped at
collection time via the `engine_ready` fixture in conftest.py.

Markers:
  - `slow`  : long-running tests (recovery cycles, soak loads)
  - `live_db` : requires a real DB connection
"""
