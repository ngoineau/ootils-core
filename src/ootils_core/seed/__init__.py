"""
ootils_core.seed — realistic dataset generator.

Generates a discrete-manufacturing dataset (5K SKU pyramid, 14 locations,
50 suppliers, multi-level BOMs, 12 months history, calibrated to produce
~7% shortages) for performance benchmarking and integration testing.

Entry point: `ootils_core.seed.generator.generate(profile, conn)` or the
CLI wrapper at `scripts/seed_realistic_dataset.py`.
"""
