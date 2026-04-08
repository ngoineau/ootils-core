"""DQ Engine — Data Quality pipeline for ingest batches."""
from .engine import run_dq, DQResult

__all__ = ["run_dq", "DQResult"]
