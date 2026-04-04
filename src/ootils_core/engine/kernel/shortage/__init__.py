"""
shortage — Shortage detection kernel for Sprint M4.

Detects inventory shortages from ProjectedInventory nodes with closing_stock < 0,
computes severity scores, and persists ShortageRecord rows.
"""
from ootils_core.engine.kernel.shortage.detector import ShortageDetector

__all__ = ["ShortageDetector"]
