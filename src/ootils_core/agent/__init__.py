"""
ootils_core.agent — Autonomous planning agent (M7 demo).

The agent is a deterministic, rule-based pipeline that:
  1. Queries the REST API for high-severity issues
  2. Retrieves causal explanations for each issue
  3. Runs simulations for issues with identifiable root causes
  4. Produces structured recommendations without human input
"""
from ootils_core.agent.demo_agent import OotilsAgent

__all__ = ["OotilsAgent"]
