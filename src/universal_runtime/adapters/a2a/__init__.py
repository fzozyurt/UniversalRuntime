"""Official A2A edge adapter.

The SDK import is intentionally kept inside this adapter. Core domain and
application services remain independent of A2A transport types.
"""

from universal_runtime.adapters.a2a.agent_card import build_agent_card
from universal_runtime.adapters.a2a.server import create_a2a_routes

__all__ = ["build_agent_card", "create_a2a_routes"]
