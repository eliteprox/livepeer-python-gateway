from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


class LivepeerGatewayError(RuntimeError):
    """Base error for the library."""


class SessionRefreshRequired(LivepeerGatewayError):
    """Raised when remote signer returns HTTP 480, indicating OrchestratorInfo refresh needed."""
    pass
