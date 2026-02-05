from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


class LivepeerGatewayError(RuntimeError):
    """Base error for the library."""


class NoOrchestratorAvailableError(LivepeerGatewayError):
    """Raised when no orchestrator could be selected."""


class SignerRefreshRequired(LivepeerGatewayError):
    """Raised when the remote signer returns HTTP 480 and a refresh is required."""


class PaymentError(LivepeerGatewayError):
    """Raised when a PaymentSession operation fails."""
