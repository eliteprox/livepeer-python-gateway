from __future__ import annotations

from dataclasses import dataclass


class LivepeerGatewayError(RuntimeError):
    """Base error for the library."""


class LivepeerHTTPError(LivepeerGatewayError):
    """Raised when an HTTP endpoint returns a non-success status."""

    def __init__(self, status_code: int, url: str, body: str = "", message: str | None = None) -> None:
        self.status_code = int(status_code)
        self.url = url
        self.body = body
        super().__init__(message or f"HTTP {status_code} from endpoint (url={url})")


@dataclass
class OrchestratorRejection:
    """Records a single orchestrator that was tried and rejected."""
    url: str
    reason: str


def _format_rejections(
    message: str,
    rejections: list[OrchestratorRejection],
    *,
    max_lines: int = 8,
) -> str:
    if not rejections:
        return message
    lines = [message, "Rejections:"]
    for rejection in rejections[:max_lines]:
        lines.append(f"  - {rejection.url}: {rejection.reason}")
    remaining = len(rejections) - max_lines
    if remaining > 0:
        lines.append(f"  ... and {remaining} more")
    return "\n".join(lines)


class NoOrchestratorAvailableError(LivepeerGatewayError):
    """Raised when no orchestrator could be selected."""

    def __init__(self, message: str, rejections: list[OrchestratorRejection] | None = None) -> None:
        super().__init__(message)
        self.rejections: list[OrchestratorRejection] = rejections or []

    def __str__(self) -> str:
        return _format_rejections(super().__str__(), self.rejections)


class SignerRefreshRequired(LivepeerGatewayError):
    """Raised when the remote signer returns HTTP 480 and a refresh is required."""

    def __init__(
        self,
        message: str,
        *,
        orchestrator_url: str | None = None,
    ) -> None:
        super().__init__(message)
        self.orchestrator_url = orchestrator_url


class SkipPaymentCycle(LivepeerGatewayError):
    """Raised when the signer returns HTTP 482 to skip a payment cycle."""


class PaymentError(LivepeerGatewayError):
    """Raised when a PaymentSession operation fails."""
