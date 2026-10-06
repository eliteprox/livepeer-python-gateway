from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import aiohttp


class LivepeerGatewayError(RuntimeError):
    """Base error for the library.

    ``payment_sent`` is True when the error comes from a ``call_runner`` attempt
    whose request carried ``Livepeer-Payment`` headers, that is, after a payment
    challenge had already been answered. A gateway may retry another runner only
    while it is False; once tickets are sent the request is bound to that runner.

    ``manifest_id`` is the answered payment challenge's ``manifest_id`` when
    ``payment_sent`` is True, and ``""`` otherwise, so a gateway can attribute
    the cost of a paid call that failed.
    """

    payment_sent: bool = False
    manifest_id: str = ""


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


RunnerFailureKind = Literal["capacity", "unreachable", "other"]


@dataclass
class RunnerRejection:
    """Records a single runner that was tried and rejected.

    ``kind`` is ``capacity`` for an HTTP 503, ``unreachable`` when the runner
    never answered, and ``other`` for every remaining failure. ``payment_sent``
    is copied from the ``call_runner`` error: True only after that attempt's
    request carried ``Livepeer-Payment`` headers. ``manifest_id`` is that
    attempt's paid challenge id, or ``""`` when it did not pay.
    """

    url: str
    reason: str
    kind: RunnerFailureKind = "other"
    payment_sent: bool = False
    manifest_id: str = ""


def runner_rejection(url: str, error: BaseException) -> RunnerRejection:
    """Record one failed runner attempt without dropping payment or its kind."""
    payment_sent = False
    manifest_id = ""
    if isinstance(error, LivepeerGatewayError) and error.payment_sent:
        payment_sent = True
        manifest_id = error.manifest_id
    return RunnerRejection(
        url=url,
        reason=str(error),
        kind=_runner_failure_kind(error),
        payment_sent=payment_sent,
        manifest_id=manifest_id,
    )


def _runner_failure_kind(error: BaseException) -> RunnerFailureKind:
    if isinstance(error, LivepeerHTTPError) and error.status_code == 503:
        return "capacity"
    current: BaseException | None = error
    while current is not None:
        if isinstance(current, (TimeoutError, ConnectionError, OSError, aiohttp.ClientConnectionError)):
            return "unreachable"
        current = current.__cause__
    return "other"


class NoOrchestratorAvailableError(LivepeerGatewayError):
    """Raised when no orchestrator could be selected."""

    def __init__(self, message: str, rejections: list[OrchestratorRejection] | None = None) -> None:
        super().__init__(message)
        self.rejections: list[OrchestratorRejection] = rejections or []

    def __str__(self) -> str:
        message = super().__str__()
        if not self.rejections:
            return message
        reasons = "; ".join(f"{r.url}: {r.reason}" for r in self.rejections)
        return f"{message}: {reasons}"


class NoRunnerAvailableError(LivepeerGatewayError):
    """Raised when no runner could be selected."""

    def __init__(self, message: str, rejections: list[RunnerRejection] | None = None) -> None:
        super().__init__(message)
        self.rejections: list[RunnerRejection] = rejections or []
        paid = [rejection for rejection in self.rejections if rejection.payment_sent]
        self.payment_sent = bool(paid)
        self.manifest_id = paid[-1].manifest_id if paid else ""

    def __str__(self) -> str:
        message = super().__str__()
        if not self.rejections:
            return message
        reasons = "; ".join(f"{r.url}: {r.reason}" for r in self.rejections)
        return f"{message}: {reasons}"


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
