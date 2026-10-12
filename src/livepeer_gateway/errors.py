from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal


RunnerAttemptOutcome = Literal[
    "pending",  # still in flight
    "succeeded",
    "payment_required",  # runner answered 402; the next attempt pays
    "refused",  # runner answered 503: no capacity
    "http",  # runner answered with any other error status
    "payment",  # the signer failed, so the request never left
    "unreachable",  # the request never reached the runner
    "timeout",  # no answer in time; the runner may have the request
    "disconnected",  # the connection dropped after the request left
    "invalid_response",
    "cancelled",
    "error",
]


@dataclass
class RunnerAttempt:
    """One request ``call_runner`` made, or set out to make, to the runner."""

    # The payment challenge this attempt answered; "" when it went out unpaid.
    manifest_id: str = ""
    # Signer auth id of the payment session this attempt opened. The signer
    # charges when it signs, so this is set even if the request never left.
    auth_id: str = ""
    # True once the request left with Livepeer-Payment headers.
    payment_sent: bool = False
    # The runner's HTTP status, when known.
    status_code: int | None = None
    outcome: RunnerAttemptOutcome = "pending"


@dataclass
class RunnerCallRecord:
    """Every attempt of one ``call_runner`` call, written as each step happens.

    The result, stream, or error of the call holds this same record. Pass one
    to ``call_runner`` to read it even when the call is cancelled.
    """

    runner_url: str = ""
    attempts: list[RunnerAttempt] = field(default_factory=list)

    @property
    def payment_sent(self) -> bool:
        """True once any attempt's request left with ``Livepeer-Payment`` headers.

        The call is then bound to this runner; another runner is safe only
        while this is False.
        """
        return any(attempt.payment_sent for attempt in self.attempts)

    @property
    def auth_ids(self) -> tuple[str, ...]:
        """Signer auth ids of every payment session the call opened, in order."""
        return tuple(dict.fromkeys(a.auth_id for a in self.attempts if a.auth_id))

    @property
    def outcome(self) -> RunnerAttemptOutcome:
        """How the last attempt ended; ``pending`` before the first one."""
        return self.attempts[-1].outcome if self.attempts else "pending"


class LivepeerGatewayError(RuntimeError):
    """Base error for the library.

    ``record`` is set on every error ``call_runner`` raises: the attempts it
    made against the runner, with what was signed, sent, and how each ended.
    ``payment_sent`` and ``auth_ids`` read from it, and are empty without one.
    """

    record: RunnerCallRecord | None = None

    @property
    def payment_sent(self) -> bool:
        """True once any attempt's request carried ``Livepeer-Payment`` headers.

        The call is then bound to this runner; another runner is safe only
        while this is False.
        """
        return self.record is not None and self.record.payment_sent

    @property
    def auth_ids(self) -> tuple[str, ...]:
        """Signer auth ids of every payment session the call opened.

        The signer charges when it signs, so these can be set while
        ``payment_sent`` is still False.
        """
        return self.record.auth_ids if self.record is not None else ()


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


@dataclass
class RunnerRejection:
    """Records a single runner that was tried and rejected."""
    url: str
    reason: str


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
