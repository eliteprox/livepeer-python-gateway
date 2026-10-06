import aiohttp

from livepeer_gateway.errors import (
    LivepeerGatewayError,
    LivepeerHTTPError,
    NoOrchestratorAvailableError,
    NoRunnerAvailableError,
    OrchestratorRejection,
    RunnerRejection,
    runner_rejection,
)


def test_no_orchestrator_available_error_without_rejections() -> None:
    error = NoOrchestratorAvailableError("No orchestrators available to select")

    assert str(error) == "No orchestrators available to select"


def test_no_orchestrator_available_error_includes_rejections() -> None:
    error = NoOrchestratorAvailableError(
        "All orchestrators failed (2 tried)",
        rejections=[
            OrchestratorRejection(
                url="https://orch-a.example.com", reason="connection refused"
            ),
            OrchestratorRejection(
                url="https://orch-b.example.com", reason="request timed out"
            ),
        ],
    )

    assert str(error) == (
        "All orchestrators failed (2 tried): "
        "https://orch-a.example.com: connection refused; "
        "https://orch-b.example.com: request timed out"
    )


def test_paid_503_is_a_capacity_rejection() -> None:
    error = LivepeerHTTPError(503, "https://runner.example.com", "busy")
    error.payment_sent = True
    error.manifest_id = "manifest-1"

    rejection = runner_rejection("https://runner.example.com", error)

    assert rejection.kind == "capacity"
    assert rejection.payment_sent is True
    assert rejection.manifest_id == "manifest-1"
    assert rejection.reason == str(error)


def test_http_400_is_another_kind_of_rejection() -> None:
    error = LivepeerHTTPError(400, "https://runner.example.com", "bad")

    rejection = runner_rejection("https://runner.example.com", error)

    assert rejection.kind == "other"
    assert rejection.payment_sent is False
    assert rejection.manifest_id == ""


def test_connection_refused_is_unreachable() -> None:
    try:
        raise LivepeerGatewayError("connection refused") from ConnectionRefusedError()
    except LivepeerGatewayError as error:
        rejection = runner_rejection("https://runner.example.com", error)

    assert rejection.kind == "unreachable"
    assert rejection.payment_sent is False


def test_server_disconnect_is_unreachable() -> None:
    try:
        raise LivepeerGatewayError("failed to reach endpoint") from aiohttp.ServerDisconnectedError()
    except LivepeerGatewayError as error:
        rejection = runner_rejection("https://runner.example.com", error)

    assert rejection.kind == "unreachable"


def test_payload_error_is_not_unreachable() -> None:
    try:
        raise LivepeerGatewayError("short read") from aiohttp.ClientPayloadError("short")
    except LivepeerGatewayError as error:
        rejection = runner_rejection("https://runner.example.com", error)

    assert rejection.kind == "other"


def test_text_mentioning_capacity_is_not_a_capacity_refusal() -> None:
    error = LivepeerGatewayError("capacity exhausted")

    rejection = runner_rejection("https://runner.example.com", error)

    assert rejection.kind == "other"


def test_no_runner_available_keeps_the_paid_rejection() -> None:
    error = NoRunnerAvailableError(
        "All runners failed (1 tried)",
        rejections=[
            RunnerRejection(
                url="https://runner.example.com",
                reason="busy",
                kind="capacity",
                payment_sent=True,
                manifest_id="manifest-1",
            )
        ],
    )

    assert error.payment_sent is True
    assert error.manifest_id == "manifest-1"
    assert str(error) == "All runners failed (1 tried): https://runner.example.com: busy"
