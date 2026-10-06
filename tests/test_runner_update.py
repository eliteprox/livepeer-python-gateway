import asyncio

from livepeer_gateway.live_runner import LiveRunnerPriceInfo, LiveRunnerRegistration


def test_update_changes_the_next_heartbeat_payload():
    registration = LiveRunnerRegistration(
        orchestrator_url="https://orch.example",
        secret="bootstrap",
        runner_url="http://127.0.0.1:8720",
        app="comfystream/flux-klein",
        price_info=LiveRunnerPriceInfo(price=0),
        metadata='{"compute":"cold"}',
        capacity=1,
    )

    asyncio.run(registration.update(metadata='{"compute":"warm"}', capacity=1))

    payload = registration._payload()
    assert payload["metadata"] == '{"compute":"warm"}'
    assert payload["capacity"] == 1


def test_note_session_ended_removes_the_session_from_the_heartbeat():
    registration = LiveRunnerRegistration(
        orchestrator_url="https://orch.example",
        secret="bootstrap",
        runner_url="http://127.0.0.1:8720",
        app="comfystream/flux-klein",
        price_info=LiveRunnerPriceInfo(price=0),
        capacity=1,
    )
    registration._reserve_session_id("sess-1")

    asyncio.run(registration.note_session_ended("sess-1"))

    assert registration._payload()["session_ids"] == []
