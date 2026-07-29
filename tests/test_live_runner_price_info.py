"""Unit tests for live-runner registration/discovery pricing (incl. fixed)."""
from __future__ import annotations

import pytest

from livepeer_gateway.live_runner import (
    LiveRunnerInstance,
    LiveRunnerPriceInfo,
    _payment_type_for_runner,
)


def test_price_info_defaults_to_usd_hour() -> None:
    info = LiveRunnerPriceInfo(price="0.05")
    assert info.currency == "usd"
    assert info.unit == "hour"
    assert info.to_json() == {"price": "0.05", "currency": "usd", "unit": "hour"}


def test_price_info_fixed_unit() -> None:
    info = LiveRunnerPriceInfo(price=0.01, unit="fixed")
    assert info.unit == "fixed"
    assert info.to_json() == {"price": 0.01, "currency": "usd", "unit": "fixed"}


@pytest.mark.parametrize("unit", ["hour", "720p", "fixed", "HOUR", " Fixed "])
def test_price_info_normalizes_unit(unit: str) -> None:
    info = LiveRunnerPriceInfo(price=1, unit=unit)
    assert info.unit == unit.strip().lower()


def test_price_info_rejects_unknown_unit() -> None:
    with pytest.raises(ValueError, match="hour, 720p, or fixed"):
        LiveRunnerPriceInfo(price=1, unit="seconds")


def test_price_info_rejects_non_usd_currency() -> None:
    with pytest.raises(ValueError, match="currency must be usd"):
        LiveRunnerPriceInfo(price=1, currency="wei")


def test_payment_type_fixed_from_discovery() -> None:
    runner = LiveRunnerInstance(
        url="https://orch.example/apps/r1/session",
        app="demo/fixed",
        runner_id="r1",
        mode="single-shot",
        orchestrator_url="https://orch.example",
        raw={"price_info": {"price": "7", "currency": "wei", "unit": "fixed"}},
    )
    assert _payment_type_for_runner(runner) == "fixed"


def test_payment_type_seconds_maps_to_live() -> None:
    runner = LiveRunnerInstance(
        url="https://orch.example/apps/r1/session",
        app="demo/live",
        runner_id="r1",
        mode="persistent",
        orchestrator_url="https://orch.example",
        raw={"price_info": {"price": "10", "currency": "wei", "unit": "seconds"}},
    )
    assert _payment_type_for_runner(runner) == "live"


def test_payment_type_defaults_to_lv2v() -> None:
    runner = LiveRunnerInstance(
        url="https://orch.example/apps/r1/session",
        app="demo/lv2v",
        runner_id="r1",
        mode="persistent",
        orchestrator_url="https://orch.example",
        raw={"price_info": {"price": "10", "currency": "wei", "unit": "720p-pixel-seconds"}},
    )
    assert _payment_type_for_runner(runner) == "lv2v"
    assert _payment_type_for_runner(None) == "lv2v"


def test_fixed_payment_session_is_not_retained_for_topups() -> None:
    from livepeer_gateway.live_runner import _is_fixed_payment_type
    from livepeer_gateway.remote_signer import LivePaymentSession

    fixed = LivePaymentSession(
        signer_url="https://signer.example",
        type="fixed",
        payment_params="params",
        manifest_id="m1",
    )
    lv2v = LivePaymentSession(
        signer_url="https://signer.example",
        type="lv2v",
        payment_params="params",
        manifest_id="m1",
    )
    assert _is_fixed_payment_type(fixed)
    assert not _is_fixed_payment_type(lv2v)
