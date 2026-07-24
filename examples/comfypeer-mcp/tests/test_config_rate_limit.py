from comfypeer_mcp.config import Settings, is_loopback_url
from comfypeer_mcp.rate_limit import RateLimitExceeded, RateLimiter


def test_is_loopback_url():
    assert is_loopback_url("http://localhost:8935/discovery")
    assert is_loopback_url("https://127.0.0.1/x")
    assert not is_loopback_url(
        "https://discovery-service-production-8955.up.railway.app/v1/discovery/raw"
    )


def test_rate_limiter_trips():
    limiter = RateLimiter(max_requests=2, window_seconds=60)
    limiter.check("k")
    limiter.check("k")
    try:
        limiter.check("k")
        assert False, "expected RateLimitExceeded"
    except RateLimitExceeded:
        pass


def test_settings_default_discovery():
    settings = Settings(
        DISCOVERY_SERVICE_URL="https://discovery.example",
        DEFAULT_DISCOVERY_URL="",
    )
    assert (
        settings.resolved_default_discovery_url()
        == "https://discovery.example/v1/discovery/raw?serviceType=live-runner"
    )
