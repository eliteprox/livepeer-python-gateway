"""Clear cached OIDC device-login tokens (~/.cache/livepeer-gateway/tokens/)."""

from livepeer_gateway.oidc_auth import clear_all_cached_tokens

if __name__ == "__main__":
    print(clear_all_cached_tokens(), "token file(s) removed")
