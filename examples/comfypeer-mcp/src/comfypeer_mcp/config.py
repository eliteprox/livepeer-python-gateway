from __future__ import annotations

from functools import lru_cache
from urllib.parse import urlparse

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    pymthouse_base_url: str = Field(
        default="https://pymthouse.com",
        alias="PYMTHOUSE_BASE_URL",
    )
    pymthouse_issuer_url: str = Field(
        default="https://pymthouse.com/api/v1/oidc",
        alias="PYMTHOUSE_ISSUER_URL",
    )
    pymthouse_public_client_id: str = Field(
        default="",
        alias="PYMTHOUSE_PUBLIC_CLIENT_ID",
    )
    pymthouse_m2m_client_id: str = Field(
        default="",
        alias="PYMTHOUSE_M2M_CLIENT_ID",
    )
    pymthouse_m2m_client_secret: str = Field(
        default="",
        alias="PYMTHOUSE_M2M_CLIENT_SECRET",
    )

    discovery_service_url: str = Field(
        default="https://discovery-service-production-8955.up.railway.app",
        alias="DISCOVERY_SERVICE_URL",
    )
    signer_url: str = Field(
        default="",
        alias="SIGNER_URL",
    )
    default_discovery_url: str = Field(
        default="",
        alias="DEFAULT_DISCOVERY_URL",
    )

    mcp_host: str = Field(default="0.0.0.0", alias="MCP_HOST")
    mcp_port: int = Field(default=8090, alias="MCP_PORT")
    mcp_public_url: str = Field(
        default="http://localhost:8090/mcp",
        alias="MCP_PUBLIC_URL",
    )

    rate_limit_max: int = Field(default=60, alias="RATE_LIMIT_MAX")
    rate_limit_window_seconds: int = Field(
        default=60,
        alias="RATE_LIMIT_WINDOW_SECONDS",
    )
    allow_loopback_discovery: bool = Field(
        default=True,
        alias="ALLOW_LOOPBACK_DISCOVERY",
    )

    def base_url(self) -> str:
        return self.pymthouse_base_url.rstrip("/")

    def discovery_base(self) -> str:
        return self.discovery_service_url.rstrip("/")

    def resolved_default_discovery_url(self) -> str:
        if self.default_discovery_url.strip():
            return self.default_discovery_url.strip()
        return f"{self.discovery_base()}/v1/discovery/raw?serviceType=live-runner"

    def require_comfypeer_app(self) -> None:
        if not self.pymthouse_public_client_id.strip():
            raise RuntimeError("PYMTHOUSE_PUBLIC_CLIENT_ID is required")
        if not self.pymthouse_m2m_client_id.strip():
            raise RuntimeError("PYMTHOUSE_M2M_CLIENT_ID is required")
        if not self.pymthouse_m2m_client_secret.strip():
            raise RuntimeError("PYMTHOUSE_M2M_CLIENT_SECRET is required")


@lru_cache
def get_settings() -> Settings:
    return Settings()


def is_loopback_url(url: str) -> bool:
    try:
        host = (urlparse(url).hostname or "").lower()
    except ValueError:
        return False
    return host in {"localhost", "127.0.0.1", "::1"} or host.endswith(".local")
