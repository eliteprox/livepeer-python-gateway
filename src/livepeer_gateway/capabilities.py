from __future__ import annotations

import json
import ssl
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Mapping, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

if TYPE_CHECKING:
    from . import lp_rpc_pb2

CAPABILITY_ID_TO_NAME: dict[int, str] = {
    -2: "Invalid",
    -1: "Unused",
    0: "H.264",
    1: "MPEGTS",
    2: "MP4",
    3: "Fractional framerates",
    4: "Storage direct",
    5: "Storage S3",
    6: "Storage GCS",
    7: "H264 Baseline profile",
    8: "H264 Main profile",
    9: "H264 High profile",
    10: "H264 Constrained, Contained High profile",
    11: "GOP",
    12: "Auth token",
    14: "MPEG7 signature",
    15: "HEVC decode",
    16: "HEVC encode",
    17: "VP8 decode",
    18: "VP9 decode",
    19: "VP8 encode",
    20: "VP9 encode",
    21: "H264 Decode YUV444 8-bit",
    22: "H264 Decode YUV422 8-bit",
    23: "H264 Decode YUV444 10-bit",
    24: "H264 Decode YUV422 10-bit",
    25: "H264 Decode YUV420 10-bit",
    26: "Segment slicing",
    27: "Text to image",
    28: "Image to image",
    29: "Image to video",
    30: "Upscale",
    31: "Audio to text",
    32: "Segment anything 2",
    33: "Llm",
    34: "Image to text",
    35: "Live video to video",
    36: "Text to speech",
    37: "byoc_external",
}

CAPABILITY_BYOC_EXTERNAL = 37


def capability_name(cap_id: int) -> str:
    return CAPABILITY_ID_TO_NAME.get(cap_id, "Unknown capability")


def format_capability(cap_id: int) -> str:
    return f"{capability_name(cap_id)} ({cap_id})"


def compute_available(capacity: int, in_use: int) -> int:
    return max(0, capacity - in_use)


def get_per_capability_map(capabilities: Any) -> Mapping[int, Any]:
    """
    Best-effort access to the PerCapability map across protobuf field name variants.
    """
    constraints = getattr(capabilities, "constraints", None)
    if constraints is None:
        return {}

    for name in ("PerCapability", "per_capability", "perCapability"):
        if hasattr(constraints, name):
            return getattr(constraints, name)

    return {}


def get_capacity_in_use(model_constraint: Any) -> int:
    if hasattr(model_constraint, "capacityInUse"):
        return int(getattr(model_constraint, "capacityInUse") or 0)
    if hasattr(model_constraint, "capacity_in_use"):
        return int(getattr(model_constraint, "capacity_in_use") or 0)
    return 0


@dataclass
class ExternalCapability:
    """Represents an external/BYOC capability offered by an orchestrator.

    Note: Pricing is not included here. Use byoc.GetBYOCJobToken() to fetch
    accurate per-sender pricing from the orchestrator's /process/token endpoint.
    """

    name: str
    description: str
    capacity: int
    capacity_in_use: int

    @property
    def capacity_available(self) -> int:
        """Return the available capacity (total - in use)."""
        return compute_available(self.capacity, self.capacity_in_use)


@dataclass
class BYOCCapabilityPrice:
    """A BYOC capability with its advertised pricing from OrchestratorInfo.capabilities_prices."""

    name: str
    price_per_unit: int
    pixels_per_unit: int


def get_byoc_capabilities_from_prices(
    info: lp_rpc_pb2.OrchestratorInfo,
) -> list[BYOCCapabilityPrice]:
    """Extract BYOC capabilities from OrchestratorInfo.capabilities_prices.

    Looks for entries with capability == CAPABILITY_BYOC_EXTERNAL (37).
    The capability name is in the constraint field.

    Args:
        info: The OrchestratorInfo protobuf message from gRPC response.

    Returns:
        List of BYOCCapabilityPrice instances with name and advertised pricing.
    """
    result: list[BYOCCapabilityPrice] = []
    for pi in info.capabilities_prices:
        if pi.capability == CAPABILITY_BYOC_EXTERNAL and pi.constraint:
            result.append(
                BYOCCapabilityPrice(
                    name=pi.constraint,
                    price_per_unit=pi.pricePerUnit,
                    pixels_per_unit=pi.pixelsPerUnit,
                )
            )
    return result


def get_external_capabilities(
    info: lp_rpc_pb2.OrchestratorInfo,
) -> list[ExternalCapability]:
    """Extract external/BYOC capabilities from OrchestratorInfo.

    Args:
        info: The OrchestratorInfo protobuf message from gRPC response.

    Returns:
        List of ExternalCapability instances parsed from the info.

    Note: Pricing is not included in the returned capabilities.
    Use byoc.GetBYOCJobToken() to fetch accurate per-sender pricing.

    Deprecated: Use get_byoc_capabilities_from_prices() or
    fetch_external_capabilities() instead.
    """
    result: list[ExternalCapability] = []
    ext_caps = getattr(info, "external_capabilities", None)
    if ext_caps is None:
        return result

    for cap in ext_caps:
        result.append(
            ExternalCapability(
                name=getattr(cap, "name", ""),
                description=getattr(cap, "description", ""),
                capacity=getattr(cap, "capacity", 0),
                capacity_in_use=getattr(cap, "capacity_in_use", 0)
                or getattr(cap, "capacityInUse", 0),
            )
        )
    return result


def fetch_external_capabilities(
    orch_url: str,
    timeout: float = 10.0,
    ssl_context: Optional[ssl.SSLContext] = None,
) -> list[ExternalCapability]:
    """Fetch external/BYOC capabilities from orchestrator's HTTP endpoint.

    Args:
        orch_url: The orchestrator URL (e.g., "https://10.10.7.61:8933").
        timeout: Request timeout in seconds.
        ssl_context: Optional SSL context for HTTPS connections.

    Returns:
        List of ExternalCapability instances.

    Raises:
        Exception: If the request fails or returns invalid data.

    Note: Pricing is not included in the returned capabilities.
    Use byoc.GetBYOCJobToken() to fetch accurate per-sender pricing.
    """
    # Normalize URL
    url = orch_url.rstrip("/") + "/byoc/capabilities"

    # Create SSL context if not provided (allow self-signed certs)
    if ssl_context is None:
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

    request = Request(url, method="GET")
    request.add_header("Accept", "application/json")

    try:
        with urlopen(request, timeout=timeout, context=ssl_context) as response:
            data = json.loads(response.read().decode("utf-8"))
    except HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", errors="replace")
        except Exception:
            pass
        raise Exception(
            f"Failed to fetch capabilities from {url}: HTTP {e.code} - {body}"
        ) from e
    except URLError as e:
        raise Exception(f"Failed to connect to {url}: {e.reason}") from e

    result: list[ExternalCapability] = []
    for cap in data:
        result.append(
            ExternalCapability(
                name=cap.get("name", ""),
                description=cap.get("description", ""),
                capacity=cap.get("capacity", 0),
                capacity_in_use=cap.get("capacity_in_use", 0)
                or cap.get("capacityInUse", 0),
            )
        )
    return result

