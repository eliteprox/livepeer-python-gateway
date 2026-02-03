"""
BYOC (Bring Your Own Container) support for Python gateway.

This module provides functions to interact with external/BYOC capabilities
offered by Livepeer orchestrators, including single-shot jobs and streaming jobs.
"""
from __future__ import annotations

import base64
import json
import logging
import ssl
import time
from dataclasses import dataclass
from typing import Any, Optional
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from . import lp_rpc_pb2
from .control import Control
from .events import Events
from .media_output import MediaOutput
from .media_publish import MediaPublish, MediaPublishConfig
from .orchestrator import (
    PaymentState,
    _normalize_https_base_url,
)

# HTTP header constants (from go-livepeer/byoc/types.go)
HEADER_JOB_REQUEST = "Livepeer"
HEADER_ETH_ADDRESS = "Livepeer-Eth-Address"
HEADER_CAPABILITY = "Livepeer-Capability"
HEADER_PAYMENT = "Livepeer-Payment"
HEADER_PAYMENT_BALANCE = "Livepeer-Balance"

# Response headers for trickle URLs (from go-livepeer/byoc/stream_orchestrator.go)
HEADER_CONTROL_URL = "X-Control-Url"
HEADER_EVENTS_URL = "X-Events-Url"
HEADER_PUBLISH_URL = "X-Publish-Url"
HEADER_SUBSCRIBE_URL = "X-Subscribe-Url"
HEADER_DATA_URL = "X-Data-Url"

# Timeout for BYOC requests
BYOC_REQUEST_TIMEOUT = 60.0


@dataclass
class BYOCJobRequest:
    """Request configuration for a BYOC job."""

    capability: str
    request: dict[str, Any]
    timeout_seconds: int = 30
    parameters: Optional[dict[str, Any]] = None


@dataclass
class BYOCJobResponse:
    """Response from a single-shot BYOC job."""

    raw: dict[str, Any]
    balance: Optional[int] = None


@dataclass
class BYOCStreamJob:
    """Represents an active BYOC streaming job."""

    raw: dict[str, Any]
    stream_id: str
    capability: str
    orchestrator_url: str
    publish_url: Optional[str] = None
    subscribe_url: Optional[str] = None
    control_url: Optional[str] = None
    events_url: Optional[str] = None
    data_url: Optional[str] = None
    balance: Optional[int] = None

    _control: Optional[Control] = None
    _events: Optional[Events] = None

    @property
    def control(self) -> Optional[Control]:
        """Get the control channel for sending commands to the BYOC worker."""
        if self._control is None and self.control_url:
            self._control = Control(self.control_url)
        return self._control

    @property
    def events(self) -> Optional[Events]:
        """Get the events channel for receiving events from the BYOC worker."""
        if self._events is None and self.events_url:
            self._events = Events(self.events_url)
        return self._events

    def start_media_publish(self, config: MediaPublishConfig) -> MediaPublish:
        """Start media publishing to this BYOC stream.

        Args:
            config: Configuration for media publishing.

        Returns:
            MediaPublish instance for publishing media frames.

        Raises:
            ValueError: If video ingress is not enabled for this stream.
        """
        if not self.publish_url:
            raise ValueError("Video ingress not enabled for this stream (no publish_url)")
        return MediaPublish(
            self.publish_url,
            fps=config.fps,
            mime_type=config.mime_type,
            keyframe_interval_s=config.keyframe_interval_s,
        )

    def media_output(
        self,
        output_path: Optional[str] = None,
        output_url: Optional[str] = None,
        max_frames: int = -1,
        write_pts: bool = False,
    ) -> MediaOutput:
        """Create a media output subscriber for receiving processed video.

        Args:
            output_path: Local path to write output frames.
            output_url: URL to stream output frames to.
            max_frames: Maximum number of frames to receive (-1 for unlimited).
            write_pts: Whether to include PTS in output filenames.

        Returns:
            MediaOutput instance for receiving processed frames.

        Raises:
            ValueError: If video egress is not enabled for this stream.
        """
        if not self.subscribe_url:
            raise ValueError("Video egress not enabled for this stream (no subscribe_url)")
        return MediaOutput(
            self.subscribe_url,
            output_path=output_path,
            output_url=output_url,
            max_frames=max_frames,
            write_pts=write_pts,
        )


def _get_capability_price(
    info: lp_rpc_pb2.OrchestratorInfo,
    capability: str,
) -> Optional[lp_rpc_pb2.PriceInfo]:
    """Get the price info for a BYOC capability.

    Args:
        info: OrchestratorInfo containing external capabilities.
        capability: Name of the BYOC capability.

    Returns:
        PriceInfo if found, None otherwise.
    """
    ext_caps = getattr(info, "external_capabilities", None)
    if ext_caps:
        for cap in ext_caps:
            if cap.name == capability:
                return cap.price_info
    return None


def _sign_byoc_job(
    signer_base_url: str,
    request_str: str,
    parameters_str: str,
) -> tuple[str, str]:
    """Sign a BYOC job request using the remote signer.

    Args:
        signer_base_url: Base URL of the remote signer.
        request_str: JSON string of the request field.
        parameters_str: JSON string of the parameters field.

    Returns:
        Tuple of (sender_address, signature_hex)
    """
    from .orchestrator import _normalize_https_origin, post_json

    base = _normalize_https_origin(signer_base_url)
    url = f"{base}/sign-byoc-job"

    payload = {
        "request": request_str,
        "parameters": parameters_str,
    }

    data = post_json(url, payload)

    sender = data.get("sender")
    signature = data.get("signature")

    if not sender or not signature:
        from .errors import LivepeerGatewayError
        raise LivepeerGatewayError(f"Invalid response from /sign-byoc-job: {data}")

    return sender, signature


def _build_job_request(
    capability: str,
    request_dict: dict[str, Any],
    timeout_seconds: int = 30,
    parameters: Optional[dict[str, Any]] = None,
    stream_id: Optional[str] = None,
    signer_base_url: Optional[str] = None,
) -> tuple[str, str]:
    """Build a BYOC job request structure.

    Args:
        capability: Name of the BYOC capability.
        request_dict: Request data dictionary.
        timeout_seconds: Timeout for the job.
        parameters: Optional job parameters.
        stream_id: Optional stream ID (generated if not provided).
        signer_base_url: Remote signer URL for signing the request.

    Returns:
        Tuple of (job_request_json, base64_encoded_request)
    """
    import uuid

    request_id = stream_id or str(uuid.uuid4())
    request_json_str = json.dumps(request_dict)
    parameters_str = json.dumps(parameters) if parameters else ""

    job_request: dict[str, Any] = {
        "id": request_id,
        "capability": capability,
        "request": request_json_str,
        "timeout_seconds": timeout_seconds,
    }

    if parameters_str:
        job_request["parameters"] = parameters_str

    # Sign the request if signer URL is provided
    if signer_base_url:
        sender, signature = _sign_byoc_job(signer_base_url, request_json_str, parameters_str)
        job_request["sender"] = sender
        job_request["sig"] = signature

    job_request_json = json.dumps(job_request)
    job_request_b64 = base64.b64encode(job_request_json.encode()).decode()

    return job_request_json, job_request_b64


def GetBYOCPayment(
    signer_base_url: str,
    info: lp_rpc_pb2.OrchestratorInfo,
    capability: str,
    state: Optional[PaymentState] = None,
    manifest_id: Optional[str] = None,
):
    """Get payment credentials for a BYOC capability from the remote signer.

    This function requests payment credentials from the remote signer using
    the "byoc" job type. The payment is time-based rather than pixel-based.

    Args:
        signer_base_url: Base URL of the remote signer.
        info: OrchestratorInfo containing ticket params and pricing.
        capability: Name of the BYOC capability (e.g., "comfystream").
        state: Previous payment state for nonce continuity.
        manifest_id: Optional manifest ID for balance tracking.

    Returns:
        GetPaymentResponse with payment credentials and updated state.
    """
    from .orchestrator import GetPayment

    return GetPayment(
        signer_base_url,
        info,
        typ="byoc",
        capability=capability,
        state=state,
        manifest_id=manifest_id,
    )


def StartBYOCJob(
    info: lp_rpc_pb2.OrchestratorInfo,
    req: BYOCJobRequest,
    signer_base_url: Optional[str] = None,
    payment_state: Optional[PaymentState] = None,
) -> BYOCJobResponse:
    """Execute a single-shot BYOC job on the orchestrator.

    Args:
        info: OrchestratorInfo from GetOrchestratorInfo.
        req: BYOC job request configuration.
        signer_base_url: Base URL of remote signer (required for signing and payments).
        payment_state: Previous payment state for nonce continuity.

    Returns:
        BYOCJobResponse with the job result.
    """
    base_url = _normalize_https_base_url(info.transcoder)
    url = f"{base_url}/process/request/{req.capability}"

    # Build and sign job request (signer_base_url is required for BYOC)
    _, job_request_b64 = _build_job_request(
        capability=req.capability,
        request_dict=req.request,
        timeout_seconds=req.timeout_seconds,
        parameters=req.parameters,
        signer_base_url=signer_base_url,
    )

    headers = {
        HEADER_JOB_REQUEST: job_request_b64,
        HEADER_CAPABILITY: req.capability,
        "Content-Type": "application/json",
    }

    # Get payment if signer is provided
    if signer_base_url:
        payment_resp = GetBYOCPayment(
            signer_base_url,
            info,
            req.capability,
            state=payment_state,
        )
        headers[HEADER_PAYMENT] = payment_resp.payment

    # Make request
    body = json.dumps(req.request).encode()
    request = Request(url, data=body, headers=headers, method="POST")
    ssl_ctx = ssl._create_unverified_context()

    try:
        with urlopen(request, timeout=BYOC_REQUEST_TIMEOUT, context=ssl_ctx) as resp:
            response_data = resp.read()
            balance_str = resp.headers.get(HEADER_PAYMENT_BALANCE)
            balance = int(balance_str) if balance_str else None

            try:
                raw = json.loads(response_data)
            except json.JSONDecodeError:
                raw = {"raw_response": response_data.decode("utf-8", errors="replace")}

            return BYOCJobResponse(raw=raw, balance=balance)

    except HTTPError as e:
        error_body = e.read().decode("utf-8", errors="replace")
        logging.error(
            "BYOC job failed: HTTP %d - %s",
            e.code,
            error_body,
        )
        raise


def StartBYOCStream(
    info: lp_rpc_pb2.OrchestratorInfo,
    capability: str,
    params: dict[str, Any],
    signer_base_url: Optional[str] = None,
    payment_state: Optional[PaymentState] = None,
    enable_video_ingress: bool = True,
    enable_video_egress: bool = True,
    enable_data_output: bool = False,
    timeout_seconds: int = 30,
) -> BYOCStreamJob:
    """Start a streaming BYOC job on the orchestrator.

    Args:
        info: OrchestratorInfo from GetOrchestratorInfo.
        capability: Name of the BYOC capability (e.g., "comfystream").
        params: Parameters to pass to the BYOC worker.
        signer_base_url: Base URL of remote signer (for paid jobs).
        payment_state: Previous payment state for nonce continuity.
        enable_video_ingress: Enable video input publishing.
        enable_video_egress: Enable video output subscribing.
        enable_data_output: Enable data output channel.
        timeout_seconds: Timeout for the stream start request.

    Returns:
        BYOCStreamJob with trickle URLs and helper methods.
    """
    import uuid

    base_url = _normalize_https_base_url(info.transcoder)
    url = f"{base_url}/ai/stream/start"

    stream_id = str(uuid.uuid4())

    # Build job parameters
    job_parameters = {
        "enable_video_ingress": enable_video_ingress,
        "enable_video_egress": enable_video_egress,
        "enable_data_output": enable_data_output,
    }

    # Build and sign job request (signer_base_url is required for BYOC)
    _, job_request_b64 = _build_job_request(
        capability=capability,
        request_dict=params,
        timeout_seconds=timeout_seconds,
        parameters=job_parameters,
        stream_id=stream_id,
        signer_base_url=signer_base_url,
    )

    headers = {
        HEADER_JOB_REQUEST: job_request_b64,
        HEADER_CAPABILITY: capability,
        "Content-Type": "application/json",
    }

    # Get payment if signer is provided
    if signer_base_url:
        payment_resp = GetBYOCPayment(
            signer_base_url,
            info,
            capability,
            state=payment_state,
            manifest_id=capability,
        )
        headers[HEADER_PAYMENT] = payment_resp.payment

    # Make request
    body = json.dumps(params).encode()
    request = Request(url, data=body, headers=headers, method="POST")
    ssl_ctx = ssl._create_unverified_context()

    try:
        with urlopen(request, timeout=BYOC_REQUEST_TIMEOUT, context=ssl_ctx) as resp:
            response_data = resp.read()

            # Extract trickle URLs from response headers
            publish_url = resp.headers.get(HEADER_PUBLISH_URL)
            subscribe_url = resp.headers.get(HEADER_SUBSCRIBE_URL)
            control_url = resp.headers.get(HEADER_CONTROL_URL)
            events_url = resp.headers.get(HEADER_EVENTS_URL)
            data_url = resp.headers.get(HEADER_DATA_URL)
            balance_str = resp.headers.get(HEADER_PAYMENT_BALANCE)
            balance = int(balance_str) if balance_str else None

            try:
                raw = json.loads(response_data)
            except json.JSONDecodeError:
                raw = {"raw_response": response_data.decode("utf-8", errors="replace")}

            return BYOCStreamJob(
                raw=raw,
                stream_id=stream_id,
                capability=capability,
                orchestrator_url=base_url,
                publish_url=publish_url,
                subscribe_url=subscribe_url,
                control_url=control_url,
                events_url=events_url,
                data_url=data_url,
                balance=balance,
            )

    except HTTPError as e:
        error_body = e.read().decode("utf-8", errors="replace")
        logging.error(
            "BYOC stream start failed: HTTP %d - %s",
            e.code,
            error_body,
        )
        raise


def SendBYOCPayment(
    info: lp_rpc_pb2.OrchestratorInfo,
    stream_id: str,
    capability: str,
    signer_base_url: str,
    payment_state: Optional[PaymentState] = None,
) -> tuple[int, Optional[PaymentState]]:
    """Send a payment to the orchestrator to top up the balance during an active stream.

    IMPORTANT: This endpoint requires MINIMUM BALANCE to work. It cannot be used
    to bootstrap initial balance before starting a stream. The initial payment
    must be included with the stream start request via StartBYOCStream.

    This function is intended for topping up balance DURING an active stream
    when the balance is running low but still above the minimum threshold.

    Args:
        info: OrchestratorInfo from GetOrchestratorInfo.
        stream_id: The stream ID (used in the job request).
        capability: Name of the BYOC capability.
        signer_base_url: Base URL of remote signer.
        payment_state: Previous payment state for nonce continuity.

    Returns:
        Tuple of (new_balance, updated_payment_state)

    Raises:
        HTTPError: If the orchestrator rejects the payment (e.g., insufficient balance).
    """
    base_url = _normalize_https_base_url(info.transcoder)
    url = f"{base_url}/ai/stream/payment"

    # Build job parameters (minimal for payment)
    job_parameters = {
        "enable_video_ingress": True,
        "enable_video_egress": True,
        "enable_data_output": False,
    }

    # Build and sign job request
    _, job_request_b64 = _build_job_request(
        capability=capability,
        request_dict={"stream_id": stream_id},
        timeout_seconds=30,
        parameters=job_parameters,
        stream_id=stream_id,
        signer_base_url=signer_base_url,
    )

    # Get payment
    payment_resp = GetBYOCPayment(
        signer_base_url,
        info,
        capability,
        state=payment_state,
        manifest_id=capability,
    )

    headers = {
        HEADER_JOB_REQUEST: job_request_b64,
        HEADER_CAPABILITY: capability,
        HEADER_PAYMENT: payment_resp.payment,
        "Content-Type": "application/json",
    }

    body = json.dumps({"stream_id": stream_id}).encode()
    request = Request(url, data=body, headers=headers, method="POST")
    ssl_ctx = ssl._create_unverified_context()

    try:
        with urlopen(request, timeout=BYOC_REQUEST_TIMEOUT, context=ssl_ctx) as resp:
            balance_str = resp.headers.get(HEADER_PAYMENT_BALANCE)
            balance = int(balance_str) if balance_str else 0
            return balance, payment_resp.state
    except HTTPError as e:
        error_body = e.read().decode("utf-8", errors="replace")
        logging.error(
            "BYOC payment failed: HTTP %d - %s",
            e.code,
            error_body,
        )
        raise


def StartBYOCStreamWithRetry(
    info: lp_rpc_pb2.OrchestratorInfo,
    capability: str,
    params: dict[str, Any],
    signer_base_url: str,
    payment_state: Optional[PaymentState] = None,
    enable_video_ingress: bool = True,
    enable_video_egress: bool = True,
    enable_data_output: bool = False,
    timeout_seconds: int = 30,
    max_retries: int = 2,
) -> BYOCStreamJob:
    """Start a streaming BYOC job with retry on transient errors.

    This function wraps StartBYOCStream with basic retry logic for transient
    network errors. For HTTP 402 (Payment Required) errors, the function
    provides detailed logging to help diagnose payment issues.

    Note: HTTP 402 errors typically indicate one of:
    - The remote signer is not generating sufficient payment tickets
    - The orchestrator is not configured to process payments (missing Recipient)
    - The pricing configuration is mismatched between client and orchestrator

    Args:
        info: OrchestratorInfo from GetOrchestratorInfo.
        capability: Name of the BYOC capability (e.g., "comfystream").
        params: Parameters to pass to the BYOC worker.
        signer_base_url: Base URL of remote signer (required).
        payment_state: Previous payment state for nonce continuity.
        enable_video_ingress: Enable video input publishing.
        enable_video_egress: Enable video output subscribing.
        enable_data_output: Enable data output channel.
        timeout_seconds: Timeout for the stream start request.
        max_retries: Maximum number of retry attempts for transient errors.

    Returns:
        BYOCStreamJob with trickle URLs and helper methods.

    Raises:
        HTTPError: If the request fails after all retries.
    """
    import uuid
    import time

    stream_id = str(uuid.uuid4())
    last_error: Optional[Exception] = None

    for attempt in range(max_retries + 1):
        try:
            return _start_byoc_stream_internal(
                info=info,
                capability=capability,
                params=params,
                signer_base_url=signer_base_url,
                payment_state=payment_state,
                enable_video_ingress=enable_video_ingress,
                enable_video_egress=enable_video_egress,
                enable_data_output=enable_data_output,
                timeout_seconds=timeout_seconds,
                stream_id=stream_id,
            )
        except HTTPError as e:
            last_error = e
            if e.code == 402:
                # Payment required - this is not a transient error
                logging.error(
                    "BYOC stream start failed with HTTP 402 (Payment Required). "
                    "This typically indicates:\n"
                    "  1. The remote signer is not generating sufficient payment (need 2+ minutes)\n"
                    "  2. The orchestrator is not configured to process payments (missing Recipient)\n"
                    "  3. Pricing mismatch between client and orchestrator\n"
                    "Check orchestrator logs for more details."
                )
                raise
            elif e.code >= 500 and attempt < max_retries:
                # Server error - retry with backoff
                wait_time = 2 ** attempt
                logging.warning(
                    "BYOC stream start failed with HTTP %d, retrying in %ds (attempt %d/%d)",
                    e.code,
                    wait_time,
                    attempt + 1,
                    max_retries,
                )
                time.sleep(wait_time)
            else:
                raise

    # Should not reach here
    if last_error:
        raise last_error
    raise RuntimeError("Unexpected end of retry loop")


def _start_byoc_stream_internal(
    info: lp_rpc_pb2.OrchestratorInfo,
    capability: str,
    params: dict[str, Any],
    signer_base_url: Optional[str] = None,
    payment_state: Optional[PaymentState] = None,
    enable_video_ingress: bool = True,
    enable_video_egress: bool = True,
    enable_data_output: bool = False,
    timeout_seconds: int = 30,
    stream_id: Optional[str] = None,
) -> BYOCStreamJob:
    """Internal helper to start a BYOC stream with a specific stream_id."""
    import uuid

    base_url = _normalize_https_base_url(info.transcoder)
    url = f"{base_url}/ai/stream/start"

    if stream_id is None:
        stream_id = str(uuid.uuid4())

    # Build job parameters
    job_parameters = {
        "enable_video_ingress": enable_video_ingress,
        "enable_video_egress": enable_video_egress,
        "enable_data_output": enable_data_output,
    }

    # Build and sign job request (signer_base_url is required for BYOC)
    _, job_request_b64 = _build_job_request(
        capability=capability,
        request_dict=params,
        timeout_seconds=timeout_seconds,
        parameters=job_parameters,
        stream_id=stream_id,
        signer_base_url=signer_base_url,
    )

    headers = {
        HEADER_JOB_REQUEST: job_request_b64,
        HEADER_CAPABILITY: capability,
        "Content-Type": "application/json",
    }

    # Get payment if signer is provided and price is non-zero
    if signer_base_url:
        # Check if the capability has non-zero pricing
        cap_price = _get_capability_price(info, capability)
        if cap_price is not None and cap_price.pricePerUnit > 0 and cap_price.pixelsPerUnit > 0:
            payment_resp = GetBYOCPayment(
                signer_base_url,
                info,
                capability,
                state=payment_state,
                manifest_id=capability,
            )
            headers[HEADER_PAYMENT] = payment_resp.payment
        else:
            logging.debug("Capability %s has zero price, skipping payment", capability)

    # Make request
    body = json.dumps(params).encode()
    request = Request(url, data=body, headers=headers, method="POST")
    ssl_ctx = ssl._create_unverified_context()

    with urlopen(request, timeout=BYOC_REQUEST_TIMEOUT, context=ssl_ctx) as resp:
        response_data = resp.read()

        # Extract trickle URLs from response headers
        publish_url = resp.headers.get(HEADER_PUBLISH_URL)
        subscribe_url = resp.headers.get(HEADER_SUBSCRIBE_URL)
        control_url = resp.headers.get(HEADER_CONTROL_URL)
        events_url = resp.headers.get(HEADER_EVENTS_URL)
        data_url = resp.headers.get(HEADER_DATA_URL)
        balance_str = resp.headers.get(HEADER_PAYMENT_BALANCE)
        balance = int(balance_str) if balance_str else None

        try:
            raw = json.loads(response_data)
        except json.JSONDecodeError:
            raw = {"raw_response": response_data.decode("utf-8", errors="replace")}

        return BYOCStreamJob(
            raw=raw,
            stream_id=stream_id,
            capability=capability,
            orchestrator_url=base_url,
            publish_url=publish_url,
            subscribe_url=subscribe_url,
            control_url=control_url,
            events_url=events_url,
            data_url=data_url,
            balance=balance,
        )


async def StopBYOCStream(
    info: lp_rpc_pb2.OrchestratorInfo,
    stream_id: str,
    capability: str,
    signer_base_url: Optional[str] = None,
    payment_state: Optional[PaymentState] = None,
) -> None:
    """Stop a streaming BYOC job.

    Args:
        info: OrchestratorInfo from GetOrchestratorInfo.
        stream_id: The stream ID returned from StartBYOCStream.
        capability: Name of the BYOC capability.
        signer_base_url: Base URL of remote signer (for paid jobs).
        payment_state: Previous payment state for nonce continuity.
    """
    import asyncio

    base_url = _normalize_https_base_url(info.transcoder)
    url = f"{base_url}/ai/stream/stop"

    # Build and sign stop request
    stop_request = {"stream_id": stream_id}
    _, job_request_b64 = _build_job_request(
        capability=capability,
        request_dict=stop_request,
        timeout_seconds=10,
        signer_base_url=signer_base_url,
    )

    headers = {
        HEADER_JOB_REQUEST: job_request_b64,
        HEADER_CAPABILITY: capability,
        "Content-Type": "application/json",
    }

    # Get payment if signer is provided and price is non-zero
    if signer_base_url:
        cap_price = _get_capability_price(info, capability)
        if cap_price is not None and cap_price.pricePerUnit > 0 and cap_price.pixelsPerUnit > 0:
            payment_resp = GetBYOCPayment(
                signer_base_url,
                info,
                capability,
                state=payment_state,
            )
            headers[HEADER_PAYMENT] = payment_resp.payment
        else:
            logging.debug("Capability %s has zero price, skipping payment for stop", capability)

    def do_request() -> bytes:
        body = json.dumps(stop_request).encode()
        request = Request(url, data=body, headers=headers, method="POST")
        ssl_ctx = ssl._create_unverified_context()

        with urlopen(request, timeout=BYOC_REQUEST_TIMEOUT, context=ssl_ctx) as resp:
            return resp.read()

    await asyncio.to_thread(do_request)
    logging.info("BYOC stream stopped: stream_id=%s", stream_id)


# Default payment interval for BYOC streaming
DEFAULT_BYOC_PAYMENT_INTERVAL = 5.0


@dataclass
class BYOCPaymentConfig:
    """Configuration for BYOC payment processing."""

    interval_s: float = DEFAULT_BYOC_PAYMENT_INTERVAL


class BYOCPaymentSender:
    """
    Periodically sends payments to keep a BYOC streaming session funded.

    Unlike LivePaymentSender which uses pixel-based pricing, BYOC uses
    time-based pricing where the cost is calculated based on elapsed time.
    """

    def __init__(
        self,
        signer_url: str,
        orchestrator_info: lp_rpc_pb2.OrchestratorInfo,
        capability: str,
        stream_id: str,
        *,
        config: Optional[BYOCPaymentConfig] = None,
        initial_state: Optional[PaymentState] = None,
    ) -> None:
        """Initialize the BYOC payment sender.

        Args:
            signer_url: Base URL of the remote signer.
            orchestrator_info: OrchestratorInfo from GetOrchestratorInfo.
            capability: Name of the BYOC capability.
            stream_id: Stream ID from StartBYOCStream.
            config: Payment configuration (interval, etc.).
            initial_state: Previous payment state for nonce continuity.
        """
        import asyncio

        self._signer_url = signer_url
        self._orch_info = orchestrator_info
        self._capability = capability
        self._stream_id = stream_id
        self._config = config or BYOCPaymentConfig()

        self._task: Optional[asyncio.Task] = None
        self._stop_event: Optional[asyncio.Event] = None
        self._last_payment_time: Optional[float] = None
        self._running = False
        self._error: Optional[BaseException] = None

        # State tracking for ticket nonces - CRITICAL for payment acceptance
        self._payment_state: Optional[PaymentState] = initial_state

    def start(self) -> None:
        """Start the background payment task."""
        import asyncio

        if self._running:
            return

        self._running = True
        self._stop_event = asyncio.Event()
        self._last_payment_time = time.monotonic()
        self._task = asyncio.create_task(self._payment_loop())

    async def stop(self) -> None:
        """Stop the background payment task."""
        import asyncio

        if not self._running:
            return

        self._running = False
        if self._stop_event:
            self._stop_event.set()

        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=5.0)
            except asyncio.TimeoutError:
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass

    async def _payment_loop(self) -> None:
        """Main payment loop that runs in the background."""
        import asyncio

        logging.info(
            "BYOCPaymentSender started: interval=%.1fs, capability=%s, stream_id=%s",
            self._config.interval_s,
            self._capability,
            self._stream_id,
        )

        while self._running and self._stop_event and not self._stop_event.is_set():
            try:
                # Wait for the interval
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=self._config.interval_s,
                    )
                    # If we get here, stop was requested
                    break
                except asyncio.TimeoutError:
                    # Normal timeout - time to send payment
                    pass

                await self._send_payment()

            except Exception as e:
                logging.error("BYOCPaymentSender error: %s", e, exc_info=True)
                self._error = e
                # Continue trying - don't stop on payment errors

        logging.info(
            "BYOCPaymentSender stopped: capability=%s, stream_id=%s",
            self._capability,
            self._stream_id,
        )

    async def _send_payment(self) -> None:
        """Send a single payment to the orchestrator."""
        import asyncio

        now = time.monotonic()
        if self._last_payment_time is None:
            self._last_payment_time = now

        seconds_since_last = now - self._last_payment_time

        logging.debug(
            "Processing BYOC payment: secs=%.2f, capability=%s, stream_id=%s",
            seconds_since_last,
            self._capability,
            self._stream_id,
        )

        # Get fresh payment credentials from remote signer
        def do_get_payment():
            return GetBYOCPayment(
                self._signer_url,
                self._orch_info,
                self._capability,
                state=self._payment_state,
                manifest_id=self._capability,  # Use capability as manifest for balance tracking
            )

        payment_resp = await asyncio.to_thread(do_get_payment)

        # Update state from response for next payment
        if payment_resp.state is not None:
            self._payment_state = payment_resp.state
            logging.debug(
                "Updated BYOC payment state: capability=%s, stream_id=%s",
                self._capability,
                self._stream_id,
            )

        # Forward payment to orchestrator's /ai/stream/payment endpoint
        await self._forward_payment_to_orchestrator(payment_resp.payment)

        self._last_payment_time = now

    async def _forward_payment_to_orchestrator(self, payment: str) -> None:
        """Forward payment to the orchestrator's BYOC payment endpoint."""
        import asyncio

        base_url = _normalize_https_base_url(self._orch_info.transcoder)
        url = f"{base_url}/ai/stream/payment"

        headers = {
            HEADER_PAYMENT: payment,
            HEADER_CAPABILITY: self._capability,
            "Content-Type": "application/json",
        }

        body = json.dumps({"stream_id": self._stream_id}).encode()

        def do_request() -> bytes:
            request = Request(url, data=body, headers=headers, method="POST")
            ssl_ctx = ssl._create_unverified_context()

            with urlopen(request, timeout=BYOC_REQUEST_TIMEOUT, context=ssl_ctx) as resp:
                if resp.status != 200:
                    from .errors import LivepeerGatewayError

                    raise LivepeerGatewayError(
                        f"Orchestrator rejected BYOC payment: HTTP {resp.status}"
                    )
                return resp.read()

        try:
            response_data = await asyncio.to_thread(do_request)
            logging.debug(
                "BYOC payment accepted by orchestrator: capability=%s, stream_id=%s, response_len=%d",
                self._capability,
                self._stream_id,
                len(response_data),
            )
        except Exception as e:
            logging.error(
                "Failed to forward BYOC payment to orchestrator: %s, url=%s",
                e,
                url,
            )
            raise
