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
class BYOCJobToken:
    """Token response from orchestrator's /process/token endpoint.

    This contains the actual pricing for the sender/capability combination,
    which may differ from the advertised pricing due to sender-specific rates
    or transaction cost adjustments.
    """

    price_per_unit: int
    pixels_per_unit: int
    balance: int
    available_capacity: int
    service_addr: str
    ticket_params: Optional[dict[str, Any]] = None  # Raw ticket params from JobToken


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
    signed_job_request: Optional[str] = None  # Required for job token refresh

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


def GetBYOCJobToken(
    info: lp_rpc_pb2.OrchestratorInfo,
    capability: str,
    signer_base_url: str,
) -> BYOCJobToken:
    """Fetch a job token from the orchestrator's /process/token endpoint.

    This retrieves accurate pricing for the sender/capability combination,
    which may differ from advertised pricing due to sender-specific rates
    or transaction cost adjustments.

    Args:
        info: OrchestratorInfo from GetOrchestratorInfo.
        capability: Name of the BYOC capability.
        signer_base_url: Base URL of remote signer for authentication.

    Returns:
        BYOCJobToken with pricing and capacity information.

    Raises:
        LivepeerGatewayError: If the token request fails.
    """
    from .errors import LivepeerGatewayError
    from .orchestrator import _get_signer_material

    # Get sender address and signature from signer
    signer_material = _get_signer_material(signer_base_url)
    if signer_material.address is None or signer_material.sig is None:
        raise LivepeerGatewayError(
            "Cannot get job token: signer did not return address/signature"
        )

    # Build the Livepeer-Eth-Address header (base64-encoded JSON)
    # Format matches Go's JobSender: {"addr": "0x...", "sig": "0x..."}
    # IMPORTANT: Use address_hex (the original checksummed address) because
    # the signature was created over the checksummed string, not lowercase bytes
    addr_hex = signer_material.address_hex
    sig_hex = "0x" + signer_material.sig.hex()
    job_sender = {"addr": addr_hex, "sig": sig_hex}
    job_sender_json = json.dumps(job_sender)
    job_sender_b64 = base64.b64encode(job_sender_json.encode()).decode()

    # Build URL for /process/token endpoint
    base_url = _normalize_https_base_url(info.transcoder)
    url = f"{base_url}/process/token"

    headers = {
        HEADER_ETH_ADDRESS: job_sender_b64,
        HEADER_CAPABILITY: capability,
    }

    request = Request(url, headers=headers, method="GET")
    ssl_ctx = ssl._create_unverified_context()

    try:
        with urlopen(request, timeout=BYOC_REQUEST_TIMEOUT, context=ssl_ctx) as resp:
            response_data = resp.read()
            token_data = json.loads(response_data)

            # Extract price info (nested under "price")
            price = token_data.get("price", {})
            price_per_unit = price.get("pricePerUnit", 0)
            pixels_per_unit = price.get("pixelsPerUnit", 1)

            # Extract ticket_params for payment generation
            ticket_params = token_data.get("ticket_params")

            return BYOCJobToken(
                price_per_unit=price_per_unit,
                pixels_per_unit=pixels_per_unit,
                balance=token_data.get("balance", 0),
                available_capacity=token_data.get("available_capacity", 0),
                service_addr=token_data.get("service_addr", ""),
                ticket_params=ticket_params,
            )
    except HTTPError as e:
        body = e.read().decode("utf-8", errors="replace") if e.fp else ""
        raise LivepeerGatewayError(
            f"Failed to get job token from orchestrator: HTTP {e.code} - {body}"
        ) from e


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
    job_token: Optional[BYOCJobToken] = None,
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
        job_token: BYOCJobToken with pricing from GetBYOCJobToken (fetched if not provided).
        state: Previous payment state for nonce continuity.
        manifest_id: Optional manifest ID for balance tracking.

    Returns:
        GetPaymentResponse with payment credentials and updated state.
    """
    from .orchestrator import GetPayment

    # Fetch job token if not provided
    if job_token is None:
        job_token = GetBYOCJobToken(info, capability, signer_base_url)

    return GetPayment(
        signer_base_url,
        info,
        typ="byoc",
        capability=capability,
        state=state,
        manifest_id=manifest_id,
        price_per_unit=job_token.price_per_unit,
        pixels_per_unit=job_token.pixels_per_unit,
        ticket_params=job_token.ticket_params,
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

    # Get payment if signer is provided and price is non-zero
    if signer_base_url:
        # Fetch job token to get accurate pricing for this sender/capability
        job_token = GetBYOCJobToken(info, req.capability, signer_base_url)
        if job_token.price_per_unit > 0 and job_token.pixels_per_unit > 0:
            payment_resp = GetBYOCPayment(
                signer_base_url,
                info,
                req.capability,
                job_token=job_token,
                state=payment_state,
            )
            headers[HEADER_PAYMENT] = payment_resp.payment
        else:
            logging.debug("Capability %s has zero price, skipping payment", req.capability)

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
                signed_job_request=job_request_b64,
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
        # Fetch job token to get accurate pricing for this sender/capability
        job_token = GetBYOCJobToken(info, capability, signer_base_url)
        if job_token.price_per_unit > 0 and job_token.pixels_per_unit > 0:
            payment_resp = GetBYOCPayment(
                signer_base_url,
                info,
                capability,
                job_token=job_token,
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
            signed_job_request=job_request_b64,
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
        # Fetch job token to get accurate pricing for this sender/capability
        job_token = GetBYOCJobToken(info, capability, signer_base_url)
        if job_token.price_per_unit > 0 and job_token.pixels_per_unit > 0:
            payment_resp = GetBYOCPayment(
                signer_base_url,
                info,
                capability,
                job_token=job_token,
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


# Default interval for BYOC job token refresh (about once a minute)
DEFAULT_BYOC_TOKEN_REFRESH_INTERVAL = 60.0


@dataclass
class BYOCTokenRefreshConfig:
    """Configuration for BYOC job token refresh."""

    interval_s: float = DEFAULT_BYOC_TOKEN_REFRESH_INTERVAL


# Backwards compatibility aliases
BYOCPaymentConfig = BYOCTokenRefreshConfig
DEFAULT_BYOC_PAYMENT_INTERVAL = DEFAULT_BYOC_TOKEN_REFRESH_INTERVAL


class BYOCTokenRefresher:
    """
    Periodically refreshes the job token to keep a BYOC streaming session active.

    Unlike LV2V which uses pixel-based live payments sent every few seconds,
    BYOC uses a signed job token that is refreshed approximately once per minute.
    """

    def __init__(
        self,
        signer_url: str,
        orchestrator_info: lp_rpc_pb2.OrchestratorInfo,
        capability: str,
        stream_id: str,
        signed_job_request: str,
        *,
        config: Optional[BYOCTokenRefreshConfig] = None,
        initial_state: Optional[PaymentState] = None,
    ) -> None:
        """Initialize the BYOC token refresher.

        Args:
            signer_url: Base URL of the remote signer.
            orchestrator_info: OrchestratorInfo from GetOrchestratorInfo.
            capability: Name of the BYOC capability.
            stream_id: Stream ID from StartBYOCStream.
            signed_job_request: The signed job request (base64) from StartBYOCStream.
            config: Token refresh configuration (interval, etc.).
            initial_state: Previous payment state for nonce continuity.
        """
        import asyncio

        self._signer_url = signer_url
        self._orch_info = orchestrator_info
        self._capability = capability
        self._stream_id = stream_id
        self._signed_job_request = signed_job_request
        self._config = config or BYOCTokenRefreshConfig()

        self._task: Optional[asyncio.Task] = None
        self._stop_event: Optional[asyncio.Event] = None
        self._last_refresh_time: Optional[float] = None
        self._running = False
        self._error: Optional[BaseException] = None

        # State tracking for ticket nonces - CRITICAL for payment acceptance
        self._payment_state: Optional[PaymentState] = initial_state

    def start(self) -> None:
        """Start the background token refresh task."""
        import asyncio

        if self._running:
            return

        self._running = True
        self._stop_event = asyncio.Event()
        self._last_refresh_time = time.monotonic()
        self._task = asyncio.create_task(self._refresh_loop())

    async def stop(self) -> None:
        """Stop the background token refresh task."""
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

    async def _refresh_loop(self) -> None:
        """Main token refresh loop that runs in the background."""
        import asyncio

        # Check if capability has zero price - no need to run refresh loop
        try:
            job_token = GetBYOCJobToken(self._orch_info, self._capability, self._signer_url)
            if job_token.price_per_unit == 0:
                logging.info(
                    "BYOCTokenRefresher: zero price capability, no token refresh needed: capability=%s, stream_id=%s",
                    self._capability,
                    self._stream_id,
                )
                return
        except Exception as e:
            logging.warning(
                "BYOCTokenRefresher: could not fetch job token, assuming paid capability: capability=%s, err=%s",
                self._capability,
                e,
            )

        logging.info(
            "BYOCTokenRefresher started: interval=%.1fs, capability=%s, stream_id=%s",
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
                    # Normal timeout - time to refresh token
                    pass

                await self._refresh_token()

            except Exception as e:
                logging.error("BYOCTokenRefresher error: %s", e, exc_info=True)
                self._error = e
                # Continue trying - don't stop on refresh errors

        logging.info(
            "BYOCTokenRefresher stopped: capability=%s, stream_id=%s",
            self._capability,
            self._stream_id,
        )

    async def _refresh_token(self) -> None:
        """Refresh the job token by sending it to the orchestrator."""
        import asyncio

        # Check if capability has zero price - skip refresh if so
        try:
            job_token = GetBYOCJobToken(self._orch_info, self._capability, self._signer_url)
            if job_token.price_per_unit == 0:
                logging.debug(
                    "Skipping BYOC token refresh - zero price: capability=%s, stream_id=%s",
                    self._capability,
                    self._stream_id,
                )
                return
        except Exception as e:
            logging.warning(
                "BYOCTokenRefresher: could not fetch job token for price check: capability=%s, err=%s",
                self._capability,
                e,
            )
            # Continue with refresh - orchestrator will reject if needed

        now = time.monotonic()
        if self._last_refresh_time is None:
            self._last_refresh_time = now

        seconds_since_last = now - self._last_refresh_time

        logging.debug(
            "Refreshing BYOC job token: secs=%.2f, capability=%s, stream_id=%s",
            seconds_since_last,
            self._capability,
            self._stream_id,
        )

        # Get fresh payment credentials from remote signer for the new token
        def do_get_payment():
            return GetBYOCPayment(
                self._signer_url,
                self._orch_info,
                self._capability,
                state=self._payment_state,
                manifest_id=self._capability,  # Use capability as manifest for balance tracking
            )

        payment_resp = await asyncio.to_thread(do_get_payment)

        # Update state from response for next refresh
        if payment_resp.state is not None:
            self._payment_state = payment_resp.state
            logging.debug(
                "Updated BYOC token state: capability=%s, stream_id=%s",
                self._capability,
                self._stream_id,
            )

        # Send the refreshed token to orchestrator's /ai/stream/payment endpoint
        await self._send_refreshed_token(payment_resp.payment)

        self._last_refresh_time = now

    async def _send_refreshed_token(self, payment: str) -> None:
        """Send the refreshed job token to the orchestrator's payment endpoint."""
        import asyncio

        base_url = _normalize_https_base_url(self._orch_info.transcoder)
        url = f"{base_url}/ai/stream/payment"

        # go-livepeer ProcessStreamPayment requires both the signed job request
        # (in Livepeer header) and payment (in Livepeer-Payment header)
        headers = {
            HEADER_JOB_REQUEST: self._signed_job_request,
            HEADER_PAYMENT: payment,
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
                        f"Orchestrator rejected BYOC token refresh: HTTP {resp.status}"
                    )
                return resp.read()

        try:
            response_data = await asyncio.to_thread(do_request)
            logging.debug(
                "BYOC job token refreshed: capability=%s, stream_id=%s, response_len=%d",
                self._capability,
                self._stream_id,
                len(response_data),
            )
        except Exception as e:
            logging.error(
                "Failed to refresh BYOC job token: %s, url=%s",
                e,
                url,
            )
            raise


# Backwards compatibility alias
BYOCPaymentSender = BYOCTokenRefresher
