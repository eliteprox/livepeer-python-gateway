from __future__ import annotations

import base64
import ssl
from dataclasses import dataclass
from typing import Any, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from . import lp_rpc_pb2
from .errors import PaymentError, SignerRefreshRequired


@dataclass(frozen=True)
class GetPaymentResponse:
    payment: str
    seg_creds: Optional[str] = None


class PaymentSession:
    def __init__(
        self,
        signer_url: Optional[str],
        info: lp_rpc_pb2.OrchestratorInfo,
        *,
        capabilities: Optional[lp_rpc_pb2.Capabilities] = None,
        max_refresh_retries: int = 3,
    ) -> None:
        self._signer_url = signer_url
        self._info = info
        self._capabilities = capabilities
        self._max_refresh_retries = max(0, int(max_refresh_retries))
        self._state: Optional[dict[str, str]] = None

    def get_payment(self) -> GetPaymentResponse:
        """
        Generate a payment via the remote signer.

        Handles signer state round-tripping internally.
        On HTTP 480, refreshes OrchestratorInfo and retries
        (up to max_refresh_retries).
        Returns payment + seg_creds for use as HTTP headers.
        """

        # Offchain mode: still send the expected headers, but with empty content.
        if not self._signer_url:
            seg = lp_rpc_pb2.SegData()
            if not self._info.HasField("auth_token"):
                raise PaymentError(
                    "Orchestrator did not provide an auth token."
                )
            seg.auth_token.CopyFrom(self._info.auth_token)
            seg = base64.b64encode(seg.SerializeToString()).decode("ascii")
            return GetPaymentResponse(seg_creds=seg, payment="")

        def _payment_request() -> GetPaymentResponse:
            from .orchestrator import _http_origin, post_json

            base = _http_origin(self._signer_url)
            url = f"{base}/generate-live-payment"

            pb = self._info.SerializeToString()
            orch_b64 = base64.b64encode(pb).decode("ascii")
            payload: dict[str, Any] = {"orchestrator": orch_b64, "type": "lv2v"}
            if self._state is not None:
                payload["state"] = self._state

            data = post_json(url, payload)

            payment = data.get("payment")
            if not isinstance(payment, str) or not payment:
                raise PaymentError(
                    f"GetPayment error: missing/invalid 'payment' in response (url={url})"
                )

            seg_creds = data.get("segCreds")
            if seg_creds is not None and not isinstance(seg_creds, str):
                raise PaymentError(
                    f"GetPayment error: invalid 'segCreds' in response (url={url})"
                )

            state = data.get("state")
            if not isinstance(state, dict):
                raise PaymentError(
                    f"Remote signer response missing 'state' object (url={url})"
                )

            self._state = state
            return GetPaymentResponse(payment=payment, seg_creds=seg_creds)

        attempts = 0
        while True:
            try:
                return _payment_request()
            except SignerRefreshRequired as e:
                if attempts >= self._max_refresh_retries:
                    raise PaymentError(
                        f"Signer refresh required after {attempts} retries: {e}"
                    ) from e
                if not self._info.transcoder:
                    raise PaymentError(
                        "OrchestratorInfo missing transcoder URL for refresh"
                    )
                from .orchestrator import GetOrchestratorInfo

                self._info = GetOrchestratorInfo(
                    self._info.transcoder,
                    signer_url=self._signer_url,
                    capabilities=self._capabilities,
                )
                attempts += 1

    def send_payment(self) -> None:
        """
        Generate a payment (via get_payment) and forward it
        to the orchestrator via POST {orch}/payment.
        """
        from .orchestrator import _extract_error_message, _http_origin

        p = self.get_payment()
        if not self._info.transcoder:
            raise PaymentError("OrchestratorInfo missing transcoder URL for payment")
        base = _http_origin(self._info.transcoder)
        url = f"{base}/payment"
        headers = {
            "Livepeer-Payment": p.payment,
            "Livepeer-Segment": p.seg_creds or "",
        }
        req = Request(url, data=b"", headers=headers, method="POST")
        ssl_ctx = ssl._create_unverified_context()
        try:
            with urlopen(req, timeout=5.0, context=ssl_ctx) as resp:
                resp.read()
        except HTTPError as e:
            body = _extract_error_message(e)
            body_part = f"; body={body!r}" if body else ""
            raise PaymentError(
                f"HTTP payment error: HTTP {e.code} from endpoint (url={url}){body_part}"
            ) from e
        except ConnectionRefusedError as e:
            raise PaymentError(
                f"HTTP payment error: connection refused (is the server running? is the host/port correct?) (url={url})"
            ) from e
        except URLError as e:
            raise PaymentError(
                f"HTTP payment error: failed to reach endpoint: {getattr(e, 'reason', e)} (url={url})"
            ) from e
        except Exception as e:
            raise PaymentError(
                f"HTTP payment error: unexpected error: {e.__class__.__name__}: {e} (url={url})"
            ) from e
