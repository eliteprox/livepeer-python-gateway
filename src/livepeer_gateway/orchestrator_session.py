from __future__ import annotations

import asyncio
import time
from typing import Optional

from . import lp_rpc_pb2
from .errors import LivepeerGatewayError
from .orchestrator import (
    GetPaymentResponse,
    GetPayment,
    LiveVideoToVideo,
    OrchestratorClient,
    StartJobRequest,
    _start_job_with_headers,
    build_capabilities,
    CAPABILITY_LIVE_VIDEO_TO_VIDEO,
)


class OrchestratorSession:
    """
    Cohesive session wrapper that reuses an OrchestratorClient, caches
    OrchestratorInfo, and coordinates payments/jobs with optional refresh.
    """

    def __init__(
        self,
        orch_url: str,
        *,
        signer_url: Optional[str] = None,
        max_age_s: Optional[float] = None,
    ) -> None:
        self._orch_url = orch_url
        self._signer_url = signer_url
        self._max_age_s = max_age_s

        self._client = OrchestratorClient(orch_url, signer_url=signer_url)
        self._info: Optional[lp_rpc_pb2.OrchestratorInfo] = None
        self._info_fetched_at: Optional[float] = None
        self._jobs: list[LiveVideoToVideo] = []

    def _is_info_valid(self) -> bool:
        if self._info is None:
            return False
        if self._max_age_s is None or self._info_fetched_at is None:
            return True
        return (time.monotonic() - self._info_fetched_at) <= self._max_age_s

    def invalidate(self) -> None:
        """
        Drop cached orchestrator info so the next call refreshes it.
        """
        self._info = None
        self._info_fetched_at = None

    def ensure_info(
        self, *, force: bool = False, caps: Optional[lp_rpc_pb2.Capabilities] = None
    ) -> lp_rpc_pb2.OrchestratorInfo:
        """
        Return cached OrchestratorInfo, refreshing if missing/expired or forced.
        """
        if not force and caps is None and self._is_info_valid():
            assert self._info is not None
            return self._info

        info = self._client.GetOrchestratorInfo(caps=caps)
        self._info = info
        self._info_fetched_at = time.monotonic()
        return info

    def refresh(self, *, caps: Optional[lp_rpc_pb2.Capabilities] = None) -> lp_rpc_pb2.OrchestratorInfo:
        """
        Force refresh of orchestrator info regardless of cache age.
        """
        return self.ensure_info(force=True, caps=caps)

    def get_payment(self, *, typ: str = "lv2v", model_id: Optional[str] = None) -> GetPaymentResponse:
        """
        Fetch payment credentials, refreshing orchestrator info once on failure.
        """
        caps = None
        if typ == "lv2v" and model_id:
            caps = build_capabilities(CAPABILITY_LIVE_VIDEO_TO_VIDEO, model_id)
        info = self.ensure_info(force=bool(caps), caps=caps)
        try:
            return GetPayment(self._signer_url, info, typ=typ, model_id=model_id)
        except LivepeerGatewayError:
            # Retry once with a fresh OrchestratorInfo (e.g., if price/auth changed).
            self.invalidate()
            info = self.ensure_info(force=True, caps=caps)
            return GetPayment(self._signer_url, info, typ=typ, model_id=model_id)

    def start_job(
        self,
        req: StartJobRequest,
        *,
        typ: str = "lv2v",
    ) -> LiveVideoToVideo:
        """
        Start a job using cached/refreshable OrchestratorInfo and payment.
        """
        caps = build_capabilities(CAPABILITY_LIVE_VIDEO_TO_VIDEO, req.model_id) if req.model_id else None
        info = self.ensure_info(force=bool(caps), caps=caps)
        payment = self.get_payment(typ=typ, model_id=req.model_id)
        headers: dict[str, Optional[str]] = {
            "Livepeer-Payment": payment.payment,
            "Livepeer-Segment": payment.seg_creds,
        }

        job = _start_job_with_headers(info, req, headers)
        self._jobs.append(job)
        return job

    async def aclose(self) -> None:
        """
        Async close for any created LiveVideoToVideo helpers.
        """
        if not self._jobs:
            return

        tasks = [job.close() for job in self._jobs]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException):
                raise result
        self._jobs.clear()

    def close(self) -> None:
        """
        Synchronous close convenience wrapper around aclose().
        """
        if not self._jobs:
            return
        try:
            asyncio.run(self.aclose())
        except RuntimeError as e:
            if "asyncio.run()" in str(e):
                # Running inside an existing loop; caller should await aclose().
                raise LivepeerGatewayError(
                    "OrchestratorSession.close() cannot run inside an active event loop; "
                    "await OrchestratorSession.aclose() instead."
                ) from e
            raise
