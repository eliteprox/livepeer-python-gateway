"""
Tests for HTTP 480 (SessionRefreshRequired) handling in OrchestratorSession.

This matches the go-livepeer gateway behavior where:
1. Remote signer returns 480 when ticket params or auth token expire
2. Gateway refreshes OrchestratorInfo and retries
3. Max 3 consecutive refresh attempts before failure
"""
import unittest
from unittest.mock import MagicMock, patch, call

from livepeer_gateway.errors import LivepeerGatewayError, SessionRefreshRequired
from livepeer_gateway.orchestrator_session import OrchestratorSession
from livepeer_gateway.orchestrator import GetPaymentResponse
from livepeer_gateway import lp_rpc_pb2


class TestSessionRefreshOnHTTP480(unittest.TestCase):
    """Test that HTTP 480 triggers OrchestratorInfo refresh and retry."""

    def _make_mock_info(self) -> lp_rpc_pb2.OrchestratorInfo:
        """Create a minimal OrchestratorInfo for testing."""
        info = lp_rpc_pb2.OrchestratorInfo()
        info.transcoder = "https://test-orch:8935"
        info.auth_token.token = b"test-token"
        info.auth_token.session_id = "test-session"
        return info

    @patch("livepeer_gateway.orchestrator_session.GetPayment")
    @patch("livepeer_gateway.orchestrator_session.OrchestratorClient")
    def test_480_triggers_refresh_and_retry(
        self,
        mock_client_class: MagicMock,
        mock_get_payment: MagicMock,
    ):
        """HTTP 480 should invalidate cache, refresh OrchestratorInfo, and retry."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.GetOrchestratorInfo.return_value = self._make_mock_info()

        # First call raises 480, second succeeds
        expected_response = GetPaymentResponse(payment="test-payment", seg_creds="test-creds")
        mock_get_payment.side_effect = [
            SessionRefreshRequired("HTTP 480"),
            expected_response,
        ]

        session = OrchestratorSession("https://test-orch:8935", signer_url="https://signer:8081")
        result = session.get_payment(typ="lv2v", model_id="test-model")

        self.assertEqual(result, expected_response)
        # Should have called GetOrchestratorInfo twice (initial + refresh)
        self.assertEqual(mock_client.GetOrchestratorInfo.call_count, 2)
        # Should have called GetPayment twice (initial + retry)
        self.assertEqual(mock_get_payment.call_count, 2)

    @patch("livepeer_gateway.orchestrator_session.GetPayment")
    @patch("livepeer_gateway.orchestrator_session.OrchestratorClient")
    def test_max_3_refresh_attempts(
        self,
        mock_client_class: MagicMock,
        mock_get_payment: MagicMock,
    ):
        """Should fail after 3 consecutive 480 responses (max refresh attempts)."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.GetOrchestratorInfo.return_value = self._make_mock_info()

        # Always return 480 to exhaust retry limit
        mock_get_payment.side_effect = SessionRefreshRequired("HTTP 480")

        session = OrchestratorSession("https://test-orch:8935", signer_url="https://signer:8081")

        with self.assertRaises(LivepeerGatewayError) as ctx:
            session.get_payment(typ="lv2v", model_id="test-model")

        self.assertIn("Too many consecutive session refreshes", str(ctx.exception))
        # Should have tried 4 times total (initial + 3 retries)
        self.assertEqual(mock_get_payment.call_count, 4)
        # Should have refreshed OrchestratorInfo 4 times (force=True after first attempt)
        self.assertEqual(mock_client.GetOrchestratorInfo.call_count, 4)

    @patch("livepeer_gateway.orchestrator_session.GetPayment")
    @patch("livepeer_gateway.orchestrator_session.OrchestratorClient")
    def test_non_480_error_retries_once(
        self,
        mock_client_class: MagicMock,
        mock_get_payment: MagicMock,
    ):
        """Non-480 errors should retry once with fresh info (existing behavior)."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.GetOrchestratorInfo.return_value = self._make_mock_info()

        # First call raises generic error, second succeeds
        expected_response = GetPaymentResponse(payment="test-payment", seg_creds="test-creds")
        mock_get_payment.side_effect = [
            LivepeerGatewayError("Some other error"),
            expected_response,
        ]

        session = OrchestratorSession("https://test-orch:8935", signer_url="https://signer:8081")
        result = session.get_payment(typ="lv2v", model_id="test-model")

        self.assertEqual(result, expected_response)
        # Should have called GetPayment twice
        self.assertEqual(mock_get_payment.call_count, 2)

    @patch("livepeer_gateway.orchestrator_session.GetPayment")
    @patch("livepeer_gateway.orchestrator_session.OrchestratorClient")
    def test_non_480_error_fails_on_second_attempt(
        self,
        mock_client_class: MagicMock,
        mock_get_payment: MagicMock,
    ):
        """Non-480 errors that persist after retry should raise."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.GetOrchestratorInfo.return_value = self._make_mock_info()

        # Both calls raise generic error
        mock_get_payment.side_effect = LivepeerGatewayError("Persistent error")

        session = OrchestratorSession("https://test-orch:8935", signer_url="https://signer:8081")

        with self.assertRaises(LivepeerGatewayError) as ctx:
            session.get_payment(typ="lv2v", model_id="test-model")

        self.assertIn("Persistent error", str(ctx.exception))
        # Should have called GetPayment twice (initial + 1 retry)
        self.assertEqual(mock_get_payment.call_count, 2)

    @patch("livepeer_gateway.orchestrator_session.GetPayment")
    @patch("livepeer_gateway.orchestrator_session.OrchestratorClient")
    def test_480_then_success_resets_state(
        self,
        mock_client_class: MagicMock,
        mock_get_payment: MagicMock,
    ):
        """After 480 + successful retry, subsequent calls should work normally."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.GetOrchestratorInfo.return_value = self._make_mock_info()

        expected_response = GetPaymentResponse(payment="test-payment", seg_creds="test-creds")
        # First call: 480 then success. Second call: immediate success.
        mock_get_payment.side_effect = [
            SessionRefreshRequired("HTTP 480"),
            expected_response,
            expected_response,
        ]

        session = OrchestratorSession("https://test-orch:8935", signer_url="https://signer:8081")

        # First get_payment triggers refresh
        result1 = session.get_payment(typ="lv2v", model_id="test-model")
        self.assertEqual(result1, expected_response)

        # Second get_payment should succeed immediately (no 480 this time)
        result2 = session.get_payment(typ="lv2v", model_id="test-model")
        self.assertEqual(result2, expected_response)


class TestSessionRefreshRequiredException(unittest.TestCase):
    """Test the SessionRefreshRequired exception itself."""

    def test_is_subclass_of_livepeer_gateway_error(self):
        """SessionRefreshRequired should be a LivepeerGatewayError subclass."""
        exc = SessionRefreshRequired("test message")
        self.assertIsInstance(exc, LivepeerGatewayError)
        self.assertIsInstance(exc, RuntimeError)

    def test_message_preserved(self):
        """Exception message should be preserved."""
        exc = SessionRefreshRequired("test message")
        self.assertEqual(str(exc), "test message")


if __name__ == "__main__":
    unittest.main()
