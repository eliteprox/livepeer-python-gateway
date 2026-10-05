"""Request encoding tests for JSON and ``multipart/form-data`` bodies."""

from __future__ import annotations

import email.message
import email.parser

from livepeer_gateway import http
from livepeer_gateway.multipart import FilePart, MultipartBody, encode_multipart


def _parse_multipart(content_type: str, body: bytes) -> list[email.message.Message]:
    message = email.parser.BytesParser().parsebytes(
        f"Content-Type: {content_type}\r\nMIME-Version: 1.0\r\n\r\n".encode() + body
    )
    assert message.is_multipart()
    return message.get_payload()


class TestRequestParts:
    def test_json_payload_unchanged(self) -> None:
        method, headers, body = http._request_parts(
            "https://runner.example.com/call", payload={"prompt": "hi"}, headers={"Accept": "*/*"}
        )
        assert method == "POST"
        assert headers["Content-Type"] == "application/json"
        assert headers["Accept"] == "*/*"
        assert body == b'{"prompt": "hi"}'

    def test_no_body_is_get_with_json_accept(self) -> None:
        method, headers, body = http._request_parts("https://runner.example.com/discovery")
        assert method == "GET"
        assert headers["Accept"] == "application/json"
        assert body is None

    def test_json_alias_is_kept(self) -> None:
        assert http._json_request_parts is http._request_parts

    def test_multipart_body_sets_boundary_and_no_json_accept(self) -> None:
        multipart = MultipartBody(
            fields={"model": "whisper-large-v3"},
            files=[FilePart("file", 'clip "1".wav', b"RIFF\x00\x01wav", "audio/wav")],
        )
        method, headers, body = http._request_parts(
            "https://runner.example.com/v1/audio/transcriptions", multipart=multipart
        )
        assert method == "POST"
        assert headers["Content-Type"] == f"multipart/form-data; boundary={multipart.boundary}"
        assert "Accept" not in headers
        assert body is not None

        parts = _parse_multipart(headers["Content-Type"], body)
        assert [p.get_param("name", header="content-disposition") for p in parts] == ["model", "file"]
        assert parts[0].get_payload(decode=True) == b"whisper-large-v3"
        assert parts[0].get_content_type() == "text/plain"
        assert parts[1].get_filename() == "clip %221%22.wav"
        assert parts[1].get_content_type() == "audio/wav"
        assert parts[1].get_payload(decode=True) == b"RIFF\x00\x01wav"

    def test_multipart_encoding_is_repeatable(self) -> None:
        multipart = MultipartBody(fields={"a": "1"}, files=[FilePart("f", "x.bin", b"\x00\xff")])
        assert encode_multipart(multipart) == encode_multipart(multipart)
        # A different body gets its own boundary.
        assert MultipartBody().boundary != MultipartBody().boundary

    def test_multipart_wins_over_payload_in_parts(self) -> None:
        multipart = MultipartBody(fields={"a": "1"})
        _, headers, body = http._request_parts(
            "https://runner.example.com/call", payload={"x": 1}, multipart=multipart
        )
        assert headers["Content-Type"].startswith("multipart/form-data; boundary=")
        assert body == encode_multipart(multipart)
