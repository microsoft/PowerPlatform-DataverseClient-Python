# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Unit tests for _AsyncFileUploadMixin."""

from __future__ import annotations

import os
import tempfile

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from PowerPlatform.Dataverse.aio.data._async_upload import _AsyncFileUploadMixin


# ---------------------------------------------------------------------------
# Test client
# ---------------------------------------------------------------------------

class _MockUploadClient(_AsyncFileUploadMixin):
    """Minimal async client that satisfies mixin dependencies."""

    def __init__(self):
        self.api = "https://example.crm.dynamics.com/api/data/v9.2"
        self._request = AsyncMock()
        self._entity_set_from_schema_name = AsyncMock(return_value="accounts")
        self._get_entity_by_table_schema_name = AsyncMock(
            return_value={"MetadataId": "meta-id-1", "EntitySetName": "accounts"}
        )
        self._get_attribute_metadata = AsyncMock(return_value={"MetadataId": "attr-id-1"})
        self._create_columns = AsyncMock(return_value=["filecolumn"])
        self._wait_for_attribute_visibility = AsyncMock()

    def _format_key(self, record_id: str) -> str:
        key = record_id.strip()
        if key.startswith("(") and key.endswith(")"):
            return key
        return f"({key})"


def _mock_response(status_code=200, headers=None):
    resp = MagicMock()
    resp.status_code = status_code
    resp.headers = headers or {}
    resp.text = ""
    resp.json.return_value = {}
    return resp


def _tmp_file(content: bytes = b"hello world") -> str:
    """Create a temporary file with given content; caller must delete."""
    f = tempfile.NamedTemporaryFile(delete=False, suffix=".bin")
    f.write(content)
    f.close()
    return f.name


# ---------------------------------------------------------------------------
# _upload_file — mode dispatch
# ---------------------------------------------------------------------------

class TestUploadFileMode:
    async def test_small_mode_calls_upload_file_small(self):
        client = _MockUploadClient()
        client._upload_file_small = AsyncMock()
        with patch("os.path.isfile", return_value=True), patch("os.path.getsize", return_value=100):
            await client._upload_file("account", "rec-1", "filecolumn", "/fake/path.txt", mode="small")
        client._upload_file_small.assert_awaited_once()

    async def test_chunk_mode_calls_upload_file_chunk(self):
        client = _MockUploadClient()
        client._upload_file_chunk = AsyncMock()
        with patch("os.path.isfile", return_value=True), patch("os.path.getsize", return_value=100):
            await client._upload_file("account", "rec-1", "filecolumn", "/fake/path.txt", mode="chunk")
        client._upload_file_chunk.assert_awaited_once()

    async def test_auto_mode_small_file_uses_small(self):
        client = _MockUploadClient()
        client._upload_file_small = AsyncMock()
        with patch("os.path.isfile", return_value=True), patch("os.path.getsize", return_value=1024):
            await client._upload_file("account", "rec-1", "filecolumn", "/fake/path.txt", mode="auto")
        client._upload_file_small.assert_awaited_once()

    async def test_auto_mode_large_file_uses_chunk(self):
        client = _MockUploadClient()
        client._upload_file_chunk = AsyncMock()
        large = 128 * 1024 * 1024 + 1
        with patch("os.path.isfile", return_value=True), patch("os.path.getsize", return_value=large):
            await client._upload_file("account", "rec-1", "filecolumn", "/fake/path.txt", mode="auto")
        client._upload_file_chunk.assert_awaited_once()

    async def test_invalid_mode_raises_value_error(self):
        client = _MockUploadClient()
        with patch("os.path.isfile", return_value=True), patch("os.path.getsize", return_value=100):
            with pytest.raises(ValueError, match="Invalid mode"):
                await client._upload_file("account", "rec-1", "filecolumn", "/fake/path.txt", mode="badmode")

    async def test_mode_is_case_insensitive(self):
        client = _MockUploadClient()
        client._upload_file_small = AsyncMock()
        with patch("os.path.isfile", return_value=True), patch("os.path.getsize", return_value=100):
            await client._upload_file("account", "rec-1", "filecolumn", "/fake/path.txt", mode="Small")
        client._upload_file_small.assert_awaited_once()


class TestUploadFileColumnCreation:
    async def test_creates_column_when_attr_metadata_missing(self):
        client = _MockUploadClient()
        client._get_attribute_metadata = AsyncMock(return_value=None)
        client._upload_file_small = AsyncMock()
        with patch("os.path.isfile", return_value=True), patch("os.path.getsize", return_value=100):
            await client._upload_file("account", "rec-1", "filecolumn", "/fake/path.txt", mode="small")
        client._create_columns.assert_awaited_once_with("account", {"filecolumn": "file"})
        client._wait_for_attribute_visibility.assert_awaited_once()

    async def test_skips_column_creation_when_attr_metadata_present(self):
        client = _MockUploadClient()
        client._upload_file_small = AsyncMock()
        with patch("os.path.isfile", return_value=True), patch("os.path.getsize", return_value=100):
            await client._upload_file("account", "rec-1", "filecolumn", "/fake/path.txt", mode="small")
        client._create_columns.assert_not_awaited()

    async def test_attribute_name_lowercased_before_dispatch(self):
        """file_name_attribute is lowercased before being passed to upload helpers."""
        client = _MockUploadClient()
        client._upload_file_small = AsyncMock()
        with patch("os.path.isfile", return_value=True), patch("os.path.getsize", return_value=100):
            await client._upload_file("account", "rec-1", "FileColumn", "/fake/path.txt", mode="small")
        call_kwargs = client._upload_file_small.call_args
        # third positional arg is file_name_attribute
        assert call_kwargs[0][2] == "filecolumn"

    async def test_mime_type_forwarded_to_upload_file_small(self):
        client = _MockUploadClient()
        client._upload_file_small = AsyncMock()
        with patch("os.path.isfile", return_value=True), patch("os.path.getsize", return_value=100):
            await client._upload_file(
                "account", "rec-1", "filecolumn", "/fake/path.txt", mode="small", mime_type="image/png"
            )
        call_kwargs = client._upload_file_small.call_args[1]
        assert call_kwargs.get("content_type") == "image/png"

    async def test_if_none_match_forwarded(self):
        client = _MockUploadClient()
        client._upload_file_small = AsyncMock()
        with patch("os.path.isfile", return_value=True), patch("os.path.getsize", return_value=100):
            await client._upload_file(
                "account", "rec-1", "filecolumn", "/fake/path.txt", mode="small", if_none_match=False
            )
        call_kwargs = client._upload_file_small.call_args[1]
        assert call_kwargs.get("if_none_match") is False


# ---------------------------------------------------------------------------
# _upload_file_small
# ---------------------------------------------------------------------------

class TestUploadFileSmall:
    async def test_uploads_successfully(self):
        client = _MockUploadClient()
        client._request.return_value = _mock_response(status_code=204)
        path = _tmp_file(b"file content")
        try:
            await client._upload_file_small("accounts", "rec-1", "filecolumn", path)
            client._request.assert_awaited_once()
        finally:
            os.unlink(path)

    async def test_empty_record_id_raises(self):
        client = _MockUploadClient()
        with pytest.raises(ValueError, match="record_id required"):
            await client._upload_file_small("accounts", "", "filecolumn", "/some/path.txt")

    async def test_file_not_found_raises(self):
        client = _MockUploadClient()
        with pytest.raises(FileNotFoundError):
            await client._upload_file_small("accounts", "rec-1", "filecolumn", "/nonexistent/path.txt")

    async def test_file_too_large_raises(self):
        path = _tmp_file(b"x")
        try:
            client = _MockUploadClient()
            limit = 128 * 1024 * 1024
            with patch("os.path.getsize", return_value=limit + 1):
                with pytest.raises(ValueError, match="exceeds single-upload limit"):
                    await client._upload_file_small("accounts", "rec-1", "filecolumn", path)
        finally:
            os.unlink(path)

    async def test_default_content_type_is_octet_stream(self):
        client = _MockUploadClient()
        client._request.return_value = _mock_response(status_code=204)
        path = _tmp_file(b"data")
        try:
            await client._upload_file_small("accounts", "rec-1", "filecolumn", path)
            headers = client._request.call_args[1].get("headers", {})
            assert headers.get("Content-Type") == "application/octet-stream"
        finally:
            os.unlink(path)

    async def test_custom_content_type_used(self):
        client = _MockUploadClient()
        client._request.return_value = _mock_response(status_code=204)
        path = _tmp_file(b"data")
        try:
            await client._upload_file_small("accounts", "rec-1", "filecolumn", path, content_type="image/jpeg")
            headers = client._request.call_args[1].get("headers", {})
            assert headers.get("Content-Type") == "image/jpeg"
        finally:
            os.unlink(path)

    async def test_if_none_match_true_sets_if_none_match_header(self):
        client = _MockUploadClient()
        client._request.return_value = _mock_response(status_code=204)
        path = _tmp_file(b"data")
        try:
            await client._upload_file_small("accounts", "rec-1", "filecolumn", path, if_none_match=True)
            headers = client._request.call_args[1].get("headers", {})
            assert headers.get("If-None-Match") == "null"
            assert "If-Match" not in headers
        finally:
            os.unlink(path)

    async def test_if_none_match_false_sets_if_match_star(self):
        client = _MockUploadClient()
        client._request.return_value = _mock_response(status_code=204)
        path = _tmp_file(b"data")
        try:
            await client._upload_file_small("accounts", "rec-1", "filecolumn", path, if_none_match=False)
            headers = client._request.call_args[1].get("headers", {})
            assert headers.get("If-Match") == "*"
            assert "If-None-Match" not in headers
        finally:
            os.unlink(path)

    async def test_x_ms_file_name_header_set(self):
        client = _MockUploadClient()
        client._request.return_value = _mock_response(status_code=204)
        path = _tmp_file(b"data")
        try:
            await client._upload_file_small("accounts", "rec-1", "filecolumn", path)
            headers = client._request.call_args[1].get("headers", {})
            assert headers.get("x-ms-file-name") == os.path.basename(path)
        finally:
            os.unlink(path)

    async def test_uses_patch_method(self):
        client = _MockUploadClient()
        client._request.return_value = _mock_response(status_code=204)
        path = _tmp_file(b"data")
        try:
            await client._upload_file_small("accounts", "rec-1", "filecolumn", path)
            assert client._request.call_args[0][0] == "patch"
        finally:
            os.unlink(path)


# ---------------------------------------------------------------------------
# _upload_file_chunk
# ---------------------------------------------------------------------------

class TestUploadFileChunk:
    async def test_uploads_successfully_in_chunks(self):
        client = _MockUploadClient()
        path = _tmp_file(b"A" * 100)
        try:
            init_resp = _mock_response(
                status_code=200,
                headers={"Location": "https://example.com/upload-session/token123"},
            )
            chunk_resp = _mock_response(status_code=206)
            client._request.side_effect = [init_resp, chunk_resp]

            await client._upload_file_chunk("accounts", "rec-1", "filecolumn", path)

            assert client._request.call_count >= 2
        finally:
            os.unlink(path)

    async def test_missing_location_header_raises(self):
        client = _MockUploadClient()
        path = _tmp_file(b"data")
        try:
            client._request.return_value = _mock_response(status_code=200, headers={})
            with pytest.raises(RuntimeError, match="Missing Location header"):
                await client._upload_file_chunk("accounts", "rec-1", "filecolumn", path)
        finally:
            os.unlink(path)

    async def test_empty_record_id_raises(self):
        client = _MockUploadClient()
        with pytest.raises(ValueError, match="record_id required"):
            await client._upload_file_chunk("accounts", "", "filecolumn", "/some/path.txt")

    async def test_file_not_found_raises(self):
        client = _MockUploadClient()
        with pytest.raises(FileNotFoundError):
            await client._upload_file_chunk("accounts", "rec-1", "filecolumn", "/nonexistent/path.txt")

    async def test_if_none_match_true_sets_header_on_init(self):
        client = _MockUploadClient()
        path = _tmp_file(b"data")
        try:
            init_resp = _mock_response(headers={"Location": "https://example.com/session/tok"})
            chunk_resp = _mock_response(status_code=206)
            client._request.side_effect = [init_resp, chunk_resp]

            await client._upload_file_chunk("accounts", "rec-1", "filecolumn", path, if_none_match=True)

            init_headers = client._request.call_args_list[0][1].get("headers", {})
            assert init_headers.get("If-None-Match") == "null"
            assert "If-Match" not in init_headers
        finally:
            os.unlink(path)

    async def test_if_none_match_false_sets_if_match_star_on_init(self):
        client = _MockUploadClient()
        path = _tmp_file(b"data")
        try:
            init_resp = _mock_response(headers={"Location": "https://example.com/session/tok"})
            chunk_resp = _mock_response(status_code=206)
            client._request.side_effect = [init_resp, chunk_resp]

            await client._upload_file_chunk("accounts", "rec-1", "filecolumn", path, if_none_match=False)

            init_headers = client._request.call_args_list[0][1].get("headers", {})
            assert init_headers.get("If-Match") == "*"
            assert "If-None-Match" not in init_headers
        finally:
            os.unlink(path)

    async def test_content_range_header_correct(self):
        """Chunk request includes correct Content-Range header."""
        client = _MockUploadClient()
        data = b"X" * 50
        path = _tmp_file(data)
        try:
            init_resp = _mock_response(headers={"Location": "https://example.com/session/tok"})
            chunk_resp = _mock_response(status_code=206)
            client._request.side_effect = [init_resp, chunk_resp]

            await client._upload_file_chunk("accounts", "rec-1", "filecolumn", path)

            chunk_headers = client._request.call_args_list[1][1].get("headers", {})
            assert chunk_headers.get("Content-Range") == f"bytes 0-49/{len(data)}"
        finally:
            os.unlink(path)

    async def test_server_chunk_size_used_when_provided(self):
        """x-ms-chunk-size response header controls chunk size."""
        client = _MockUploadClient()
        data = b"Y" * 20
        path = _tmp_file(data)
        try:
            init_resp = _mock_response(
                headers={"Location": "https://example.com/session/tok", "x-ms-chunk-size": "10"}
            )
            chunk_resp1 = _mock_response(status_code=206)
            chunk_resp2 = _mock_response(status_code=204)
            client._request.side_effect = [init_resp, chunk_resp1, chunk_resp2]

            await client._upload_file_chunk("accounts", "rec-1", "filecolumn", path)

            # With chunk size 10 and 20 bytes, should make exactly 2 chunk requests
            assert client._request.call_count == 3  # 1 init + 2 chunks
        finally:
            os.unlink(path)

    async def test_malformed_chunk_size_header_falls_back_to_default(self):
        """Non-integer x-ms-chunk-size falls back to 4 MB default."""
        client = _MockUploadClient()
        path = _tmp_file(b"data")
        try:
            init_resp = _mock_response(
                headers={"Location": "https://example.com/session/tok", "x-ms-chunk-size": "not-a-number"}
            )
            chunk_resp = _mock_response(status_code=206)
            client._request.side_effect = [init_resp, chunk_resp]

            # Should not raise — falls back to 4 MB default
            await client._upload_file_chunk("accounts", "rec-1", "filecolumn", path)
        finally:
            os.unlink(path)

    async def test_zero_byte_file_sends_one_chunk_request(self):
        """A zero-byte file still completes the init request; chunk loop exits immediately."""
        client = _MockUploadClient()
        path = _tmp_file(b"")
        try:
            init_resp = _mock_response(headers={"Location": "https://example.com/session/tok"})
            client._request.return_value = init_resp

            await client._upload_file_chunk("accounts", "rec-1", "filecolumn", path)

            # Only the init PATCH is sent; the chunk loop reads 0 bytes and exits
            assert client._request.call_count == 1
        finally:
            os.unlink(path)
