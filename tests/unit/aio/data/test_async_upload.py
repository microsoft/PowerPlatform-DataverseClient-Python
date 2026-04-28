# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Unit tests for _AsyncFileUploadMixin."""

import os
import tempfile
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from PowerPlatform.Dataverse.aio.data._async_odata import _AsyncODataClient

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_client() -> _AsyncODataClient:
    """Return _AsyncODataClient with _request mocked at the HTTP boundary."""
    auth = MagicMock()
    auth._acquire_token = AsyncMock(return_value=MagicMock(access_token="token"))
    client = _AsyncODataClient(auth, "https://example.crm.dynamics.com")
    client._request = AsyncMock()
    return client


def _resp(status=200, headers=None, json_data=None):
    """Create a mock aiohttp-compatible response."""
    r = MagicMock()
    r.status = status
    r.headers = headers or {}
    r.text = AsyncMock(return_value="")
    r.json = AsyncMock(return_value=json_data or {})
    r.read = AsyncMock(return_value=b"")
    return r


def _seed_cache(client, table="account", entity_set="accounts", pk="accountid"):
    """Pre-populate entity-set and primary-ID caches to bypass HTTP for schema-name lookups."""
    key = client._normalize_cache_key(table)
    client._logical_to_entityset_cache[key] = entity_set
    client._logical_primaryid_cache[key] = pk


def _entity_def(meta_id="meta-001", entity_set="accounts", logical="account"):
    """Return a minimal EntityDefinitions value-list response body."""
    return {
        "value": [
            {
                "LogicalName": logical,
                "EntitySetName": entity_set,
                "PrimaryIdAttribute": "accountid",
                "MetadataId": meta_id,
                "SchemaName": "Account",
            }
        ]
    }


# ---------------------------------------------------------------------------
# _upload_file_small()
# ---------------------------------------------------------------------------


class TestUploadFileSmall:
    """Tests for _upload_file_small(), the single-request upload path for small files."""

    async def test_success_uploads_with_patch(self):
        """A successful upload issues a PATCH with x-ms-file-name and If-None-Match headers."""
        client = _make_client()
        client._request.return_value = _resp(status=204)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
            f.write(b"hello world")
            path = f.name
        try:
            await client._upload_file_small("accounts", "guid-1", "new_document", path)
            call_args = client._request.call_args
            assert call_args.args[0] == "patch"
            headers = call_args.kwargs.get("headers", {})
            assert "x-ms-file-name" in headers
            assert headers.get("If-None-Match") == "null"
        finally:
            os.unlink(path)

    async def test_success_with_overwrite(self):
        """When if_none_match=False, an If-Match: * header is sent instead of If-None-Match."""
        client = _make_client()
        client._request.return_value = _resp(status=204)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
            f.write(b"hello world")
            path = f.name
        try:
            await client._upload_file_small("accounts", "guid-1", "new_document", path, if_none_match=False)
            headers = client._request.call_args.kwargs.get("headers", {})
            assert headers.get("If-Match") == "*"
            assert "If-None-Match" not in headers
        finally:
            os.unlink(path)

    async def test_explicit_mime_type(self):
        """An explicit content_type is forwarded as the Content-Type header."""
        client = _make_client()
        client._request.return_value = _resp(status=204)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as f:
            f.write(b"%PDF")
            path = f.name
        try:
            await client._upload_file_small("accounts", "guid-1", "new_document", path, content_type="application/pdf")
            headers = client._request.call_args.kwargs.get("headers", {})
            assert headers.get("Content-Type") == "application/pdf"
        finally:
            os.unlink(path)

    async def test_empty_record_id_raises(self):
        """ValueError is raised immediately when record_id is an empty string."""
        client = _make_client()
        with pytest.raises(ValueError, match="record_id required"):
            await client._upload_file_small("accounts", "", "new_doc", "/any/path")

    async def test_file_not_found_raises(self):
        """FileNotFoundError is raised when the specified file path does not exist."""
        client = _make_client()
        with pytest.raises(FileNotFoundError):
            await client._upload_file_small("accounts", "guid-1", "new_doc", "/nonexistent/path.txt")

    async def test_file_too_large_raises(self):
        """ValueError is raised when the file size exceeds the single-upload size limit."""
        client = _make_client()
        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
            f.write(b"x")
            path = f.name
        try:
            with patch("os.path.getsize", return_value=200 * 1024 * 1024):
                with pytest.raises(ValueError, match="exceeds single-upload limit"):
                    await client._upload_file_small("accounts", "guid-1", "new_doc", path)
        finally:
            os.unlink(path)


# ---------------------------------------------------------------------------
# _upload_file_chunk()
# ---------------------------------------------------------------------------


class TestUploadFileChunk:
    """Tests for _upload_file_chunk(), the chunked upload path for large files."""

    async def test_success_single_chunk(self):
        """A small file completes in two requests: session init and one chunk PUT."""
        client = _make_client()
        location = "https://example.crm.dynamics.com/api/data/v9.2/accounts(guid-1)/new_document?sessiontoken=xyz"
        init_resp = _resp(status=200, headers={"Location": location, "x-ms-chunk-size": "4194304"})
        chunk_resp = _resp(status=204)
        client._request.side_effect = [init_resp, chunk_resp]
        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
            f.write(b"hello world")
            path = f.name
        try:
            await client._upload_file_chunk("accounts", "guid-1", "new_document", path)
            assert client._request.call_count == 2
        finally:
            os.unlink(path)

    async def test_success_with_if_match(self):
        """When if_none_match=False, an If-Match: * header is included in the session-init request."""
        client = _make_client()
        location = "https://example.crm.dynamics.com/api/data/v9.2/accounts(guid-1)/new_document?sessiontoken=abc"
        init_resp = _resp(status=200, headers={"Location": location})
        chunk_resp = _resp(status=204)
        client._request.side_effect = [init_resp, chunk_resp]
        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
            f.write(b"data")
            path = f.name
        try:
            await client._upload_file_chunk("accounts", "guid-1", "new_document", path, if_none_match=False)
            init_headers = client._request.call_args_list[0].kwargs.get("headers", {})
            assert init_headers.get("If-Match") == "*"
        finally:
            os.unlink(path)

    async def test_empty_record_id_raises(self):
        """ValueError is raised immediately when record_id is an empty string."""
        client = _make_client()
        with pytest.raises(ValueError, match="record_id required"):
            await client._upload_file_chunk("accounts", "", "new_doc", "/any/path")

    async def test_file_not_found_raises(self):
        """FileNotFoundError is raised when the specified file path does not exist."""
        client = _make_client()
        with pytest.raises(FileNotFoundError):
            await client._upload_file_chunk("accounts", "guid-1", "new_doc", "/nonexistent/path.txt")

    async def test_missing_location_header_raises(self):
        """RuntimeError is raised when the session-init response lacks a Location header."""
        client = _make_client()
        client._request.return_value = _resp(status=200, headers={})
        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
            f.write(b"data")
            path = f.name
        try:
            with pytest.raises(RuntimeError, match="Missing Location header"):
                await client._upload_file_chunk("accounts", "guid-1", "new_doc", path)
        finally:
            os.unlink(path)

    async def test_invalid_chunk_size_falls_back_to_default(self):
        """A non-integer x-ms-chunk-size header is ignored and the 4MB default is used."""
        client = _make_client()
        location = "https://example.crm.dynamics.com/api/data/v9.2/accounts(guid-1)/new_doc?tok=x"
        init_resp = _resp(status=200, headers={"Location": location, "x-ms-chunk-size": "invalid"})
        chunk_resp = _resp(status=204)
        client._request.side_effect = [init_resp, chunk_resp]
        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
            f.write(b"hello")
            path = f.name
        try:
            await client._upload_file_chunk("accounts", "guid-1", "new_doc", path)
        finally:
            os.unlink(path)


# ---------------------------------------------------------------------------
# _upload_file() — auto mode dispatch
# ---------------------------------------------------------------------------


class TestUploadFile:
    """Tests for _upload_file(), the high-level dispatcher that selects the upload path."""

    async def test_small_file_uses_small_mode(self):
        """mode='small' routes to _upload_file_small without calling _upload_file_chunk."""
        client = _make_client()
        _seed_cache(client)
        client._get_entity_by_table_schema_name = AsyncMock(
            return_value={"MetadataId": "meta-1", "LogicalName": "account"}
        )
        client._get_attribute_metadata = AsyncMock(return_value={"MetadataId": "attr-1"})
        client._upload_file_small = AsyncMock(return_value=None)
        client._upload_file_chunk = AsyncMock(return_value=None)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
            f.write(b"small content")
            path = f.name
        try:
            await client._upload_file("account", "guid-1", "new_doc", path, mode="small")
            client._upload_file_small.assert_called_once()
            client._upload_file_chunk.assert_not_called()
        finally:
            os.unlink(path)

    async def test_chunk_mode_uses_chunk_upload(self):
        """mode='chunk' routes to _upload_file_chunk without calling _upload_file_small."""
        client = _make_client()
        _seed_cache(client)
        client._get_entity_by_table_schema_name = AsyncMock(
            return_value={"MetadataId": "meta-1", "LogicalName": "account"}
        )
        client._get_attribute_metadata = AsyncMock(return_value={"MetadataId": "attr-1"})
        client._upload_file_small = AsyncMock(return_value=None)
        client._upload_file_chunk = AsyncMock(return_value=None)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".bin") as f:
            f.write(b"big content")
            path = f.name
        try:
            await client._upload_file("account", "guid-1", "new_doc", path, mode="chunk")
            client._upload_file_chunk.assert_called_once()
        finally:
            os.unlink(path)

    async def test_invalid_mode_raises(self):
        """ValueError is raised when an unrecognised mode string is supplied."""
        client = _make_client()
        _seed_cache(client)
        client._get_entity_by_table_schema_name = AsyncMock(
            return_value={"MetadataId": "meta-1", "LogicalName": "account"}
        )
        client._get_attribute_metadata = AsyncMock(return_value={"MetadataId": "attr-1"})
        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
            f.write(b"data")
            path = f.name
        try:
            with pytest.raises(ValueError, match="Invalid mode"):
                await client._upload_file("account", "guid-1", "new_doc", path, mode="badmode")
        finally:
            os.unlink(path)

    async def test_auto_mode_file_not_found_raises(self):
        """FileNotFoundError is raised in default auto mode when the file path does not exist."""
        client = _make_client()
        _seed_cache(client)
        client._get_entity_by_table_schema_name = AsyncMock(
            return_value={"MetadataId": "meta-1", "LogicalName": "account"}
        )
        client._get_attribute_metadata = AsyncMock(return_value={"MetadataId": "attr-1"})
        with pytest.raises(FileNotFoundError):
            await client._upload_file("account", "guid-1", "new_doc", "/nonexistent/file.txt")

    async def test_attribute_not_found_creates_it(self):
        """When attribute metadata is missing, _create_columns and _wait_for_attribute_visibility are called."""
        client = _make_client()
        _seed_cache(client)
        client._get_entity_by_table_schema_name = AsyncMock(
            return_value={"MetadataId": "meta-1", "LogicalName": "account"}
        )
        client._get_attribute_metadata = AsyncMock(return_value=None)
        client._create_columns = AsyncMock(return_value=["new_doc"])
        client._wait_for_attribute_visibility = AsyncMock(return_value=None)
        client._upload_file_small = AsyncMock(return_value=None)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
            f.write(b"data")
            path = f.name
        try:
            await client._upload_file("account", "guid-1", "new_doc", path, mode="small")
            client._create_columns.assert_called_once_with("account", {"new_doc": "file"})
            client._wait_for_attribute_visibility.assert_called_once()
        finally:
            os.unlink(path)
