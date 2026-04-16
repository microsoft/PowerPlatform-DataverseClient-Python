# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Unit tests for AsyncFileOperations (client.files namespace)."""

import pytest
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

from azure.core.credentials_async import AsyncTokenCredential

from PowerPlatform.Dataverse.aio.async_client import AsyncDataverseClient
from PowerPlatform.Dataverse.aio.operations.async_files import AsyncFileOperations

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_client_with_mock_odata():
    """
    Return (client, mock_od).

    client._scoped_odata() is patched to yield mock_od without making any
    real HTTP or OData calls.
    """
    credential = AsyncMock(spec=AsyncTokenCredential)
    client = AsyncDataverseClient("https://example.crm.dynamics.com", credential)
    od = AsyncMock()

    @asynccontextmanager
    async def _fake_scoped_odata():
        yield od

    client._scoped_odata = _fake_scoped_odata
    return client, od


# ---------------------------------------------------------------------------
# Namespace
# ---------------------------------------------------------------------------


class TestAsyncFileOperationsNamespace:
    """Tests that the files namespace is correctly exposed on the client."""

    def test_namespace_exists(self):
        """client.files exposes an AsyncFileOperations instance."""
        credential = AsyncMock(spec=AsyncTokenCredential)
        client = AsyncDataverseClient("https://example.crm.dynamics.com", credential)
        assert isinstance(client.files, AsyncFileOperations)


# ---------------------------------------------------------------------------
# upload
# ---------------------------------------------------------------------------


class TestAsyncFileUpload:
    """Tests for files.upload() — parameter forwarding to the OData layer."""

    async def test_upload_calls_upload_file_with_defaults(self):
        """upload() with only required args calls _upload_file with mode=None, mime_type=None, if_none_match=True."""
        client, od = _make_client_with_mock_odata()

        result = await client.files.upload("account", "record-guid-1", "new_Document", "/path/to/file.pdf")

        od._upload_file.assert_awaited_once_with(
            "account",
            "record-guid-1",
            "new_Document",
            "/path/to/file.pdf",
            mode=None,
            mime_type=None,
            if_none_match=True,
        )
        assert result is None

    async def test_upload_returns_none(self):
        """upload() returns None on success."""
        client, od = _make_client_with_mock_odata()
        od._upload_file.return_value = None

        result = await client.files.upload("account", "record-guid-1", "new_Document", "/path/to/file.pdf")

        assert result is None

    async def test_upload_passes_mime_type(self):
        """upload() forwards a custom mime_type to _upload_file."""
        client, od = _make_client_with_mock_odata()

        await client.files.upload(
            "account",
            "record-guid-1",
            "new_Document",
            "/path/to/contract.pdf",
            mime_type="application/pdf",
        )

        od._upload_file.assert_awaited_once_with(
            "account",
            "record-guid-1",
            "new_Document",
            "/path/to/contract.pdf",
            mode=None,
            mime_type="application/pdf",
            if_none_match=True,
        )

    async def test_upload_passes_if_none_match_false(self):
        """upload() forwards if_none_match=False to _upload_file."""
        client, od = _make_client_with_mock_odata()

        await client.files.upload(
            "account",
            "record-guid-1",
            "new_Document",
            "/path/to/file.pdf",
            if_none_match=False,
        )

        od._upload_file.assert_awaited_once_with(
            "account",
            "record-guid-1",
            "new_Document",
            "/path/to/file.pdf",
            mode=None,
            mime_type=None,
            if_none_match=False,
        )

    async def test_upload_passes_mode_chunk(self):
        """upload() forwards mode='chunk' to _upload_file."""
        client, od = _make_client_with_mock_odata()

        await client.files.upload(
            "account",
            "record-guid-1",
            "new_Document",
            "/path/to/large_file.bin",
            mode="chunk",
        )

        od._upload_file.assert_awaited_once_with(
            "account",
            "record-guid-1",
            "new_Document",
            "/path/to/large_file.bin",
            mode="chunk",
            mime_type=None,
            if_none_match=True,
        )

    async def test_upload_all_params_combined(self):
        """upload() correctly forwards all optional parameters together."""
        client, od = _make_client_with_mock_odata()

        await client.files.upload(
            "account",
            "record-guid-1",
            "new_Document",
            "/path/to/file.pdf",
            mode="small",
            mime_type="application/pdf",
            if_none_match=False,
        )

        od._upload_file.assert_awaited_once_with(
            "account",
            "record-guid-1",
            "new_Document",
            "/path/to/file.pdf",
            mode="small",
            mime_type="application/pdf",
            if_none_match=False,
        )

    async def test_upload_different_tables_and_columns(self):
        """upload() works correctly with non-standard table and column names."""
        client, od = _make_client_with_mock_odata()

        await client.files.upload("new_contract", "contract-guid-1", "new_Attachment", "/tmp/doc.docx")

        od._upload_file.assert_awaited_once_with(
            "new_contract",
            "contract-guid-1",
            "new_Attachment",
            "/tmp/doc.docx",
            mode=None,
            mime_type=None,
            if_none_match=True,
        )
