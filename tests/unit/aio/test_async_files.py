# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Unit tests for AsyncFileOperations (client.files namespace)."""

import pytest
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

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
    credential = AsyncMock()
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
    def test_namespace_exists(self):
        credential = AsyncMock()
        client = AsyncDataverseClient("https://example.crm.dynamics.com", credential)
        assert isinstance(client.files, AsyncFileOperations)


# ---------------------------------------------------------------------------
# upload
# ---------------------------------------------------------------------------

class TestAsyncFileUpload:
    async def test_upload_calls_upload_file_with_defaults(self):
        client, od = _make_client_with_mock_odata()

        result = await client.files.upload(
            "account", "record-guid-1", "new_Document", "/path/to/file.pdf"
        )

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
        client, od = _make_client_with_mock_odata()
        od._upload_file.return_value = None

        result = await client.files.upload(
            "account", "record-guid-1", "new_Document", "/path/to/file.pdf"
        )

        assert result is None

    async def test_upload_passes_mime_type(self):
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
        client, od = _make_client_with_mock_odata()

        await client.files.upload(
            "new_contract", "contract-guid-1", "new_Attachment", "/tmp/doc.docx"
        )

        od._upload_file.assert_awaited_once_with(
            "new_contract",
            "contract-guid-1",
            "new_Attachment",
            "/tmp/doc.docx",
            mode=None,
            mime_type=None,
            if_none_match=True,
        )
