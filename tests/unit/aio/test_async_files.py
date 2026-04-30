# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import pytest

from PowerPlatform.Dataverse.aio.operations.async_files import AsyncFileOperations


class TestAsyncFileOperationsNamespace:
    def test_namespace_type(self, async_client):
        assert isinstance(async_client.files, AsyncFileOperations)


class TestAsyncFileUpload:
    async def test_upload_delegates_to_upload_file(self, async_client, mock_od):
        """upload() calls od._upload_file with all provided arguments."""
        await async_client.files.upload(
            "account",
            "guid-1",
            "new_Document",
            "/path/to/file.pdf",
            mode="small",
            mime_type="application/pdf",
            if_none_match=False,
        )

        mock_od._upload_file.assert_called_once_with(
            "account",
            "guid-1",
            "new_Document",
            "/path/to/file.pdf",
            mode="small",
            mime_type="application/pdf",
            if_none_match=False,
        )

    async def test_upload_default_args(self, async_client, mock_od):
        """upload() passes None/True for optional args when not specified."""
        await async_client.files.upload("account", "guid-1", "new_Doc", "/path/file.txt")

        mock_od._upload_file.assert_called_once_with(
            "account",
            "guid-1",
            "new_Doc",
            "/path/file.txt",
            mode=None,
            mime_type=None,
            if_none_match=True,
        )

    async def test_upload_returns_none(self, async_client, mock_od):
        """upload() returns None."""
        result = await async_client.files.upload("account", "guid-1", "new_Doc", "/path/file.txt")
        assert result is None
