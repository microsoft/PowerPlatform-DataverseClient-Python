# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import unittest
from unittest.mock import MagicMock

from azure.core.credentials import TokenCredential

from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.operations.files import FileOperations


class TestFileOperations(unittest.TestCase):
    """Unit tests for the client.files namespace (FileOperations)."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.client = DataverseClient("https://example.crm.dynamics.com", self.mock_credential)
        self.client._odata = MagicMock()

    # ---------------------------------------------------------------- namespace

    def test_namespace_exists(self):
        """The client.files attribute should be a FileOperations instance."""
        self.assertIsInstance(self.client.files, FileOperations)

    # ----------------------------------------------------------------- upload

    def test_upload(self):
        """upload() should call _upload_file with correct args."""
        self.client.files.upload(
            "account",
            "guid-1",
            "new_Document",
            "/path/to/file.pdf",
            mode="small",
            mime_type="application/pdf",
            if_none_match=False,
        )

        self.client._odata._upload_file.assert_called_once_with(
            "account",
            "guid-1",
            "new_Document",
            "/path/to/file.pdf",
            mode="small",
            mime_type="application/pdf",
            if_none_match=False,
        )

    def test_upload_defaults(self):
        """upload() should pass default keyword args."""
        self.client.files.upload("account", "guid-1", "new_Document", "/path/to/file.pdf")

        self.client._odata._upload_file.assert_called_once_with(
            "account",
            "guid-1",
            "new_Document",
            "/path/to/file.pdf",
            mode=None,
            mime_type=None,
            if_none_match=True,
        )

    def test_upload_returns_none(self):
        """upload() should return None."""
        result = self.client.files.upload("account", "guid-1", "new_Document", "/path/to/file.pdf")
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
