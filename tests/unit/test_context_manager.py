# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Unit tests for DataverseClient context manager support."""

import unittest
from unittest.mock import MagicMock, patch

import requests
from azure.core.credentials import TokenCredential

from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.core._http import _HttpClient
from PowerPlatform.Dataverse.core.results import RequestTelemetryData


class TestContextManager(unittest.TestCase):
    """Test context manager support on DataverseClient."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.base_url = "https://example.crm.dynamics.com"

    def test_enter_creates_session(self):
        """Test that __enter__ creates a session."""
        client = DataverseClient(self.base_url, self.mock_credential)
        self.assertIsNone(client._session)

        result = client.__enter__()

        self.assertIsInstance(client._session, requests.Session)
        self.assertTrue(client._owns_session)
        self.assertIs(result, client)

    def test_exit_closes_session(self):
        """Test that __exit__ closes the session."""
        client = DataverseClient(self.base_url, self.mock_credential)
        client.__enter__()

        mock_session = MagicMock(spec=requests.Session)
        client._session = mock_session
        client._owns_session = True

        client.__exit__(None, None, None)

        mock_session.close.assert_called_once()
        self.assertIsNone(client._session)
        self.assertFalse(client._owns_session)

    def test_context_manager_protocol(self):
        """Test full context manager protocol."""
        with DataverseClient(self.base_url, self.mock_credential) as client:
            self.assertIsInstance(client, DataverseClient)
            self.assertIsInstance(client._session, requests.Session)

        # After exiting context, session should be closed
        self.assertIsNone(client._session)

    def test_close_method(self):
        """Test explicit close() method."""
        client = DataverseClient(self.base_url, self.mock_credential)
        client.__enter__()

        client.close()

        self.assertIsNone(client._session)
        self.assertFalse(client._owns_session)

    def test_close_idempotent(self):
        """Test that close() can be called multiple times safely."""
        client = DataverseClient(self.base_url, self.mock_credential)
        client.__enter__()

        # Should not raise
        client.close()
        client.close()
        client.close()

    def test_close_without_enter(self):
        """Test that close() works even without __enter__."""
        client = DataverseClient(self.base_url, self.mock_credential)

        # Should not raise
        client.close()
        self.assertIsNone(client._session)

    def test_exit_with_exception(self):
        """Test that __exit__ runs cleanup even on exception."""
        client = DataverseClient(self.base_url, self.mock_credential)

        # Simulate exception in context
        try:
            with client:
                session = client._session
                self.assertIsNotNone(session)
                raise ValueError("Test exception")
        except ValueError:
            pass

        # Session should still be closed
        self.assertIsNone(client._session)

    def test_session_passed_to_odata(self):
        """Test that session is passed to OData client."""
        with DataverseClient(self.base_url, self.mock_credential) as client:
            # Trigger OData client creation
            odata = client._get_odata()
            self.assertIs(odata._http._session, client._session)

    def test_works_without_context_manager(self):
        """Test that client works without context manager (backward compatibility)."""
        client = DataverseClient(self.base_url, self.mock_credential)

        # Should work without __enter__
        self.assertIsNone(client._session)

        # Mock the OData client for testing
        client._odata = MagicMock()
        client._odata._entity_set_from_schema_name.return_value = "accounts"
        client._odata._create.return_value = ("guid-123", RequestTelemetryData())

        # This should work
        result = client.records.create("account", {"name": "Test"})
        self.assertEqual(result, "guid-123")

    def test_close_also_closes_odata(self):
        """Test that close() also closes the OData client."""
        client = DataverseClient(self.base_url, self.mock_credential)
        client.__enter__()

        # Mock the OData client
        mock_odata = MagicMock()
        client._odata = mock_odata

        client.close()

        mock_odata.close.assert_called_once()
        self.assertIsNone(client._odata)

    def test_nested_enter_reuses_session(self):
        """Test that nested __enter__ reuses the existing session."""
        client = DataverseClient(self.base_url, self.mock_credential)

        with client:
            session1 = client._session
            # Nested enter should reuse session
            client.__enter__()
            self.assertIs(client._session, session1)


class TestHttpClientSession(unittest.TestCase):
    """Test _HttpClient session support."""

    def test_request_uses_session_when_provided(self):
        """Test that _request uses session when available."""
        mock_session = MagicMock(spec=requests.Session)
        mock_response = MagicMock(spec=requests.Response)
        mock_session.request.return_value = mock_response

        client = _HttpClient(session=mock_session)
        result = client._request("GET", "https://example.com")

        mock_session.request.assert_called_once()
        self.assertIs(result, mock_response)

    def test_request_uses_requests_without_session(self):
        """Test that _request uses requests.request without session."""
        client = _HttpClient()

        with patch("requests.request") as mock_request:
            mock_response = MagicMock(spec=requests.Response)
            mock_request.return_value = mock_response

            result = client._request("GET", "https://example.com")

            mock_request.assert_called_once()
            self.assertIs(result, mock_response)

    def test_close_closes_session(self):
        """Test that close() closes the session."""
        mock_session = MagicMock(spec=requests.Session)
        client = _HttpClient(session=mock_session)

        client.close()

        mock_session.close.assert_called_once()
        self.assertIsNone(client._session)

    def test_close_without_session(self):
        """Test that close() works without session."""
        client = _HttpClient()

        # Should not raise
        client.close()

    def test_close_idempotent(self):
        """Test that close() can be called multiple times."""
        mock_session = MagicMock(spec=requests.Session)
        client = _HttpClient(session=mock_session)

        client.close()
        client.close()  # Should not raise

        # Only closed once
        mock_session.close.assert_called_once()


class TestODataClientSession(unittest.TestCase):
    """Test _ODataClient session support."""

    def test_session_passed_to_http_client(self):
        """Test that session is passed through to _HttpClient."""
        from PowerPlatform.Dataverse.data._odata import _ODataClient

        mock_auth = MagicMock()
        mock_session = MagicMock(spec=requests.Session)

        client = _ODataClient(
            mock_auth,
            "https://example.crm.dynamics.com",
            session=mock_session,
        )

        self.assertIs(client._http._session, mock_session)

    def test_close_closes_http_client(self):
        """Test that close() closes the HTTP client."""
        from PowerPlatform.Dataverse.data._odata import _ODataClient

        mock_auth = MagicMock()
        mock_session = MagicMock(spec=requests.Session)

        client = _ODataClient(
            mock_auth,
            "https://example.crm.dynamics.com",
            session=mock_session,
        )

        client.close()

        mock_session.close.assert_called_once()


class TestContextManagerIntegration(unittest.TestCase):
    """Integration-style tests for context manager with full operation flow."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.base_url = "https://example.crm.dynamics.com"

    def test_multiple_operations_share_session(self):
        """Test that multiple operations within context share the same session."""
        with DataverseClient(self.base_url, self.mock_credential) as client:
            session = client._session
            self.assertIsNotNone(session)

            # Mock OData client
            client._odata = MagicMock()
            client._odata._entity_set_from_schema_name.return_value = "accounts"
            mock_metadata = RequestTelemetryData()
            client._odata._create.return_value = ("guid-1", mock_metadata)
            client._odata._update.return_value = (None, mock_metadata)
            client._odata._delete.return_value = (None, mock_metadata)

            # Multiple operations
            client.records.create("account", {"name": "Test1"})
            client.records.update("account", "guid-1", {"name": "Updated"})
            client.records.delete("account", "guid-1")

            # Session should still be the same
            self.assertIs(client._session, session)

    def test_context_manager_with_exception_handling(self):
        """Test that resources are cleaned up even with exception."""
        client = DataverseClient(self.base_url, self.mock_credential)

        try:
            with client:
                session = client._session
                self.assertIsNotNone(session)
                raise RuntimeError("Simulated error")
        except RuntimeError:
            pass

        # Session should be closed
        self.assertIsNone(client._session)


if __name__ == "__main__":
    unittest.main()
