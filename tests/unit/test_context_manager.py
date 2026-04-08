# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Unit tests for DataverseClient context manager and lifecycle support."""

import unittest
from unittest.mock import MagicMock, patch

from azure.core.credentials import TokenCredential

from PowerPlatform.Dataverse.client import DataverseClient


class TestContextManagerProtocol(unittest.TestCase):
    """Tests for the __enter__ / __exit__ context manager protocol."""

    def setUp(self):
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.base_url = "https://example.crm.dynamics.com"

    def test_enter_returns_self(self):
        """__enter__ should return the client instance itself."""
        client = DataverseClient(self.base_url, self.mock_credential)
        result = client.__enter__()
        self.assertIs(result, client)
        client.close()

    def test_with_statement_works(self):
        """Client should be usable as a context manager with 'with' statement."""
        with DataverseClient(self.base_url, self.mock_credential) as client:
            self.assertIsInstance(client, DataverseClient)
            self.assertFalse(client._closed)
        self.assertTrue(client._closed)

    def test_exit_calls_close(self):
        """__exit__ should call close()."""
        client = DataverseClient(self.base_url, self.mock_credential)
        client.__enter__()
        with patch.object(client, "close") as mock_close:
            client.__exit__(None, None, None)
            mock_close.assert_called_once()

    def test_exit_does_not_suppress_exceptions(self):
        """__exit__ should not suppress exceptions (returns None)."""
        with self.assertRaises(ValueError):
            with DataverseClient(self.base_url, self.mock_credential) as client:
                raise ValueError("test error")
        # Client should still be closed after the exception
        self.assertTrue(client._closed)


class TestSessionLifecycle(unittest.TestCase):
    """Tests for requests.Session creation and teardown."""

    def setUp(self):
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.base_url = "https://example.crm.dynamics.com"

    def test_session_none_before_enter(self):
        """Session should be None before entering context manager."""
        client = DataverseClient(self.base_url, self.mock_credential)
        self.assertIsNone(client._session)

    def test_session_created_on_enter(self):
        """Entering context manager should create a requests.Session."""
        import requests

        client = DataverseClient(self.base_url, self.mock_credential)
        client.__enter__()
        self.assertIsNotNone(client._session)
        self.assertIsInstance(client._session, requests.Session)
        client.close()

    def test_session_closed_on_exit(self):
        """Exiting context manager should close and nullify the session."""
        with DataverseClient(self.base_url, self.mock_credential) as client:
            self.assertIsNotNone(client._session)
        self.assertIsNone(client._session)

    def test_session_threaded_to_http_client(self):
        """Session should be passed through _ODataClient to _HttpClient."""
        with DataverseClient(self.base_url, self.mock_credential) as client:
            # Trigger lazy initialization of _odata
            odata = client._get_odata()
            self.assertIs(odata._http._session, client._session)

    def test_no_session_without_context_manager(self):
        """Client without 'with' should have no session."""
        client = DataverseClient(self.base_url, self.mock_credential)
        self.assertIsNone(client._session)

    def test_reentrant_enter_reuses_session(self):
        """Calling __enter__ twice should reuse the existing session."""
        client = DataverseClient(self.base_url, self.mock_credential)
        client.__enter__()
        session1 = client._session
        client.__enter__()
        session2 = client._session
        self.assertIs(session1, session2)
        client.close()


class TestCloseMethod(unittest.TestCase):
    """Tests for the close() method."""

    def setUp(self):
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.base_url = "https://example.crm.dynamics.com"

    def test_close_sets_closed_flag(self):
        """close() should set _closed to True."""
        client = DataverseClient(self.base_url, self.mock_credential)
        self.assertFalse(client._closed)
        client.close()
        self.assertTrue(client._closed)

    def test_close_idempotent(self):
        """Calling close() multiple times should not raise."""
        client = DataverseClient(self.base_url, self.mock_credential)
        client.close()
        client.close()  # Should not raise
        self.assertTrue(client._closed)

    def test_close_on_never_used_client(self):
        """close() on a client that never made any calls should not raise."""
        client = DataverseClient(self.base_url, self.mock_credential)
        # _odata is None, _session is None -- close should handle this
        client.close()
        self.assertTrue(client._closed)
        self.assertIsNone(client._odata)
        self.assertIsNone(client._session)

    def test_close_nullifies_odata(self):
        """close() should close and nullify the _odata client."""
        client = DataverseClient(self.base_url, self.mock_credential)
        client._odata = MagicMock()
        client.close()
        self.assertIsNone(client._odata)

    def test_close_calls_odata_close(self):
        """close() should call _odata.close() if _odata exists."""
        client = DataverseClient(self.base_url, self.mock_credential)
        mock_odata = MagicMock()
        client._odata = mock_odata
        client.close()
        mock_odata.close.assert_called_once()

    def test_close_nullifies_session(self):
        """close() should close and nullify the session."""
        with DataverseClient(self.base_url, self.mock_credential) as client:
            self.assertIsNotNone(client._session)
        self.assertIsNone(client._session)

    def test_close_clears_odata_caches(self):
        """close() should clear all three internal caches via _odata.close()."""
        client = DataverseClient(self.base_url, self.mock_credential)
        # Create a real _odata to verify cache clearing
        odata = client._get_odata()
        odata._logical_to_entityset_cache["test"] = "value"
        odata._logical_primaryid_cache["test"] = "value"
        odata._picklist_label_cache[("test", "attr")] = {"map": {}, "ts": 0}

        client.close()

        # _odata is now None, but we held a reference
        self.assertEqual(len(odata._logical_to_entityset_cache), 0)
        self.assertEqual(len(odata._logical_primaryid_cache), 0)
        self.assertEqual(len(odata._picklist_label_cache), 0)


class TestClosedStateGuard(unittest.TestCase):
    """Tests that operations raise RuntimeError after the client is closed."""

    def setUp(self):
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.base_url = "https://example.crm.dynamics.com"
        self.client = DataverseClient(self.base_url, self.mock_credential)
        self.client._odata = MagicMock()
        self.client.close()

    def test_check_closed_raises(self):
        """_check_closed() should raise RuntimeError on a closed client."""
        with self.assertRaises(RuntimeError) as ctx:
            self.client._check_closed()
        self.assertIn("closed", str(ctx.exception))

    def test_records_create_raises_after_close(self):
        with self.assertRaises(RuntimeError):
            self.client.records.create("account", {"name": "test"})

    def test_records_update_raises_after_close(self):
        with self.assertRaises(RuntimeError):
            self.client.records.update("account", "guid", {"name": "test"})

    def test_records_delete_raises_after_close(self):
        with self.assertRaises(RuntimeError):
            self.client.records.delete("account", "guid")

    def test_records_get_raises_after_close(self):
        with self.assertRaises(RuntimeError):
            self.client.records.get("account", "guid")

    def test_query_sql_raises_after_close(self):
        with self.assertRaises(RuntimeError):
            self.client.query.sql("SELECT name FROM account")

    def test_tables_get_raises_after_close(self):
        with self.assertRaises(RuntimeError):
            self.client.tables.get("account")

    def test_tables_create_raises_after_close(self):
        with self.assertRaises(RuntimeError):
            self.client.tables.create("new_Test", {"new_Name": "string"})

    def test_tables_list_raises_after_close(self):
        with self.assertRaises(RuntimeError):
            self.client.tables.list()

    def test_files_upload_raises_after_close(self):
        with self.assertRaises(RuntimeError):
            self.client.files.upload("account", "guid", "col", "/tmp/f.txt")

    def test_flush_cache_raises_after_close(self):
        with self.assertRaises(RuntimeError):
            self.client.flush_cache("picklist")

    def test_enter_raises_after_close(self):
        """__enter__ should raise RuntimeError on a closed client."""
        with self.assertRaises(RuntimeError):
            self.client.__enter__()

    def test_error_message_is_clear(self):
        """The RuntimeError message should clearly state the client is closed."""
        with self.assertRaises(RuntimeError) as ctx:
            self.client.records.create("account", {"name": "test"})
        self.assertEqual(str(ctx.exception), "DataverseClient is closed")


class TestBackwardCompatibility(unittest.TestCase):
    """Tests that the client works without using context manager."""

    def setUp(self):
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.base_url = "https://example.crm.dynamics.com"

    def test_client_works_without_context_manager(self):
        """Client should function normally without 'with' statement."""
        client = DataverseClient(self.base_url, self.mock_credential)
        client._odata = MagicMock()
        client._odata._entity_set_from_schema_name.return_value = "accounts"
        client._odata._create.return_value = "guid-123"

        result = client.records.create("account", {"name": "Contoso"})
        self.assertEqual(result, "guid-123")

    def test_http_uses_requests_without_session(self):
        """Without context manager, _HttpClient should have no session."""
        client = DataverseClient(self.base_url, self.mock_credential)
        odata = client._get_odata()
        self.assertIsNone(odata._http._session)

    def test_close_available_without_context_manager(self):
        """close() should work even if context manager was never used."""
        client = DataverseClient(self.base_url, self.mock_credential)
        client._odata = MagicMock()
        client.close()
        self.assertTrue(client._closed)
        with self.assertRaises(RuntimeError):
            client.records.create("account", {"name": "test"})

    def test_flush_cache_delegates_to_odata(self):
        """flush_cache() calls _flush_cache on the OData client and returns its result."""
        client = DataverseClient(self.base_url, self.mock_credential)
        client._odata = MagicMock()
        client._odata._flush_cache.return_value = 3

        result = client.flush_cache("picklist")

        client._odata._flush_cache.assert_called_once_with("picklist")
        self.assertEqual(result, 3)


class TestExceptionHandling(unittest.TestCase):
    """Tests for exception handling during context manager usage."""

    def setUp(self):
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.base_url = "https://example.crm.dynamics.com"

    def test_close_called_even_on_exception(self):
        """close() should be called even when an exception occurs inside 'with'."""
        try:
            with DataverseClient(self.base_url, self.mock_credential) as client:
                raise ValueError("something went wrong")
        except ValueError:
            pass
        self.assertTrue(client._closed)
        self.assertIsNone(client._session)

    def test_exception_propagates(self):
        """Exceptions inside 'with' should propagate to the caller."""
        with self.assertRaises(TypeError):
            with DataverseClient(self.base_url, self.mock_credential) as client:
                raise TypeError("test")
