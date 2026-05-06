# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Tests for _list_columns data-layer method."""

import unittest
from unittest.mock import MagicMock, Mock

from PowerPlatform.Dataverse.data._odata import _ODataClient
from PowerPlatform.Dataverse.core.errors import MetadataError


class MockODataClient:
    """Minimal stand-in for _ODataClient that exposes only what _list_columns needs."""

    def __init__(self, api_base: str):
        self.api = api_base
        self._mock_request = MagicMock()

    def _request(self, method, url, **kwargs):
        return self._mock_request(method, url, **kwargs)

    def _escape_odata_quotes(self, value: str) -> str:
        return value.replace("'", "''")

    # Delegate to the real implementation under test
    def _get_entity_by_table_schema_name(self, table_schema_name, headers=None):
        return self._mock_get_entity(table_schema_name)

    _mock_get_entity = None

    # Attach the real _list_columns from _ODataClient
    _list_columns = _ODataClient._list_columns


class TestListColumns(unittest.TestCase):
    """Tests for _ODataClient._list_columns."""

    def setUp(self):
        self.client = MockODataClient("https://example.crm.dynamics.com/api/data/v9.2")
        self.client._mock_get_entity = MagicMock(
            return_value={
                "MetadataId": "ent-guid-1",
                "LogicalName": "account",
                "SchemaName": "Account",
            }
        )

    # ------------------------------------------------ URL construction

    def test_uses_entity_metadata_id_in_url(self):
        """_list_columns() should build the URL with the entity MetadataId."""
        mock_response = Mock()
        mock_response.json.return_value = {"value": []}
        self.client._mock_request.return_value = mock_response

        self.client._list_columns("account")

        call_args = self.client._mock_request.call_args
        self.assertEqual(call_args[0][0], "get")
        self.assertIn("EntityDefinitions(ent-guid-1)/Attributes", call_args[0][1])

    def test_no_params_when_no_select_or_filter(self):
        """_list_columns() with no select/filter should send no $select or $filter."""
        mock_response = Mock()
        mock_response.json.return_value = {"value": []}
        self.client._mock_request.return_value = mock_response

        self.client._list_columns("account")

        call_args = self.client._mock_request.call_args
        params = call_args[1].get("params", {})
        self.assertNotIn("$select", params)
        self.assertNotIn("$filter", params)

    # ------------------------------------------------ $select param

    def test_select_param_is_joined(self):
        """_list_columns() should join select list into a comma-separated $select."""
        mock_response = Mock()
        mock_response.json.return_value = {"value": []}
        self.client._mock_request.return_value = mock_response

        self.client._list_columns("account", select=["LogicalName", "AttributeType"])

        call_args = self.client._mock_request.call_args
        params = call_args[1].get("params", {})
        self.assertEqual(params["$select"], "LogicalName,AttributeType")

    # ------------------------------------------------ $filter param

    def test_filter_param_is_passed_through(self):
        """_list_columns() should forward the filter string as $filter."""
        mock_response = Mock()
        mock_response.json.return_value = {"value": []}
        self.client._mock_request.return_value = mock_response

        self.client._list_columns("account", filter="AttributeType eq 'String'")

        call_args = self.client._mock_request.call_args
        params = call_args[1].get("params", {})
        self.assertEqual(params["$filter"], "AttributeType eq 'String'")

    # ------------------------------------------------ return value

    def test_returns_value_array(self):
        """_list_columns() should return the 'value' array from the response."""
        expected = [
            {"LogicalName": "name", "AttributeType": "String"},
            {"LogicalName": "accountid", "AttributeType": "Uniqueidentifier"},
        ]
        mock_response = Mock()
        mock_response.json.return_value = {"value": expected}
        self.client._mock_request.return_value = mock_response

        result = self.client._list_columns("account")

        self.assertEqual(result, expected)

    def test_returns_empty_list_when_no_value_key(self):
        """_list_columns() should return [] when response has no 'value' key."""
        mock_response = Mock()
        mock_response.json.return_value = {}
        self.client._mock_request.return_value = mock_response

        result = self.client._list_columns("account")

        self.assertEqual(result, [])

    # ------------------------------------------------ MetadataError

    def test_raises_metadata_error_when_table_not_found(self):
        """_list_columns() should raise MetadataError when entity is not found."""
        self.client._mock_get_entity.return_value = None

        with self.assertRaises(MetadataError):
            self.client._list_columns("nonexistent_table")

    def test_raises_metadata_error_when_entity_missing_metadata_id(self):
        """_list_columns() should raise MetadataError when MetadataId is absent."""
        self.client._mock_get_entity.return_value = {"LogicalName": "account"}

        with self.assertRaises(MetadataError):
            self.client._list_columns("account")


if __name__ == "__main__":
    unittest.main()
