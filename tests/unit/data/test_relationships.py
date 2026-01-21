# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Tests for relationship metadata operations."""

import unittest
from unittest.mock import MagicMock, Mock

from PowerPlatform.Dataverse.data._relationships import _RelationshipOperationsMixin
from PowerPlatform.Dataverse.models.metadata import (
    LookupAttributeMetadata,
    OneToManyRelationshipMetadata,
    ManyToManyRelationshipMetadata,
    Label,
    LocalizedLabel,
)


class TestExtractIdFromHeader(unittest.TestCase):
    """Tests for _extract_id_from_header method."""

    def setUp(self):
        """Create a minimal mixin instance for testing."""
        self.mixin = _RelationshipOperationsMixin()

    def test_extract_id_from_standard_header(self):
        """Test extracting GUID from standard OData-EntityId header."""
        header = "https://example.crm.dynamics.com/api/data/v9.2/RelationshipDefinitions(12345678-1234-1234-1234-123456789abc)"
        result = self.mixin._extract_id_from_header(header)
        self.assertEqual(result, "12345678-1234-1234-1234-123456789abc")

    def test_extract_id_from_header_uppercase_guid(self):
        """Test extracting uppercase GUID."""
        header = "https://example.crm.dynamics.com/api/data/v9.2/RelationshipDefinitions(ABCDEF12-3456-7890-ABCD-EF1234567890)"
        result = self.mixin._extract_id_from_header(header)
        self.assertEqual(result, "ABCDEF12-3456-7890-ABCD-EF1234567890")

    def test_extract_id_from_none_header(self):
        """Test that None header returns None."""
        result = self.mixin._extract_id_from_header(None)
        self.assertIsNone(result)

    def test_extract_id_from_empty_header(self):
        """Test that empty header returns None."""
        result = self.mixin._extract_id_from_header("")
        self.assertIsNone(result)

    def test_extract_id_from_header_without_guid(self):
        """Test that header without GUID returns None."""
        header = "https://example.crm.dynamics.com/api/data/v9.2/RelationshipDefinitions"
        result = self.mixin._extract_id_from_header(header)
        self.assertIsNone(result)


class MockODataClient(_RelationshipOperationsMixin):
    """Mock client that inherits from mixin for integration testing."""

    def __init__(self, api_base: str):
        self.api = api_base
        self._mock_request = MagicMock()
        self._mock_headers = {"Authorization": "Bearer test-token"}

    def _headers(self):
        return self._mock_headers.copy()

    def _request(self, method, url, **kwargs):
        return self._mock_request(method, url, **kwargs)

    def _escape_odata_quotes(self, value: str) -> str:
        """Escape single quotes for OData filter values."""
        return value.replace("'", "''")


class TestCreateOneToManyRelationship(unittest.TestCase):
    """Tests for _create_one_to_many_relationship method."""

    def setUp(self):
        """Set up test fixtures."""
        self.client = MockODataClient("https://example.crm.dynamics.com/api/data/v9.2")

        # Create test metadata objects
        self.lookup = LookupAttributeMetadata(
            schema_name="new_AccountId",
            display_name=Label(localized_labels=[LocalizedLabel(label="Account", language_code=1033)]),
        )
        self.relationship = OneToManyRelationshipMetadata(
            schema_name="new_account_orders",
            referenced_entity="account",
            referencing_entity="new_order",
            referenced_attribute="accountid",
        )

    def test_create_relationship_url(self):
        """Test that correct URL is used."""
        mock_response = Mock()
        mock_response.headers = {
            "OData-EntityId": "https://example.crm.dynamics.com/api/data/v9.2/RelationshipDefinitions(12345678-1234-1234-1234-123456789abc)"
        }
        self.client._mock_request.return_value = mock_response

        self.client._create_one_to_many_relationship(self.lookup, self.relationship)

        # Verify URL
        call_args = self.client._mock_request.call_args
        self.assertEqual(call_args[0][0], "post")
        self.assertEqual(call_args[0][1], "https://example.crm.dynamics.com/api/data/v9.2/RelationshipDefinitions")

    def test_create_relationship_payload_includes_lookup(self):
        """Test that payload includes both relationship and lookup metadata."""
        mock_response = Mock()
        mock_response.headers = {
            "OData-EntityId": "https://example.crm.dynamics.com/api/data/v9.2/RelationshipDefinitions(12345678-1234-1234-1234-123456789abc)"
        }
        self.client._mock_request.return_value = mock_response

        self.client._create_one_to_many_relationship(self.lookup, self.relationship)

        # Verify payload
        call_args = self.client._mock_request.call_args
        payload = call_args[1]["json"]
        self.assertIn("@odata.type", payload)
        self.assertEqual(payload["@odata.type"], "Microsoft.Dynamics.CRM.OneToManyRelationshipMetadata")
        self.assertIn("Lookup", payload)
        self.assertEqual(payload["Lookup"]["SchemaName"], "new_AccountId")

    def test_create_relationship_with_solution(self):
        """Test that solution header is added when specified."""
        mock_response = Mock()
        mock_response.headers = {
            "OData-EntityId": "https://example.crm.dynamics.com/api/data/v9.2/RelationshipDefinitions(12345678-1234-1234-1234-123456789abc)"
        }
        self.client._mock_request.return_value = mock_response

        self.client._create_one_to_many_relationship(self.lookup, self.relationship, solution_unique_name="MySolution")

        # Verify solution header
        call_args = self.client._mock_request.call_args
        headers = call_args[1]["headers"]
        self.assertEqual(headers["MSCRM.SolutionUniqueName"], "MySolution")

    def test_create_relationship_returns_result(self):
        """Test that result dictionary is correctly populated."""
        mock_response = Mock()
        mock_response.headers = {
            "OData-EntityId": "https://example.crm.dynamics.com/api/data/v9.2/RelationshipDefinitions(12345678-1234-1234-1234-123456789abc)"
        }
        self.client._mock_request.return_value = mock_response

        result = self.client._create_one_to_many_relationship(self.lookup, self.relationship)

        self.assertEqual(result["relationship_id"], "12345678-1234-1234-1234-123456789abc")
        self.assertEqual(result["relationship_schema_name"], "new_account_orders")
        self.assertEqual(result["lookup_schema_name"], "new_AccountId")
        self.assertEqual(result["referenced_entity"], "account")
        self.assertEqual(result["referencing_entity"], "new_order")


class TestCreateManyToManyRelationship(unittest.TestCase):
    """Tests for _create_many_to_many_relationship method."""

    def setUp(self):
        """Set up test fixtures."""
        self.client = MockODataClient("https://example.crm.dynamics.com/api/data/v9.2")

        self.relationship = ManyToManyRelationshipMetadata(
            schema_name="new_account_contact",
            entity1_logical_name="account",
            entity2_logical_name="contact",
        )

    def test_create_m2m_relationship_url(self):
        """Test that correct URL is used."""
        mock_response = Mock()
        mock_response.headers = {
            "OData-EntityId": "https://example.crm.dynamics.com/api/data/v9.2/RelationshipDefinitions(abcd1234-abcd-1234-abcd-1234abcd5678)"
        }
        self.client._mock_request.return_value = mock_response

        self.client._create_many_to_many_relationship(self.relationship)

        call_args = self.client._mock_request.call_args
        self.assertEqual(call_args[0][0], "post")
        self.assertEqual(call_args[0][1], "https://example.crm.dynamics.com/api/data/v9.2/RelationshipDefinitions")

    def test_create_m2m_relationship_returns_result(self):
        """Test that result dictionary is correctly populated."""
        mock_response = Mock()
        mock_response.headers = {
            "OData-EntityId": "https://example.crm.dynamics.com/api/data/v9.2/RelationshipDefinitions(abcd1234-abcd-1234-abcd-1234abcd5678)"
        }
        self.client._mock_request.return_value = mock_response

        result = self.client._create_many_to_many_relationship(self.relationship)

        self.assertEqual(result["relationship_id"], "abcd1234-abcd-1234-abcd-1234abcd5678")
        self.assertEqual(result["relationship_schema_name"], "new_account_contact")
        self.assertEqual(result["entity1_logical_name"], "account")
        self.assertEqual(result["entity2_logical_name"], "contact")


class TestDeleteRelationship(unittest.TestCase):
    """Tests for _delete_relationship method."""

    def setUp(self):
        """Set up test fixtures."""
        self.client = MockODataClient("https://example.crm.dynamics.com/api/data/v9.2")

    def test_delete_relationship_url(self):
        """Test that correct URL is constructed."""
        mock_response = Mock()
        self.client._mock_request.return_value = mock_response

        self.client._delete_relationship("12345678-1234-1234-1234-123456789abc")

        call_args = self.client._mock_request.call_args
        self.assertEqual(call_args[0][0], "delete")
        self.assertEqual(
            call_args[0][1],
            "https://example.crm.dynamics.com/api/data/v9.2/RelationshipDefinitions(12345678-1234-1234-1234-123456789abc)",
        )

    def test_delete_relationship_has_if_match_header(self):
        """Test that If-Match header is set."""
        mock_response = Mock()
        self.client._mock_request.return_value = mock_response

        self.client._delete_relationship("12345678-1234-1234-1234-123456789abc")

        call_args = self.client._mock_request.call_args
        headers = call_args[1]["headers"]
        self.assertEqual(headers["If-Match"], "*")


class TestGetRelationship(unittest.TestCase):
    """Tests for _get_relationship method."""

    def setUp(self):
        """Set up test fixtures."""
        self.client = MockODataClient("https://example.crm.dynamics.com/api/data/v9.2")

    def test_get_relationship_url_and_filter(self):
        """Test that correct URL and filter are used."""
        mock_response = Mock()
        mock_response.json.return_value = {"value": []}
        self.client._mock_request.return_value = mock_response

        self.client._get_relationship("new_account_orders")

        call_args = self.client._mock_request.call_args
        self.assertEqual(call_args[0][0], "get")
        self.assertEqual(call_args[0][1], "https://example.crm.dynamics.com/api/data/v9.2/RelationshipDefinitions")
        self.assertEqual(call_args[1]["params"]["$filter"], "SchemaName eq 'new_account_orders'")

    def test_get_relationship_escapes_quotes(self):
        """Test that single quotes in schema name are escaped."""
        mock_response = Mock()
        mock_response.json.return_value = {"value": []}
        self.client._mock_request.return_value = mock_response

        # Schema names shouldn't have quotes, but test the escaping anyway
        self.client._get_relationship("schema'name")

        call_args = self.client._mock_request.call_args
        self.assertEqual(call_args[1]["params"]["$filter"], "SchemaName eq 'schema''name'")

    def test_get_relationship_returns_first_result(self):
        """Test that first result is returned when found."""
        mock_response = Mock()
        mock_response.json.return_value = {
            "value": [
                {"SchemaName": "new_account_orders", "MetadataId": "12345"},
                {"SchemaName": "other", "MetadataId": "67890"},
            ]
        }
        self.client._mock_request.return_value = mock_response

        result = self.client._get_relationship("new_account_orders")

        self.assertEqual(result["SchemaName"], "new_account_orders")
        self.assertEqual(result["MetadataId"], "12345")

    def test_get_relationship_returns_none_when_not_found(self):
        """Test that None is returned when not found."""
        mock_response = Mock()
        mock_response.json.return_value = {"value": []}
        self.client._mock_request.return_value = mock_response

        result = self.client._get_relationship("nonexistent")

        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
