# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import unittest
from unittest.mock import MagicMock

from azure.core.credentials import TokenCredential

from PowerPlatform.Dataverse.client import DataverseClient


class TestDataverseClient(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create mock credential
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.base_url = "https://example.crm.dynamics.com"

        # Initialize the client under test
        self.client = DataverseClient(self.base_url, self.mock_credential)

        # Mock the internal _odata client
        # This ensures we verify logic without making actual HTTP calls
        self.client._odata = MagicMock()

    def test_create_single(self):
        """Test create method with a single record."""
        # Setup mock return values
        # _create must return a GUID string
        self.client._odata._create.return_value = "00000000-0000-0000-0000-000000000000"
        # _entity_set_from_schema_name should return the plural entity set name
        self.client._odata._entity_set_from_schema_name.return_value = "accounts"

        # Execute test
        self.client.create("account", {"name": "Contoso Ltd"})

        # Verify
        # Ensure _entity_set_from_schema_name was called and its result ("accounts") was passed to _create
        self.client._odata._create.assert_called_once_with("accounts", "account", {"name": "Contoso Ltd"})

    def test_create_multiple(self):
        """Test create method with multiple records."""
        payloads = [{"name": "Company A"}, {"name": "Company B"}, {"name": "Company C"}]

        # Setup mock return values
        # _create_multiple must return a list of GUID strings
        self.client._odata._create_multiple.return_value = [
            "00000000-0000-0000-0000-000000000001",
            "00000000-0000-0000-0000-000000000002",
            "00000000-0000-0000-0000-000000000003",
        ]
        self.client._odata._entity_set_from_schema_name.return_value = "accounts"

        # Execute test
        self.client.create("account", payloads)

        # Verify
        self.client._odata._create_multiple.assert_called_once_with("accounts", "account", payloads)

    def test_update_single(self):
        """Test update method with a single record."""
        self.client.update("account", "00000000-0000-0000-0000-000000000000", {"telephone1": "555-0199"})
        self.client._odata._update.assert_called_once_with(
            "account", "00000000-0000-0000-0000-000000000000", {"telephone1": "555-0199"}
        )

    def test_update_multiple(self):
        """Test update method with multiple records (broadcast)."""
        ids = [
            "00000000-0000-0000-0000-000000000001",
            "00000000-0000-0000-0000-000000000002",
        ]
        changes = {"statecode": 1}

        self.client.update("account", ids, changes)
        self.client._odata._update_by_ids.assert_called_once_with("account", ids, changes)

    def test_delete_single(self):
        """Test delete method with a single record."""
        self.client.delete("account", "00000000-0000-0000-0000-000000000000")
        self.client._odata._delete.assert_called_once_with("account", "00000000-0000-0000-0000-000000000000")

    def test_delete_multiple(self):
        """Test delete method with multiple records."""
        ids = [
            "00000000-0000-0000-0000-000000000001",
            "00000000-0000-0000-0000-000000000002",
        ]
        # Mock return value for bulk delete job ID
        self.client._odata._delete_multiple.return_value = "job-guid-123"

        job_id = self.client.delete("account", ids)

        self.client._odata._delete_multiple.assert_called_once_with("account", ids)
        self.assertEqual(job_id, "job-guid-123")

    def test_get_single(self):
        """Test get method with a single record ID."""
        # Setup mock return value
        expected_record = {"accountid": "00000000-0000-0000-0000-000000000000", "name": "Contoso"}
        self.client._odata._get.return_value = expected_record

        result = self.client.get("account", "00000000-0000-0000-0000-000000000000")

        self.client._odata._get.assert_called_once_with("account", "00000000-0000-0000-0000-000000000000", select=None)
        self.assertEqual(result, expected_record)

    def test_get_multiple(self):
        """Test get method for querying multiple records."""
        # Setup mock return value (iterator)
        expected_batch = [{"accountid": "1", "name": "A"}, {"accountid": "2", "name": "B"}]
        self.client._odata._get_multiple.return_value = iter([expected_batch])

        # Execute query
        result_iterator = self.client.get("account", filter="statecode eq 0", top=10)

        # Consume iterator to verify content
        results = list(result_iterator)

        self.client._odata._get_multiple.assert_called_once_with(
            "account",
            select=None,
            filter="statecode eq 0",
            orderby=None,
            top=10,
            expand=None,
            page_size=None,
        )
        self.assertEqual(results, [expected_batch])


class TestCreateLookupField(unittest.TestCase):
    """Tests for DataverseClient.create_lookup_field convenience method."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.base_url = "https://example.crm.dynamics.com"
        self.client = DataverseClient(self.base_url, self.mock_credential)

        # Mock create_one_to_many_relationship since create_lookup_field calls it
        self.client.create_one_to_many_relationship = MagicMock(
            return_value={
                "relationship_id": "12345678-1234-1234-1234-123456789abc",
                "relationship_schema_name": "account_new_order_new_AccountId",
                "lookup_schema_name": "new_AccountId",
                "referenced_entity": "account",
                "referencing_entity": "new_order",
            }
        )

    def test_basic_lookup_field_creation(self):
        """Test basic lookup field creation with minimal parameters."""
        self.client.create_lookup_field(
            referencing_table="new_order",
            lookup_field_name="new_AccountId",
            referenced_table="account",
        )

        # Verify create_one_to_many_relationship was called
        self.client.create_one_to_many_relationship.assert_called_once()

        # Get the call arguments
        call_args = self.client.create_one_to_many_relationship.call_args
        lookup = call_args[0][0]
        relationship = call_args[0][1]
        solution = call_args.kwargs.get("solution")

        # Verify lookup metadata
        self.assertEqual(lookup.schema_name, "new_AccountId")
        self.assertEqual(lookup.required_level, "None")

        # Verify relationship metadata
        self.assertEqual(relationship.referenced_entity, "account")
        self.assertEqual(relationship.referencing_entity, "new_order")
        self.assertEqual(relationship.referenced_attribute, "accountid")

        # Verify no solution (keyword-only, defaults to None)
        self.assertIsNone(solution)

    def test_lookup_with_display_name(self):
        """Test that display_name is correctly set."""
        self.client.create_lookup_field(
            referencing_table="new_order",
            lookup_field_name="new_AccountId",
            referenced_table="account",
            display_name="Parent Account",
        )

        call_args = self.client.create_one_to_many_relationship.call_args
        lookup = call_args[0][0]

        # Verify display name is in the label
        label_dict = lookup.display_name.to_dict()
        self.assertEqual(label_dict["LocalizedLabels"][0]["Label"], "Parent Account")

    def test_lookup_with_default_display_name(self):
        """Test that display_name defaults to referenced table name."""
        self.client.create_lookup_field(
            referencing_table="new_order",
            lookup_field_name="new_AccountId",
            referenced_table="account",
        )

        call_args = self.client.create_one_to_many_relationship.call_args
        lookup = call_args[0][0]

        # Verify display name defaults to referenced table
        label_dict = lookup.display_name.to_dict()
        self.assertEqual(label_dict["LocalizedLabels"][0]["Label"], "account")

    def test_lookup_with_description(self):
        """Test that description is correctly set."""
        self.client.create_lookup_field(
            referencing_table="new_order",
            lookup_field_name="new_AccountId",
            referenced_table="account",
            description="The customer account for this order",
        )

        call_args = self.client.create_one_to_many_relationship.call_args
        lookup = call_args[0][0]

        # Verify description is set
        self.assertIsNotNone(lookup.description)
        desc_dict = lookup.description.to_dict()
        self.assertEqual(desc_dict["LocalizedLabels"][0]["Label"], "The customer account for this order")

    def test_lookup_required_true(self):
        """Test that required=True sets ApplicationRequired level."""
        self.client.create_lookup_field(
            referencing_table="new_order",
            lookup_field_name="new_AccountId",
            referenced_table="account",
            required=True,
        )

        call_args = self.client.create_one_to_many_relationship.call_args
        lookup = call_args[0][0]

        self.assertEqual(lookup.required_level, "ApplicationRequired")

    def test_lookup_required_false(self):
        """Test that required=False sets None level."""
        self.client.create_lookup_field(
            referencing_table="new_order",
            lookup_field_name="new_AccountId",
            referenced_table="account",
            required=False,
        )

        call_args = self.client.create_one_to_many_relationship.call_args
        lookup = call_args[0][0]

        self.assertEqual(lookup.required_level, "None")

    def test_cascade_delete_configuration(self):
        """Test that cascade_delete is correctly passed to relationship."""
        self.client.create_lookup_field(
            referencing_table="new_order",
            lookup_field_name="new_AccountId",
            referenced_table="account",
            cascade_delete="Cascade",
        )

        call_args = self.client.create_one_to_many_relationship.call_args
        relationship = call_args[0][1]

        cascade_dict = relationship.cascade_configuration.to_dict()
        self.assertEqual(cascade_dict["Delete"], "Cascade")

    def test_solution_passed(self):
        """Test that solution is passed through."""
        self.client.create_lookup_field(
            referencing_table="new_order",
            lookup_field_name="new_AccountId",
            referenced_table="account",
            solution="MySolution",
        )

        call_args = self.client.create_one_to_many_relationship.call_args
        solution = call_args.kwargs.get("solution")

        self.assertEqual(solution, "MySolution")

    def test_custom_language_code(self):
        """Test that custom language_code is used for labels."""
        self.client.create_lookup_field(
            referencing_table="new_order",
            lookup_field_name="new_AccountId",
            referenced_table="account",
            display_name="Compte",
            language_code=1036,  # French
        )

        call_args = self.client.create_one_to_many_relationship.call_args
        lookup = call_args[0][0]

        label_dict = lookup.display_name.to_dict()
        self.assertEqual(label_dict["LocalizedLabels"][0]["LanguageCode"], 1036)
        self.assertEqual(label_dict["LocalizedLabels"][0]["Label"], "Compte")

    def test_generated_relationship_name(self):
        """Test that relationship name is auto-generated correctly."""
        self.client.create_lookup_field(
            referencing_table="new_order",
            lookup_field_name="new_AccountId",
            referenced_table="account",
        )

        call_args = self.client.create_one_to_many_relationship.call_args
        relationship = call_args[0][1]

        # Should be: {referenced}_{referencing}_{lookup_field}
        self.assertEqual(relationship.schema_name, "account_new_order_new_AccountId")

    def test_referenced_attribute_auto_generated(self):
        """Test that referenced_attribute defaults to {table}id."""
        self.client.create_lookup_field(
            referencing_table="new_order",
            lookup_field_name="new_AccountId",
            referenced_table="account",
        )

        call_args = self.client.create_one_to_many_relationship.call_args
        relationship = call_args[0][1]

        self.assertEqual(relationship.referenced_attribute, "accountid")

    def test_returns_result(self):
        """Test that the method returns the result from create_one_to_many_relationship."""
        expected_result = {
            "relationship_id": "test-guid",
            "relationship_schema_name": "test_schema",
            "lookup_schema_name": "test_lookup",
        }
        self.client.create_one_to_many_relationship.return_value = expected_result

        result = self.client.create_lookup_field(
            referencing_table="new_order",
            lookup_field_name="new_AccountId",
            referenced_table="account",
        )

        self.assertEqual(result, expected_result)
