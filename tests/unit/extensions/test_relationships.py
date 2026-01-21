# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Tests for relationship extension helpers."""

import unittest
from unittest.mock import MagicMock, patch

from PowerPlatform.Dataverse.extensions.relationships import create_lookup_field
from PowerPlatform.Dataverse.models.metadata import (
    LookupAttributeMetadata,
    OneToManyRelationshipMetadata,
)


class TestCreateLookupField(unittest.TestCase):
    """Tests for create_lookup_field helper function."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_client = MagicMock()
        self.mock_client.create_one_to_many_relationship.return_value = {
            "relationship_id": "12345678-1234-1234-1234-123456789abc",
            "relationship_schema_name": "account_new_order_new_AccountId",
            "lookup_schema_name": "new_AccountId",
            "referenced_entity": "account",
            "referencing_entity": "new_order",
        }

    def test_basic_lookup_field_creation(self):
        """Test basic lookup field creation with minimal parameters."""
        result = create_lookup_field(
            self.mock_client,
            referencing_table="new_order",
            lookup_field_name="new_AccountId",
            referenced_table="account",
        )

        # Verify client method was called
        self.mock_client.create_one_to_many_relationship.assert_called_once()

        # Get the call arguments
        call_args = self.mock_client.create_one_to_many_relationship.call_args
        lookup, relationship, solution = call_args[0]

        # Verify lookup metadata
        self.assertIsInstance(lookup, LookupAttributeMetadata)
        self.assertEqual(lookup.schema_name, "new_AccountId")
        self.assertEqual(lookup.required_level, "None")

        # Verify relationship metadata
        self.assertIsInstance(relationship, OneToManyRelationshipMetadata)
        self.assertEqual(relationship.referenced_entity, "account")
        self.assertEqual(relationship.referencing_entity, "new_order")
        self.assertEqual(relationship.referenced_attribute, "accountid")

        # Verify no solution
        self.assertIsNone(solution)

    def test_lookup_with_display_name(self):
        """Test that display_name is correctly set."""
        create_lookup_field(
            self.mock_client,
            referencing_table="new_order",
            lookup_field_name="new_AccountId",
            referenced_table="account",
            display_name="Parent Account",
        )

        call_args = self.mock_client.create_one_to_many_relationship.call_args
        lookup = call_args[0][0]

        # Verify display name is in the label
        label_dict = lookup.display_name.to_dict()
        self.assertEqual(label_dict["LocalizedLabels"][0]["Label"], "Parent Account")

    def test_lookup_with_default_display_name(self):
        """Test that display_name defaults to referenced table name."""
        create_lookup_field(
            self.mock_client,
            referencing_table="new_order",
            lookup_field_name="new_AccountId",
            referenced_table="account",
        )

        call_args = self.mock_client.create_one_to_many_relationship.call_args
        lookup = call_args[0][0]

        # Verify display name defaults to referenced table
        label_dict = lookup.display_name.to_dict()
        self.assertEqual(label_dict["LocalizedLabels"][0]["Label"], "account")

    def test_lookup_with_description(self):
        """Test that description is correctly set."""
        create_lookup_field(
            self.mock_client,
            referencing_table="new_order",
            lookup_field_name="new_AccountId",
            referenced_table="account",
            description="The customer account for this order",
        )

        call_args = self.mock_client.create_one_to_many_relationship.call_args
        lookup = call_args[0][0]

        # Verify description is set
        self.assertIsNotNone(lookup.description)
        desc_dict = lookup.description.to_dict()
        self.assertEqual(desc_dict["LocalizedLabels"][0]["Label"], "The customer account for this order")

    def test_lookup_required_true(self):
        """Test that required=True sets ApplicationRequired level."""
        create_lookup_field(
            self.mock_client,
            referencing_table="new_order",
            lookup_field_name="new_AccountId",
            referenced_table="account",
            required=True,
        )

        call_args = self.mock_client.create_one_to_many_relationship.call_args
        lookup = call_args[0][0]

        self.assertEqual(lookup.required_level, "ApplicationRequired")

    def test_lookup_required_false(self):
        """Test that required=False sets None level."""
        create_lookup_field(
            self.mock_client,
            referencing_table="new_order",
            lookup_field_name="new_AccountId",
            referenced_table="account",
            required=False,
        )

        call_args = self.mock_client.create_one_to_many_relationship.call_args
        lookup = call_args[0][0]

        self.assertEqual(lookup.required_level, "None")

    def test_cascade_delete_configuration(self):
        """Test that cascade_delete is correctly passed to relationship."""
        create_lookup_field(
            self.mock_client,
            referencing_table="new_order",
            lookup_field_name="new_AccountId",
            referenced_table="account",
            cascade_delete="Cascade",
        )

        call_args = self.mock_client.create_one_to_many_relationship.call_args
        relationship = call_args[0][1]

        cascade_dict = relationship.cascade_configuration.to_dict()
        self.assertEqual(cascade_dict["Delete"], "Cascade")

    def test_solution_unique_name_passed(self):
        """Test that solution_unique_name is passed through."""
        create_lookup_field(
            self.mock_client,
            referencing_table="new_order",
            lookup_field_name="new_AccountId",
            referenced_table="account",
            solution_unique_name="MySolution",
        )

        call_args = self.mock_client.create_one_to_many_relationship.call_args
        solution = call_args[0][2]

        self.assertEqual(solution, "MySolution")

    def test_custom_language_code(self):
        """Test that custom language_code is used for labels."""
        create_lookup_field(
            self.mock_client,
            referencing_table="new_order",
            lookup_field_name="new_AccountId",
            referenced_table="account",
            display_name="Compte",
            language_code=1036,  # French
        )

        call_args = self.mock_client.create_one_to_many_relationship.call_args
        lookup = call_args[0][0]

        label_dict = lookup.display_name.to_dict()
        self.assertEqual(label_dict["LocalizedLabels"][0]["LanguageCode"], 1036)
        self.assertEqual(label_dict["LocalizedLabels"][0]["Label"], "Compte")

    def test_generated_relationship_name(self):
        """Test that relationship name is auto-generated correctly."""
        create_lookup_field(
            self.mock_client,
            referencing_table="new_order",
            lookup_field_name="new_AccountId",
            referenced_table="account",
        )

        call_args = self.mock_client.create_one_to_many_relationship.call_args
        relationship = call_args[0][1]

        # Should be: {referenced}_{referencing}_{lookup_field}
        self.assertEqual(relationship.schema_name, "account_new_order_new_AccountId")

    def test_referenced_attribute_auto_generated(self):
        """Test that referenced_attribute defaults to {table}id."""
        create_lookup_field(
            self.mock_client,
            referencing_table="new_order",
            lookup_field_name="new_AccountId",
            referenced_table="account",
        )

        call_args = self.mock_client.create_one_to_many_relationship.call_args
        relationship = call_args[0][1]

        self.assertEqual(relationship.referenced_attribute, "accountid")

    def test_returns_client_result(self):
        """Test that the function returns the client's result."""
        expected_result = {
            "relationship_id": "test-guid",
            "relationship_schema_name": "test_schema",
            "lookup_schema_name": "test_lookup",
        }
        self.mock_client.create_one_to_many_relationship.return_value = expected_result

        result = create_lookup_field(
            self.mock_client,
            referencing_table="new_order",
            lookup_field_name="new_AccountId",
            referenced_table="account",
        )

        self.assertEqual(result, expected_result)


if __name__ == "__main__":
    unittest.main()
