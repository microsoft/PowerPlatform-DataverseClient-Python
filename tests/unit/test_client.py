# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import unittest
from unittest.mock import MagicMock

from azure.core.credentials import TokenCredential

from PowerPlatform.Dataverse.client import DataverseClient


class TestDataverseClientConstruction(unittest.TestCase):
    """Tests for DataverseClient construction and lifecycle."""

    def test_empty_base_url_raises(self):
        """DataverseClient raises ValueError when base_url is empty."""
        mock_credential = MagicMock(spec=TokenCredential)
        with self.assertRaises(ValueError):
            DataverseClient("", mock_credential)

    def test_trailing_slash_stripped(self):
        """DataverseClient strips trailing slash from base_url."""
        mock_credential = MagicMock(spec=TokenCredential)
        client = DataverseClient("https://example.crm.dynamics.com/", mock_credential)
        self.assertEqual(client._base_url, "https://example.crm.dynamics.com")

    def test_namespace_attributes_present(self):
        """Client exposes records, query, tables, files, dataframe, batch namespaces."""
        mock_credential = MagicMock(spec=TokenCredential)
        client = DataverseClient("https://example.crm.dynamics.com", mock_credential)
        for attr in ("records", "query", "tables", "files", "dataframe", "batch"):
            self.assertTrue(hasattr(client, attr), f"Missing namespace: {attr}")


class TestDataverseClientRequiresHttps(unittest.TestCase):
    """``base_url`` must use HTTPS. A bare ``http://`` would put the OAuth bearer
    token on the wire in plaintext on the very first request -- by the time the
    user noticed, the credential would already be exposed. The check fires at
    construction so the leak is impossible.
    """

    def setUp(self):
        self.mock_credential = MagicMock(spec=TokenCredential)

    def test_bug_repro_http_url_is_rejected(self):
        """Exact bug-report repro: ``http://myorg.crmtest.dynamics.com/``."""
        with self.assertRaises(ValueError) as ctx:
            DataverseClient("http://myorg.crmtest.dynamics.com/", self.mock_credential)
        self.assertIn("HTTPS", str(ctx.exception))

    def test_http_url_error_quotes_the_offending_value(self):
        """The error must echo the bad URL so the user sees which input was
        wrong -- otherwise the message in a stack trace is decontextualized."""
        bad_url = "http://example.crm.dynamics.com"
        with self.assertRaises(ValueError) as ctx:
            DataverseClient(bad_url, self.mock_credential)
        self.assertIn(bad_url, str(ctx.exception))

    def test_https_url_accepted(self):
        """Positive control: ``https://`` constructs normally."""
        client = DataverseClient("https://example.crm.dynamics.com", self.mock_credential)
        self.assertEqual(client._base_url, "https://example.crm.dynamics.com")

    def test_scheme_check_is_case_insensitive(self):
        """``HTTPS://`` / ``Https://`` are valid HTTPS URLs per RFC 3986;
        rejecting them on case alone would be a footgun. The check must use
        case-insensitive scheme comparison."""
        for variant in ("HTTPS://example.crm.dynamics.com", "Https://example.crm.dynamics.com"):
            with self.subTest(url=variant):
                client = DataverseClient(variant, self.mock_credential)
                self.assertTrue(client._base_url.lower().startswith("https://"))

    def test_uppercase_http_is_rejected(self):
        """The case-insensitive comparison must still reject ``HTTP://`` --
        otherwise an attacker-controlled config string with mixed case would
        bypass the guard."""
        with self.assertRaises(ValueError):
            DataverseClient("HTTP://example.crm.dynamics.com", self.mock_credential)

    def test_no_scheme_is_rejected(self):
        """Bare hostname (no scheme) must be rejected -- it would otherwise
        be passed through to the request layer and produce a confusing
        ``InvalidSchema`` error downstream instead of a clear ValueError."""
        with self.assertRaises(ValueError):
            DataverseClient("example.crm.dynamics.com", self.mock_credential)

    def test_unrelated_scheme_is_rejected(self):
        """``ftp://`` / ``file://`` / ``ws://`` etc. must be rejected. The
        bug isn't only about ``http://`` -- any non-HTTPS scheme is unsafe."""
        for url in ("ftp://example.com", "file:///etc/passwd", "ws://example.com"):
            with self.subTest(url=url):
                with self.assertRaises(ValueError):
                    DataverseClient(url, self.mock_credential)

    def test_https_lookalike_scheme_is_rejected(self):
        """Defensive: schemes that *contain* ``https`` but aren't ``https://``
        (e.g. a typo like ``htttps://``, or a custom scheme like
        ``https-fake://``) must not slip through ``startswith("https://")``
        as written. This locks in the exact prefix the validator checks."""
        for url in ("htttps://example.com", "https-fake://example.com", "shttp://example.com"):
            with self.subTest(url=url):
                with self.assertRaises(ValueError):
                    DataverseClient(url, self.mock_credential)

    def test_https_check_runs_after_empty_check(self):
        """Empty/missing ``base_url`` should still surface the existing
        'base_url is required' message rather than the HTTPS one -- the
        user can't fix HTTPS on something they haven't typed yet."""
        with self.assertRaises(ValueError) as ctx:
            DataverseClient("", self.mock_credential)
        self.assertIn("base_url is required", str(ctx.exception))
        # And ``None`` should also surface the empty-required path, not HTTPS.
        with self.assertRaises(ValueError) as ctx2:
            DataverseClient(None, self.mock_credential)  # type: ignore[arg-type]
        self.assertIn("base_url is required", str(ctx2.exception))


class TestRemovedClientMethods(unittest.TestCase):
    """Verify all 12 deprecated flat methods were removed from DataverseClient in 1.0 GA.

    These methods previously delegated to namespace equivalents (records.*, query.*,
    tables.*, files.*). They were fully removed; callers must use the namespaces directly.
    """

    def setUp(self):
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.client = DataverseClient("https://example.crm.dynamics.com", self.mock_credential)

    def test_create_removed(self):
        with self.assertRaises(AttributeError):
            self.client.create("account", {"name": "Test"})

    def test_update_removed(self):
        with self.assertRaises(AttributeError):
            self.client.update("account", "guid-1", {"name": "Test"})

    def test_delete_removed(self):
        with self.assertRaises(AttributeError):
            self.client.delete("account", "guid-1")

    def test_get_removed(self):
        with self.assertRaises(AttributeError):
            self.client.get("account", "guid-1")

    def test_query_sql_removed(self):
        with self.assertRaises(AttributeError):
            self.client.query_sql("SELECT name FROM account")

    def test_get_table_info_removed(self):
        with self.assertRaises(AttributeError):
            self.client.get_table_info("account")

    def test_create_table_removed(self):
        with self.assertRaises(AttributeError):
            self.client.create_table("new_Test", {})

    def test_delete_table_removed(self):
        with self.assertRaises(AttributeError):
            self.client.delete_table("new_Test")

    def test_list_tables_removed(self):
        with self.assertRaises(AttributeError):
            self.client.list_tables()

    def test_create_columns_removed(self):
        with self.assertRaises(AttributeError):
            self.client.create_columns("account", {})

    def test_delete_columns_removed(self):
        with self.assertRaises(AttributeError):
            self.client.delete_columns("account", [])

    def test_upload_file_removed(self):
        with self.assertRaises(AttributeError):
            self.client.upload_file("account", "guid-1", "file_col", "/path/file.pdf")


class TestCreateLookupField(unittest.TestCase):
    """Tests for client.tables.create_lookup_field convenience method."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.base_url = "https://example.crm.dynamics.com"
        self.client = DataverseClient(self.base_url, self.mock_credential)

        # Mock create_one_to_many since create_lookup_field calls it
        self.client.tables.create_one_to_many_relationship = MagicMock(
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
        self.client.tables.create_lookup_field(
            referencing_table="new_order",
            lookup_field_name="new_AccountId",
            referenced_table="account",
        )

        # Verify create_one_to_many_relationship was called
        self.client.tables.create_one_to_many_relationship.assert_called_once()

        # Get the call arguments
        call_args = self.client.tables.create_one_to_many_relationship.call_args
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
        self.client.tables.create_lookup_field(
            referencing_table="new_order",
            lookup_field_name="new_AccountId",
            referenced_table="account",
            display_name="Parent Account",
        )

        call_args = self.client.tables.create_one_to_many_relationship.call_args
        lookup = call_args[0][0]

        # Verify display name is in the label
        label_dict = lookup.display_name.to_dict()
        self.assertEqual(label_dict["LocalizedLabels"][0]["Label"], "Parent Account")

    def test_lookup_with_default_display_name(self):
        """Test that display_name defaults to referenced table name."""
        self.client.tables.create_lookup_field(
            referencing_table="new_order",
            lookup_field_name="new_AccountId",
            referenced_table="account",
        )

        call_args = self.client.tables.create_one_to_many_relationship.call_args
        lookup = call_args[0][0]

        # Verify display name defaults to referenced table
        label_dict = lookup.display_name.to_dict()
        self.assertEqual(label_dict["LocalizedLabels"][0]["Label"], "account")

    def test_lookup_with_description(self):
        """Test that description is correctly set."""
        self.client.tables.create_lookup_field(
            referencing_table="new_order",
            lookup_field_name="new_AccountId",
            referenced_table="account",
            description="The customer account for this order",
        )

        call_args = self.client.tables.create_one_to_many_relationship.call_args
        lookup = call_args[0][0]

        # Verify description is set
        self.assertIsNotNone(lookup.description)
        desc_dict = lookup.description.to_dict()
        self.assertEqual(desc_dict["LocalizedLabels"][0]["Label"], "The customer account for this order")

    def test_lookup_required_true(self):
        """Test that required=True sets ApplicationRequired level."""
        self.client.tables.create_lookup_field(
            referencing_table="new_order",
            lookup_field_name="new_AccountId",
            referenced_table="account",
            required=True,
        )

        call_args = self.client.tables.create_one_to_many_relationship.call_args
        lookup = call_args[0][0]

        self.assertEqual(lookup.required_level, "ApplicationRequired")

    def test_lookup_required_false(self):
        """Test that required=False sets None level."""
        self.client.tables.create_lookup_field(
            referencing_table="new_order",
            lookup_field_name="new_AccountId",
            referenced_table="account",
            required=False,
        )

        call_args = self.client.tables.create_one_to_many_relationship.call_args
        lookup = call_args[0][0]

        self.assertEqual(lookup.required_level, "None")

    def test_cascade_delete_configuration(self):
        """Test that cascade_delete is correctly passed to relationship."""
        self.client.tables.create_lookup_field(
            referencing_table="new_order",
            lookup_field_name="new_AccountId",
            referenced_table="account",
            cascade_delete="Cascade",
        )

        call_args = self.client.tables.create_one_to_many_relationship.call_args
        relationship = call_args[0][1]

        cascade_dict = relationship.cascade_configuration.to_dict()
        self.assertEqual(cascade_dict["Delete"], "Cascade")

    def test_solution_passed(self):
        """Test that solution is passed through."""
        self.client.tables.create_lookup_field(
            referencing_table="new_order",
            lookup_field_name="new_AccountId",
            referenced_table="account",
            solution="MySolution",
        )

        call_args = self.client.tables.create_one_to_many_relationship.call_args
        solution = call_args.kwargs.get("solution")

        self.assertEqual(solution, "MySolution")

    def test_custom_language_code(self):
        """Test that custom language_code is used for labels."""
        self.client.tables.create_lookup_field(
            referencing_table="new_order",
            lookup_field_name="new_AccountId",
            referenced_table="account",
            display_name="Compte",
            language_code=1036,  # French
        )

        call_args = self.client.tables.create_one_to_many_relationship.call_args
        lookup = call_args[0][0]

        label_dict = lookup.display_name.to_dict()
        self.assertEqual(label_dict["LocalizedLabels"][0]["LanguageCode"], 1036)
        self.assertEqual(label_dict["LocalizedLabels"][0]["Label"], "Compte")

    def test_generated_relationship_name(self):
        """Test that relationship name is auto-generated correctly."""
        self.client.tables.create_lookup_field(
            referencing_table="new_order",
            lookup_field_name="new_AccountId",
            referenced_table="account",
        )

        call_args = self.client.tables.create_one_to_many_relationship.call_args
        relationship = call_args[0][1]

        # Should be: {referenced}_{referencing}_{lookup_field}
        self.assertEqual(relationship.schema_name, "account_new_order_new_AccountId")

    def test_referenced_attribute_auto_generated(self):
        """Test that referenced_attribute defaults to {table}id."""
        self.client.tables.create_lookup_field(
            referencing_table="new_order",
            lookup_field_name="new_AccountId",
            referenced_table="account",
        )

        call_args = self.client.tables.create_one_to_many_relationship.call_args
        relationship = call_args[0][1]

        self.assertEqual(relationship.referenced_attribute, "accountid")

    def test_mixed_case_table_names_lowered(self):
        """Test that mixed-case table names are auto-lowered to logical names.

        Only table names (entity logical names) are lowered.
        lookup_field_name is a SchemaName and keeps its original casing.
        """
        self.client.tables.create_lookup_field(
            referencing_table="new_SQLTask",
            lookup_field_name="new_TeamId",
            referenced_table="new_SQLTeam",
        )

        call_args = self.client.tables.create_one_to_many_relationship.call_args
        lookup = call_args[0][0]
        relationship = call_args[0][1]

        # Entity names must be lowercased (Dataverse logical names)
        self.assertEqual(relationship.referenced_entity, "new_sqlteam")
        self.assertEqual(relationship.referencing_entity, "new_sqltask")
        self.assertEqual(relationship.referenced_attribute, "new_sqlteamid")

        # Schema_name: table names lowered, lookup_field_name keeps casing
        self.assertEqual(relationship.schema_name, "new_sqlteam_new_sqltask_new_TeamId")

        # Display name defaults to original (un-lowered) referenced_table
        label_dict = lookup.display_name.to_dict()
        self.assertEqual(label_dict["LocalizedLabels"][0]["Label"], "new_SQLTeam")

    def test_returns_result(self):
        """Test that the method returns the result from create_one_to_many_relationship."""
        expected_result = {
            "relationship_id": "test-guid",
            "relationship_schema_name": "test_schema",
            "lookup_schema_name": "test_lookup",
        }
        self.client.tables.create_one_to_many_relationship.return_value = expected_result

        result = self.client.tables.create_lookup_field(
            referencing_table="new_order",
            lookup_field_name="new_AccountId",
            referenced_table="account",
        )

        self.assertEqual(result, expected_result)
