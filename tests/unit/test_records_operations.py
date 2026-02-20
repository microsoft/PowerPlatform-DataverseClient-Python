# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import unittest
from unittest.mock import MagicMock

from azure.core.credentials import TokenCredential

from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.models.upsert import UpsertItem
from PowerPlatform.Dataverse.operations.records import RecordOperations


class TestRecordOperations(unittest.TestCase):
    """Unit tests for the client.records namespace (RecordOperations)."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.client = DataverseClient("https://example.crm.dynamics.com", self.mock_credential)
        self.client._odata = MagicMock()

    # ---------------------------------------------------------------- namespace

    def test_namespace_exists(self):
        """The client.records attribute should be a RecordOperations instance."""
        self.assertIsInstance(self.client.records, RecordOperations)

    # ------------------------------------------------------------------ create

    def test_create_single(self):
        """create() with a single dict should call _create and return a str."""
        self.client._odata._entity_set_from_schema_name.return_value = "accounts"
        self.client._odata._create.return_value = "guid-123"

        result = self.client.records.create("account", {"name": "Contoso Ltd"})

        self.client._odata._entity_set_from_schema_name.assert_called_once_with("account")
        self.client._odata._create.assert_called_once_with("accounts", "account", {"name": "Contoso Ltd"})
        self.assertIsInstance(result, str)
        self.assertEqual(result, "guid-123")

    def test_create_bulk(self):
        """create() with a list of dicts should call _create_multiple and return list[str]."""
        payloads = [{"name": "Company A"}, {"name": "Company B"}]
        self.client._odata._entity_set_from_schema_name.return_value = "accounts"
        self.client._odata._create_multiple.return_value = ["guid-1", "guid-2"]

        result = self.client.records.create("account", payloads)

        self.client._odata._create_multiple.assert_called_once_with("accounts", "account", payloads)
        self.assertIsInstance(result, list)
        self.assertEqual(result, ["guid-1", "guid-2"])

    def test_create_single_returns_string(self):
        """Single-record create must return a bare string, not a list."""
        self.client._odata._entity_set_from_schema_name.return_value = "contacts"
        self.client._odata._create.return_value = "single-guid"

        result = self.client.records.create("contact", {"firstname": "Jane"})

        self.assertIsInstance(result, str)
        self.assertNotIsInstance(result, list)
        self.assertEqual(result, "single-guid")

    # ------------------------------------------------------------------ update

    def test_update_single(self):
        """update() with a str id and dict changes should call _update."""
        self.client.records.update(
            "account",
            "00000000-0000-0000-0000-000000000000",
            {"telephone1": "555-0199"},
        )

        self.client._odata._update.assert_called_once_with(
            "account", "00000000-0000-0000-0000-000000000000", {"telephone1": "555-0199"}
        )

    def test_update_broadcast(self):
        """update() with list of ids and a single dict should call _update_by_ids (broadcast)."""
        ids = ["id-1", "id-2", "id-3"]
        changes = {"statecode": 1}

        self.client.records.update("account", ids, changes)

        self.client._odata._update_by_ids.assert_called_once_with("account", ids, changes)

    def test_update_paired(self):
        """update() with list of ids and list of dicts should call _update_by_ids (paired)."""
        ids = ["id-1", "id-2"]
        changes = [{"name": "Name A"}, {"name": "Name B"}]

        self.client.records.update("account", ids, changes)

        self.client._odata._update_by_ids.assert_called_once_with("account", ids, changes)

    # ------------------------------------------------------------------ delete

    def test_delete_single(self):
        """delete() with a str id should call _delete and return None."""
        result = self.client.records.delete("account", "guid-to-delete")

        self.client._odata._delete.assert_called_once_with("account", "guid-to-delete")
        self.assertIsNone(result)

    def test_delete_bulk(self):
        """delete() with a list of ids (default use_bulk_delete=True) should call _delete_multiple."""
        self.client._odata._delete_multiple.return_value = "job-guid-456"
        ids = ["id-1", "id-2", "id-3"]

        result = self.client.records.delete("account", ids)

        self.client._odata._delete_multiple.assert_called_once_with("account", ids)
        self.assertIsInstance(result, str)
        self.assertEqual(result, "job-guid-456")

    def test_delete_bulk_sequential(self):
        """delete() with use_bulk_delete=False should call _delete once per id."""
        ids = ["id-1", "id-2", "id-3"]

        result = self.client.records.delete("account", ids, use_bulk_delete=False)

        self.assertEqual(self.client._odata._delete.call_count, 3)
        self.client._odata._delete.assert_any_call("account", "id-1")
        self.client._odata._delete.assert_any_call("account", "id-2")
        self.client._odata._delete.assert_any_call("account", "id-3")
        self.client._odata._delete_multiple.assert_not_called()
        self.assertIsNone(result)

    def test_delete_empty_list(self):
        """delete() with an empty list should return None without calling _delete."""
        result = self.client.records.delete("account", [])

        self.client._odata._delete.assert_not_called()
        self.client._odata._delete_multiple.assert_not_called()
        self.assertIsNone(result)

    # --------------------------------------------------------------------- get

    def test_get_single(self):
        """get() with a record_id should call _get with correct params and return a dict."""
        expected = {"accountid": "guid-1", "name": "Contoso"}
        self.client._odata._get.return_value = expected

        result = self.client.records.get("account", "guid-1", select=["name", "telephone1"])

        self.client._odata._get.assert_called_once_with("account", "guid-1", select=["name", "telephone1"])
        self.assertIsInstance(result, dict)
        self.assertEqual(result, expected)

    # ------------------------------------------------------------------ upsert

    def test_upsert_single_calls_upsert(self):
        """upsert() with a single item (UpsertItem or dict) should call _upsert."""
        cases = [
            (
                "UpsertItem",
                UpsertItem(alternate_key={"accountnumber": "ACC-001"}, record={"name": "Contoso"}),
            ),
            (
                "dict",
                {"alternate_key": {"accountnumber": "ACC-001"}, "record": {"name": "Contoso"}},
            ),
        ]
        for label, item in cases:
            with self.subTest(input=label):
                self.client._odata._upsert.reset_mock()
                self.client._odata._upsert_multiple.reset_mock()
                self.client._odata._entity_set_from_schema_name.reset_mock()
                self.client._odata._entity_set_from_schema_name.return_value = "accounts"

                result = self.client.records.upsert("account", [item])

                self.client._odata._entity_set_from_schema_name.assert_called_once_with("account")
                self.client._odata._upsert.assert_called_once_with(
                    "accounts", "account", {"accountnumber": "ACC-001"}, {"name": "Contoso"}
                )
                self.client._odata._upsert_multiple.assert_not_called()
                self.assertIsNone(result)

    def test_upsert_multiple_calls_upsert_multiple(self):
        """upsert() with multiple items (UpsertItem or dict) should call _upsert_multiple."""
        expected_alt_keys = [
            {"accountnumber": "ACC-001", "address1_postalcode": "98052"},
            {"accountnumber": "ACC-002", "address1_postalcode": "10001"},
        ]
        expected_records = [{"name": "Contoso Ltd"}, {"name": "Fabrikam Inc"}]
        cases = [
            (
                "UpsertItem",
                [
                    UpsertItem(
                        alternate_key={
                            "accountnumber": "ACC-001",
                            "address1_postalcode": "98052",
                        },
                        record={"name": "Contoso Ltd"},
                    ),
                    UpsertItem(
                        alternate_key={
                            "accountnumber": "ACC-002",
                            "address1_postalcode": "10001",
                        },
                        record={"name": "Fabrikam Inc"},
                    ),
                ],
            ),
            (
                "dict",
                [
                    {
                        "alternate_key": {
                            "accountnumber": "ACC-001",
                            "address1_postalcode": "98052",
                        },
                        "record": {"name": "Contoso Ltd"},
                    },
                    {
                        "alternate_key": {
                            "accountnumber": "ACC-002",
                            "address1_postalcode": "10001",
                        },
                        "record": {"name": "Fabrikam Inc"},
                    },
                ],
            ),
        ]
        for label, items in cases:
            with self.subTest(input=label):
                self.client._odata._upsert.reset_mock()
                self.client._odata._upsert_multiple.reset_mock()
                self.client._odata._entity_set_from_schema_name.reset_mock()
                self.client._odata._entity_set_from_schema_name.return_value = "accounts"

                result = self.client.records.upsert("account", items)

                self.client._odata._entity_set_from_schema_name.assert_called_once_with("account")
                self.client._odata._upsert_multiple.assert_called_once_with(
                    "accounts", "account", expected_alt_keys, expected_records
                )
                self.client._odata._upsert.assert_not_called()
                self.assertIsNone(result)

    def test_upsert_returns_none(self):
        """upsert() should always return None."""
        self.client._odata._entity_set_from_schema_name.return_value = "contacts"
        item = UpsertItem(alternate_key={"externalid": "EXT-42"}, record={"firstname": "Jane"})

        result = self.client.records.upsert("contact", [item])

        self.assertIsNone(result)

    def test_upsert_empty_list_raises_type_error(self):
        """upsert() with an empty list should raise TypeError."""
        with self.assertRaises(TypeError):
            self.client.records.upsert("account", [])

    def test_upsert_non_list_raises_type_error(self):
        """upsert() with a non-list argument should raise TypeError."""
        item = UpsertItem(alternate_key={"accountnumber": "X"}, record={"name": "Y"})
        with self.assertRaises(TypeError):
            self.client.records.upsert("account", item)

    def test_upsert_invalid_item_type_raises_type_error(self):
        """upsert() with an item that is neither UpsertItem nor a valid dict should raise TypeError."""
        with self.assertRaises(TypeError):
            self.client.records.upsert("account", [42])

    def test_upsert_dict_missing_record_key_raises_type_error(self):
        """upsert() with a dict missing the 'record' key should raise TypeError."""
        with self.assertRaises(TypeError):
            self.client.records.upsert("account", [{"alternate_key": {"name": "acc1"}}])

    def test_upsert_composite_alternate_key(self):
        """upsert() with a composite alternate key including an int value routes correctly."""
        self.client._odata._entity_set_from_schema_name.return_value = "accounts"
        item = UpsertItem(
            alternate_key={"accountnumber": "ACC-001", "numberofemployees": 250},
            record={"name": "Contoso Ltd"},
        )

        self.client.records.upsert("account", [item])

        self.client._odata._upsert.assert_called_once_with(
            "accounts",
            "account",
            {"accountnumber": "ACC-001", "numberofemployees": 250},
            {"name": "Contoso Ltd"},
        )
        self.client._odata._upsert_multiple.assert_not_called()


if __name__ == "__main__":
    unittest.main()
