# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import unittest
from unittest.mock import MagicMock

from azure.core.credentials import TokenCredential

from PowerPlatform.Dataverse.client import DataverseClient
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

    def test_get_single_with_query_params_raises(self):
        """get() with record_id and query params should raise ValueError."""
        with self.assertRaises(ValueError):
            self.client.records.get("account", "guid-1", filter="statecode eq 0")

    def test_get_paginated(self):
        """get() without record_id should yield pages from _get_multiple."""
        page_1 = [{"accountid": "1", "name": "A"}]
        page_2 = [{"accountid": "2", "name": "B"}]
        self.client._odata._get_multiple.return_value = iter([page_1, page_2])

        pages = list(self.client.records.get("account"))

        self.assertEqual(len(pages), 2)
        self.assertEqual(pages[0], page_1)
        self.assertEqual(pages[1], page_2)

    def test_get_paginated_with_all_params(self):
        """get() without record_id should pass all query params to _get_multiple."""
        self.client._odata._get_multiple.return_value = iter([])

        list(
            self.client.records.get(
                "account",
                select=["name", "telephone1"],
                filter="statecode eq 0",
                orderby=["name asc", "createdon desc"],
                top=50,
                expand=["primarycontactid"],
                page_size=25,
            )
        )

        self.client._odata._get_multiple.assert_called_once_with(
            "account",
            select=["name", "telephone1"],
            filter="statecode eq 0",
            orderby=["name asc", "createdon desc"],
            top=50,
            expand=["primarycontactid"],
            page_size=25,
        )


if __name__ == "__main__":
    unittest.main()
