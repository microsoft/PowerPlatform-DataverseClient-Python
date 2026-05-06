# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Phase 3 GA regression tests.

Covers:
- records.get() deprecation (DeprecationWarning, still functional)
- records.retrieve() — single record, None on 404
- records.list() — QueryResult, accepts str/FilterExpression filter
- DataverseModel Protocol and isinstance() check
- records.create(DataverseModel) dispatch
- records.update(DataverseModel, record_id) dispatch
- DataverseModel exported from models and package root
- execute() emits zero DeprecationWarning (internal records.get() suppressed)
"""

import unittest
import warnings
from dataclasses import dataclass
from unittest.mock import MagicMock, patch

from azure.core.credentials import TokenCredential

from PowerPlatform.Dataverse.models.record import QueryResult, Record
from PowerPlatform.Dataverse.models.protocol import DataverseModel


def _make_client():
    cred = MagicMock(spec=TokenCredential)
    from PowerPlatform.Dataverse.client import DataverseClient

    client = DataverseClient("https://example.crm.dynamics.com", cred)
    client._odata = MagicMock()
    client._odata._get_multiple = MagicMock()
    client._odata._get_single = MagicMock()
    client._odata._get = MagicMock()
    client._odata._create = MagicMock()
    client._odata._create_multiple = MagicMock()
    client._odata._update = MagicMock()
    client._odata._update_by_ids = MagicMock()
    client._odata._entity_set_from_schema_name = MagicMock(side_effect=lambda t: t + "s")
    return client


# ---------------------------------------------------------------------------
# Sample DataverseModel implementation for tests


@dataclass
class _Account:
    __entity_logical_name__ = "account"
    __entity_set_name__ = "accounts"
    name: str = ""
    telephone1: str = ""

    def to_dict(self) -> dict:
        return {"name": self.name, "telephone1": self.telephone1}

    @classmethod
    def from_dict(cls, data: dict) -> "_Account":
        return cls(name=data.get("name", ""), telephone1=data.get("telephone1", ""))


# ---------------------------------------------------------------------------


class TestDataverseModelProtocol(unittest.TestCase):
    """DataverseModel Protocol structural checks."""

    def test_account_satisfies_protocol(self):
        self.assertIsInstance(_Account(), DataverseModel)

    def test_plain_dict_does_not_satisfy_protocol(self):
        self.assertNotIsInstance({"name": "X"}, DataverseModel)

    def test_missing_entity_logical_name_fails(self):
        class _Bad:
            __entity_set_name__ = "bads"

            def to_dict(self):
                return {}

            @classmethod
            def from_dict(cls, d):
                return cls()

        self.assertNotIsInstance(_Bad(), DataverseModel)

    def test_missing_to_dict_fails(self):
        class _Bad:
            __entity_logical_name__ = "bad"
            __entity_set_name__ = "bads"

            @classmethod
            def from_dict(cls, d):
                return cls()

        self.assertNotIsInstance(_Bad(), DataverseModel)

    def test_importable_from_models(self):
        from PowerPlatform.Dataverse.models import DataverseModel as dm

        self.assertIsNotNone(dm)

    def test_importable_from_package_root(self):
        from PowerPlatform.Dataverse import DataverseModel as dm

        self.assertIsNotNone(dm)

    def test_import_no_deprecation_warning(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            from PowerPlatform.Dataverse import DataverseModel  # noqa: F401
        dep = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertEqual(dep, [])


class TestRecordsGetDeprecation(unittest.TestCase):
    """records.get() fires DeprecationWarning, still functional."""

    def setUp(self):
        self.client = _make_client()

    def test_get_single_warns(self):
        self.client._odata._get.return_value = {"accountid": "1", "name": "Contoso"}
        with self.assertWarns(DeprecationWarning) as ctx:
            self.client.records.get("account", "guid-1")
        self.assertIn("retrieve", str(ctx.warning))

    def test_get_multiple_warns(self):
        self.client._odata._get_multiple.return_value = iter([])
        with self.assertWarns(DeprecationWarning) as ctx:
            list(self.client.records.get("account", filter="statecode eq 0"))
        self.assertIn("list", str(ctx.warning))

    def test_get_single_still_returns_record(self):
        self.client._odata._get.return_value = {"accountid": "1", "name": "Contoso"}
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            record = self.client.records.get("account", "guid-1")
        self.assertIsInstance(record, Record)
        self.assertEqual(record["name"], "Contoso")

    def test_get_multiple_still_returns_pages(self):
        self.client._odata._get_multiple.return_value = iter([[{"name": "A", "accountid": "1"}]])
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            pages = list(self.client.records.get("account", filter="statecode eq 0"))
        self.assertEqual(len(pages), 1)

    def test_get_warning_message_single_id(self):
        self.client._odata._get.return_value = {"accountid": "1"}
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            self.client.records.get("account", "guid-1")
        msgs = [str(w.message) for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertTrue(any("retrieve" in m for m in msgs))

    def test_get_warning_message_filter_form(self):
        self.client._odata._get_multiple.return_value = iter([])
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            list(self.client.records.get("account", filter="statecode eq 0"))
        msgs = [str(w.message) for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertTrue(any("list" in m for m in msgs))


class TestRecordsRetrieve(unittest.TestCase):
    """records.retrieve() returns Record or None, no warning."""

    def setUp(self):
        self.client = _make_client()

    def test_retrieve_returns_record(self):
        self.client._odata._get.return_value = {"accountid": "abc", "name": "Contoso"}
        record = self.client.records.retrieve("account", "abc")
        self.assertIsInstance(record, Record)
        self.assertEqual(record["name"], "Contoso")

    def test_retrieve_passes_select(self):
        self.client._odata._get.return_value = {"accountid": "abc", "name": "Contoso"}
        self.client.records.retrieve("account", "abc", select=["name"])
        self.client._odata._get.assert_called_once_with("account", "abc", select=["name"])

    def test_retrieve_no_deprecation_warning(self):
        self.client._odata._get.return_value = {"accountid": "abc", "name": "Contoso"}
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            self.client.records.retrieve("account", "abc")
        dep = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertEqual(dep, [], f"retrieve() must not emit DeprecationWarning: {dep}")

    def test_retrieve_returns_none_on_404(self):
        exc = Exception("Not Found")
        resp_mock = MagicMock()
        resp_mock.status_code = 404
        exc.response = resp_mock
        self.client._odata._get.side_effect = exc
        result = self.client.records.retrieve("account", "nonexistent-guid")
        self.assertIsNone(result)

    def test_retrieve_reraises_non_404(self):
        exc = Exception("Server Error")
        resp_mock = MagicMock()
        resp_mock.status_code = 500
        exc.response = resp_mock
        self.client._odata._get.side_effect = exc
        with self.assertRaises(Exception):
            self.client.records.retrieve("account", "some-guid")

    def test_retrieve_reraises_when_no_response_attr(self):
        self.client._odata._get.side_effect = ValueError("Bad input")
        with self.assertRaises(ValueError):
            self.client.records.retrieve("account", "some-guid")

    def test_retrieve_record_id_set(self):
        self.client._odata._get.return_value = {"name": "Contoso"}
        record = self.client.records.retrieve("account", "my-id")
        self.assertEqual(record.id, "my-id")

    def test_retrieve_table_set(self):
        self.client._odata._get.return_value = {"name": "Contoso"}
        record = self.client.records.retrieve("account", "my-id")
        self.assertEqual(record.table, "account")


class TestRecordsList(unittest.TestCase):
    """records.list() returns QueryResult, no warning."""

    def setUp(self):
        self.client = _make_client()

    def test_list_returns_query_result(self):
        self.client._odata._get_multiple.return_value = iter([])
        result = self.client.records.list("account")
        self.assertIsInstance(result, QueryResult)

    def test_list_collects_all_pages(self):
        self.client._odata._get_multiple.return_value = iter(
            [
                [{"name": "A", "accountid": "1"}],
                [{"name": "B", "accountid": "2"}, {"name": "C", "accountid": "3"}],
            ]
        )
        result = self.client.records.list("account")
        self.assertEqual(len(result), 3)

    def test_list_no_deprecation_warning(self):
        self.client._odata._get_multiple.return_value = iter([])
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            self.client.records.list("account", filter="statecode eq 0")
        dep = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertEqual(dep, [], f"list() must not emit DeprecationWarning: {dep}")

    def test_list_passes_string_filter(self):
        self.client._odata._get_multiple.return_value = iter([])
        self.client.records.list("account", filter="statecode eq 0")
        call_kwargs = self.client._odata._get_multiple.call_args[1]
        self.assertEqual(call_kwargs["filter"], "statecode eq 0")

    def test_list_passes_filter_expression(self):
        from PowerPlatform.Dataverse.models.filters import col

        self.client._odata._get_multiple.return_value = iter([])
        expr = col("statecode") == 0
        self.client.records.list("account", filter=expr)
        call_kwargs = self.client._odata._get_multiple.call_args[1]
        self.assertEqual(call_kwargs["filter"], "statecode eq 0")

    def test_list_passes_select(self):
        self.client._odata._get_multiple.return_value = iter([])
        self.client.records.list("account", select=["name", "revenue"])
        call_kwargs = self.client._odata._get_multiple.call_args[1]
        self.assertEqual(call_kwargs["select"], ["name", "revenue"])

    def test_list_passes_top(self):
        self.client._odata._get_multiple.return_value = iter([])
        self.client.records.list("account", top=50)
        call_kwargs = self.client._odata._get_multiple.call_args[1]
        self.assertEqual(call_kwargs["top"], 50)

    def test_list_none_filter_passes_none(self):
        self.client._odata._get_multiple.return_value = iter([])
        self.client.records.list("account")
        call_kwargs = self.client._odata._get_multiple.call_args[1]
        self.assertIsNone(call_kwargs["filter"])

    def test_list_result_iterable(self):
        self.client._odata._get_multiple.return_value = iter(
            [
                [{"name": "X", "accountid": "1"}],
            ]
        )
        result = self.client.records.list("account")
        records = list(result)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["name"], "X")

    def test_list_result_to_dataframe(self):
        import pandas as pd

        self.client._odata._get_multiple.return_value = iter(
            [
                [{"name": "A", "accountid": "1"}, {"name": "B", "accountid": "2"}],
            ]
        )
        df = self.client.records.list("account", select=["name"]).to_dataframe()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)


class TestCreateWithDataverseModel(unittest.TestCase):
    """records.create() accepts DataverseModel."""

    def setUp(self):
        self.client = _make_client()
        self.client._odata._create.return_value = "new-guid-123"
        self.client._odata._create_multiple.return_value = ["guid-1", "guid-2"]

    def test_create_single_entity(self):
        account = _Account(name="Contoso", telephone1="555-0100")
        result = self.client.records.create(account)
        self.assertEqual(result, "new-guid-123")

    def test_create_single_entity_uses_logical_name(self):
        account = _Account(name="Contoso")
        self.client.records.create(account)
        self.client._odata._entity_set_from_schema_name.assert_called_with("account")

    def test_create_single_entity_calls_to_dict(self):
        account = _Account(name="Contoso", telephone1="555-0100")
        self.client.records.create(account)
        self.client._odata._create.assert_called_once()
        call_args = self.client._odata._create.call_args
        self.assertEqual(call_args[0][2]["name"], "Contoso")

    def test_create_list_of_entities(self):
        entities = [_Account(name="A"), _Account(name="B")]
        result = self.client.records.create(entities)
        self.assertEqual(result, ["guid-1", "guid-2"])

    def test_create_list_uses_first_entity_logical_name(self):
        entities = [_Account(name="A"), _Account(name="B")]
        self.client.records.create(entities)
        self.client._odata._entity_set_from_schema_name.assert_called_with("account")

    def test_create_entity_no_deprecation_warning(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            self.client.records.create(_Account(name="Contoso"))
        dep = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertEqual(dep, [])

    def test_create_dict_path_still_works(self):
        result = self.client.records.create("account", {"name": "Contoso"})
        self.assertEqual(result, "new-guid-123")

    def test_create_list_dict_path_still_works(self):
        result = self.client.records.create("account", [{"name": "A"}, {"name": "B"}])
        self.assertEqual(result, ["guid-1", "guid-2"])


class TestUpdateWithDataverseModel(unittest.TestCase):
    """records.update() accepts DataverseModel as first arg."""

    def setUp(self):
        self.client = _make_client()

    def test_update_single_entity_with_id(self):
        account = _Account(name="Updated Name")
        self.client.records.update(account, "guid-abc")
        self.client._odata._update.assert_called_once_with(
            "account", "guid-abc", {"name": "Updated Name", "telephone1": ""}
        )

    def test_update_entity_no_id_raises(self):
        account = _Account(name="Updated")
        with self.assertRaises(TypeError):
            self.client.records.update(account)

    def test_update_entity_no_deprecation_warning(self):
        account = _Account(name="Updated")
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            self.client.records.update(account, "guid-abc")
        dep = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertEqual(dep, [])

    def test_update_dict_path_still_works(self):
        self.client.records.update("account", "guid-1", {"name": "New Name"})
        self.client._odata._update.assert_called_with("account", "guid-1", {"name": "New Name"})


class TestExecuteNoDeprecationFromRecordsGet(unittest.TestCase):
    """execute() suppresses DeprecationWarning from the internal records.get() call."""

    def setUp(self):
        self.client = _make_client()
        self.client._odata._get_multiple.return_value = iter([])

    def test_execute_flat_no_warning(self):
        from PowerPlatform.Dataverse.models.filters import col

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            self.client.query.builder("account").select("name").where(col("statecode") == 0).execute()
        dep = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertEqual(dep, [], f"execute() leaked DeprecationWarning: {dep}")

    def test_to_dataframe_no_records_get_warning(self):
        """to_dataframe() emits its own deprecation but must not leak records.get()'s warning."""
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            self.client.query.builder("account").select("name").to_dataframe()
        dep = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        # Exactly one DeprecationWarning: from QueryBuilder.to_dataframe() itself.
        # The internal records.get() deprecation must remain suppressed.
        self.assertEqual(len(dep), 1, f"Unexpected warnings: {dep}")
        self.assertIn("QueryBuilder.to_dataframe()", str(dep[0].message))


if __name__ == "__main__":
    unittest.main()
