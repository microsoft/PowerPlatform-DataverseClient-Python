# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import warnings
import pytest
from unittest.mock import MagicMock

from PowerPlatform.Dataverse.aio.operations.async_records import AsyncRecordOperations
from PowerPlatform.Dataverse.core.errors import HttpError
from PowerPlatform.Dataverse.models.record import QueryResult, Record
from PowerPlatform.Dataverse.models.upsert import UpsertItem

# ---------------------------------------------------------------------------
# Async generator helpers used by list/list_pages tests
# ---------------------------------------------------------------------------


async def _agen(*pages):
    """Yield each argument as one page from an async generator."""
    for p in pages:
        yield p


class TestAsyncRecordOperationsNamespace:
    """Verify the namespace attribute type."""

    def test_namespace_type(self, async_client):
        assert isinstance(async_client.records, AsyncRecordOperations)


class TestAsyncRecordCreate:
    """Tests for AsyncRecordOperations.create."""

    async def test_create_single(self, async_client, mock_od):
        """create() with a single dict calls _entity_set_from_schema_name and _create."""
        mock_od._entity_set_from_schema_name.return_value = "accounts"
        mock_od._create.return_value = "guid-123"

        result = await async_client.records.create("account", {"name": "Contoso"})

        mock_od._entity_set_from_schema_name.assert_called_once_with("account")
        mock_od._create.assert_called_once_with("accounts", "account", {"name": "Contoso"})
        assert result == "guid-123"
        assert isinstance(result, str)

    async def test_create_bulk(self, async_client, mock_od):
        """create() with a list of dicts calls _create_multiple."""
        payloads = [{"name": "A"}, {"name": "B"}]
        mock_od._entity_set_from_schema_name.return_value = "accounts"
        mock_od._create_multiple.return_value = ["guid-1", "guid-2"]

        result = await async_client.records.create("account", payloads)

        mock_od._create_multiple.assert_called_once_with("accounts", "account", payloads)
        assert result == ["guid-1", "guid-2"]

    async def test_create_single_non_string_return_raises(self, async_client, mock_od):
        """create() raises TypeError if _create returns a non-string."""
        mock_od._entity_set_from_schema_name.return_value = "accounts"
        mock_od._create.return_value = 12345

        with pytest.raises(TypeError):
            await async_client.records.create("account", {"name": "X"})

    async def test_create_bulk_non_list_return_raises(self, async_client, mock_od):
        """create() raises TypeError if _create_multiple returns a non-list."""
        mock_od._entity_set_from_schema_name.return_value = "accounts"
        mock_od._create_multiple.return_value = "not-a-list"

        with pytest.raises(TypeError):
            await async_client.records.create("account", [{"name": "X"}])

    async def test_create_invalid_data_type_raises(self, async_client, mock_od):
        """create() raises TypeError if data is neither dict nor list."""
        mock_od._entity_set_from_schema_name.return_value = "accounts"
        with pytest.raises(TypeError):
            await async_client.records.create("account", "invalid")


class TestAsyncRecordUpdate:
    """Tests for AsyncRecordOperations.update."""

    async def test_update_single(self, async_client, mock_od):
        """update() with a str id and dict changes calls _update."""
        await async_client.records.update("account", "guid-1", {"telephone1": "555"})
        mock_od._update.assert_called_once_with("account", "guid-1", {"telephone1": "555"})

    async def test_update_broadcast(self, async_client, mock_od):
        """update() with list of ids and single dict calls _update_by_ids."""
        await async_client.records.update("account", ["id-1", "id-2"], {"statecode": 1})
        mock_od._update_by_ids.assert_called_once_with("account", ["id-1", "id-2"], {"statecode": 1})

    async def test_update_paired(self, async_client, mock_od):
        """update() with list of ids and list of dicts calls _update_by_ids."""
        await async_client.records.update("account", ["id-1", "id-2"], [{"name": "A"}, {"name": "B"}])
        mock_od._update_by_ids.assert_called_once_with("account", ["id-1", "id-2"], [{"name": "A"}, {"name": "B"}])

    async def test_update_single_non_dict_changes_raises(self, async_client, mock_od):
        """update() raises TypeError if ids is str but changes is not a dict."""
        with pytest.raises(TypeError):
            await async_client.records.update("account", "guid-1", ["not", "a", "dict"])

    async def test_update_invalid_ids_type_raises(self, async_client, mock_od):
        """update() raises TypeError if ids is neither str nor list."""
        with pytest.raises(TypeError):
            await async_client.records.update("account", 12345, {"name": "X"})

    async def test_update_returns_none(self, async_client, mock_od):
        """update() returns None."""
        result = await async_client.records.update("account", "guid-1", {"name": "X"})
        assert result is None


class TestAsyncRecordDelete:
    """Tests for AsyncRecordOperations.delete."""

    async def test_delete_single(self, async_client, mock_od):
        """delete() with a str id calls _delete and returns None."""
        result = await async_client.records.delete("account", "guid-to-delete")
        mock_od._delete.assert_called_once_with("account", "guid-to-delete")
        assert result is None

    async def test_delete_bulk(self, async_client, mock_od):
        """delete() with a list of ids uses _delete_multiple by default."""
        mock_od._delete_multiple.return_value = "job-guid-456"
        result = await async_client.records.delete("account", ["id-1", "id-2", "id-3"])
        mock_od._delete_multiple.assert_called_once_with("account", ["id-1", "id-2", "id-3"])
        assert result == "job-guid-456"

    async def test_delete_bulk_sequential(self, async_client, mock_od):
        """delete() with use_bulk_delete=False calls _delete once per id."""
        result = await async_client.records.delete("account", ["id-1", "id-2"], use_bulk_delete=False)
        assert mock_od._delete.call_count == 2
        mock_od._delete.assert_any_call("account", "id-1")
        mock_od._delete.assert_any_call("account", "id-2")
        mock_od._delete_multiple.assert_not_called()
        assert result is None

    async def test_delete_empty_list(self, async_client, mock_od):
        """delete() with an empty list returns None without calling _delete."""
        result = await async_client.records.delete("account", [])
        mock_od._delete.assert_not_called()
        mock_od._delete_multiple.assert_not_called()
        assert result is None

    async def test_delete_invalid_ids_type_raises(self, async_client, mock_od):
        """delete() raises TypeError if ids is neither str nor list."""
        with pytest.raises(TypeError):
            await async_client.records.delete("account", 12345)

    async def test_delete_list_with_non_string_guid_raises(self, async_client, mock_od):
        """delete() raises TypeError if the ids list contains non-string entries."""
        with pytest.raises(TypeError):
            await async_client.records.delete("account", ["valid-guid", 42])


class TestAsyncRecordUpsert:
    """Tests for AsyncRecordOperations.upsert."""

    async def test_upsert_single_upsert_item(self, async_client, mock_od):
        """upsert() with a single UpsertItem calls _upsert."""
        mock_od._entity_set_from_schema_name.return_value = "accounts"
        item = UpsertItem(alternate_key={"accountnumber": "ACC-001"}, record={"name": "Contoso"})

        result = await async_client.records.upsert("account", [item])

        mock_od._upsert.assert_called_once_with(
            "accounts", "account", {"accountnumber": "ACC-001"}, {"name": "Contoso"}
        )
        mock_od._upsert_multiple.assert_not_called()
        assert result is None

    async def test_upsert_single_dict(self, async_client, mock_od):
        """upsert() with a single dict item calls _upsert."""
        mock_od._entity_set_from_schema_name.return_value = "accounts"
        item = {"alternate_key": {"accountnumber": "ACC-001"}, "record": {"name": "Contoso"}}

        await async_client.records.upsert("account", [item])

        mock_od._upsert.assert_called_once_with(
            "accounts", "account", {"accountnumber": "ACC-001"}, {"name": "Contoso"}
        )

    async def test_upsert_multiple_calls_upsert_multiple(self, async_client, mock_od):
        """upsert() with multiple items calls _upsert_multiple."""
        mock_od._entity_set_from_schema_name.return_value = "accounts"
        items = [
            UpsertItem(alternate_key={"accountnumber": "A"}, record={"name": "Contoso"}),
            UpsertItem(alternate_key={"accountnumber": "B"}, record={"name": "Fabrikam"}),
        ]

        await async_client.records.upsert("account", items)

        mock_od._upsert_multiple.assert_called_once_with(
            "accounts",
            "account",
            [{"accountnumber": "A"}, {"accountnumber": "B"}],
            [{"name": "Contoso"}, {"name": "Fabrikam"}],
        )
        mock_od._upsert.assert_not_called()

    async def test_upsert_empty_list_raises(self, async_client, mock_od):
        """upsert() with an empty list raises TypeError."""
        with pytest.raises(TypeError):
            await async_client.records.upsert("account", [])

    async def test_upsert_non_list_raises(self, async_client, mock_od):
        """upsert() with a non-list argument raises TypeError."""
        item = UpsertItem(alternate_key={"accountnumber": "X"}, record={"name": "Y"})
        with pytest.raises(TypeError):
            await async_client.records.upsert("account", item)

    async def test_upsert_invalid_item_raises(self, async_client, mock_od):
        """upsert() with an item that is neither UpsertItem nor valid dict raises TypeError."""
        with pytest.raises(TypeError):
            await async_client.records.upsert("account", [42])

    async def test_upsert_dict_missing_record_key_raises(self, async_client, mock_od):
        """upsert() with a dict missing the 'record' key raises TypeError."""
        with pytest.raises(TypeError):
            await async_client.records.upsert("account", [{"alternate_key": {"name": "acc1"}}])


# ---------------------------------------------------------------------------
# retrieve()
# ---------------------------------------------------------------------------


class TestAsyncRecordRetrieve:
    """Tests for AsyncRecordOperations.retrieve()."""

    async def test_retrieve_returns_record(self, async_client, mock_od):
        """retrieve() returns a Record instance."""
        mock_od._get.return_value = {"accountid": "abc", "name": "Contoso"}
        result = await async_client.records.retrieve("account", "abc")
        assert isinstance(result, Record)
        assert result["name"] == "Contoso"

    async def test_retrieve_passes_select(self, async_client, mock_od):
        """retrieve() passes select= to _get."""
        mock_od._get.return_value = {"accountid": "abc", "name": "Contoso"}
        await async_client.records.retrieve("account", "abc", select=["name"])
        mock_od._get.assert_called_once_with("account", "abc", select=["name"], expand=None, include_annotations=None)

    async def test_retrieve_passes_expand(self, async_client, mock_od):
        """retrieve() passes expand= to _get."""
        mock_od._get.return_value = {
            "accountid": "abc",
            "primarycontactid": {"contactid": "cid", "fullname": "John Doe"},
        }
        result = await async_client.records.retrieve("account", "abc", expand=["primarycontactid"])
        mock_od._get.assert_called_once_with(
            "account", "abc", select=None, expand=["primarycontactid"], include_annotations=None
        )
        assert result["primarycontactid"]["fullname"] == "John Doe"

    async def test_retrieve_passes_select_and_expand(self, async_client, mock_od):
        """retrieve() passes both select= and expand= to _get."""
        mock_od._get.return_value = {"name": "Contoso", "primarycontactid": {"fullname": "John"}}
        await async_client.records.retrieve("account", "abc", select=["name"], expand=["primarycontactid"])
        mock_od._get.assert_called_once_with(
            "account", "abc", select=["name"], expand=["primarycontactid"], include_annotations=None
        )

    async def test_retrieve_passes_include_annotations(self, async_client, mock_od):
        """retrieve() passes include_annotations= to _get."""
        annotation = "OData.Community.Display.V1.FormattedValue"
        mock_od._get.return_value = {
            "accountid": "abc",
            "statuscode": 1,
            f"statuscode@{annotation}": "Active",
        }
        result = await async_client.records.retrieve("account", "abc", include_annotations=annotation)
        mock_od._get.assert_called_once_with("account", "abc", select=None, expand=None, include_annotations=annotation)
        assert result[f"statuscode@{annotation}"] == "Active"

    async def test_retrieve_no_deprecation_warning(self, async_client, mock_od):
        """retrieve() does not emit DeprecationWarning."""
        mock_od._get.return_value = {"accountid": "abc", "name": "Contoso"}
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            await async_client.records.retrieve("account", "abc")
        dep = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        assert dep == [], f"retrieve() must not emit DeprecationWarning: {dep}"

    async def test_retrieve_returns_none_on_404(self, async_client, mock_od):
        """retrieve() returns None when _get raises HttpError with status 404."""
        mock_od._get.side_effect = HttpError("Not Found", 404)
        result = await async_client.records.retrieve("account", "nonexistent")
        assert result is None

    async def test_retrieve_reraises_non_404(self, async_client, mock_od):
        """retrieve() re-raises HttpError for non-404 status codes."""
        mock_od._get.side_effect = HttpError("Server Error", 500)
        with pytest.raises(HttpError):
            await async_client.records.retrieve("account", "some-id")

    async def test_retrieve_reraises_non_http_errors(self, async_client, mock_od):
        """retrieve() re-raises non-HttpError exceptions unchanged."""
        mock_od._get.side_effect = ValueError("Bad input")
        with pytest.raises(ValueError):
            await async_client.records.retrieve("account", "some-id")

    async def test_retrieve_record_id_set(self, async_client, mock_od):
        """retrieve() sets record.id from the record_id argument."""
        mock_od._get.return_value = {"name": "Contoso"}
        record = await async_client.records.retrieve("account", "my-id")
        assert record.id == "my-id"

    async def test_retrieve_table_set(self, async_client, mock_od):
        """retrieve() sets record.table from the table argument."""
        mock_od._get.return_value = {"name": "Contoso"}
        record = await async_client.records.retrieve("account", "my-id")
        assert record.table == "account"


# ---------------------------------------------------------------------------
# list()
# ---------------------------------------------------------------------------


class TestAsyncRecordList:
    """Tests for AsyncRecordOperations.list()."""

    async def test_list_returns_query_result(self, async_client, mock_od):
        """list() returns a QueryResult."""
        mock_od._get_multiple = MagicMock(return_value=_agen())
        result = await async_client.records.list("account")
        assert isinstance(result, QueryResult)

    async def test_list_collects_all_pages(self, async_client, mock_od):
        """list() collects records from all pages into one QueryResult."""
        mock_od._get_multiple = MagicMock(
            return_value=_agen(
                [{"name": "A", "accountid": "1"}],
                [{"name": "B", "accountid": "2"}, {"name": "C", "accountid": "3"}],
            )
        )
        result = await async_client.records.list("account")
        assert len(result) == 3

    async def test_list_no_deprecation_warning(self, async_client, mock_od):
        """list() does not emit DeprecationWarning."""
        mock_od._get_multiple = MagicMock(return_value=_agen())
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            await async_client.records.list("account", filter="statecode eq 0")
        dep = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        assert dep == [], f"list() must not emit DeprecationWarning: {dep}"

    async def test_list_passes_string_filter(self, async_client, mock_od):
        """list() passes a string filter to _get_multiple."""
        mock_od._get_multiple = MagicMock(return_value=_agen())
        await async_client.records.list("account", filter="statecode eq 0")
        assert mock_od._get_multiple.call_args[1]["filter"] == "statecode eq 0"

    async def test_list_passes_filter_expression(self, async_client, mock_od):
        """list() converts a FilterExpression to string before passing to _get_multiple."""
        from PowerPlatform.Dataverse.models.filters import col

        mock_od._get_multiple = MagicMock(return_value=_agen())
        await async_client.records.list("account", filter=col("statecode") == 0)
        assert mock_od._get_multiple.call_args[1]["filter"] == "statecode eq 0"

    async def test_list_passes_select(self, async_client, mock_od):
        """list() passes select= to _get_multiple."""
        mock_od._get_multiple = MagicMock(return_value=_agen())
        await async_client.records.list("account", select=["name", "revenue"])
        assert mock_od._get_multiple.call_args[1]["select"] == ["name", "revenue"]

    async def test_list_passes_top(self, async_client, mock_od):
        """list() passes top= to _get_multiple."""
        mock_od._get_multiple = MagicMock(return_value=_agen())
        await async_client.records.list("account", top=50)
        assert mock_od._get_multiple.call_args[1]["top"] == 50

    async def test_list_none_filter_passes_none(self, async_client, mock_od):
        """list() passes filter=None to _get_multiple when no filter specified."""
        mock_od._get_multiple = MagicMock(return_value=_agen())
        await async_client.records.list("account")
        assert mock_od._get_multiple.call_args[1]["filter"] is None

    async def test_list_result_iterable(self, async_client, mock_od):
        """list() result is iterable and contains Record instances."""
        mock_od._get_multiple = MagicMock(return_value=_agen([{"name": "X", "accountid": "1"}]))
        result = await async_client.records.list("account")
        records = list(result)
        assert len(records) == 1
        assert records[0]["name"] == "X"

    async def test_list_result_to_dataframe(self, async_client, mock_od):
        """list() result can be converted to a DataFrame."""
        import pandas as pd

        mock_od._get_multiple = MagicMock(
            return_value=_agen([{"name": "A", "accountid": "1"}, {"name": "B", "accountid": "2"}])
        )
        df = (await async_client.records.list("account", select=["name"])).to_dataframe()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2

    async def test_list_passes_orderby(self, async_client, mock_od):
        """list() passes orderby= to _get_multiple."""
        mock_od._get_multiple = MagicMock(return_value=_agen())
        await async_client.records.list("account", orderby=["name asc"])
        assert mock_od._get_multiple.call_args[1]["orderby"] == ["name asc"]

    async def test_list_passes_expand(self, async_client, mock_od):
        """list() passes expand= to _get_multiple."""
        mock_od._get_multiple = MagicMock(return_value=_agen())
        await async_client.records.list("account", expand=["primarycontactid"])
        assert mock_od._get_multiple.call_args[1]["expand"] == ["primarycontactid"]

    async def test_list_passes_page_size(self, async_client, mock_od):
        """list() passes page_size= to _get_multiple."""
        mock_od._get_multiple = MagicMock(return_value=_agen())
        await async_client.records.list("account", page_size=200)
        assert mock_od._get_multiple.call_args[1]["page_size"] == 200

    async def test_list_passes_count(self, async_client, mock_od):
        """list() passes count=True to _get_multiple."""
        mock_od._get_multiple = MagicMock(return_value=_agen())
        await async_client.records.list("account", count=True)
        assert mock_od._get_multiple.call_args[1]["count"] is True

    async def test_list_passes_include_annotations(self, async_client, mock_od):
        """list() passes include_annotations= to _get_multiple."""
        annotation = "OData.Community.Display.V1.FormattedValue"
        mock_od._get_multiple = MagicMock(return_value=_agen())
        await async_client.records.list("account", include_annotations=annotation)
        assert mock_od._get_multiple.call_args[1]["include_annotations"] == annotation


# ---------------------------------------------------------------------------
# list_pages()
# ---------------------------------------------------------------------------


class TestAsyncRecordListPages:
    """Tests for AsyncRecordOperations.list_pages()."""

    async def test_list_pages_is_async_generator(self, async_client, mock_od):
        """list_pages() returns an async generator."""
        import inspect

        mock_od._get_multiple = MagicMock(return_value=_agen())
        result = async_client.records.list_pages("account")
        assert inspect.isasyncgen(result)

    async def test_list_pages_yields_query_result_per_page(self, async_client, mock_od):
        """list_pages() yields one QueryResult per HTTP page."""
        mock_od._get_multiple = MagicMock(
            return_value=_agen([{"name": "A", "accountid": "1"}], [{"name": "B", "accountid": "2"}])
        )
        pages = []
        async for page in async_client.records.list_pages("account"):
            pages.append(page)
        assert len(pages) == 2
        for page in pages:
            assert isinstance(page, QueryResult)

    async def test_list_pages_page_contents(self, async_client, mock_od):
        """list_pages() preserves per-page record counts."""
        mock_od._get_multiple = MagicMock(
            return_value=_agen(
                [{"name": "A", "accountid": "1"}],
                [{"name": "B", "accountid": "2"}, {"name": "C", "accountid": "3"}],
            )
        )
        pages = []
        async for page in async_client.records.list_pages("account"):
            pages.append(page)
        assert len(pages[0]) == 1
        assert len(pages[1]) == 2

    async def test_list_pages_no_deprecation_warning(self, async_client, mock_od):
        """list_pages() does not emit DeprecationWarning."""
        mock_od._get_multiple = MagicMock(return_value=_agen())
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            async for _ in async_client.records.list_pages("account", filter="statecode eq 0"):
                pass
        dep = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        assert dep == [], f"list_pages() must not emit DeprecationWarning: {dep}"

    async def test_list_pages_passes_filter(self, async_client, mock_od):
        """list_pages() passes filter= to _get_multiple."""
        mock_od._get_multiple = MagicMock(return_value=_agen())
        async for _ in async_client.records.list_pages("account", filter="statecode eq 0"):
            pass
        assert mock_od._get_multiple.call_args[1]["filter"] == "statecode eq 0"

    async def test_list_pages_passes_select(self, async_client, mock_od):
        """list_pages() passes select= to _get_multiple."""
        mock_od._get_multiple = MagicMock(return_value=_agen())
        async for _ in async_client.records.list_pages("account", select=["name"]):
            pass
        assert mock_od._get_multiple.call_args[1]["select"] == ["name"]

    async def test_list_pages_passes_top(self, async_client, mock_od):
        """list_pages() passes top= to _get_multiple."""
        mock_od._get_multiple = MagicMock(return_value=_agen())
        async for _ in async_client.records.list_pages("account", top=50):
            pass
        assert mock_od._get_multiple.call_args[1]["top"] == 50

    async def test_list_pages_passes_orderby(self, async_client, mock_od):
        """list_pages() passes orderby= to _get_multiple."""
        mock_od._get_multiple = MagicMock(return_value=_agen())
        async for _ in async_client.records.list_pages("account", orderby=["name asc"]):
            pass
        assert mock_od._get_multiple.call_args[1]["orderby"] == ["name asc"]

    async def test_list_pages_passes_expand(self, async_client, mock_od):
        """list_pages() passes expand= to _get_multiple."""
        mock_od._get_multiple = MagicMock(return_value=_agen())
        async for _ in async_client.records.list_pages("account", expand=["primarycontactid"]):
            pass
        assert mock_od._get_multiple.call_args[1]["expand"] == ["primarycontactid"]

    async def test_list_pages_passes_page_size(self, async_client, mock_od):
        """list_pages() passes page_size= to _get_multiple."""
        mock_od._get_multiple = MagicMock(return_value=_agen())
        async for _ in async_client.records.list_pages("account", page_size=200):
            pass
        assert mock_od._get_multiple.call_args[1]["page_size"] == 200

    async def test_list_pages_passes_count(self, async_client, mock_od):
        """list_pages() passes count=True to _get_multiple."""
        mock_od._get_multiple = MagicMock(return_value=_agen())
        async for _ in async_client.records.list_pages("account", count=True):
            pass
        assert mock_od._get_multiple.call_args[1]["count"] is True

    async def test_list_pages_passes_include_annotations(self, async_client, mock_od):
        """list_pages() passes include_annotations= to _get_multiple."""
        annotation = "OData.Community.Display.V1.FormattedValue"
        mock_od._get_multiple = MagicMock(return_value=_agen())
        async for _ in async_client.records.list_pages("account", include_annotations=annotation):
            pass
        assert mock_od._get_multiple.call_args[1]["include_annotations"] == annotation
