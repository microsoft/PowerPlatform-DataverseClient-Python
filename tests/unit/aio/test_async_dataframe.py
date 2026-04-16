# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Unit tests for AsyncDataFrameOperations (client.dataframe namespace)."""

import pandas as pd
import pytest
from unittest.mock import AsyncMock, MagicMock

from azure.core.credentials_async import AsyncTokenCredential

from PowerPlatform.Dataverse.aio.async_client import AsyncDataverseClient
from PowerPlatform.Dataverse.aio.operations.async_dataframe import AsyncDataFrameOperations
from PowerPlatform.Dataverse.models.record import Record

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_client_with_mock_records():
    """
    Return (client, mock_records).

    client.records is replaced with a MagicMock whose async methods can be
    configured per test without making any real HTTP calls.
    """
    credential = AsyncMock(spec=AsyncTokenCredential)
    client = AsyncDataverseClient("https://example.crm.dynamics.com", credential)
    client.records = MagicMock()
    client.records.get = AsyncMock()
    client.records.create = AsyncMock()
    client.records.update = AsyncMock()
    client.records.delete = AsyncMock()
    return client


async def _agen(*pages):
    """Async generator that yields each page in *pages*."""
    for page in pages:
        yield page


def _make_record(table: str, data: dict) -> Record:
    return Record.from_api_response(table, data)


# ---------------------------------------------------------------------------
# Namespace
# ---------------------------------------------------------------------------


class TestAsyncDataFrameOperationsNamespace:
    """Tests that the dataframe namespace is correctly exposed on the client."""

    def test_namespace_exists(self):
        """client.dataframe exposes an AsyncDataFrameOperations instance."""
        credential = AsyncMock(spec=AsyncTokenCredential)
        client = AsyncDataverseClient("https://example.crm.dynamics.com", credential)
        assert isinstance(client.dataframe, AsyncDataFrameOperations)


# ---------------------------------------------------------------------------
# get — single record
# ---------------------------------------------------------------------------


class TestAsyncDataFrameGetSingle:
    """Tests for dataframe.get() when called with a specific record_id (single-record path)."""

    async def test_get_single_returns_dataframe_with_one_row(self):
        """get() with a record_id returns a one-row DataFrame containing the record's fields."""
        client = _make_client_with_mock_records()
        record = _make_record("account", {"accountid": "guid-1", "name": "Contoso"})
        client.records.get = AsyncMock(return_value=record)

        result = await client.dataframe.get("account", "guid-1")

        client.records.get.assert_awaited_once_with("account", "guid-1", select=None)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.iloc[0]["name"] == "Contoso"

    async def test_get_single_passes_select(self):
        """get() forwards the select list to records.get when a record_id is provided."""
        client = _make_client_with_mock_records()
        record = _make_record("account", {"accountid": "guid-1", "name": "Contoso"})
        client.records.get = AsyncMock(return_value=record)

        await client.dataframe.get("account", "guid-1", select=["name"])

        client.records.get.assert_awaited_once_with("account", "guid-1", select=["name"])

    async def test_get_single_with_filter_raises(self):
        """get() raises ValueError when filter is combined with a record_id."""
        client = _make_client_with_mock_records()
        with pytest.raises(ValueError):
            await client.dataframe.get("account", "guid-1", filter="statecode eq 0")

    async def test_get_single_with_orderby_raises(self):
        """get() raises ValueError when orderby is combined with a record_id."""
        client = _make_client_with_mock_records()
        with pytest.raises(ValueError):
            await client.dataframe.get("account", "guid-1", orderby=["name asc"])

    async def test_get_single_with_top_raises(self):
        """get() raises ValueError when top is combined with a record_id."""
        client = _make_client_with_mock_records()
        with pytest.raises(ValueError):
            await client.dataframe.get("account", "guid-1", top=10)

    async def test_get_single_blank_record_id_raises(self):
        """get() raises ValueError when record_id is a blank string."""
        client = _make_client_with_mock_records()
        with pytest.raises(ValueError):
            await client.dataframe.get("account", "  ")

    async def test_get_single_empty_string_record_id_raises(self):
        """get() raises ValueError when record_id is an empty string."""
        client = _make_client_with_mock_records()
        with pytest.raises(ValueError):
            await client.dataframe.get("account", "")


# ---------------------------------------------------------------------------
# get — multi-page
# ---------------------------------------------------------------------------


class TestAsyncDataFrameGetMultiPage:
    """Tests for dataframe.get() without a record_id — collects paginated results into a DataFrame."""

    async def test_get_multipage_collects_all_rows(self):
        """get() without a record_id collects all pages into a single DataFrame."""
        client = _make_client_with_mock_records()
        page1 = [_make_record("account", {"accountid": "1", "name": "A"})]
        page2 = [_make_record("account", {"accountid": "2", "name": "B"})]
        client.records.get = AsyncMock(return_value=_agen(page1, page2))

        result = await client.dataframe.get("account")

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert list(result["name"]) == ["A", "B"]

    async def test_get_multipage_no_rows_returns_empty_dataframe(self):
        """get() with zero results returns an empty DataFrame."""
        client = _make_client_with_mock_records()
        client.records.get = AsyncMock(return_value=_agen())

        result = await client.dataframe.get("account")

        assert isinstance(result, pd.DataFrame)
        assert result.empty

    async def test_get_multipage_no_rows_with_select_returns_columns(self):
        """get() with select but zero results returns an empty DataFrame with the requested columns."""
        client = _make_client_with_mock_records()
        client.records.get = AsyncMock(return_value=_agen())

        result = await client.dataframe.get("account", select=["name"])

        assert isinstance(result, pd.DataFrame)
        assert result.empty
        assert list(result.columns) == ["name"]

    async def test_get_multipage_passes_kwargs(self):
        """get() without a record_id forwards all kwargs to records.get."""
        client = _make_client_with_mock_records()
        client.records.get = AsyncMock(return_value=_agen())

        await client.dataframe.get(
            "account",
            select=["name"],
            filter="statecode eq 0",
            orderby=["name asc"],
            top=50,
            expand=["primarycontactid"],
            page_size=25,
        )

        client.records.get.assert_awaited_once_with(
            "account",
            select=["name"],
            filter="statecode eq 0",
            orderby=["name asc"],
            top=50,
            expand=["primarycontactid"],
            page_size=25,
            count=False,
            include_annotations=None,
        )


# ---------------------------------------------------------------------------
# create
# ---------------------------------------------------------------------------


class TestAsyncDataFrameCreate:
    """Tests for dataframe.create() — DataFrame rows to Dataverse records."""

    async def test_create_returns_series_of_guids(self):
        """create() returns a Series of GUID strings matching the created records."""
        client = _make_client_with_mock_records()
        df = pd.DataFrame([{"name": "Contoso"}, {"name": "Fabrikam"}])
        client.records.create = AsyncMock(return_value=["guid-1", "guid-2"])

        result = await client.dataframe.create("account", df)

        client.records.create.assert_awaited_once()
        assert isinstance(result, pd.Series)
        assert list(result) == ["guid-1", "guid-2"]

    async def test_create_series_aligned_to_df_index(self):
        """The returned Series index matches the input DataFrame's index."""
        client = _make_client_with_mock_records()
        df = pd.DataFrame([{"name": "A"}, {"name": "B"}], index=[10, 20])
        client.records.create = AsyncMock(return_value=["g-1", "g-2"])

        result = await client.dataframe.create("account", df)

        assert list(result.index) == [10, 20]

    async def test_create_non_dataframe_raises_type_error(self):
        """create() raises TypeError when passed a non-DataFrame."""
        client = _make_client_with_mock_records()
        with pytest.raises(TypeError):
            await client.dataframe.create("account", [{"name": "Contoso"}])

    async def test_create_empty_dataframe_raises_value_error(self):
        """create() raises ValueError when passed an empty DataFrame."""
        client = _make_client_with_mock_records()
        with pytest.raises(ValueError):
            await client.dataframe.create("account", pd.DataFrame())

    async def test_create_all_null_rows_raises_value_error(self):
        """create() raises ValueError when all DataFrame rows contain only null values."""
        client = _make_client_with_mock_records()
        df = pd.DataFrame([{"name": None}, {"name": None}])
        with pytest.raises(ValueError):
            await client.dataframe.create("account", df)

    async def test_create_calls_records_create_with_list_of_dicts(self):
        """create() converts the DataFrame to a list of dicts before calling records.create."""
        client = _make_client_with_mock_records()
        df = pd.DataFrame([{"name": "Contoso"}])
        client.records.create = AsyncMock(return_value=["guid-1"])

        await client.dataframe.create("account", df)

        call_args = client.records.create.call_args
        assert call_args[0][0] == "account"
        assert isinstance(call_args[0][1], list)
        assert call_args[0][1][0]["name"] == "Contoso"


# ---------------------------------------------------------------------------
# update
# ---------------------------------------------------------------------------


class TestAsyncDataFrameUpdate:
    """Tests for dataframe.update() — DataFrame rows to Dataverse PATCH operations."""

    async def test_update_multiple_rows_passes_list(self):
        """update() with multiple rows passes parallel lists of IDs and change dicts."""
        client = _make_client_with_mock_records()
        df = pd.DataFrame(
            [
                {"accountid": "guid-1", "telephone1": "555-0100"},
                {"accountid": "guid-2", "telephone1": "555-0200"},
            ]
        )
        client.records.update = AsyncMock()

        await client.dataframe.update("account", df, id_column="accountid")

        client.records.update.assert_awaited_once_with(
            "account",
            ["guid-1", "guid-2"],
            [{"telephone1": "555-0100"}, {"telephone1": "555-0200"}],
        )

    async def test_update_single_row_passes_string_id(self):
        """update() with a single-row DataFrame passes a scalar ID and a single change dict."""
        client = _make_client_with_mock_records()
        df = pd.DataFrame([{"accountid": "guid-1", "telephone1": "555-0100"}])
        client.records.update = AsyncMock()

        await client.dataframe.update("account", df, id_column="accountid")

        client.records.update.assert_awaited_once_with(
            "account",
            "guid-1",
            {"telephone1": "555-0100"},
        )

    async def test_update_non_dataframe_raises_type_error(self):
        """update() raises TypeError when passed a non-DataFrame."""
        client = _make_client_with_mock_records()
        with pytest.raises(TypeError):
            await client.dataframe.update("account", [{"accountid": "g1", "name": "A"}], "accountid")

    async def test_update_empty_dataframe_raises_value_error(self):
        """update() raises ValueError when passed an empty DataFrame."""
        client = _make_client_with_mock_records()
        with pytest.raises(ValueError):
            await client.dataframe.update("account", pd.DataFrame(), "accountid")

    async def test_update_missing_id_column_raises_value_error(self):
        """update() raises ValueError when the id_column is absent from the DataFrame."""
        client = _make_client_with_mock_records()
        df = pd.DataFrame([{"name": "Contoso"}])
        with pytest.raises(ValueError):
            await client.dataframe.update("account", df, id_column="accountid")

    async def test_update_invalid_guid_values_raises_value_error(self):
        """update() raises ValueError when the id_column contains null values."""
        client = _make_client_with_mock_records()
        df = pd.DataFrame([{"accountid": None, "name": "Contoso"}])
        with pytest.raises(ValueError):
            await client.dataframe.update("account", df, id_column="accountid")

    async def test_update_only_id_column_raises_value_error(self):
        """update() raises ValueError when the DataFrame contains only the id_column with no payload fields."""
        client = _make_client_with_mock_records()
        df = pd.DataFrame([{"accountid": "guid-1"}])
        with pytest.raises(ValueError):
            await client.dataframe.update("account", df, id_column="accountid")


# ---------------------------------------------------------------------------
# delete
# ---------------------------------------------------------------------------


class TestAsyncDataFrameDelete:
    """Tests for dataframe.delete() — Series of IDs to Dataverse delete operations."""

    async def test_delete_multiple_calls_delete_with_list(self):
        """delete() with more than one ID calls records.delete with a list and bulk_delete=True."""
        client = _make_client_with_mock_records()
        ids = pd.Series(["guid-1", "guid-2", "guid-3"])
        client.records.delete = AsyncMock(return_value="job-guid-456")

        result = await client.dataframe.delete("account", ids)

        client.records.delete.assert_awaited_once_with("account", ["guid-1", "guid-2", "guid-3"], use_bulk_delete=True)

    async def test_delete_single_guid_uses_string_form(self):
        """delete() with exactly one ID calls records.delete with a scalar string."""
        client = _make_client_with_mock_records()
        ids = pd.Series(["guid-1"])
        client.records.delete = AsyncMock(return_value=None)

        result = await client.dataframe.delete("account", ids)

        client.records.delete.assert_awaited_once_with("account", "guid-1")
        assert result is None

    async def test_delete_empty_series_returns_none(self):
        """delete() with an empty Series makes no call and returns None."""
        client = _make_client_with_mock_records()
        ids = pd.Series([], dtype=str)

        result = await client.dataframe.delete("account", ids)

        client.records.delete.assert_not_awaited()
        assert result is None

    async def test_delete_non_series_raises_type_error(self):
        """delete() raises TypeError when passed a non-Series."""
        client = _make_client_with_mock_records()
        with pytest.raises(TypeError):
            await client.dataframe.delete("account", ["guid-1", "guid-2"])

    async def test_delete_series_with_bad_values_raises_value_error(self):
        """delete() raises ValueError when the Series contains null values."""
        client = _make_client_with_mock_records()
        ids = pd.Series(["guid-1", None])
        with pytest.raises(ValueError):
            await client.dataframe.delete("account", ids)

    async def test_delete_series_with_blank_string_raises_value_error(self):
        """delete() raises ValueError when the Series contains blank strings."""
        client = _make_client_with_mock_records()
        ids = pd.Series(["guid-1", "  "])
        with pytest.raises(ValueError):
            await client.dataframe.delete("account", ids)
