# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import pytest
import pandas as pd
from contextlib import asynccontextmanager
from unittest.mock import MagicMock

from azure.core.credentials_async import AsyncTokenCredential

from PowerPlatform.Dataverse.aio.async_client import AsyncDataverseClient
from PowerPlatform.Dataverse.aio.operations.async_dataframe import AsyncDataFrameOperations


def _make_client_with_od(mock_od):
    cred = MagicMock(spec=AsyncTokenCredential)
    client = AsyncDataverseClient("https://example.crm.dynamics.com", cred)

    @asynccontextmanager
    async def _fake_scoped():
        yield mock_od

    client._scoped_odata = _fake_scoped
    return client


class TestAsyncDataFrameOperationsNamespace:
    def test_namespace_type(self, async_client):
        assert isinstance(async_client.dataframe, AsyncDataFrameOperations)


class TestAsyncDataFrameSql:
    async def test_sql_returns_dataframe(self, async_client, mock_od):
        """sql() executes a SQL query and returns a DataFrame."""
        mock_od._query_sql.return_value = [
            {"name": "Contoso", "accountid": "guid-1"},
            {"name": "Fabrikam", "accountid": "guid-2"},
        ]

        df = await async_client.dataframe.sql("SELECT name FROM account")

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert "name" in df.columns

    async def test_sql_empty_result_returns_empty_dataframe(self, async_client, mock_od):
        """sql() returns an empty DataFrame when no rows match."""
        mock_od._query_sql.return_value = []
        df = await async_client.dataframe.sql("SELECT name FROM account WHERE 1=0")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0


class TestAsyncDataFrameCreate:
    async def test_create_returns_series_of_guids(self, async_client, mock_od):
        """create() returns a Series of GUIDs aligned with the input DataFrame."""
        mock_od._entity_set_from_schema_name.return_value = "accounts"
        mock_od._create_multiple.return_value = ["guid-1", "guid-2"]

        df = pd.DataFrame([{"name": "Contoso"}, {"name": "Fabrikam"}])
        result = await async_client.dataframe.create("account", df)

        assert isinstance(result, pd.Series)
        assert list(result) == ["guid-1", "guid-2"]

    async def test_create_non_dataframe_raises(self, async_client, mock_od):
        """create() raises TypeError if records is not a DataFrame."""
        with pytest.raises(TypeError):
            await async_client.dataframe.create("account", [{"name": "X"}])

    async def test_create_empty_dataframe_raises(self, async_client, mock_od):
        """create() raises ValueError if records is empty."""
        with pytest.raises(ValueError):
            await async_client.dataframe.create("account", pd.DataFrame())

    async def test_create_all_null_row_raises(self, async_client, mock_od):
        """create() raises ValueError if any row has no non-null values."""
        mock_od._entity_set_from_schema_name.return_value = "accounts"
        df = pd.DataFrame([{"name": None}])
        with pytest.raises(ValueError, match="no non-null values"):
            await async_client.dataframe.create("account", df)

    async def test_create_id_count_mismatch_raises(self, async_client, mock_od):
        """create() raises ValueError if the server returns wrong number of IDs."""
        mock_od._entity_set_from_schema_name.return_value = "accounts"
        mock_od._create_multiple.return_value = ["guid-1"]  # 1 ID for 2 rows

        df = pd.DataFrame([{"name": "A"}, {"name": "B"}])
        with pytest.raises(ValueError, match="returned"):
            await async_client.dataframe.create("account", df)


class TestAsyncDataFrameUpdate:
    async def test_update_single_row(self, async_client, mock_od):
        """update() with a single-row DataFrame calls records.update once."""
        df = pd.DataFrame([{"accountid": "guid-1", "telephone1": "555"}])
        await async_client.dataframe.update("account", df, id_column="accountid")
        mock_od._update.assert_called_once_with("account", "guid-1", {"telephone1": "555"})

    async def test_update_multiple_rows(self, async_client, mock_od):
        """update() with multiple rows calls records.update with lists."""
        df = pd.DataFrame(
            [
                {"accountid": "guid-1", "telephone1": "555"},
                {"accountid": "guid-2", "telephone1": "666"},
            ]
        )
        await async_client.dataframe.update("account", df, id_column="accountid")
        mock_od._update_by_ids.assert_called_once_with(
            "account",
            ["guid-1", "guid-2"],
            [{"telephone1": "555"}, {"telephone1": "666"}],
        )

    async def test_update_non_dataframe_raises(self, async_client, mock_od):
        """update() raises TypeError if changes is not a DataFrame."""
        with pytest.raises(TypeError):
            await async_client.dataframe.update("account", [{}], id_column="id")

    async def test_update_empty_dataframe_raises(self, async_client, mock_od):
        """update() raises ValueError if changes is empty."""
        with pytest.raises(ValueError):
            await async_client.dataframe.update("account", pd.DataFrame(), id_column="id")

    async def test_update_missing_id_column_raises(self, async_client, mock_od):
        """update() raises ValueError if id_column is not in the DataFrame."""
        df = pd.DataFrame([{"name": "X"}])
        with pytest.raises(ValueError, match="id_column"):
            await async_client.dataframe.update("account", df, id_column="accountid")

    async def test_update_invalid_ids_raises(self, async_client, mock_od):
        """update() raises ValueError if id_column contains invalid (non-string) values."""
        df = pd.DataFrame([{"accountid": None, "name": "X"}])
        with pytest.raises(ValueError, match="invalid values"):
            await async_client.dataframe.update("account", df, id_column="accountid")

    async def test_update_no_change_columns_raises(self, async_client, mock_od):
        """update() raises ValueError if no columns exist besides id_column."""
        df = pd.DataFrame([{"accountid": "guid-1"}])
        with pytest.raises(ValueError, match="No columns to update"):
            await async_client.dataframe.update("account", df, id_column="accountid")

    async def test_update_all_null_rows_skipped(self, async_client, mock_od):
        """update() skips rows where all change values are NaN/None."""
        df = pd.DataFrame([{"accountid": "guid-1", "telephone1": None}])
        await async_client.dataframe.update("account", df, id_column="accountid")
        # All values are null -> no updates sent
        mock_od._update.assert_not_called()
        mock_od._update_by_ids.assert_not_called()


class TestAsyncDataFrameDelete:
    async def test_delete_single(self, async_client, mock_od):
        """delete() with a single-element Series calls records.delete once."""
        ids = pd.Series(["guid-1"])
        result = await async_client.dataframe.delete("account", ids)
        mock_od._delete.assert_called_once_with("account", "guid-1")
        assert result is None

    async def test_delete_multiple_bulk(self, async_client, mock_od):
        """delete() with multiple IDs and use_bulk_delete=True uses BulkDelete."""
        mock_od._delete_multiple.return_value = "job-guid"
        ids = pd.Series(["guid-1", "guid-2"])
        result = await async_client.dataframe.delete("account", ids)
        mock_od._delete_multiple.assert_called_once_with("account", ["guid-1", "guid-2"])
        assert result == "job-guid"

    async def test_delete_empty_series_returns_none(self, async_client, mock_od):
        """delete() with an empty Series returns None without calling _delete."""
        result = await async_client.dataframe.delete("account", pd.Series([], dtype=str))
        mock_od._delete.assert_not_called()
        assert result is None

    async def test_delete_non_series_raises(self, async_client, mock_od):
        """delete() raises TypeError if ids is not a pandas Series."""
        with pytest.raises(TypeError):
            await async_client.dataframe.delete("account", ["guid-1", "guid-2"])

    async def test_delete_invalid_ids_raises(self, async_client, mock_od):
        """delete() raises ValueError if any ID is not a non-empty string."""
        ids = pd.Series(["guid-1", None])
        with pytest.raises(ValueError, match="invalid values"):
            await async_client.dataframe.delete("account", ids)
