# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Unit tests for AsyncQueryOperations and AsyncQueryBuilder."""

import pytest
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

from azure.core.credentials_async import AsyncTokenCredential

from PowerPlatform.Dataverse.aio.async_client import AsyncDataverseClient
from PowerPlatform.Dataverse.aio.operations.async_query import AsyncQueryBuilder, AsyncQueryOperations
from PowerPlatform.Dataverse.models.record import Record

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_client_with_mock_odata():
    """Return (client, mock_od) with _scoped_odata patched."""
    credential = AsyncMock(spec=AsyncTokenCredential)
    client = AsyncDataverseClient("https://example.crm.dynamics.com", credential)
    od = AsyncMock()

    @asynccontextmanager
    async def _fake_scoped_odata():
        yield od

    client._scoped_odata = _fake_scoped_odata
    return client, od


# ---------------------------------------------------------------------------
# AsyncQueryOperations namespace
# ---------------------------------------------------------------------------


class TestAsyncQueryOperationsNamespace:
    """Tests that the query namespace and builder factory are correctly exposed."""

    def test_namespace_exists(self):
        """Verifies that client.query is an AsyncQueryOperations instance."""
        credential = AsyncMock(spec=AsyncTokenCredential)
        client = AsyncDataverseClient("https://example.crm.dynamics.com", credential)
        assert isinstance(client.query, AsyncQueryOperations)

    def test_builder_returns_async_query_builder(self):
        """client.query.builder() returns an AsyncQueryBuilder instance."""
        credential = AsyncMock(spec=AsyncTokenCredential)
        client = AsyncDataverseClient("https://example.crm.dynamics.com", credential)
        qb = client.query.builder("account")
        assert isinstance(qb, AsyncQueryBuilder)

    def test_builder_binds_query_ops(self):
        """The builder returned by builder() has _query_ops bound to client.query."""
        credential = AsyncMock(spec=AsyncTokenCredential)
        client = AsyncDataverseClient("https://example.crm.dynamics.com", credential)
        qb = client.query.builder("account")
        assert qb._query_ops is client.query


# ---------------------------------------------------------------------------
# AsyncQueryOperations.sql
# ---------------------------------------------------------------------------


class TestAsyncQuerySql:
    """Tests for AsyncQueryOperations.sql executing raw SQL queries."""

    async def test_sql_returns_list_of_records(self):
        """Returns a list of Record objects wrapping each row returned by the SQL query."""
        client, od = _make_client_with_mock_odata()
        od._query_sql.return_value = [
            {"accountid": "1", "name": "Contoso"},
            {"accountid": "2", "name": "Fabrikam"},
        ]

        result = await client.query.sql("SELECT TOP 2 accountid, name FROM account")

        od._query_sql.assert_awaited_once_with("SELECT TOP 2 accountid, name FROM account")
        assert len(result) == 2
        assert all(isinstance(r, Record) for r in result)
        assert result[0]["name"] == "Contoso"
        assert result[1]["name"] == "Fabrikam"

    async def test_sql_empty_result_returns_empty_list(self):
        """Returns an empty list when the SQL query yields no rows."""
        client, od = _make_client_with_mock_odata()
        od._query_sql.return_value = []

        result = await client.query.sql("SELECT TOP 0 name FROM account")

        assert result == []


# ---------------------------------------------------------------------------
# AsyncQueryBuilder — fluent builder
# ---------------------------------------------------------------------------


class TestAsyncQueryBuilderFluent:
    """Tests for the AsyncQueryBuilder fluent API and build() output."""

    def test_select_returns_builder(self):
        """select() returns the builder instance for fluent chaining."""
        credential = AsyncMock(spec=AsyncTokenCredential)
        client = AsyncDataverseClient("https://example.crm.dynamics.com", credential)
        qb = client.query.builder("account").select("name", "telephone1")
        assert isinstance(qb, AsyncQueryBuilder)

    def test_filter_eq_returns_builder(self):
        """filter_eq() returns the builder instance for fluent chaining."""
        credential = AsyncMock(spec=AsyncTokenCredential)
        client = AsyncDataverseClient("https://example.crm.dynamics.com", credential)
        qb = client.query.builder("account").select("name").filter_eq("statecode", 0)
        assert isinstance(qb, AsyncQueryBuilder)

    def test_top_returns_builder(self):
        """top() returns the builder instance for fluent chaining."""
        credential = AsyncMock(spec=AsyncTokenCredential)
        client = AsyncDataverseClient("https://example.crm.dynamics.com", credential)
        qb = client.query.builder("account").select("name").top(10)
        assert isinstance(qb, AsyncQueryBuilder)

    def test_build_produces_params_dict(self):
        """build() returns a dict containing table, select, and top keys with correct values."""
        credential = AsyncMock(spec=AsyncTokenCredential)
        client = AsyncDataverseClient("https://example.crm.dynamics.com", credential)
        params = client.query.builder("account").select("name", "telephone1").filter_eq("statecode", 0).top(50).build()
        assert params["table"] == "account"
        assert "name" in params["select"]
        assert params["top"] == 50

    @pytest.mark.asyncio
    async def test_execute_without_builder_raises(self):
        """execute() without _query_ops bound should raise RuntimeError."""
        qb = AsyncQueryBuilder("account")
        with pytest.raises(RuntimeError):
            await qb.execute()

    @pytest.mark.asyncio
    async def test_no_constraints_raises(self):
        """execute() with no select/filter/top raises ValueError."""
        credential = AsyncMock(spec=AsyncTokenCredential)
        client = AsyncDataverseClient("https://example.crm.dynamics.com", credential)
        qb = client.query.builder("account")
        with pytest.raises(ValueError):
            await qb.execute()


# ---------------------------------------------------------------------------
# AsyncQueryBuilder.execute — individual records
# ---------------------------------------------------------------------------


class TestAsyncQueryBuilderExecute:
    """Tests for AsyncQueryBuilder.execute() streaming records and pages."""

    async def test_execute_yields_individual_records(self):
        """execute() yields each Record individually across all pages."""
        client, od = _make_client_with_mock_odata()
        page_1 = [{"accountid": "1", "name": "A"}]
        page_2 = [{"accountid": "2", "name": "B"}]

        async def _mock_get_multiple(*args, **kwargs):
            for page in [page_1, page_2]:
                yield page

        od._get_multiple = _mock_get_multiple

        gen = await client.query.builder("account").select("name").filter_eq("statecode", 0).execute()

        records = [rec async for rec in gen]
        assert len(records) == 2
        assert isinstance(records[0], Record)
        assert records[0]["name"] == "A"
        assert records[1]["name"] == "B"

    async def test_execute_by_page_yields_pages(self):
        """execute(by_page=True) yields each page as a list of Records."""
        client, od = _make_client_with_mock_odata()
        page_1 = [{"accountid": "1", "name": "A"}]
        page_2 = [{"accountid": "2", "name": "B"}]

        async def _mock_get_multiple(*args, **kwargs):
            for page in [page_1, page_2]:
                yield page

        od._get_multiple = _mock_get_multiple

        pages_gen = await client.query.builder("account").select("name").filter_eq("statecode", 0).execute(by_page=True)

        pages = [page async for page in pages_gen]
        assert len(pages) == 2
        assert isinstance(pages[0][0], Record)


# ---------------------------------------------------------------------------
# AsyncQueryBuilder.to_dataframe
# ---------------------------------------------------------------------------


class TestAsyncQueryBuilderToDataframe:
    """Tests for AsyncQueryBuilder.to_dataframe() producing a pandas DataFrame."""

    async def test_to_dataframe_calls_dataframe_get(self):
        """to_dataframe() delegates to client.dataframe.get and returns the resulting DataFrame."""
        import pandas as pd

        credential = AsyncMock(spec=AsyncTokenCredential)
        client = AsyncDataverseClient("https://example.crm.dynamics.com", credential)
        expected_df = pd.DataFrame([{"name": "Contoso"}])

        # Mock client.dataframe.get to return our DataFrame
        client.dataframe = MagicMock()
        client.dataframe.get = AsyncMock(return_value=expected_df)

        result = await client.query.builder("account").select("name").top(10).to_dataframe()

        client.dataframe.get.assert_awaited_once()
        assert isinstance(result, pd.DataFrame)
        assert list(result["name"]) == ["Contoso"]

    async def test_to_dataframe_without_builder_raises(self):
        """to_dataframe() raises RuntimeError when _query_ops is not bound."""
        qb = AsyncQueryBuilder("account")
        with pytest.raises(RuntimeError):
            await qb.to_dataframe()
