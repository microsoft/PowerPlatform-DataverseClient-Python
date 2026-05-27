# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import pytest
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

from azure.core.credentials_async import AsyncTokenCredential

from PowerPlatform.Dataverse.aio.async_client import AsyncDataverseClient
from PowerPlatform.Dataverse.aio.operations.async_query import AsyncQueryOperations
from PowerPlatform.Dataverse.aio.models.async_fetchxml_query import AsyncFetchXmlQuery
from PowerPlatform.Dataverse.aio.models.async_query_builder import AsyncQueryBuilder
from PowerPlatform.Dataverse.models.record import QueryResult, Record


def _make_async_client_with_od(mock_od):
    """Helper: create async client with mocked _scoped_odata."""
    cred = MagicMock(spec=AsyncTokenCredential)
    client = AsyncDataverseClient("https://example.crm.dynamics.com", cred)

    @asynccontextmanager
    async def _fake_scoped():
        yield mock_od

    client._scoped_odata = _fake_scoped
    return client


_SIMPLE_FETCHXML = '<fetch top="5"><entity name="account"><attribute name="name"/></entity></fetch>'


class TestAsyncQueryOperationsNamespace:
    def test_namespace_type(self, async_client):
        assert isinstance(async_client.query, AsyncQueryOperations)

    def test_builder_returns_async_query_builder(self, async_client):
        """builder() returns an AsyncQueryBuilder bound to this client."""
        qb = async_client.query.builder("account")
        assert isinstance(qb, AsyncQueryBuilder)
        assert qb._query_ops is async_client.query

    def test_fetchxml_returns_async_fetchxml_query(self, async_client):
        """fetchxml() returns an AsyncFetchXmlQuery for valid XML."""
        q = async_client.query.fetchxml(_SIMPLE_FETCHXML)
        assert isinstance(q, AsyncFetchXmlQuery)
        assert q._entity_name == "account"


class TestAsyncQueryBuilder:
    async def test_execute_returns_query_result(self, async_client, mock_od):
        """builder().execute() collects all pages into a QueryResult."""

        async def _pages(*args, **kwargs):
            yield [{"name": "Contoso", "accountid": "g1"}]
            yield [{"name": "Fabrikam", "accountid": "g2"}]

        mock_od._get_multiple = _pages

        result = await async_client.query.builder("account").select("name").execute()

        assert isinstance(result, QueryResult)
        assert len(result) == 2
        assert result[0]["name"] == "Contoso"
        assert result[1]["name"] == "Fabrikam"

    async def test_execute_pages_yields_per_page(self, async_client, mock_od):
        """builder().execute_pages() yields one QueryResult per page."""

        async def _pages(*args, **kwargs):
            yield [{"name": "A", "accountid": "g1"}]
            yield [{"name": "B", "accountid": "g2"}]

        mock_od._get_multiple = _pages

        pages = []
        async for page in async_client.query.builder("account").select("name").execute_pages():
            pages.append(page)

        assert len(pages) == 2
        assert pages[0][0]["name"] == "A"
        assert pages[1][0]["name"] == "B"

    async def test_execute_raises_without_scope(self, async_client):
        """execute() raises ValueError when no select/where/top/page_size is set."""
        with pytest.raises(ValueError, match="full-table scans"):
            await async_client.query.builder("account").execute()

    async def test_execute_raises_when_unbound(self):
        """execute() raises RuntimeError when builder was not created via client.query.builder()."""
        qb = AsyncQueryBuilder("account")
        qb.select("name")
        with pytest.raises(RuntimeError, match="client.query.builder"):
            await qb.execute()

    async def test_execute_pages_raises_without_scope(self, async_client):
        """execute_pages() raises ValueError when no scope constraint is set."""
        with pytest.raises(ValueError, match="full-table scans"):
            async for _ in async_client.query.builder("account").execute_pages():
                pass

    def test_chaining_methods_return_self(self, async_client):
        """All fluent methods return the same AsyncQueryBuilder instance."""
        from PowerPlatform.Dataverse.models.filters import col

        qb = async_client.query.builder("account")
        assert qb.select("name") is qb
        assert qb.where(col("statecode") == 0) is qb
        assert qb.order_by("name") is qb
        assert qb.top(10) is qb
        assert qb.page_size(5) is qb


class TestAsyncFetchXmlQueryFactory:
    def test_fetchxml_invalid_type_raises(self, async_client):
        """fetchxml() raises ValidationError when xml is not a string."""
        from PowerPlatform.Dataverse.core.errors import ValidationError

        with pytest.raises(ValidationError):
            async_client.query.fetchxml(123)

    def test_fetchxml_empty_raises(self, async_client):
        """fetchxml() raises ValidationError for empty string."""
        from PowerPlatform.Dataverse.core.errors import ValidationError

        with pytest.raises(ValidationError):
            async_client.query.fetchxml("   ")

    def test_fetchxml_malformed_raises(self, async_client):
        """fetchxml() raises ValidationError for malformed XML."""
        from PowerPlatform.Dataverse.core.errors import ValidationError

        with pytest.raises(ValidationError, match="not well-formed"):
            async_client.query.fetchxml("<fetch><entity name='account'>")

    def test_fetchxml_missing_entity_element_raises(self, async_client):
        """fetchxml() raises ValueError when <entity> element is absent."""
        with pytest.raises(ValueError, match="<entity>"):
            async_client.query.fetchxml("<fetch><filter/></fetch>")

    def test_fetchxml_missing_entity_name_raises(self, async_client):
        """fetchxml() raises ValueError when <entity> has no name attribute."""
        with pytest.raises(ValueError, match="name"):
            async_client.query.fetchxml("<fetch><entity><attribute name='x'/></entity></fetch>")


class TestAsyncFetchXmlQueryExecution:
    async def test_execute_returns_query_result(self, async_client, mock_od):
        """AsyncFetchXmlQuery.execute() collects all pages into a QueryResult."""
        mock_od._entity_set_from_schema_name = AsyncMock(return_value="accounts")

        resp = MagicMock()
        resp.json = MagicMock(
            return_value={
                "value": [{"name": "Contoso", "accountid": "g1"}],
                "@Microsoft.Dynamics.CRM.morerecords": False,
            }
        )
        mock_od._request = AsyncMock(return_value=resp)

        result = await async_client.query.fetchxml(_SIMPLE_FETCHXML).execute()

        assert isinstance(result, QueryResult)
        assert len(result) == 1
        assert result[0]["name"] == "Contoso"

    async def test_execute_pages_yields_pages(self, async_client, mock_od):
        """AsyncFetchXmlQuery.execute_pages() yields one QueryResult per page."""
        mock_od._entity_set_from_schema_name = AsyncMock(return_value="accounts")

        resp = MagicMock()
        resp.json = MagicMock(
            return_value={
                "value": [{"name": "Contoso", "accountid": "g1"}],
                "@Microsoft.Dynamics.CRM.morerecords": False,
            }
        )
        mock_od._request = AsyncMock(return_value=resp)

        pages = []
        async for page in async_client.query.fetchxml(_SIMPLE_FETCHXML).execute_pages():
            pages.append(page)

        assert len(pages) == 1
        assert pages[0][0]["name"] == "Contoso"


class TestAsyncQuerySql:
    async def test_sql_returns_records(self, async_client, mock_od):
        """sql() calls _query_sql and wraps results in Record objects."""
        mock_od._query_sql.return_value = [
            {"name": "Contoso", "accountid": "guid-1"},
            {"name": "Fabrikam", "accountid": "guid-2"},
        ]

        result = await async_client.query.sql("SELECT TOP 2 name FROM account")

        mock_od._query_sql.assert_called_once_with("SELECT TOP 2 name FROM account")
        assert len(result) == 2
        assert all(isinstance(r, Record) for r in result)
        assert result[0]["name"] == "Contoso"
        assert result[1]["name"] == "Fabrikam"

    async def test_sql_empty_result(self, async_client, mock_od):
        """sql() returns an empty list when no rows match."""
        mock_od._query_sql.return_value = []
        result = await async_client.query.sql("SELECT name FROM account WHERE name = 'X'")
        assert result == []


class TestAsyncQuerySqlColumns:
    async def test_sql_columns_filters_virtual_and_system(self, async_client, mock_od):
        """sql_columns() calls tables.list_columns and filters out virtual/system columns."""
        mock_od._list_columns.return_value = [
            {
                "LogicalName": "name",
                "AttributeType": "String",
                "IsPrimaryId": False,
                "IsPrimaryName": True,
                "DisplayName": {},
                "AttributeOf": None,
            },
            {
                "LogicalName": "accountid",
                "AttributeType": "Uniqueidentifier",
                "IsPrimaryId": True,
                "IsPrimaryName": False,
                "DisplayName": {},
                "AttributeOf": None,
            },
            {
                "LogicalName": "versionnumber",
                "AttributeType": "BigInt",
                "IsPrimaryId": False,
                "IsPrimaryName": False,
                "DisplayName": {},
                "AttributeOf": None,
            },
        ]

        cols = await async_client.query.sql_columns("account")

        # versionnumber is a system column — excluded by default
        names = [c["name"] for c in cols]
        assert "versionnumber" not in names
        assert "accountid" in names
        assert "name" in names

    async def test_sql_columns_include_system(self, async_client, mock_od):
        """sql_columns(include_system=True) includes system columns."""
        mock_od._list_columns.return_value = [
            {
                "LogicalName": "versionnumber",
                "AttributeType": "BigInt",
                "IsPrimaryId": False,
                "IsPrimaryName": False,
                "DisplayName": {},
                "AttributeOf": None,
            }
        ]

        cols = await async_client.query.sql_columns("account", include_system=True)
        assert any(c["name"] == "versionnumber" for c in cols)

    async def test_sql_columns_excludes_attribute_of(self, async_client, mock_od):
        """sql_columns() excludes columns where AttributeOf is set."""
        mock_od._list_columns.return_value = [
            {
                "LogicalName": "parentcustomeridname",
                "AttributeType": "String",
                "IsPrimaryId": False,
                "IsPrimaryName": False,
                "DisplayName": {},
                "AttributeOf": "parentcustomerid",
            }
        ]

        cols = await async_client.query.sql_columns("contact")
        assert cols == []

    async def test_sql_columns_skips_empty_logical_name(self, async_client, mock_od):
        """sql_columns() skips columns where LogicalName is empty."""
        mock_od._list_columns.return_value = [
            {
                "LogicalName": "",
                "AttributeType": "String",
                "IsPrimaryId": False,
                "IsPrimaryName": False,
                "DisplayName": {},
                "AttributeOf": None,
            },
            {
                "LogicalName": "name",
                "AttributeType": "String",
                "IsPrimaryId": False,
                "IsPrimaryName": True,
                "DisplayName": {},
                "AttributeOf": None,
            },
        ]
        cols = await async_client.query.sql_columns("account")
        names = [c["name"] for c in cols]
        assert "" not in names
        assert "name" in names

    async def test_sql_columns_extracts_display_label(self, async_client, mock_od):
        """sql_columns() extracts label from UserLocalizedLabel when present."""
        mock_od._list_columns.return_value = [
            {
                "LogicalName": "name",
                "AttributeType": "String",
                "IsPrimaryId": False,
                "IsPrimaryName": True,
                "DisplayName": {"UserLocalizedLabel": {"Label": "Account Name", "LanguageCode": 1033}},
                "AttributeOf": None,
            },
        ]
        cols = await async_client.query.sql_columns("account")
        assert len(cols) == 1
        assert cols[0]["label"] == "Account Name"


class TestAsyncQueryOdataExpands:
    async def test_odata_expands_returns_nav_properties(self, async_client, mock_od):
        """odata_expands() returns navigation property metadata."""
        mock_od._list_table_relationships.return_value = [
            {
                "ReferencingEntity": "contact",
                "ReferencingEntityNavigationPropertyName": "parentcustomerid_account",
                "ReferencedEntity": "account",
                "ReferencingAttribute": "parentcustomerid",
                "SchemaName": "contact_customer_accounts",
            }
        ]
        mock_od._entity_set_from_schema_name.return_value = "accounts"

        result = await async_client.query.odata_expands("contact")

        assert len(result) == 1
        assert result[0]["nav_property"] == "parentcustomerid_account"
        assert result[0]["target_table"] == "account"

    async def test_odata_expands_filters_non_referencing(self, async_client, mock_od):
        """odata_expands() skips relationships where ReferencingEntity != table."""
        mock_od._list_table_relationships.return_value = [
            {
                "ReferencingEntity": "account",  # not "contact"
                "ReferencingEntityNavigationPropertyName": "ownerid_systemuser",
                "ReferencedEntity": "systemuser",
                "ReferencingAttribute": "ownerid",
                "SchemaName": "account_owner_rel",
            }
        ]
        mock_od._entity_set_from_schema_name.return_value = "systemusers"

        result = await async_client.query.odata_expands("contact")
        assert result == []

    async def test_odata_expands_skips_empty_nav_prop(self, async_client, mock_od):
        """odata_expands() skips relationships with empty nav_prop or target."""
        mock_od._list_table_relationships.return_value = [
            {
                "ReferencingEntity": "contact",
                "ReferencingEntityNavigationPropertyName": "",  # empty nav prop
                "ReferencedEntity": "account",
                "ReferencingAttribute": "parentcustomerid",
                "SchemaName": "contact_customer_accounts",
            }
        ]
        mock_od._entity_set_from_schema_name.return_value = "accounts"

        result = await async_client.query.odata_expands("contact")
        assert result == []

    async def test_odata_expands_handles_entity_set_resolution_failure(self, async_client, mock_od):
        """odata_expands() sets target_entity_set to '' when resolution raises."""
        from PowerPlatform.Dataverse.core.errors import MetadataError

        mock_od._list_table_relationships.return_value = [
            {
                "ReferencingEntity": "contact",
                "ReferencingEntityNavigationPropertyName": "parentcustomerid_account",
                "ReferencedEntity": "account",
                "ReferencingAttribute": "parentcustomerid",
                "SchemaName": "contact_customer_accounts",
            }
        ]
        mock_od._entity_set_from_schema_name.side_effect = MetadataError("not found")

        result = await async_client.query.odata_expands("contact")

        assert len(result) == 1
        assert result[0]["target_entity_set"] == ""


class TestAsyncFetchXmlQueryFactoryUrlLength:
    def test_fetchxml_url_too_long_raises(self, async_client):
        """fetchxml() raises ValidationError when encoded XML exceeds the URL length limit."""
        from PowerPlatform.Dataverse.core.errors import ValidationError

        # Build XML long enough to exceed _MAX_URL_LENGTH when encoded
        long_xml = '<fetch><entity name="account">' + '<attribute name="x"/>' * 1200 + "</entity></fetch>"
        with pytest.raises(ValidationError, match="URL length limit"):
            async_client.query.fetchxml(long_xml)


class TestAsyncFetchXmlQueryPaging:
    """Tests for multi-page FetchXML execution paths."""

    async def test_execute_multi_page_with_cookie(self, async_client, mock_od):
        """execute() follows paging cookies across multiple pages."""
        import urllib.parse

        mock_od._entity_set_from_schema_name = AsyncMock(return_value="accounts")

        inner = '<cookie page="1"><accountid last="g1" first="g1" /></cookie>'
        encoded = urllib.parse.quote(urllib.parse.quote(inner))
        paging_cookie = f'<cookie pagenumber="2" pagingcookie="{encoded}" istracking="false" />'

        page1 = MagicMock()
        page1.json = MagicMock(
            return_value={
                "value": [{"name": "Contoso", "accountid": "g1"}],
                "@Microsoft.Dynamics.CRM.morerecords": True,
                "@Microsoft.Dynamics.CRM.fetchxmlpagingcookie": paging_cookie,
            }
        )
        page2 = MagicMock()
        page2.json = MagicMock(
            return_value={
                "value": [{"name": "Fabrikam", "accountid": "g2"}],
                "@Microsoft.Dynamics.CRM.morerecords": False,
            }
        )
        mock_od._request = AsyncMock(side_effect=[page1, page2])

        result = await async_client.query.fetchxml(_SIMPLE_FETCHXML).execute()

        assert len(result) == 2
        assert result[0]["name"] == "Contoso"
        assert result[1]["name"] == "Fabrikam"

    async def test_execute_multi_page_cookie_parse_error_fallback(self, async_client, mock_od):
        """execute() falls back to simple paging when the cookie XML is malformed."""
        import warnings

        mock_od._entity_set_from_schema_name = AsyncMock(return_value="accounts")

        page1 = MagicMock()
        page1.json = MagicMock(
            return_value={
                "value": [{"name": "Contoso", "accountid": "g1"}],
                "@Microsoft.Dynamics.CRM.morerecords": True,
                "@Microsoft.Dynamics.CRM.fetchxmlpagingcookie": "<<<not valid xml>>>",
            }
        )
        page2 = MagicMock()
        page2.json = MagicMock(
            return_value={
                "value": [{"name": "Fabrikam", "accountid": "g2"}],
                "@Microsoft.Dynamics.CRM.morerecords": False,
            }
        )
        mock_od._request = AsyncMock(side_effect=[page1, page2])

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = await async_client.query.fetchxml(_SIMPLE_FETCHXML).execute()

        assert len(result) == 2
        assert any("paging cookie could not be parsed" in str(warning.message) for warning in w)

    async def test_execute_multi_page_no_cookie_simple_paging(self, async_client, mock_od):
        """execute() falls back to simple page-number paging when no cookie is returned."""
        import warnings

        mock_od._entity_set_from_schema_name = AsyncMock(return_value="accounts")

        page1 = MagicMock()
        page1.json = MagicMock(
            return_value={
                "value": [{"name": "Contoso", "accountid": "g1"}],
                "@Microsoft.Dynamics.CRM.morerecords": True,
                # No fetchxmlpagingcookie key
            }
        )
        page2 = MagicMock()
        page2.json = MagicMock(
            return_value={
                "value": [{"name": "Fabrikam", "accountid": "g2"}],
                "@Microsoft.Dynamics.CRM.morerecords": False,
            }
        )
        mock_od._request = AsyncMock(side_effect=[page1, page2])

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = await async_client.query.fetchxml(_SIMPLE_FETCHXML).execute()

        assert len(result) == 2
        assert any("simple paging" in str(warning.message) for warning in w)

    async def test_execute_raises_on_max_pages_exceeded(self, async_client, mock_od):
        """execute() raises ValidationError when paging exceeds the maximum page limit."""
        import urllib.parse
        import warnings
        from PowerPlatform.Dataverse.core.errors import ValidationError

        mock_od._entity_set_from_schema_name = AsyncMock(return_value="accounts")

        def _make_page_resp(page_num: int):
            inner = f'<cookie page="{page_num}"><accountid last="x" first="x" /></cookie>'
            encoded = urllib.parse.quote(urllib.parse.quote(inner))
            cookie = f'<cookie pagenumber="{page_num + 1}" pagingcookie="{encoded}" istracking="false" />'
            resp = MagicMock()
            resp.json = MagicMock(
                return_value={
                    "value": [{"name": f"Record{page_num}", "accountid": f"g{page_num}"}],
                    "@Microsoft.Dynamics.CRM.morerecords": True,
                    "@Microsoft.Dynamics.CRM.fetchxmlpagingcookie": cookie,
                }
            )
            return resp

        # Always return morerecords=True to trigger the limit
        mock_od._request = AsyncMock(side_effect=lambda *a, **kw: _make_page_resp(1))

        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            with pytest.raises(ValidationError, match="exceeded"):
                await async_client.query.fetchxml(_SIMPLE_FETCHXML).execute()
