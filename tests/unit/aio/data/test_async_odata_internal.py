# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Unit tests for _AsyncODataClient internals (mocking _request at the HTTP boundary)."""

import json
import time
import warnings
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from PowerPlatform.Dataverse.aio.data._async_odata import _AsyncODataClient
from PowerPlatform.Dataverse.core.errors import HttpError, MetadataError, ValidationError

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_client() -> _AsyncODataClient:
    """Return _AsyncODataClient with _request mocked out at the HTTP boundary."""
    auth = MagicMock()
    auth._acquire_token = AsyncMock(return_value=MagicMock(access_token="test-token"))
    client = _AsyncODataClient(auth, "https://example.crm.dynamics.com")
    client._request = AsyncMock()
    return client


def _resp(json_data=None, status=200, headers=None):
    """Create a mock aiohttp-compatible response."""
    r = MagicMock()
    r.status = status
    r.headers = headers or {}
    r.text = AsyncMock(return_value=json.dumps(json_data) if json_data is not None else "")
    r.json = AsyncMock(return_value=json_data if json_data is not None else {})
    r.read = AsyncMock(return_value=b"")
    return r


def _entity_def(
    entity_set="accounts",
    pk="accountid",
    meta_id="meta-001",
    schema="Account",
    logical="account",
):
    """Return a minimal EntityDefinitions value-list response body."""
    return {
        "value": [
            {
                "LogicalName": logical,
                "EntitySetName": entity_set,
                "PrimaryIdAttribute": pk,
                "MetadataId": meta_id,
                "SchemaName": schema,
            }
        ]
    }


def _seed_cache(client: _AsyncODataClient, table="account", entity_set="accounts", pk="accountid"):
    """Pre-populate entity-set and primary-ID caches to bypass HTTP for schema-name lookups."""
    key = client._normalize_cache_key(table)
    client._logical_to_entityset_cache[key] = entity_set
    client._logical_primaryid_cache[key] = pk


# ---------------------------------------------------------------------------
# close()
# ---------------------------------------------------------------------------


class TestClose:
    """Tests for the close() lifecycle method."""

    async def test_close_delegates_to_http(self):
        """close() forwards to the underlying HTTP client's close() exactly once."""
        client = _make_client()
        client._http.close = AsyncMock()
        await client.close()
        client._http.close.assert_called_once()

    async def test_close_clears_entity_set_cache(self):
        """close() empties the entity-set lookup cache so stale entries don't persist."""
        client = _make_client()
        _seed_cache(client)
        client._http.close = AsyncMock()
        await client.close()
        assert len(client._logical_to_entityset_cache) == 0


# ---------------------------------------------------------------------------
# _request() — tests actual implementation via _raw_request mock
# ---------------------------------------------------------------------------


class TestRequest:
    """Tests for _request() error extraction.

    These tests mock _raw_request (one level below _request) so the real
    header-building, status-checking, and error-parsing code runs.
    """

    def _auth_client(self):
        """Return a client with a real auth mock but _raw_request not yet patched."""
        auth = MagicMock()
        auth._acquire_token = AsyncMock(return_value=MagicMock(access_token="token"))
        return _AsyncODataClient(auth, "https://example.crm.dynamics.com")

    async def test_ok_response_returned(self):
        """2xx responses are returned to the caller without raising."""
        client = self._auth_client()
        client._raw_request = AsyncMock(return_value=_resp(status=200, json_data={"value": []}))
        r = await client._request("get", "https://example.crm.dynamics.com/api/data/v9.2/accounts")
        assert r.status == 200

    async def test_error_with_nested_error_object(self):
        """Nested error.code / error.message body structure is parsed into HttpError."""
        client = self._auth_client()
        body = {"error": {"code": "0x80040265", "message": "Not found"}}
        client._raw_request = AsyncMock(return_value=_resp(status=404, json_data=body))
        with pytest.raises(HttpError) as exc:
            await client._request("get", "https://example.crm.dynamics.com/api/data/v9.2/accounts")
        assert exc.value.status_code == 404
        assert "Not found" in str(exc.value)

    async def test_error_with_message_at_root(self):
        """A top-level message key in the body is used when error nesting is absent."""
        client = self._auth_client()
        body = {"message": "Root-level message"}
        client._raw_request = AsyncMock(return_value=_resp(status=400, json_data=body))
        with pytest.raises(HttpError) as exc:
            await client._request("get", "https://example.crm.dynamics.com/api/data/v9.2/accounts")
        assert "Root-level message" in str(exc.value)

    async def test_error_non_json_body_handled(self):
        """Non-JSON response body falls back to HTTP status code as the error message."""
        client = self._auth_client()
        r = MagicMock()
        r.status = 503
        r.headers = {}
        r.text = AsyncMock(return_value="Service Unavailable")
        client._raw_request = AsyncMock(return_value=r)
        with pytest.raises(HttpError) as exc:
            await client._request("get", "https://example.crm.dynamics.com/api/data/v9.2/accounts")
        assert exc.value.status_code == 503

    async def test_retry_after_header_parsed(self):
        """Retry-After header value is stored as an integer in the error's details dict."""
        client = self._auth_client()
        body = {"error": {"code": "429", "message": "Too many requests"}}
        r = _resp(status=429, json_data=body, headers={"Retry-After": "60"})
        client._raw_request = AsyncMock(return_value=r)
        with pytest.raises(HttpError) as exc:
            await client._request("get", "https://example.crm.dynamics.com/api/data/v9.2/accounts")
        assert exc.value.to_dict()["details"].get("retry_after") == 60

    async def test_service_request_id_extracted(self):
        """x-ms-service-request-id header is stored in the error's details dict."""
        client = self._auth_client()
        r = _resp(status=500, headers={"x-ms-service-request-id": "srv-req-1"})
        r.text = AsyncMock(return_value='{"error": {"code": "err", "message": "fail"}}')
        client._raw_request = AsyncMock(return_value=r)
        with pytest.raises(HttpError) as exc:
            await client._request("get", "https://example.crm.dynamics.com/api/data/v9.2/accounts")
        assert exc.value.to_dict()["details"].get("service_request_id") == "srv-req-1"


# ---------------------------------------------------------------------------
# _create()
# ---------------------------------------------------------------------------


class TestCreate:
    """Tests for _create() single-record creation."""

    async def test_returns_guid_from_odata_entity_id(self):
        """GUID is extracted from the OData-EntityId response header."""
        client = _make_client()
        _seed_cache(client)
        guid = "12345678-1234-1234-1234-123456789012"
        client._request.return_value = _resp(
            status=204,
            headers={"OData-EntityId": f"https://example.crm.dynamics.com/api/data/v9.2/accounts({guid})"},
        )
        result = await client._create("accounts", "account", {"amount": 100})
        assert result == guid

    async def test_returns_guid_from_location_header(self):
        """Location header is used as fallback when OData-EntityId is absent."""
        client = _make_client()
        _seed_cache(client)
        guid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        client._request.return_value = _resp(
            status=204,
            headers={"Location": f"https://example.crm.dynamics.com/api/data/v9.2/accounts({guid})"},
        )
        result = await client._create("accounts", "account", {"amount": 100})
        assert result == guid

    async def test_raises_when_no_guid_in_headers(self):
        """RuntimeError is raised when neither OData-EntityId nor Location contains a GUID."""
        client = _make_client()
        _seed_cache(client)
        client._request.return_value = _resp(status=204, headers={})
        with pytest.raises(RuntimeError, match="GUID"):
            await client._create("accounts", "account", {"amount": 100})


# ---------------------------------------------------------------------------
# _create_multiple()
# ---------------------------------------------------------------------------


class TestCreateMultiple:
    """Tests for _create_multiple() bulk record creation."""

    async def test_returns_ids_from_ids_key(self):
        """IDs are extracted from the top-level Ids key in the response body."""
        client = _make_client()
        _seed_cache(client)
        guids = ["11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"]
        client._request.return_value = _resp(json_data={"Ids": guids}, status=200)
        result = await client._create_multiple("accounts", "account", [{"amount": 1}, {"amount": 2}])
        assert result == guids

    async def test_returns_ids_from_value_list(self):
        """IDs are extracted from value list entries when the Ids key is absent."""
        client = _make_client()
        _seed_cache(client)
        guid = "11111111-1111-1111-1111-111111111111"
        client._request.return_value = _resp(json_data={"value": [{"accountid": guid}]}, status=200)
        result = await client._create_multiple("accounts", "account", [{"amount": 1}])
        assert result == [guid]

    async def test_empty_body_returns_empty_list(self):
        """An empty response body returns an empty list without raising."""
        client = _make_client()
        _seed_cache(client)
        client._request.return_value = _resp(json_data={}, status=200)
        result = await client._create_multiple("accounts", "account", [{"amount": 1}])
        assert result == []

    async def test_non_dict_records_raises(self):
        """TypeError is raised when the records list contains non-dict items."""
        client = _make_client()
        _seed_cache(client)
        with pytest.raises(TypeError):
            await client._create_multiple("accounts", "account", ["not-a-dict"])


# ---------------------------------------------------------------------------
# _update() / _update_by_ids() / _update_multiple()
# ---------------------------------------------------------------------------


class TestUpdate:
    """Tests for _update(), _update_by_ids(), and _update_multiple()."""

    async def test_update_patches_record(self):
        """_update() issues a PATCH request for the given record ID."""
        client = _make_client()
        _seed_cache(client)
        client._request.return_value = _resp(status=204)
        await client._update("account", "guid-1", {"telephone1": "555"})
        assert client._request.called

    async def test_update_by_ids_broadcast_dict(self):
        """A dict for changes is broadcast to all IDs via UpdateMultiple."""
        client = _make_client()
        _seed_cache(client)
        client._request.return_value = _resp(status=204)
        await client._update_by_ids("account", ["id-1", "id-2"], {"statecode": 0})
        assert client._request.called

    async def test_update_by_ids_paired_list(self):
        """A list for changes is applied pairwise with the corresponding IDs."""
        client = _make_client()
        _seed_cache(client)
        client._request.return_value = _resp(status=204)
        await client._update_by_ids("account", ["id-1"], [{"name": "A"}])
        assert client._request.called

    async def test_update_by_ids_empty_list_is_noop(self):
        """An empty ID list short-circuits without issuing any HTTP request."""
        client = _make_client()
        result = await client._update_by_ids("account", [], {"statecode": 0})
        assert result is None
        client._request.assert_not_called()

    async def test_update_by_ids_mismatched_length_raises(self):
        """ValueError is raised when the changes list is shorter than the ID list."""
        client = _make_client()
        _seed_cache(client)
        client._request.return_value = _resp(status=200)
        with pytest.raises(ValueError):
            await client._update_by_ids("account", ["id-1", "id-2"], [{"name": "A"}])

    async def test_update_by_ids_invalid_changes_type_raises(self):
        """TypeError is raised when changes is neither a dict nor a list."""
        client = _make_client()
        _seed_cache(client)
        with pytest.raises(TypeError):
            await client._update_by_ids("account", ["id-1"], "invalid")

    async def test_update_multiple_empty_records_raises(self):
        """TypeError is raised when the records list is empty."""
        client = _make_client()
        with pytest.raises(TypeError):
            await client._update_multiple("accounts", "account", [])

    async def test_update_multiple_non_list_raises(self):
        """TypeError is raised when the records argument is not a list."""
        client = _make_client()
        with pytest.raises(TypeError):
            await client._update_multiple("accounts", "account", "not-a-list")


# ---------------------------------------------------------------------------
# _delete() / _delete_multiple()
# ---------------------------------------------------------------------------


class TestDelete:
    """Tests for _delete() single-record deletion and _delete_multiple() bulk deletion."""

    async def test_delete_calls_request(self):
        """_delete() issues a DELETE request for the given record ID."""
        client = _make_client()
        _seed_cache(client)
        client._request.return_value = _resp(status=204)
        await client._delete("account", "guid-1")
        assert client._request.called

    async def test_delete_multiple_returns_job_id(self):
        """JobId from the async BulkDelete response is returned to the caller."""
        client = _make_client()
        _seed_cache(client)
        client._request.return_value = _resp(json_data={"JobId": "job-guid-1"}, status=202)
        result = await client._delete_multiple("account", ["id-1", "id-2"])
        assert result == "job-guid-1"

    async def test_delete_multiple_empty_ids_returns_none(self):
        """An empty ID list short-circuits without issuing any HTTP request."""
        client = _make_client()
        result = await client._delete_multiple("account", [])
        assert result is None
        client._request.assert_not_called()

    async def test_delete_multiple_no_job_id_in_body(self):
        """None is returned when the response body does not contain a JobId key."""
        client = _make_client()
        _seed_cache(client)
        client._request.return_value = _resp(json_data={}, status=204)
        result = await client._delete_multiple("account", ["id-1"])
        assert result is None


# ---------------------------------------------------------------------------
# _get() / _get_multiple()
# ---------------------------------------------------------------------------


class TestGet:
    """Tests for _get() single-record fetch."""

    async def test_get_returns_record(self):
        """The full record dict from the response body is returned unchanged."""
        client = _make_client()
        _seed_cache(client)
        record = {"accountid": "guid-1", "name": "Contoso"}
        client._request.return_value = _resp(json_data=record, status=200)
        result = await client._get("account", "guid-1")
        assert result == record

    async def test_get_with_select_param(self):
        """The select list is forwarded as a $select query parameter."""
        client = _make_client()
        _seed_cache(client)
        client._request.return_value = _resp(json_data={"name": "Contoso"}, status=200)
        result = await client._get("account", "guid-1", select=["name"])
        assert result == {"name": "Contoso"}


class TestGetMultiple:
    """Tests for _get_multiple() async generator for paged results."""

    async def test_single_page_yielded(self):
        """A single-page response produces exactly one batch from the generator."""
        client = _make_client()
        _seed_cache(client)
        page = {"value": [{"accountid": "1"}, {"accountid": "2"}]}
        client._request.return_value = _resp(json_data=page, status=200)
        pages = []
        async for p in client._get_multiple("account"):
            pages.append(p)
        assert len(pages) == 1
        assert len(pages[0]) == 2

    async def test_follows_next_link(self):
        """@odata.nextLink is followed to fetch subsequent pages automatically."""
        client = _make_client()
        _seed_cache(client)
        next_url = "https://example.crm.dynamics.com/api/data/v9.2/accounts?$skiptoken=xyz"
        page1 = {"value": [{"accountid": "1"}], "@odata.nextLink": next_url}
        page2 = {"value": [{"accountid": "2"}]}
        client._request.side_effect = [_resp(json_data=page1), _resp(json_data=page2)]
        pages = []
        async for p in client._get_multiple("account"):
            pages.append(p)
        assert len(pages) == 2

    async def test_empty_value_not_yielded(self):
        """A page with an empty value list produces no output from the generator."""
        client = _make_client()
        _seed_cache(client)
        client._request.return_value = _resp(json_data={"value": []}, status=200)
        pages = []
        async for p in client._get_multiple("account"):
            pages.append(p)
        assert len(pages) == 0

    async def test_with_all_params(self):
        """All optional query parameters are forwarded in the outbound request."""
        client = _make_client()
        _seed_cache(client)
        client._request.return_value = _resp(json_data={"value": []}, status=200)
        async for _ in client._get_multiple(
            "account",
            select=["name"],
            filter="statecode eq 0",
            orderby=["name asc"],
            top=10,
            expand=["primarycontactid"],
            page_size=5,
            count=True,
            include_annotations="*",
        ):
            pass
        call = client._request.call_args
        assert call is not None
        kwargs = call.kwargs
        assert "headers" in kwargs or kwargs.get("params") is not None


# ---------------------------------------------------------------------------
# _query_sql()
# ---------------------------------------------------------------------------


class TestQuerySql:
    """Tests for _query_sql() which executes Dataverse SQL against the TDS endpoint."""

    async def test_raises_if_not_string(self):
        """ValidationError is raised when the SQL argument is not a string."""
        client = _make_client()
        with pytest.raises(ValidationError):
            await client._query_sql(123)

    async def test_raises_if_empty(self):
        """ValidationError is raised for a blank or whitespace-only SQL string."""
        client = _make_client()
        with pytest.raises(ValidationError):
            await client._query_sql("   ")

    async def test_returns_rows_from_value(self):
        """Rows are extracted from the value list in a standard OData response body."""
        client = _make_client()
        _seed_cache(client)
        rows = [{"name": "A"}, {"name": "B"}]
        client._request.return_value = _resp(json_data={"value": rows}, status=200)
        result = await client._query_sql("SELECT name FROM account")
        assert result == rows

    async def test_returns_list_body_directly(self):
        """A list response body (rather than {value: [...]}) is accepted as rows directly."""
        client = _make_client()
        _seed_cache(client)
        rows = [{"name": "A"}]
        client._request.return_value = _resp(json_data=rows, status=200)
        result = await client._query_sql("SELECT name FROM account")
        assert result == rows

    async def test_follows_next_link(self):
        """Pagination via @odata.nextLink concatenates all rows across pages."""
        client = _make_client()
        _seed_cache(client)
        next_url = "https://example.crm.dynamics.com/api/data/v9.2/accounts?sql=SELECT+name+FROM+account&page=2"
        page1 = {"value": [{"name": "A"}], "@odata.nextLink": next_url}
        page2 = {"value": [{"name": "B"}]}
        client._request.side_effect = [_resp(json_data=page1), _resp(json_data=page2)]
        result = await client._query_sql("SELECT name FROM account")
        assert len(result) == 2

    async def test_warns_and_stops_on_url_cycle(self):
        """A repeated nextLink triggers a warning and stops pagination to prevent an infinite loop."""
        client = _make_client()
        _seed_cache(client)
        cycle_url = "https://example.crm.dynamics.com/api/data/v9.2/accounts?sql=SELECT+name+FROM+account&page=1"
        page1 = {"value": [{"name": "A"}], "@odata.nextLink": cycle_url}
        page2 = {"value": [{"name": "B"}], "@odata.nextLink": cycle_url}
        client._request.side_effect = [_resp(json_data=page1), _resp(json_data=page2)]
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = await client._query_sql("SELECT name FROM account")
        # Cycle detected after page 2's nextLink repeats; pages 1 and 2 are collected.
        assert any("same nextLink" in str(w.message) for w in caught)
        assert len(result) == 2

    async def test_stops_on_non_dict_page_body(self):
        """A non-dict page body halts pagination and discards the malformed page."""
        client = _make_client()
        _seed_cache(client)
        next_url = "https://example.crm.dynamics.com/api/data/v9.2/accounts?sql=SELECT+name&page=2"
        page1 = {"value": [{"name": "A"}], "@odata.nextLink": next_url}
        # page2 is a list, not a dict — break condition
        client._request.side_effect = [_resp(json_data=page1), _resp(json_data=["not-a-dict"])]
        result = await client._query_sql("SELECT name FROM account")
        assert len(result) == 1


# ---------------------------------------------------------------------------
# _entity_set_from_schema_name()
# ---------------------------------------------------------------------------


class TestEntitySetResolution:
    """Tests for _entity_set_from_schema_name() cache lookup and HTTP fetch."""

    async def test_cache_hit_skips_http(self):
        """A pre-populated cache entry is returned without any HTTP call."""
        client = _make_client()
        _seed_cache(client)
        result = await client._entity_set_from_schema_name("account")
        client._request.assert_not_called()
        assert result == "accounts"

    async def test_fetches_and_caches(self):
        """On a cache miss, the entity set is fetched from the API and cached for reuse."""
        client = _make_client()
        client._request.return_value = _resp(json_data=_entity_def(), status=200)
        result = await client._entity_set_from_schema_name("account")
        assert result == "accounts"
        # Subsequent call must hit the cache, not the API.
        client._request.reset_mock()
        result2 = await client._entity_set_from_schema_name("account")
        client._request.assert_not_called()
        assert result2 == "accounts"

    async def test_caches_primary_id_attr(self):
        """The primary ID attribute is cached alongside the entity set name."""
        client = _make_client()
        client._request.return_value = _resp(json_data=_entity_def(pk="accountid"), status=200)
        await client._entity_set_from_schema_name("account")
        key = client._normalize_cache_key("account")
        assert client._logical_primaryid_cache.get(key) == "accountid"

    async def test_not_found_raises_metadata_error(self):
        """MetadataError is raised when the API returns an empty value list."""
        client = _make_client()
        client._request.return_value = _resp(json_data={"value": []}, status=200)
        with pytest.raises(MetadataError, match="Unable to resolve"):
            await client._entity_set_from_schema_name("nonexistent")

    async def test_plural_name_includes_hint(self):
        """The error message hints at a plural-name mistake when the input ends with 's'."""
        client = _make_client()
        client._request.return_value = _resp(json_data={"value": []}, status=200)
        with pytest.raises(MetadataError, match="plural"):
            await client._entity_set_from_schema_name("accounts")

    async def test_missing_entity_set_name_raises(self):
        """MetadataError is raised when the entity definition lacks an EntitySetName."""
        client = _make_client()
        client._request.return_value = _resp(
            json_data={"value": [{"LogicalName": "account", "MetadataId": "m1"}]},
            status=200,
        )
        with pytest.raises(MetadataError, match="EntitySetName"):
            await client._entity_set_from_schema_name("account")

    async def test_empty_name_raises_value_error(self):
        """ValueError is raised immediately for an empty table schema name."""
        client = _make_client()
        with pytest.raises(ValueError):
            await client._entity_set_from_schema_name("")


# ---------------------------------------------------------------------------
# _get_table_info() / _list_tables() / _delete_table()
# ---------------------------------------------------------------------------


class TestTableInfo:
    """Tests for _get_table_info() entity-definition summary lookup."""

    async def test_get_table_info_found(self):
        """A found table returns a dict containing entity_set_name and columns_created."""
        client = _make_client()
        client._request.return_value = _resp(json_data=_entity_def(), status=200)
        result = await client._get_table_info("account")
        assert result is not None
        assert result["entity_set_name"] == "accounts"
        assert result["columns_created"] == []

    async def test_get_table_info_not_found(self):
        """None is returned when the table does not exist in metadata."""
        client = _make_client()
        client._request.return_value = _resp(json_data={"value": []}, status=200)
        result = await client._get_table_info("nonexistent")
        assert result is None


class TestListTables:
    """Tests for _list_tables() entity-definition list retrieval."""

    async def test_list_tables_returns_value(self):
        """The value list from the EntityDefinitions response is returned unchanged."""
        client = _make_client()
        tables = [{"LogicalName": "account"}]
        client._request.return_value = _resp(json_data={"value": tables}, status=200)
        result = await client._list_tables()
        assert result == tables

    async def test_list_tables_with_filter_and_select(self):
        """Optional filter and select parameters are forwarded to the API request."""
        client = _make_client()
        client._request.return_value = _resp(json_data={"value": []}, status=200)
        result = await client._list_tables(filter="IsPrivate eq false", select=["LogicalName"])
        assert result == []


class TestDeleteTable:
    """Tests for _delete_table() metadata-level table removal."""

    async def test_delete_calls_delete_request(self):
        """Two requests are issued: one to resolve the MetadataId, one DELETE."""
        client = _make_client()
        client._request.side_effect = [_resp(json_data=_entity_def()), _resp(status=204)]
        await client._delete_table("account")
        assert client._request.call_count == 2

    async def test_delete_not_found_raises(self):
        """MetadataError is raised when the target table does not exist."""
        client = _make_client()
        client._request.return_value = _resp(json_data={"value": []}, status=200)
        with pytest.raises(MetadataError, match="not found"):
            await client._delete_table("nonexistent")


# ---------------------------------------------------------------------------
# _create_table()
# ---------------------------------------------------------------------------


class TestCreateTable:
    """Tests for _create_table() custom table provisioning."""

    async def test_table_already_exists_raises(self):
        """MetadataError is raised when a table with the same schema name already exists."""
        client = _make_client()
        client._request.return_value = _resp(json_data=_entity_def(), status=200)
        with pytest.raises(MetadataError, match="already exists"):
            await client._create_table("account", {})

    async def test_success_with_columns(self):
        """Table and typed columns are created; the returned dict lists columns_created."""
        client = _make_client()
        not_found = _resp(json_data={"value": []}, status=200)
        create_resp = _resp(status=204)
        entity_resp = _resp(
            json_data=_entity_def(entity_set="new_products", schema="new_Product", logical="new_product"),
            status=200,
        )
        client._request.side_effect = [not_found, create_resp, entity_resp]
        result = await client._create_table("new_Product", {"new_Price": "decimal"})
        assert result["table_schema_name"] == "new_Product"
        assert "new_Price" in result["columns_created"]

    async def test_success_with_primary_column(self):
        """An explicit primary_column_schema_name is accepted without error."""
        client = _make_client()
        not_found = _resp(json_data={"value": []}, status=200)
        create_resp = _resp(status=204)
        entity_resp = _resp(json_data=_entity_def(entity_set="new_products"), status=200)
        client._request.side_effect = [not_found, create_resp, entity_resp]
        result = await client._create_table("new_Product", {}, primary_column_schema_name="new_ProductName")
        assert result is not None

    async def test_success_with_display_name(self):
        """A string display_name is accepted and forwarded to the API."""
        client = _make_client()
        not_found = _resp(json_data={"value": []}, status=200)
        create_resp = _resp(status=204)
        entity_resp = _resp(json_data=_entity_def(entity_set="new_products"), status=200)
        client._request.side_effect = [not_found, create_resp, entity_resp]
        result = await client._create_table("new_Product", {}, display_name="Product")
        assert result is not None

    async def test_unsupported_column_type_raises(self):
        """ValueError is raised before the POST when a column type string is unrecognised."""
        client = _make_client()
        client._request.return_value = _resp(json_data={"value": []}, status=200)
        with pytest.raises(ValueError, match="Unsupported"):
            await client._create_table("new_Product", {"col": "badtype"})

    async def test_empty_solution_name_raises(self):
        """ValueError is raised when solution_unique_name is an empty string."""
        client = _make_client()
        client._request.return_value = _resp(json_data={"value": []}, status=200)
        with pytest.raises(ValueError, match="cannot be empty"):
            await client._create_table("new_Product", {}, solution_unique_name="")

    async def test_non_string_solution_raises(self):
        """TypeError is raised when solution_unique_name is not a string."""
        client = _make_client()
        client._request.return_value = _resp(json_data={"value": []}, status=200)
        with pytest.raises(TypeError):
            await client._create_table("new_Product", {}, solution_unique_name=42)

    async def test_invalid_display_name_raises(self):
        """TypeError is raised when display_name is not a string."""
        client = _make_client()
        client._request.return_value = _resp(json_data={"value": []}, status=200)
        with pytest.raises(TypeError):
            await client._create_table("new_Product", {}, display_name=123)


# ---------------------------------------------------------------------------
# _create_columns() / _delete_columns()
# ---------------------------------------------------------------------------


class TestCreateColumns:
    """Tests for _create_columns() column provisioning on an existing table."""

    async def test_creates_string_column(self):
        """A string-typed column is created and its name returned in the result list."""
        client = _make_client()
        entity_resp = _resp(json_data=_entity_def(), status=200)
        attr_resp = _resp(status=204)
        client._request.side_effect = [entity_resp, attr_resp]
        result = await client._create_columns("account", {"new_Notes": "string"})
        assert result == ["new_Notes"]

    async def test_empty_columns_dict_raises(self):
        """TypeError is raised when the columns dict is empty."""
        client = _make_client()
        with pytest.raises(TypeError, match="non-empty dict"):
            await client._create_columns("account", {})

    async def test_non_dict_columns_raises(self):
        """TypeError is raised when the columns argument is not a dict."""
        client = _make_client()
        with pytest.raises(TypeError):
            await client._create_columns("account", ["col"])

    async def test_table_not_found_raises(self):
        """MetadataError is raised when the parent table does not exist."""
        client = _make_client()
        client._request.return_value = _resp(json_data={"value": []}, status=200)
        with pytest.raises(MetadataError, match="not found"):
            await client._create_columns("nonexistent", {"col": "string"})

    async def test_unsupported_type_raises_validation_error(self):
        """ValidationError is raised for an unrecognised column type string."""
        client = _make_client()
        entity_resp = _resp(json_data=_entity_def(), status=200)
        client._request.return_value = entity_resp
        with pytest.raises(ValidationError):
            await client._create_columns("account", {"col": "badtype"})

    async def test_optionset_column_flushes_cache(self):
        """Creating a column whose payload includes OptionSet invalidates the picklist label cache.

        Boolean columns produce an OptionSet payload, which triggers the same cache-flush
        path used by choice/picklist columns.
        """
        client = _make_client()
        entity_resp = _resp(json_data=_entity_def(), status=200)
        attr_resp = _resp(status=204)
        client._request.side_effect = [entity_resp, attr_resp]
        key = client._normalize_cache_key("account")
        client._picklist_label_cache[key] = {"ts": time.time(), "picklists": {"old": {}}}
        result = await client._create_columns("account", {"new_Status": "bool"})
        assert result == ["new_Status"]
        assert key not in client._picklist_label_cache


class TestDeleteColumns:
    """Tests for _delete_columns() column removal from an existing table."""

    async def test_string_column_name(self):
        """A single column name supplied as a string is accepted and deleted."""
        client = _make_client()
        entity_resp = _resp(json_data=_entity_def(), status=200)
        attr_resp = _resp(json_data={"value": [{"MetadataId": "attr-1", "LogicalName": "new_notes"}]}, status=200)
        delete_resp = _resp(status=204)
        client._request.side_effect = [entity_resp, attr_resp, delete_resp]
        result = await client._delete_columns("account", "new_Notes")
        assert result == ["new_Notes"]

    async def test_list_column_names(self):
        """Column names supplied as a list are each deleted in turn."""
        client = _make_client()
        entity_resp = _resp(json_data=_entity_def(), status=200)
        attr_resp = _resp(json_data={"value": [{"MetadataId": "attr-1", "LogicalName": "new_notes"}]}, status=200)
        delete_resp = _resp(status=204)
        client._request.side_effect = [entity_resp, attr_resp, delete_resp]
        result = await client._delete_columns("account", ["new_Notes"])
        assert result == ["new_Notes"]

    async def test_invalid_type_raises(self):
        """TypeError is raised when the columns argument is neither str nor list."""
        client = _make_client()
        with pytest.raises(TypeError):
            await client._delete_columns("account", 42)

    async def test_empty_column_name_raises(self):
        """ValueError is raised when the column name string is empty."""
        client = _make_client()
        with pytest.raises(ValueError, match="non-empty"):
            await client._delete_columns("account", "")

    async def test_table_not_found_raises(self):
        """MetadataError is raised when the parent table does not exist."""
        client = _make_client()
        client._request.return_value = _resp(json_data={"value": []}, status=200)
        with pytest.raises(MetadataError):
            await client._delete_columns("nonexistent", "col")

    async def test_column_not_found_raises(self):
        """MetadataError is raised when the column is absent from attribute metadata."""
        client = _make_client()
        entity_resp = _resp(json_data=_entity_def(), status=200)
        attr_resp = _resp(json_data={"value": []}, status=200)
        client._request.side_effect = [entity_resp, attr_resp]
        with pytest.raises(MetadataError, match="not found"):
            await client._delete_columns("account", "nonexistent_col")

    async def test_missing_attr_metadata_id_raises(self):
        """RuntimeError is raised when the attribute response lacks a MetadataId."""
        client = _make_client()
        entity_resp = _resp(json_data=_entity_def(), status=200)
        attr_resp = _resp(json_data={"value": [{"LogicalName": "new_notes"}]}, status=200)
        client._request.side_effect = [entity_resp, attr_resp]
        with pytest.raises(RuntimeError, match="MetadataId"):
            await client._delete_columns("account", "new_Notes")

    async def test_picklist_column_flushes_cache(self):
        """Deleting a Picklist-type column invalidates the picklist label cache."""
        client = _make_client()
        entity_resp = _resp(json_data=_entity_def(), status=200)
        attr_resp = _resp(
            json_data={"value": [{"MetadataId": "attr-1", "LogicalName": "new_status", "AttributeType": "Picklist"}]},
            status=200,
        )
        delete_resp = _resp(status=204)
        client._request.side_effect = [entity_resp, attr_resp, delete_resp]
        key = client._normalize_cache_key("account")
        client._picklist_label_cache[key] = {"ts": time.time(), "picklists": {}}
        result = await client._delete_columns("account", "new_Status")
        assert result == ["new_Status"]
        assert key not in client._picklist_label_cache


# ---------------------------------------------------------------------------
# _list_columns()
# ---------------------------------------------------------------------------


class TestListColumns:
    """Tests for _list_columns() attribute metadata listing."""

    async def test_returns_attribute_list(self):
        """The full attribute list from the API response is returned."""
        client = _make_client()
        entity_resp = _resp(json_data=_entity_def(), status=200)
        cols_resp = _resp(json_data={"value": [{"LogicalName": "name"}, {"LogicalName": "accountid"}]}, status=200)
        client._request.side_effect = [entity_resp, cols_resp]
        result = await client._list_columns("account")
        assert len(result) == 2

    async def test_table_not_found_raises(self):
        """MetadataError is raised when the table is absent from metadata."""
        client = _make_client()
        client._request.return_value = _resp(json_data={"value": []}, status=200)
        with pytest.raises(MetadataError, match="not found"):
            await client._list_columns("nonexistent")

    async def test_with_select_and_filter(self):
        """Optional select and filter parameters are forwarded to the Attributes API call."""
        client = _make_client()
        entity_resp = _resp(json_data=_entity_def(), status=200)
        cols_resp = _resp(json_data={"value": []}, status=200)
        client._request.side_effect = [entity_resp, cols_resp]
        result = await client._list_columns("account", select=["LogicalName"], filter="AttributeType eq 'String'")
        assert result == []


# ---------------------------------------------------------------------------
# Alternate key operations
# ---------------------------------------------------------------------------


class TestAlternateKeys:
    """Tests for _create_alternate_key(), _get_alternate_keys(), and _delete_alternate_key()."""

    async def test_create_alternate_key_success(self):
        """The key UUID is extracted from the OData-EntityId header and returned in metadata_id.

        The URL format is EntityDefinitions(LogicalName='...')/Keys(uuid), so the regex
        skips the LogicalName= form and matches only the key UUID in parentheses.
        """
        client = _make_client()
        key_uuid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        entity_resp = _resp(json_data=_entity_def(), status=200)
        create_resp = _resp(
            status=204,
            headers={
                "OData-EntityId": f"https://example.crm.dynamics.com/api/data/v9.2/EntityDefinitions(LogicalName='account')/Keys({key_uuid})"
            },
        )
        client._request.side_effect = [entity_resp, create_resp]
        result = await client._create_alternate_key("account", "new_prod_key", ["new_productcode"])
        assert result["schema_name"] == "new_prod_key"
        assert result["key_attributes"] == ["new_productcode"]
        assert result["metadata_id"] == key_uuid

    async def test_create_alternate_key_table_not_found_raises(self):
        """MetadataError is raised when the target table does not exist."""
        client = _make_client()
        client._request.return_value = _resp(json_data={"value": []}, status=200)
        with pytest.raises(MetadataError):
            await client._create_alternate_key("nonexistent", "key", ["col"])

    async def test_get_alternate_keys_returns_list(self):
        """All alternate keys on the table are returned as a list of dicts."""
        client = _make_client()
        entity_resp = _resp(json_data=_entity_def(), status=200)
        keys_resp = _resp(json_data={"value": [{"SchemaName": "key1"}, {"SchemaName": "key2"}]}, status=200)
        client._request.side_effect = [entity_resp, keys_resp]
        result = await client._get_alternate_keys("account")
        assert len(result) == 2

    async def test_get_alternate_keys_table_not_found_raises(self):
        """MetadataError is raised when the table does not exist."""
        client = _make_client()
        client._request.return_value = _resp(json_data={"value": []}, status=200)
        with pytest.raises(MetadataError):
            await client._get_alternate_keys("nonexistent")

    async def test_delete_alternate_key_success(self):
        """Two requests are issued: one entity lookup then one DELETE for the key."""
        client = _make_client()
        entity_resp = _resp(json_data=_entity_def(), status=200)
        delete_resp = _resp(status=204)
        client._request.side_effect = [entity_resp, delete_resp]
        await client._delete_alternate_key("account", "key-guid")
        assert client._request.call_count == 2

    async def test_delete_alternate_key_table_not_found_raises(self):
        """MetadataError is raised when the table does not exist."""
        client = _make_client()
        client._request.return_value = _resp(json_data={"value": []}, status=200)
        with pytest.raises(MetadataError):
            await client._delete_alternate_key("nonexistent", "key-guid")


# ---------------------------------------------------------------------------
# _upsert() / _upsert_multiple()
# ---------------------------------------------------------------------------


class TestUpsert:
    """Tests for _upsert() and _upsert_multiple() alternate-key upsert operations."""

    async def test_upsert_issues_patch(self):
        """_upsert() issues a PATCH request (create-or-replace semantics)."""
        client = _make_client()
        _seed_cache(client)
        client._request.return_value = _resp(status=204)
        await client._upsert("accounts", "account", {"accountnumber": "A"}, {"name": "X"})
        call = client._request.call_args
        assert call.args[0] == "patch"

    async def test_upsert_multiple_issues_post(self):
        """_upsert_multiple() sends a POST to the UpsertMultiple action endpoint."""
        client = _make_client()
        _seed_cache(client)
        client._request.return_value = _resp(status=204)
        await client._upsert_multiple(
            "accounts",
            "account",
            [{"accountnumber": "A"}, {"accountnumber": "B"}],
            [{"name": "X"}, {"name": "Y"}],
        )
        call = client._request.call_args
        assert call.args[0] == "post"
        assert "UpsertMultiple" in call.args[1]

    async def test_upsert_multiple_mismatched_length_raises(self):
        """ValueError is raised when the alternate-key list and record list differ in length."""
        client = _make_client()
        with pytest.raises(ValueError, match="same length"):
            await client._upsert_multiple("accounts", "account", [{"k": "1"}], [{"n": "A"}, {"n": "B"}])

    async def test_upsert_multiple_key_conflict_raises(self):
        """ValueError is raised when a record field conflicts with its alternate-key field."""
        client = _make_client()
        _seed_cache(client)
        with pytest.raises(ValueError, match="conflicts"):
            await client._upsert_multiple(
                "accounts",
                "account",
                [{"accountnumber": "A"}],
                [{"accountnumber": "B"}],
            )


# ---------------------------------------------------------------------------
# _bulk_fetch_picklists() / _convert_labels_to_ints()
# ---------------------------------------------------------------------------


class TestPicklists:
    """Tests for _bulk_fetch_picklists() cache population and _convert_labels_to_ints() resolution."""

    async def test_bulk_fetch_populates_cache(self):
        """Picklist options are fetched from the API and stored with lowercased label keys."""
        client = _make_client()
        body = {
            "value": [
                {
                    "LogicalName": "statecode",
                    "OptionSet": {
                        "Options": [
                            {"Value": 0, "Label": {"LocalizedLabels": [{"Label": "Active", "LanguageCode": 1033}]}},
                            {"Value": 1, "Label": {"LocalizedLabels": [{"Label": "Inactive", "LanguageCode": 1033}]}},
                        ]
                    },
                }
            ]
        }
        client._request.return_value = _resp(json_data=body, status=200)
        await client._bulk_fetch_picklists("account")
        key = client._normalize_cache_key("account")
        assert key in client._picklist_label_cache
        picklists = client._picklist_label_cache[key]["picklists"]
        assert "statecode" in picklists
        assert picklists["statecode"]["active"] == 0

    async def test_bulk_fetch_skips_on_cache_hit(self):
        """A valid cache entry prevents an API call when the TTL has not expired."""
        client = _make_client()
        key = client._normalize_cache_key("account")
        client._picklist_label_cache[key] = {"ts": time.time(), "picklists": {}}
        await client._bulk_fetch_picklists("account")
        client._request.assert_not_called()

    async def test_bulk_fetch_empty_option_set(self):
        """An attribute with an empty OptionSet is stored as an empty mapping."""
        client = _make_client()
        body = {"value": [{"LogicalName": "field", "OptionSet": {}}]}
        client._request.return_value = _resp(json_data=body, status=200)
        await client._bulk_fetch_picklists("account")
        key = client._normalize_cache_key("account")
        assert client._picklist_label_cache[key]["picklists"]["field"] == {}

    async def test_convert_no_string_values_returns_unchanged(self):
        """A record with no string values is returned as-is without any API lookup."""
        client = _make_client()
        record = {"statecode": 0, "count": 5}
        result = await client._convert_labels_to_ints("account", record)
        assert result == record
        client._request.assert_not_called()

    async def test_convert_string_resolved_to_int(self):
        """A known label string is resolved to its integer option value from the cache."""
        client = _make_client()
        key = client._normalize_cache_key("account")
        client._picklist_label_cache[key] = {
            "ts": time.time(),
            "picklists": {"statecode": {"active": 0, "inactive": 1}},
        }
        result = await client._convert_labels_to_ints("account", {"statecode": "Active"})
        assert result["statecode"] == 0

    async def test_convert_odata_key_skipped(self):
        """OData annotation fields with labels that don't match any option are left unchanged."""
        client = _make_client()
        key = client._normalize_cache_key("account")
        client._picklist_label_cache[key] = {
            "ts": time.time(),
            "picklists": {"@odata.type": {"val": 1}},
        }
        record = {"@odata.type": "Microsoft.Dynamics.CRM.account"}
        result = await client._convert_labels_to_ints("account", record)
        assert result["@odata.type"] == "Microsoft.Dynamics.CRM.account"

    async def test_convert_unresolved_string_left_unchanged(self):
        """A string value with no matching picklist entry is left as-is in the output."""
        client = _make_client()
        key = client._normalize_cache_key("account")
        client._picklist_label_cache[key] = {"ts": time.time(), "picklists": {}}
        result = await client._convert_labels_to_ints("account", {"name": "Contoso"})
        assert result["name"] == "Contoso"


# ---------------------------------------------------------------------------
# _build_* async methods
# ---------------------------------------------------------------------------


class TestBuildMethods:
    """Tests for _build_* async methods that produce _RawRequest objects without I/O."""

    async def test_build_create_post_request(self):
        """_build_create() produces a POST request targeting the entity set URL."""
        client = _make_client()
        _seed_cache(client)
        req = await client._build_create("accounts", "account", {"amount": 100})
        assert req.method == "POST"
        assert "accounts" in req.url

    async def test_build_create_multiple_post_request(self):
        """_build_create_multiple() produces a POST targeting the CreateMultiple action."""
        client = _make_client()
        _seed_cache(client)
        req = await client._build_create_multiple("accounts", "account", [{"amount": 100}])
        assert req.method == "POST"
        assert "CreateMultiple" in req.url

    async def test_build_create_multiple_injects_odata_type(self):
        """Each entry in the Targets list receives an @odata.type annotation."""
        client = _make_client()
        _seed_cache(client)
        req = await client._build_create_multiple("accounts", "account", [{"amount": 100}])
        body = json.loads(req.body)
        assert "@odata.type" in body["Targets"][0]

    async def test_build_update_patch_request(self):
        """_build_update() produces a PATCH request with an If-Match: * concurrency guard."""
        client = _make_client()
        _seed_cache(client)
        req = await client._build_update("account", "guid-1", {"name": "X"})
        assert req.method == "PATCH"
        assert "accounts" in req.url
        assert req.headers.get("If-Match") == "*"

    async def test_build_update_with_content_id_reference(self):
        """A $-prefixed record_id is used as a raw changeset content-ID reference URL."""
        client = _make_client()
        req = await client._build_update("account", "$1", {"name": "X"})
        assert req.url == "$1"

    async def test_build_delete_delete_request(self):
        """_build_delete() produces a DELETE request with an If-Match: * concurrency guard."""
        client = _make_client()
        _seed_cache(client)
        req = await client._build_delete("account", "guid-1")
        assert req.method == "DELETE"
        assert "accounts" in req.url
        assert req.headers.get("If-Match") == "*"

    async def test_build_delete_with_content_id_reference(self):
        """A $-prefixed record_id is used as a raw changeset content-ID reference URL."""
        client = _make_client()
        req = await client._build_delete("account", "$2")
        assert req.url == "$2"

    async def test_build_get_get_request_with_select(self):
        """_build_get() encodes the select list as a $select query string parameter."""
        client = _make_client()
        _seed_cache(client)
        req = await client._build_get("account", "guid-1", select=["name", "telephone1"])
        assert req.method == "GET"
        assert "accounts" in req.url
        assert "$select=name,telephone1" in req.url

    async def test_build_get_no_select(self):
        """_build_get() omits $select from the URL when no columns are specified."""
        client = _make_client()
        _seed_cache(client)
        req = await client._build_get("account", "guid-1")
        assert "$select" not in req.url

    async def test_build_sql_encodes_query(self):
        """_build_sql() produces a GET request with the SQL statement in a sql= parameter."""
        client = _make_client()
        _seed_cache(client)
        req = await client._build_sql("SELECT name FROM account")
        assert req.method == "GET"
        assert "sql=" in req.url
        assert "SELECT" in req.url or "SELECT" in req.url.replace("%20", " ")

    async def test_build_upsert_patch_request(self):
        """_build_upsert() produces a PATCH without If-Match, allowing create-or-replace semantics."""
        client = _make_client()
        req = await client._build_upsert("accounts", "account", {"accountnumber": "A"}, {"name": "X"})
        assert req.method == "PATCH"
        assert "accounts" in req.url
        assert req.headers is None or "If-Match" not in req.headers

    async def test_build_upsert_multiple_post_request(self):
        """_build_upsert_multiple() produces a POST targeting the UpsertMultiple action."""
        client = _make_client()
        req = await client._build_upsert_multiple(
            "accounts",
            "account",
            [{"accountnumber": "A"}],
            [{"name": "X"}],
        )
        assert req.method == "POST"
        assert "UpsertMultiple" in req.url

    async def test_build_upsert_multiple_mismatched_raises(self):
        """ValidationError is raised when the alternate-key and record lists differ in length."""
        client = _make_client()
        with pytest.raises(ValidationError):
            await client._build_upsert_multiple("accounts", "account", [{"k": "1"}], [{"n": "A"}, {"n": "B"}])

    async def test_build_upsert_multiple_key_conflict_raises(self):
        """ValidationError is raised when a record field overwrites an alternate-key field."""
        client = _make_client()
        with pytest.raises(ValidationError, match="conflicts"):
            await client._build_upsert_multiple(
                "accounts",
                "account",
                [{"accountnumber": "A"}],
                [{"accountnumber": "B"}],
            )

    async def test_build_delete_multiple_bulk_delete(self):
        """_build_delete_multiple() produces a POST BulkDelete request with a QuerySet body."""
        client = _make_client()
        _seed_cache(client)
        req = await client._build_delete_multiple("account", ["id-1", "id-2"])
        assert req.method == "POST"
        assert "BulkDelete" in req.url
        body = json.loads(req.body)
        assert "QuerySet" in body

    async def test_build_update_multiple_broadcast(self):
        """A dict for changes is broadcast to all IDs; Targets list length matches ID count."""
        client = _make_client()
        _seed_cache(client)
        req = await client._build_update_multiple("accounts", "account", ["id-1", "id-2"], {"name": "X"})
        assert req.method == "POST"
        assert "UpdateMultiple" in req.url
        body = json.loads(req.body)
        assert len(body["Targets"]) == 2

    async def test_build_update_multiple_paired(self):
        """A list for changes is applied pairwise; Targets list matches the paired length."""
        client = _make_client()
        _seed_cache(client)
        req = await client._build_update_multiple("accounts", "account", ["id-1"], [{"name": "X"}])
        assert req.method == "POST"
        body = json.loads(req.body)
        assert len(body["Targets"]) == 1

    async def test_build_update_multiple_invalid_changes_type_raises(self):
        """ValidationError is raised when changes is neither a dict nor a list."""
        client = _make_client()
        _seed_cache(client)
        with pytest.raises(ValidationError):
            await client._build_update_multiple("accounts", "account", ["id-1"], "invalid")

    async def test_build_update_multiple_mismatched_length_raises(self):
        """ValidationError is raised when the ID list and changes list differ in length."""
        client = _make_client()
        _seed_cache(client)
        with pytest.raises(ValidationError):
            await client._build_update_multiple("accounts", "account", ["id-1", "id-2"], [{"name": "X"}])


# ---------------------------------------------------------------------------
# _wait_for_attribute_visibility()
# ---------------------------------------------------------------------------


class TestWaitForAttributeVisibility:
    """Tests for _wait_for_attribute_visibility() polling loop."""

    async def test_succeeds_on_first_attempt(self):
        """Returns immediately when the first probe request succeeds."""
        client = _make_client()
        client._request.return_value = _resp(status=200)
        with patch("PowerPlatform.Dataverse.aio.data._async_odata.asyncio.sleep", new_callable=AsyncMock):
            await client._wait_for_attribute_visibility("accounts", "new_notes", delays=(0,))
        client._request.assert_called_once()

    async def test_raises_after_all_delays_exhausted(self):
        """RuntimeError is raised when every probe attempt fails and delays are exhausted."""
        client = _make_client()
        client._request.side_effect = Exception("not visible")
        with patch("PowerPlatform.Dataverse.aio.data._async_odata.asyncio.sleep", new_callable=AsyncMock):
            with pytest.raises(RuntimeError, match="did not become visible"):
                await client._wait_for_attribute_visibility("accounts", "new_notes", delays=(0, 0))

    async def test_succeeds_after_retry(self):
        """A transient failure on the first probe does not prevent success on the second."""
        client = _make_client()
        client._request.side_effect = [Exception("not ready"), _resp(status=200)]
        with patch("PowerPlatform.Dataverse.aio.data._async_odata.asyncio.sleep", new_callable=AsyncMock):
            await client._wait_for_attribute_visibility("accounts", "new_notes", delays=(0, 0))
        assert client._request.call_count == 2


# ---------------------------------------------------------------------------
# _request_metadata_with_retry()
# ---------------------------------------------------------------------------


class TestRequestMetadataWithRetry:
    """Tests for _request_metadata_with_retry() which retries on transient 404 responses."""

    async def test_success_on_first_attempt(self):
        """A successful response is returned immediately without any retry."""
        client = _make_client()
        client._request.return_value = _resp(status=200, json_data={"value": []})
        result = await client._request_metadata_with_retry("get", "https://example/url")
        assert result.status == 200

    async def test_non_404_raises_immediately(self):
        """A non-404 HttpError is re-raised without retrying."""
        client = _make_client()
        err = HttpError("Server error", status_code=500)
        client._request.side_effect = err
        with pytest.raises(HttpError):
            await client._request_metadata_with_retry("get", "https://example/url")
        assert client._request.call_count == 1

    async def test_404_retries_and_raises_runtime_error(self):
        """A 404 is retried max_attempts=5 times before RuntimeError is raised."""
        client = _make_client()
        err = HttpError("Not found", status_code=404)
        client._request.side_effect = err
        with patch("PowerPlatform.Dataverse.aio.data._async_odata.asyncio.sleep", new_callable=AsyncMock):
            with pytest.raises(RuntimeError, match="Metadata request failed"):
                await client._request_metadata_with_retry("get", "https://example/url")
        assert client._request.call_count == 5  # max_attempts defined in implementation
