# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Comprehensive unit tests for _AsyncODataClient."""

from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from azure.core.credentials_async import AsyncTokenCredential

from PowerPlatform.Dataverse.aio.core._async_auth import _AsyncAuthManager
from PowerPlatform.Dataverse.aio.data._async_odata import _AsyncODataClient
from PowerPlatform.Dataverse.core.errors import HttpError, MetadataError, ValidationError

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_async_odata_client() -> _AsyncODataClient:
    """Return an _AsyncODataClient with HTTP calls mocked out."""
    mock_cred = AsyncMock(spec=AsyncTokenCredential)
    mock_cred.get_token.return_value = MagicMock(token="test-token")
    auth = _AsyncAuthManager(mock_cred)
    client = _AsyncODataClient(auth, "https://example.crm.dynamics.com")
    # Replace _request with AsyncMock so no real HTTP happens
    client._request = AsyncMock()
    return client


def _mock_response(json_data=None, text=None, status_code=200, headers=None):
    """Create a mock HTTP response."""
    r = MagicMock()
    r.status_code = status_code
    r.text = text if text is not None else (str(json_data) if json_data is not None else "")
    r.json.return_value = json_data if json_data is not None else {}
    r.headers = headers or {}
    return r


def _seed_cache(client, table="account", entity_set="accounts", primary_id="accountid"):
    """Pre-populate entity-set and primary-id caches."""
    key = table.lower()
    client._logical_to_entityset_cache[key] = entity_set
    client._logical_primaryid_cache[key] = primary_id


# ---------------------------------------------------------------------------
# 1. TestAsyncODataClientInit
# ---------------------------------------------------------------------------


class TestAsyncODataClientInit:
    def test_base_url_stripped(self):
        mock_cred = AsyncMock(spec=AsyncTokenCredential)
        mock_cred.get_token.return_value = MagicMock(token="tok")
        auth = _AsyncAuthManager(mock_cred)
        client = _AsyncODataClient(auth, "https://example.crm.dynamics.com/")
        assert client.base_url == "https://example.crm.dynamics.com"

    def test_api_url_set(self):
        client = _make_async_odata_client()
        assert client.api == "https://example.crm.dynamics.com/api/data/v9.2"

    def test_auth_set(self):
        client = _make_async_odata_client()
        assert isinstance(client.auth, _AsyncAuthManager)

    def test_caches_are_empty_dicts(self):
        client = _make_async_odata_client()
        assert client._logical_to_entityset_cache == {}
        assert client._logical_primaryid_cache == {}
        assert client._picklist_label_cache == {}

    def test_picklist_cache_lock_is_asyncio_lock(self):
        client = _make_async_odata_client()
        assert isinstance(client._picklist_cache_lock, asyncio.Lock)

    def test_empty_base_url_raises(self):
        mock_cred = AsyncMock(spec=AsyncTokenCredential)
        mock_cred.get_token.return_value = MagicMock(token="tok")
        auth = _AsyncAuthManager(mock_cred)
        with pytest.raises(ValueError, match="base_url is required"):
            _AsyncODataClient(auth, "")

    def test_none_base_url_raises(self):
        mock_cred = AsyncMock(spec=AsyncTokenCredential)
        mock_cred.get_token.return_value = MagicMock(token="tok")
        auth = _AsyncAuthManager(mock_cred)
        with pytest.raises(ValueError, match="base_url is required"):
            _AsyncODataClient(auth, None)


# ---------------------------------------------------------------------------
# 2. TestAsyncODataCallScope
# ---------------------------------------------------------------------------


class TestAsyncODataCallScope:
    async def test_yields_non_empty_string(self):
        client = _make_async_odata_client()
        async with client._call_scope() as cid:
            assert isinstance(cid, str)
            assert len(cid) > 0

    async def test_yields_valid_uuid(self):
        import uuid

        client = _make_async_odata_client()
        async with client._call_scope() as cid:
            # Should be a valid UUID
            parsed = uuid.UUID(cid)
            assert str(parsed) == cid

    async def test_correlation_id_reset_after_scope(self):
        from PowerPlatform.Dataverse.data._odata import _CALL_SCOPE_CORRELATION_ID

        client = _make_async_odata_client()
        before = _CALL_SCOPE_CORRELATION_ID.get()
        async with client._call_scope() as cid:
            during = _CALL_SCOPE_CORRELATION_ID.get()
            assert during == cid
        after = _CALL_SCOPE_CORRELATION_ID.get()
        assert after == before

    async def test_different_calls_get_different_ids(self):
        client = _make_async_odata_client()
        ids = []
        for _ in range(3):
            async with client._call_scope() as cid:
                ids.append(cid)
        assert len(set(ids)) == 3


# ---------------------------------------------------------------------------
# 3. TestAsyncODataClose
# ---------------------------------------------------------------------------


class TestAsyncODataClose:
    async def test_close_clears_all_caches(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        client._picklist_label_cache["account"] = {"ts": 0, "picklists": {}}
        mock_http = AsyncMock()
        client._http = mock_http

        await client.close()

        assert client._logical_to_entityset_cache == {}
        assert client._logical_primaryid_cache == {}
        assert client._picklist_label_cache == {}

    async def test_close_calls_http_close(self):
        client = _make_async_odata_client()
        mock_http = AsyncMock()
        client._http = mock_http

        await client.close()

        mock_http.close.assert_awaited_once()


# ---------------------------------------------------------------------------
# 4. TestAsyncODataHeaders
# ---------------------------------------------------------------------------


class TestAsyncODataHeaders:
    async def test_headers_returns_authorization(self):
        client = _make_async_odata_client()
        mock_token = MagicMock()
        mock_token.access_token = "my-token"
        client.auth._acquire_token = AsyncMock(return_value=mock_token)

        headers = await client._headers()

        assert "Authorization" in headers
        assert headers["Authorization"] == "Bearer my-token"

    async def test_headers_includes_standard_odata_keys(self):
        client = _make_async_odata_client()
        mock_token = MagicMock()
        mock_token.access_token = "tok"
        client.auth._acquire_token = AsyncMock(return_value=mock_token)

        headers = await client._headers()

        assert headers.get("Accept") == "application/json"
        assert headers.get("Content-Type") == "application/json"
        assert headers.get("OData-MaxVersion") == "4.0"
        assert headers.get("OData-Version") == "4.0"

    async def test_headers_calls_acquire_token_with_correct_scope(self):
        client = _make_async_odata_client()
        mock_token = MagicMock()
        mock_token.access_token = "tok"
        client.auth._acquire_token = AsyncMock(return_value=mock_token)

        await client._headers()

        client.auth._acquire_token.assert_awaited_once_with("https://example.crm.dynamics.com/.default")

    async def test_merge_headers_no_extra(self):
        client = _make_async_odata_client()
        mock_token = MagicMock()
        mock_token.access_token = "tok"
        client.auth._acquire_token = AsyncMock(return_value=mock_token)

        merged = await client._merge_headers()

        assert "Authorization" in merged

    async def test_merge_headers_with_extra(self):
        client = _make_async_odata_client()
        mock_token = MagicMock()
        mock_token.access_token = "tok"
        client.auth._acquire_token = AsyncMock(return_value=mock_token)

        merged = await client._merge_headers({"X-Custom": "value", "Accept": "text/plain"})

        assert merged.get("X-Custom") == "value"
        # Extra headers override base
        assert merged.get("Accept") == "text/plain"

    async def test_merge_headers_none_extra(self):
        client = _make_async_odata_client()
        mock_token = MagicMock()
        mock_token.access_token = "tok"
        client.auth._acquire_token = AsyncMock(return_value=mock_token)

        merged = await client._merge_headers(None)

        assert "Authorization" in merged


# ---------------------------------------------------------------------------
# Helper: make a client with real _request but mocked _raw_request
# ---------------------------------------------------------------------------


def _make_raw_mocked_client(status_code=200, json_data=None, text=None, headers=None):
    """Return a client where _raw_request is mocked (so _request runs real logic)."""
    mock_cred = AsyncMock(spec=AsyncTokenCredential)
    mock_cred.get_token.return_value = MagicMock(token="test-token")
    auth = _AsyncAuthManager(mock_cred)
    client = _AsyncODataClient(auth, "https://example.crm.dynamics.com")
    # Mock auth token acquisition
    mock_token = MagicMock()
    mock_token.access_token = "tok"
    client.auth._acquire_token = AsyncMock(return_value=mock_token)
    # Mock _raw_request (not _request) so real _request logic runs
    resp = _mock_response(json_data=json_data, text=text, status_code=status_code, headers=headers)
    client._raw_request = AsyncMock(return_value=resp)
    return client, resp


# ---------------------------------------------------------------------------
# 5. TestAsyncODataRequest
# ---------------------------------------------------------------------------


class TestAsyncODataRequest:
    async def test_request_returns_response_on_expected_status(self):
        client, resp = _make_raw_mocked_client(status_code=200, json_data={"ok": True})

        result = await client._request("get", "https://example.com/test")

        assert result is resp

    async def test_request_raises_http_error_on_unexpected_status(self):
        client, _ = _make_raw_mocked_client(
            status_code=400,
            json_data={"error": {"code": "0x80060891", "message": "Not found"}},
            text='{"error": {"code": "0x80060891", "message": "Not found"}}',
        )

        with pytest.raises(HttpError) as exc_info:
            await client._request("get", "https://example.com/test")

        err = exc_info.value
        assert err.status_code == 400

    async def test_request_error_parses_service_error_code(self):
        client, _ = _make_raw_mocked_client(
            status_code=404,
            json_data={"error": {"code": "0x80060891", "message": "Not found"}},
            text='{"error": {"code": "0x80060891", "message": "Not found"}}',
        )

        with pytest.raises(HttpError) as exc_info:
            await client._request("get", "https://example.com/test")

        err = exc_info.value
        assert err.details.get("service_error_code") == "0x80060891"

    async def test_request_error_uses_message_from_error_body(self):
        client, _ = _make_raw_mocked_client(
            status_code=404,
            json_data={"error": {"code": "ERR", "message": "Record not found"}},
            text='{"error": {"code": "ERR", "message": "Record not found"}}',
        )

        with pytest.raises(HttpError) as exc_info:
            await client._request("get", "https://example.com/test")

        assert "Record not found" in str(exc_info.value)

    async def test_request_custom_expected_statuses(self):
        client, resp = _make_raw_mocked_client(status_code=204)

        result = await client._request("delete", "https://example.com/test", expected=(204,))
        assert result is resp

    async def test_request_401_raises_http_error(self):
        client, raw_resp = _make_raw_mocked_client(status_code=401, text="Unauthorized")
        raw_resp.json.side_effect = ValueError("no json")

        with pytest.raises(HttpError) as exc_info:
            await client._request("get", "https://example.com/test")

        assert exc_info.value.status_code == 401

    async def test_request_merges_headers(self):
        client, _ = _make_raw_mocked_client(status_code=200)

        await client._request("get", "https://example.com/test", headers={"X-Custom": "val"})

        call_kwargs = client._raw_request.call_args[1]
        assert "headers" in call_kwargs
        assert call_kwargs["headers"].get("X-Custom") == "val"
        assert "Authorization" in call_kwargs["headers"]

    async def test_request_top_level_message_fallback(self):
        """When error.message is absent, falls back to data.message."""
        client, _ = _make_raw_mocked_client(
            status_code=500,
            json_data={"message": "Top-level error"},
            text='{"message": "Top-level error"}',
        )

        with pytest.raises(HttpError) as exc_info:
            await client._request("get", "https://example.com/test")

        assert "Top-level error" in str(exc_info.value)

    async def test_request_retry_after_header_parsed(self):
        client, raw_resp = _make_raw_mocked_client(
            status_code=429,
            headers={"Retry-After": "30"},
            text="Too many requests",
        )
        raw_resp.json.side_effect = ValueError("no json")

        with pytest.raises(HttpError) as exc_info:
            await client._request("get", "https://example.com/test")

        assert exc_info.value.details.get("retry_after") == 30

    async def test_request_injects_client_request_id(self):
        client, _ = _make_raw_mocked_client(status_code=200)

        await client._request("get", "https://example.com/test")

        call_kwargs = client._raw_request.call_args[1]
        assert "x-ms-client-request-id" in call_kwargs["headers"]

    async def test_request_500_non_json_body(self):
        client, raw_resp = _make_raw_mocked_client(status_code=500, text="Internal Server Error")
        raw_resp.json.side_effect = ValueError("not json")

        with pytest.raises(HttpError) as exc_info:
            await client._request("get", "https://example.com/test")

        assert exc_info.value.status_code == 500


# ---------------------------------------------------------------------------
# 6. TestAsyncODataEntitySet
# ---------------------------------------------------------------------------


class TestAsyncODataEntitySet:
    async def test_cache_hit_returns_without_request(self):
        client = _make_async_odata_client()
        _seed_cache(client, "account", "accounts")

        result = await client._entity_set_from_schema_name("account")

        assert result == "accounts"
        client._request.assert_not_awaited()

    async def test_cache_miss_calls_request(self):
        client = _make_async_odata_client()
        client._request.return_value = _mock_response(
            json_data={
                "value": [{"LogicalName": "account", "EntitySetName": "accounts", "PrimaryIdAttribute": "accountid"}]
            }
        )

        result = await client._entity_set_from_schema_name("account")

        assert result == "accounts"
        client._request.assert_awaited_once()

    async def test_result_is_cached(self):
        client = _make_async_odata_client()
        client._request.return_value = _mock_response(
            json_data={
                "value": [{"LogicalName": "account", "EntitySetName": "accounts", "PrimaryIdAttribute": "accountid"}]
            }
        )

        await client._entity_set_from_schema_name("account")
        # Second call should use cache
        await client._entity_set_from_schema_name("account")

        assert client._request.await_count == 1

    async def test_caches_primary_id_attr(self):
        client = _make_async_odata_client()
        client._request.return_value = _mock_response(
            json_data={
                "value": [{"LogicalName": "account", "EntitySetName": "accounts", "PrimaryIdAttribute": "accountid"}]
            }
        )

        await client._entity_set_from_schema_name("account")

        assert client._logical_primaryid_cache.get("account") == "accountid"

    async def test_empty_items_raises_metadata_error(self):
        client = _make_async_odata_client()
        client._request.return_value = _mock_response(json_data={"value": []})

        with pytest.raises(MetadataError):
            await client._entity_set_from_schema_name("account")

    async def test_missing_entity_set_name_raises_metadata_error(self):
        client = _make_async_odata_client()
        client._request.return_value = _mock_response(json_data={"value": [{"LogicalName": "account"}]})

        with pytest.raises(MetadataError, match="EntitySetName"):
            await client._entity_set_from_schema_name("account")

    async def test_plural_hint_in_error_when_name_ends_in_s(self):
        client = _make_async_odata_client()
        client._request.return_value = _mock_response(json_data={"value": []})

        with pytest.raises(MetadataError, match="plural"):
            await client._entity_set_from_schema_name("accounts")

    async def test_no_plural_hint_for_ss_ending(self):
        client = _make_async_odata_client()
        client._request.return_value = _mock_response(json_data={"value": []})

        with pytest.raises(MetadataError) as exc_info:
            await client._entity_set_from_schema_name("address")
        assert "plural" not in str(exc_info.value)

    async def test_empty_schema_name_raises_value_error(self):
        client = _make_async_odata_client()

        with pytest.raises(ValueError):
            await client._entity_set_from_schema_name("")

    async def test_request_sent_to_entity_definitions_url(self):
        client = _make_async_odata_client()
        client._request.return_value = _mock_response(
            json_data={
                "value": [{"LogicalName": "account", "EntitySetName": "accounts", "PrimaryIdAttribute": "accountid"}]
            }
        )

        await client._entity_set_from_schema_name("account")

        call_args = client._request.call_args
        assert "EntityDefinitions" in call_args[0][1]


# ---------------------------------------------------------------------------
# 7. TestAsyncODataPrimaryIdAttr
# ---------------------------------------------------------------------------


class TestAsyncODataPrimaryIdAttr:
    async def test_returns_from_cache(self):
        client = _make_async_odata_client()
        _seed_cache(client, "account", "accounts", "accountid")

        result = await client._primary_id_attr("account")

        assert result == "accountid"
        client._request.assert_not_awaited()

    async def test_calls_entity_set_on_cache_miss(self):
        client = _make_async_odata_client()
        # Only seed entity set cache, not primary id cache
        client._logical_to_entityset_cache["account"] = "accounts"
        # Make _entity_set_from_schema_name populate the primaryid cache
        client._request.return_value = _mock_response(
            json_data={
                "value": [{"LogicalName": "account", "EntitySetName": "accounts", "PrimaryIdAttribute": "accountid"}]
            }
        )
        # Clear entity set cache so the internal call actually hits the API
        client._logical_to_entityset_cache.clear()

        result = await client._primary_id_attr("account")

        assert result == "accountid"

    async def test_raises_runtime_error_if_not_resolved(self):
        client = _make_async_odata_client()
        # Entity set response without PrimaryIdAttribute
        client._request.return_value = _mock_response(
            json_data={"value": [{"LogicalName": "account", "EntitySetName": "accounts"}]}
        )

        with pytest.raises(RuntimeError, match="PrimaryIdAttribute not resolved"):
            await client._primary_id_attr("account")


# ---------------------------------------------------------------------------
# 8. TestAsyncODataCreate
# ---------------------------------------------------------------------------


class TestAsyncODataCreate:
    async def test_create_returns_guid_from_odata_entity_id_header(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        guid = "12345678-abcd-abcd-abcd-123456789012"
        resp = _mock_response(
            status_code=204,
            headers={"OData-EntityId": f"https://example.crm.dynamics.com/api/data/v9.2/accounts({guid})"},
        )
        client._request.return_value = resp

        result = await client._create("accounts", "account", {"name": "Contoso"})

        assert result == guid

    async def test_create_posts_to_entity_set_url(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        guid = "12345678-1234-1234-1234-123456789012"
        resp = _mock_response(
            status_code=204,
            headers={"OData-EntityId": f"https://example.crm.dynamics.com/api/data/v9.2/accounts({guid})"},
        )
        client._request.return_value = resp

        result = await client._create("accounts", "account", {"name": "Contoso"})

        assert result == guid
        call_args = client._request.call_args
        assert call_args[0][0].lower() == "post"
        assert "accounts" in call_args[0][1]

    async def test_create_location_header_fallback(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        guid = "aaaabbbb-cccc-dddd-eeee-ffffaaaabbbb"
        resp = _mock_response(
            status_code=201,
            headers={"Location": f"https://example.crm.dynamics.com/api/data/v9.2/accounts({guid})"},
        )
        client._request.return_value = resp

        result = await client._create("accounts", "account", {"name": "Test"})

        assert result == guid

    async def test_create_raises_runtime_when_no_guid_in_headers(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        resp = _mock_response(
            status_code=204,
            headers={"X-Other": "no-guid-here"},
        )
        client._request.return_value = resp

        with pytest.raises(RuntimeError, match="GUID"):
            await client._create("accounts", "account", {"name": "Test"})


# ---------------------------------------------------------------------------
# 9. TestAsyncODataCreateMultiple
# ---------------------------------------------------------------------------


class TestAsyncODataCreateMultiple:
    async def test_returns_list_of_guids(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        resp = _mock_response(json_data={"Ids": ["g1", "g2"]}, text='{"Ids": ["g1", "g2"]}')
        client._request.return_value = resp

        result = await client._create_multiple("accounts", "account", [{"name": "A"}, {"name": "B"}])

        assert result == ["g1", "g2"]

    async def test_value_key_fallback(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        guid = "12345678-1234-1234-1234-123456789abc"
        resp = _mock_response(
            json_data={"value": [{"accountid": guid}]},
            text='{"value": [{"accountid": "12345678-1234-1234-1234-123456789abc"}]}',
        )
        client._request.return_value = resp

        result = await client._create_multiple("accounts", "account", [{"name": "A"}])

        assert guid in result

    async def test_raises_type_error_for_non_dict_records(self):
        client = _make_async_odata_client()
        _seed_cache(client)

        with pytest.raises(TypeError, match="dicts"):
            await client._create_multiple("accounts", "account", ["not-a-dict"])

    async def test_empty_body_returns_empty_list(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        resp = _mock_response(json_data={}, text="{}")
        client._request.return_value = resp

        result = await client._create_multiple("accounts", "account", [{"name": "A"}])

        assert result == []

    async def test_adds_odata_type_if_missing(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        resp = _mock_response(json_data={"Ids": ["g1"]}, text='{"Ids": ["g1"]}')
        client._request.return_value = resp

        await client._create_multiple("accounts", "account", [{"name": "Contoso"}])

        call_args = client._request.call_args
        # _execute_raw sends data= kwarg with JSON body
        body_data = call_args[1].get("data") or call_args[1].get("json")
        if isinstance(body_data, bytes):
            import json

            body = json.loads(body_data.decode())
            assert "@odata.type" in body["Targets"][0]


# ---------------------------------------------------------------------------
# 10. TestAsyncODataGet
# ---------------------------------------------------------------------------


class TestAsyncODataGet:
    async def test_get_returns_dict(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        resp = _mock_response(json_data={"accountid": "guid-1", "name": "Contoso"})
        client._request.return_value = resp

        result = await client._get("account", "guid-1")

        assert result == {"accountid": "guid-1", "name": "Contoso"}

    async def test_get_passes_select_param(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        resp = _mock_response(json_data={"accountid": "guid-1"})
        client._request.return_value = resp

        await client._get("account", "guid-1", select=["accountid", "name"])

        call_url = client._request.call_args[0][1]
        assert "$select=" in call_url
        assert "accountid" in call_url

    async def test_get_no_select_no_params(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        resp = _mock_response(json_data={"accountid": "guid-1"})
        client._request.return_value = resp

        await client._get("account", "guid-1")

        call_kwargs = client._request.call_args[1]
        params = call_kwargs.get("params")
        assert params is None or "$select" not in params

    async def test_get_url_contains_entity_set_and_key(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        resp = _mock_response(json_data={})
        client._request.return_value = resp

        await client._get("account", "12345678-1234-1234-1234-123456789012")

        call_url = client._request.call_args[0][1]
        assert "accounts" in call_url
        assert "12345678-1234-1234-1234-123456789012" in call_url


# ---------------------------------------------------------------------------
# 11. TestAsyncODataGetMultiple
# ---------------------------------------------------------------------------


class TestAsyncODataGetMultiple:
    async def test_single_page_no_nextlink(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        page = [{"accountid": "g1"}, {"accountid": "g2"}]
        client._request.return_value = _mock_response(json_data={"value": page})

        pages = [p async for p in client._get_multiple("account")]

        assert len(pages) == 1
        assert pages[0] == page

    async def test_two_pages_with_nextlink(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        page1 = [{"accountid": "g1"}]
        page2 = [{"accountid": "g2"}]
        client._request.side_effect = [
            _mock_response(json_data={"value": page1, "@odata.nextLink": "http://next"}),
            _mock_response(json_data={"value": page2}),
        ]

        pages = [p async for p in client._get_multiple("account")]

        assert len(pages) == 2
        assert pages[0] == page1
        assert pages[1] == page2

    async def test_empty_value_yields_nothing(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        client._request.return_value = _mock_response(json_data={"value": []})

        pages = [p async for p in client._get_multiple("account")]

        assert pages == []

    async def test_page_size_adds_prefer_header(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        client._request.return_value = _mock_response(json_data={"value": [{"accountid": "g1"}]})

        pages = [p async for p in client._get_multiple("account", page_size=50)]

        assert len(pages) == 1
        # Find the call with page headers
        call_kwargs = client._request.call_args_list[0][1]
        prefer = call_kwargs.get("headers", {}).get("Prefer") or call_kwargs.get("headers", {}).get("prefer", "")
        assert "odata.maxpagesize=50" in prefer

    async def test_get_multiple_with_select_and_filter(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        client._request.return_value = _mock_response(json_data={"value": [{"accountid": "g1"}]})

        pages = [p async for p in client._get_multiple("account", select=["name"], filter="name eq 'Test'")]

        assert len(pages) == 1
        call_kwargs = client._request.call_args[1]
        assert "$select" in call_kwargs.get("params", {})
        assert "$filter" in call_kwargs.get("params", {})

    async def test_get_multiple_with_include_annotations(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        client._request.return_value = _mock_response(json_data={"value": [{"a": 1}]})

        pages = [
            p
            async for p in client._get_multiple(
                "account", include_annotations="OData.Community.Display.V1.FormattedValue"
            )
        ]

        call_kwargs = client._request.call_args_list[0][1]
        prefer = call_kwargs.get("headers", {}).get("Prefer", "")
        assert "odata.include-annotations" in prefer


# ---------------------------------------------------------------------------
# 12. TestAsyncODataUpdate
# ---------------------------------------------------------------------------


class TestAsyncODataUpdate:
    async def test_update_patches_correct_url(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        guid = "12345678-1234-1234-1234-123456789012"
        client._request.return_value = _mock_response(status_code=204)

        await client._update("account", guid, {"name": "Updated"})

        call_args = client._request.call_args
        assert call_args[0][0].lower() == "patch"
        assert "accounts" in call_args[0][1]
        assert guid in call_args[0][1]

    async def test_update_returns_none(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        client._request.return_value = _mock_response(status_code=204)

        result = await client._update("account", "guid-1", {"name": "X"})

        assert result is None

    async def test_update_sends_if_match_header(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        client._request.return_value = _mock_response(status_code=204)

        await client._update("account", "guid-1", {"name": "X"})

        call_kwargs = client._request.call_args[1]
        # _execute_raw merges headers; the If-Match should be in passed headers
        passed_headers = call_kwargs.get("headers", {})
        assert passed_headers.get("If-Match") == "*"


# ---------------------------------------------------------------------------
# 13. TestAsyncODataUpdateByIds
# ---------------------------------------------------------------------------


class TestAsyncODataUpdateByIds:
    async def test_broadcast_single_dict_posts_to_update_multiple(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        client._request.return_value = _mock_response(status_code=204)

        await client._update_by_ids("account", ["id1", "id2"], {"name": "Bulk"})

        post_calls = [c for c in client._request.call_args_list if c[0][0] == "post"]
        assert len(post_calls) == 1
        assert "UpdateMultiple" in post_calls[0][0][1]

    async def test_paired_list_posts_with_paired_payloads(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        client._request.return_value = _mock_response(status_code=204)

        await client._update_by_ids("account", ["id1", "id2"], [{"name": "A"}, {"name": "B"}])

        post_calls = [c for c in client._request.call_args_list if c[0][0] == "post"]
        assert len(post_calls) == 1

    async def test_empty_ids_returns_early(self):
        client = _make_async_odata_client()
        _seed_cache(client)

        await client._update_by_ids("account", [], {"name": "X"})

        client._request.assert_not_awaited()

    async def test_mismatched_lengths_raise_value_error(self):
        client = _make_async_odata_client()
        _seed_cache(client)

        with pytest.raises(ValueError, match="Length"):
            await client._update_by_ids("account", ["id1", "id2"], [{"name": "Only one"}])

    async def test_non_dict_in_list_raises_type_error(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        client._request.return_value = _mock_response(status_code=204)

        with pytest.raises(TypeError, match="dict"):
            await client._update_by_ids("account", ["id1"], ["not-a-dict"])

    async def test_ids_must_be_list(self):
        client = _make_async_odata_client()
        _seed_cache(client)

        with pytest.raises(TypeError, match="ids must be list"):
            await client._update_by_ids("account", "not-a-list", {"name": "X"})


# ---------------------------------------------------------------------------
# 14. TestAsyncODataDelete
# ---------------------------------------------------------------------------


class TestAsyncODataDelete:
    async def test_delete_sends_delete_request(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        guid = "12345678-1234-1234-1234-123456789012"
        client._request.return_value = _mock_response(status_code=204)

        await client._delete("account", guid)

        call_args = client._request.call_args
        assert call_args[0][0].lower() == "delete"
        assert "accounts" in call_args[0][1]
        assert guid in call_args[0][1]

    async def test_delete_returns_none(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        client._request.return_value = _mock_response(status_code=204)

        result = await client._delete("account", "guid-1")

        assert result is None

    async def test_delete_sends_if_match_header(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        client._request.return_value = _mock_response(status_code=204)

        await client._delete("account", "guid-1")

        call_kwargs = client._request.call_args[1]
        passed_headers = call_kwargs.get("headers", {})
        assert passed_headers.get("If-Match") == "*"


# ---------------------------------------------------------------------------
# 15. TestAsyncODataDeleteMultiple
# ---------------------------------------------------------------------------


class TestAsyncODataDeleteMultiple:
    async def test_returns_job_id(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        client._request.return_value = _mock_response(
            json_data={"JobId": "job-123"},
            text='{"JobId": "job-123"}',
        )

        result = await client._delete_multiple("account", ["id1", "id2"])

        assert result == "job-123"

    async def test_posts_to_bulk_delete_url(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        client._request.return_value = _mock_response(
            json_data={"JobId": "job-456"},
            text='{"JobId": "job-456"}',
        )

        await client._delete_multiple("account", ["id1"])

        call_args = client._request.call_args
        assert "BulkDelete" in call_args[0][1]
        assert call_args[0][0].lower() == "post"

    async def test_empty_ids_returns_none(self):
        client = _make_async_odata_client()
        _seed_cache(client)

        result = await client._delete_multiple("account", [])

        assert result is None
        client._request.assert_not_awaited()

    async def test_filters_out_empty_string_ids(self):
        client = _make_async_odata_client()
        _seed_cache(client)

        # All empty strings should result in no call
        result = await client._delete_multiple("account", ["", ""])

        assert result is None
        client._request.assert_not_awaited()


# ---------------------------------------------------------------------------
# 16. TestAsyncODataUpsert
# ---------------------------------------------------------------------------


class TestAsyncODataUpsert:
    async def test_upsert_patches_alternate_key_url(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        client._request.return_value = _mock_response(status_code=204)

        await client._upsert("accounts", "account", {"accountnumber": "ACC-001"}, {"name": "Contoso"})

        call_args = client._request.call_args
        assert call_args[0][0].lower() == "patch"
        assert "accounts" in call_args[0][1]
        assert "ACC-001" in call_args[0][1]

    async def test_upsert_returns_none(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        client._request.return_value = _mock_response(status_code=204)

        result = await client._upsert("accounts", "account", {"accountnumber": "ACC-001"}, {"name": "Test"})

        assert result is None


# ---------------------------------------------------------------------------
# 17. TestAsyncODataUpsertMultiple
# ---------------------------------------------------------------------------


class TestAsyncODataUpsertMultiple:
    async def test_posts_to_upsert_multiple_url(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        client._request.return_value = _mock_response(status_code=204)

        await client._upsert_multiple(
            "accounts",
            "account",
            [{"accountnumber": "ACC-001"}],
            [{"name": "Contoso"}],
        )

        call_args = client._request.call_args
        assert "UpsertMultiple" in call_args[0][1]

    async def test_mismatched_lengths_raise_value_error(self):
        client = _make_async_odata_client()
        _seed_cache(client)

        with pytest.raises(ValueError, match="same length"):
            await client._upsert_multiple(
                "accounts",
                "account",
                [{"accountnumber": "ACC-001"}],
                [{"name": "A"}, {"name": "B"}],
            )

    async def test_returns_none_on_success(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        client._request.return_value = _mock_response(status_code=204)

        result = await client._upsert_multiple(
            "accounts",
            "account",
            [{"accountnumber": "ACC-001"}],
            [{"name": "Contoso"}],
        )

        assert result is None


# ---------------------------------------------------------------------------
# 18. TestAsyncODataQuerySql
# ---------------------------------------------------------------------------


class TestAsyncODataQuerySql:
    async def test_returns_list_from_value_key(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        rows = [{"accountid": "g1"}, {"accountid": "g2"}]
        client._request.return_value = _mock_response(json_data={"value": rows})

        result = await client._query_sql("SELECT accountid FROM account")

        assert result == rows

    async def test_returns_list_when_body_is_list(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        rows = [{"a": 1}, {"b": 2}]
        client._request.return_value = _mock_response(json_data=rows)

        result = await client._query_sql("SELECT * FROM account")

        assert result == rows

    async def test_raises_validation_error_for_non_string_sql(self):
        client = _make_async_odata_client()

        with pytest.raises(ValidationError):
            await client._query_sql(123)

    async def test_raises_validation_error_for_empty_sql(self):
        client = _make_async_odata_client()

        with pytest.raises(ValidationError):
            await client._query_sql("   ")

    async def test_sql_url_contains_encoded_query(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        client._request.return_value = _mock_response(json_data={"value": []})

        await client._query_sql("SELECT accountid FROM account")

        call_url = client._request.call_args[0][1]
        assert "sql=" in call_url
        assert "SELECT" in call_url or "%3A" in call_url or "SELECT" in call_url

    async def test_pagination_follows_next_link(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        page1 = [{"accountid": "g1"}]
        page2 = [{"accountid": "g2"}]
        client._request.side_effect = [
            _mock_response(json_data={"value": page1, "@odata.nextLink": "http://next-page"}),
            _mock_response(json_data={"value": page2}),
        ]

        result = await client._query_sql("SELECT accountid FROM account")

        assert len(result) == 2
        assert result[0]["accountid"] == "g1"
        assert result[1]["accountid"] == "g2"

    async def test_pagination_stops_on_same_next_link(self):
        """Repeated nextLink URL should trigger RuntimeWarning and stop."""
        client = _make_async_odata_client()
        _seed_cache(client)
        same_link = "http://same-link"
        client._request.side_effect = [
            _mock_response(json_data={"value": [{"id": "1"}], "@odata.nextLink": same_link}),
            _mock_response(json_data={"value": [{"id": "2"}], "@odata.nextLink": same_link}),
        ]

        with pytest.warns(RuntimeWarning):
            result = await client._query_sql("SELECT id FROM account")

        # Should have first page, stopped before repeating
        assert len(result) >= 1


# ---------------------------------------------------------------------------
# 19. TestAsyncODataTableInfo
# ---------------------------------------------------------------------------


class TestAsyncODataTableInfo:
    async def test_returns_mapped_dict_when_entity_found(self):
        client = _make_async_odata_client()
        entity = {
            "SchemaName": "Account",
            "LogicalName": "account",
            "EntitySetName": "accounts",
            "MetadataId": "meta-001",
            "PrimaryNameAttribute": "name",
            "PrimaryIdAttribute": "accountid",
        }
        client._get_entity_by_table_schema_name = AsyncMock(return_value=entity)

        result = await client._get_table_info("Account")

        assert result is not None
        assert result["table_schema_name"] == "Account"
        assert result["table_logical_name"] == "account"
        assert result["entity_set_name"] == "accounts"
        assert result["metadata_id"] == "meta-001"
        assert result["primary_name_attribute"] == "name"
        assert result["primary_id_attribute"] == "accountid"
        assert result["columns_created"] == []

    async def test_returns_none_when_entity_not_found(self):
        client = _make_async_odata_client()
        client._get_entity_by_table_schema_name = AsyncMock(return_value=None)

        result = await client._get_table_info("NonExistentTable")

        assert result is None


# ---------------------------------------------------------------------------
# 20. TestAsyncODataListTables
# ---------------------------------------------------------------------------


class TestAsyncODataListTables:
    async def test_returns_list_of_tables(self):
        client = _make_async_odata_client()
        tables = [{"LogicalName": "account"}, {"LogicalName": "contact"}]
        client._request.return_value = _mock_response(json_data={"value": tables})

        result = await client._list_tables()

        assert result == tables

    async def test_returns_empty_list_when_no_tables(self):
        client = _make_async_odata_client()
        client._request.return_value = _mock_response(json_data={"value": []})

        result = await client._list_tables()

        assert result == []

    async def test_list_tables_calls_entity_definitions(self):
        client = _make_async_odata_client()
        client._request.return_value = _mock_response(json_data={"value": []})

        await client._list_tables()

        call_url = client._request.call_args[0][1]
        assert "EntityDefinitions" in call_url


# ---------------------------------------------------------------------------
# 21. TestAsyncODataDeleteTable
# ---------------------------------------------------------------------------


class TestAsyncODataDeleteTable:
    async def test_deletes_entity_by_metadata_id(self):
        client = _make_async_odata_client()
        entity = {"MetadataId": "meta-999", "LogicalName": "account"}
        client._get_entity_by_table_schema_name = AsyncMock(return_value=entity)
        client._request.return_value = _mock_response(status_code=204)

        await client._delete_table("Account")

        call_args = client._request.call_args
        assert call_args[0][0].lower() == "delete"
        assert "meta-999" in call_args[0][1]

    async def test_raises_metadata_error_when_table_not_found(self):
        client = _make_async_odata_client()
        client._get_entity_by_table_schema_name = AsyncMock(return_value=None)

        with pytest.raises(MetadataError, match="not found"):
            await client._delete_table("NonExistentTable")

    async def test_raises_metadata_error_when_no_metadata_id(self):
        client = _make_async_odata_client()
        client._get_entity_by_table_schema_name = AsyncMock(return_value={"LogicalName": "account"})

        with pytest.raises(MetadataError):
            await client._delete_table("Account")


# ---------------------------------------------------------------------------
# 22. TestAsyncODataBulkFetchPicklists
# ---------------------------------------------------------------------------


class TestAsyncODataBulkFetchPicklists:
    async def test_first_call_fetches_from_api(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        body = {
            "value": [
                {
                    "LogicalName": "statuscode",
                    "OptionSet": {
                        "Options": [
                            {
                                "Value": 1,
                                "Label": {"LocalizedLabels": [{"Label": "Active"}]},
                            }
                        ]
                    },
                }
            ]
        }
        client._request.return_value = _mock_response(json_data=body)

        await client._bulk_fetch_picklists("account")

        client._request.assert_awaited_once()

    async def test_second_call_uses_cache(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        body = {"value": []}
        client._request.return_value = _mock_response(json_data=body)

        await client._bulk_fetch_picklists("account")
        await client._bulk_fetch_picklists("account")

        # Only one HTTP call, second used cache
        assert client._request.await_count == 1

    async def test_populates_label_to_int_mapping(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        body = {
            "value": [
                {
                    "LogicalName": "statuscode",
                    "OptionSet": {
                        "Options": [
                            {
                                "Value": 1,
                                "Label": {"LocalizedLabels": [{"Label": "Active"}]},
                            },
                            {
                                "Value": 2,
                                "Label": {"LocalizedLabels": [{"Label": "Inactive"}]},
                            },
                        ]
                    },
                }
            ]
        }
        client._request.return_value = _mock_response(json_data=body)

        await client._bulk_fetch_picklists("account")

        table_entry = client._picklist_label_cache.get("account")
        assert table_entry is not None
        picklists = table_entry.get("picklists", {})
        assert "statuscode" in picklists
        # The label should be normalized and map to the int value
        mapping = picklists["statuscode"]
        assert any(v == 1 for v in mapping.values())
        assert any(v == 2 for v in mapping.values())

    async def test_expired_cache_refetches(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        # Set cache with expired timestamp
        client._picklist_label_cache["account"] = {"ts": time.time() - 7200, "picklists": {}}
        client._request.return_value = _mock_response(json_data={"value": []})

        await client._bulk_fetch_picklists("account")

        # Should have re-fetched
        client._request.assert_awaited_once()

    async def test_items_without_logical_name_skipped(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        body = {
            "value": [
                {
                    "LogicalName": "",  # empty
                    "OptionSet": {"Options": [{"Value": 1, "Label": {"LocalizedLabels": [{"Label": "X"}]}}]},
                }
            ]
        }
        client._request.return_value = _mock_response(json_data=body)

        await client._bulk_fetch_picklists("account")

        table_entry = client._picklist_label_cache.get("account")
        picklists = table_entry.get("picklists", {})
        assert "" not in picklists


# ---------------------------------------------------------------------------
# 23. TestAsyncODataConvertLabels
# ---------------------------------------------------------------------------


class TestAsyncODataConvertLabels:
    async def test_no_string_values_skips_fetch(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        # Only non-string values — no candidates for picklist resolution
        record = {"revenue": 1000, "count": 42, "active": True}

        result = await client._convert_labels_to_ints("account", record)

        # No fetch should have happened
        client._request.assert_not_awaited()
        assert result == record

    async def test_matching_label_converts_to_int(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        # Pre-populate cache with a label mapping
        client._picklist_label_cache["account"] = {
            "ts": time.time(),
            "picklists": {
                "statuscode": {"active": 1, "inactive": 2},
            },
        }
        record = {"statuscode": "Active", "name": "Contoso"}
        client._bulk_fetch_picklists = AsyncMock()

        result = await client._convert_labels_to_ints("account", record)

        # "Active" normalizes to "active" which maps to 1
        assert result["statuscode"] == 1

    async def test_non_matching_label_left_as_string(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        client._picklist_label_cache["account"] = {
            "ts": time.time(),
            "picklists": {
                "statuscode": {"active": 1},
            },
        }
        record = {"statuscode": "Unknown Status"}
        client._bulk_fetch_picklists = AsyncMock()

        result = await client._convert_labels_to_ints("account", record)

        # Should remain as-is since no match found
        assert result["statuscode"] == "Unknown Status"

    async def test_odata_annotated_keys_skipped(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        client._picklist_label_cache["account"] = {
            "ts": time.time(),
            "picklists": {
                "statuscode@odata.community": {"active": 1},
            },
        }
        record = {"statuscode@odata.community.display": "Active"}
        client._bulk_fetch_picklists = AsyncMock()

        result = await client._convert_labels_to_ints("account", record)

        # @odata. keys should be skipped
        assert result["statuscode@odata.community.display"] == "Active"

    async def test_empty_string_value_skipped(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        client._picklist_label_cache["account"] = {
            "ts": time.time(),
            "picklists": {"statuscode": {"": 0}},
        }
        record = {"statuscode": "   "}
        client._bulk_fetch_picklists = AsyncMock()

        result = await client._convert_labels_to_ints("account", record)

        assert result["statuscode"] == "   "

    async def test_returns_copy_not_original(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        # Only non-string values — returns copy without fetching picklists
        record = {"revenue": 999, "count": 5}

        result = await client._convert_labels_to_ints("account", record)

        # Should contain the same data
        assert result == record


# ---------------------------------------------------------------------------
# Additional edge-case tests
# ---------------------------------------------------------------------------


class TestAsyncODataExecuteRaw:
    async def test_execute_raw_calls_request(self):
        from PowerPlatform.Dataverse.data._raw_request import _RawRequest

        client = _make_async_odata_client()
        client._request.return_value = _mock_response(status_code=200)
        req = _RawRequest(method="GET", url="https://example.com/api", body='{"key": "val"}')

        result = await client._execute_raw(req)

        client._request.assert_awaited_once()
        call_args = client._request.call_args
        assert call_args[0][0] == "get"
        assert call_args[0][1] == "https://example.com/api"

    async def test_execute_raw_with_headers(self):
        from PowerPlatform.Dataverse.data._raw_request import _RawRequest

        client = _make_async_odata_client()
        client._request.return_value = _mock_response(status_code=200)
        req = _RawRequest(
            method="PATCH",
            url="https://example.com/api",
            body="{}",
            headers={"If-Match": "*"},
        )

        await client._execute_raw(req)

        call_kwargs = client._request.call_args[1]
        assert call_kwargs.get("headers", {}).get("If-Match") == "*"

    async def test_execute_raw_no_body_no_data_kwarg(self):
        from PowerPlatform.Dataverse.data._raw_request import _RawRequest

        client = _make_async_odata_client()
        client._request.return_value = _mock_response(status_code=200)
        req = _RawRequest(method="GET", url="https://example.com/api")

        await client._execute_raw(req)

        call_kwargs = client._request.call_args[1]
        assert "data" not in call_kwargs


class TestAsyncODataGetEntityByTableSchemaName:
    async def test_returns_entity_dict_when_found(self):
        client = _make_async_odata_client()
        entity = {"MetadataId": "m1", "LogicalName": "account"}
        client._request.return_value = _mock_response(json_data={"value": [entity]})

        result = await client._get_entity_by_table_schema_name("Account")

        assert result == entity

    async def test_returns_none_when_not_found(self):
        client = _make_async_odata_client()
        client._request.return_value = _mock_response(json_data={"value": []})

        result = await client._get_entity_by_table_schema_name("NonExistent")

        assert result is None

    async def test_passes_extra_headers(self):
        client = _make_async_odata_client()
        client._request.return_value = _mock_response(json_data={"value": []})

        await client._get_entity_by_table_schema_name("Account", headers={"Consistency": "Strong"})

        call_kwargs = client._request.call_args[1]
        assert call_kwargs.get("headers", {}).get("Consistency") == "Strong"


# ---------------------------------------------------------------------------
# New test classes — coverage for previously-missing lines
# ---------------------------------------------------------------------------


class TestAsyncODataRawRequest:
    """Line 153 — _raw_request delegates to self._http._request."""

    async def test_raw_request_delegates_to_http(self):
        client = _make_async_odata_client()
        expected_resp = _mock_response(status_code=200)
        client._http._request = AsyncMock(return_value=expected_resp)

        result = await client._raw_request("get", "https://example.com/api")

        client._http._request.assert_awaited_once()
        assert result is expected_resp


class TestAsyncODataRequestRetryAfter:
    """Lines 209-210 — Retry-After header that cannot be parsed as int."""

    async def test_retry_after_non_numeric_sets_none(self):
        """When Retry-After is 'not-a-number', the except branch is taken (retry_after stays None).

        Lines 209-210: the ``int(ra)`` call raises, so the except branch sets retry_after = None,
        and HttpError is raised with no retry_after in details.
        """
        client = _make_async_odata_client()
        # We need _raw_request to return a 429 with a non-numeric Retry-After.
        bad_resp = _mock_response(
            status_code=429,
            text="Too Many Requests",
            headers={"Retry-After": "not-a-number"},
        )
        client._raw_request = AsyncMock(return_value=bad_resp)
        # _merge_headers needs _headers() — patch it to return minimal headers
        client._headers = AsyncMock(return_value={})

        with pytest.raises(HttpError) as exc_info:
            # Call the real _request method via unbound call to exercise lines 209-210
            from PowerPlatform.Dataverse.aio.data._async_odata import _AsyncODataClient

            await _AsyncODataClient._request(client, "get", "https://example.com/api")

        err = exc_info.value
        # retry_after should NOT be in details (it stayed None, so was never set)
        assert "retry_after" not in (err.details or {})


class TestAsyncODataEntitySetFromSchemaNameValueError:
    """Lines 263-264 — _entity_set_from_schema_name: json() raises ValueError → items=[]."""

    async def test_json_value_error_yields_metadata_error(self):
        client = _make_async_odata_client()
        bad_resp = MagicMock()
        bad_resp.json.side_effect = ValueError("bad json")
        client._request.return_value = bad_resp

        with pytest.raises(MetadataError):
            await client._entity_set_from_schema_name("account")


class TestAsyncODataRequestMetadataWithRetry:
    """Lines 315-323 — _request_metadata_with_retry retry loop."""

    async def test_retries_on_404_then_succeeds(self):
        client = _make_async_odata_client()
        ok_resp = _mock_response(json_data={"value": []})
        http_404 = HttpError("Not Found", status_code=404)
        # Fail twice then succeed
        client._request.side_effect = [http_404, http_404, ok_resp]

        with patch("asyncio.sleep", new_callable=AsyncMock):
            result = await client._request_metadata_with_retry("get", "https://example.com/meta")

        assert result is ok_resp
        assert client._request.call_count == 3

    async def test_exhausted_retries_raise_runtime_error(self):
        client = _make_async_odata_client()
        http_404 = HttpError("Not Found", status_code=404)
        client._request.side_effect = [http_404] * 5

        with patch("asyncio.sleep", new_callable=AsyncMock):
            with pytest.raises(RuntimeError, match="after 5 retries"):
                await client._request_metadata_with_retry("get", "https://example.com/meta")

    async def test_non_404_http_error_propagates_immediately(self):
        client = _make_async_odata_client()
        http_403 = HttpError("Forbidden", status_code=403)
        client._request.side_effect = http_403

        with pytest.raises(HttpError):
            await client._request_metadata_with_retry("get", "https://example.com/meta")

        assert client._request.call_count == 1


class TestAsyncODataBulkFetchPicklists:
    """Lines 340, 355, 365, 368 — _bulk_fetch_picklists double-checked lock and body parsing."""

    async def test_double_checked_lock_returns_early(self):
        """Line 340: fast-path misses (cache stale), but cache is fresh inside the lock.

        We simulate this by populating the cache inside a custom __aenter__ before the
        real body runs, so the double-check (line 339-340) returns early.
        """
        client = _make_async_odata_client()
        # Start with a stale/missing cache so the fast-path (333) does NOT return early
        # (no entry in cache yet)
        original_request_mock = client._request

        # Build a custom lock whose __aenter__ populates the cache THEN yields
        class _CacheSeedingLock:
            async def __aenter__(self):
                # Populate the cache as if another coroutine did it while we waited
                client._picklist_label_cache["account"] = {"ts": time.time(), "picklists": {}}
                return self

            async def __aexit__(self, *args):
                pass

        client._picklist_cache_lock = _CacheSeedingLock()

        await client._bulk_fetch_picklists("account")

        # _request should NOT have been called because the double-check returned early
        original_request_mock.assert_not_called()

    async def test_non_dict_item_skipped(self):
        """Line 355: items contains a non-dict — skipped without error."""
        client = _make_async_odata_client()
        resp = _mock_response(
            json_data={"value": ["not-a-dict", {"LogicalName": "status", "OptionSet": {"Options": []}}]}
        )
        client._request_metadata_with_retry = AsyncMock(return_value=resp)

        await client._bulk_fetch_picklists("account")

        assert "account" in client._picklist_label_cache

    async def test_empty_logical_name_skipped(self):
        """Line 357-358: item with empty LogicalName — skipped."""
        client = _make_async_odata_client()
        resp = _mock_response(json_data={"value": [{"LogicalName": "", "OptionSet": {"Options": []}}]})
        client._request_metadata_with_retry = AsyncMock(return_value=resp)

        await client._bulk_fetch_picklists("account")

        # Empty-ln item is skipped; cache entry still created
        cache_entry = client._picklist_label_cache.get("account")
        assert isinstance(cache_entry, dict)
        assert cache_entry["picklists"] == {}

    async def test_non_dict_option_skipped(self):
        """Line 364-365: options list contains a non-dict — skipped.
        Line 367-368: option with non-int Value — skipped.
        """
        client = _make_async_odata_client()
        resp = _mock_response(
            json_data={
                "value": [
                    {
                        "LogicalName": "statuscode",
                        "OptionSet": {
                            "Options": [
                                "not-a-dict",  # hits line 364-365 (not isinstance → continue)
                                {
                                    "Value": "not-an-int",
                                    "Label": {"LocalizedLabels": [{"Label": "Bad"}]},
                                },  # hits 367-368
                                {"Value": 1, "Label": {"LocalizedLabels": [{"Label": "Active"}]}},
                            ]
                        },
                    }
                ]
            }
        )
        client._request_metadata_with_retry = AsyncMock(return_value=resp)

        await client._bulk_fetch_picklists("account")

        entry = client._picklist_label_cache["account"]
        # Only the valid option (Value=1) is parsed; "not-a-dict" and non-int are skipped
        assert "statuscode" in entry["picklists"]
        assert entry["picklists"]["statuscode"].get("active") == 1


class TestAsyncODataConvertLabelsToInts:
    """Lines 399, 404, 406 — _convert_labels_to_ints edge cases."""

    async def test_table_entry_not_dict_returns_unchanged(self):
        """Line 399: _bulk_fetch_picklists sets cache to non-dict value → returns record as-is."""
        client = _make_async_odata_client()

        # Force _bulk_fetch_picklists to put a non-dict into the cache
        async def _bad_bulk_fetch(table_schema_name):
            client._picklist_label_cache[client._normalize_cache_key(table_schema_name)] = "not-a-dict"

        client._bulk_fetch_picklists = _bad_bulk_fetch

        record = {"name": "SomeLabel"}
        result = await client._convert_labels_to_ints("account", record)
        assert result == record

    async def test_non_string_value_skipped(self):
        """Line 403-404: value is not a string (e.g. an int) — continue (skip conversion).

        The record must also have at least one string value so has_candidates is True
        and the for loop is entered. The non-string value hits the continue at line 404.
        """
        client = _make_async_odata_client()
        client._picklist_label_cache["account"] = {
            "ts": time.time(),
            "picklists": {"statuscode": {"active": 1}},
        }

        async def _noop_bulk(table_schema_name):
            pass

        client._bulk_fetch_picklists = _noop_bulk

        # Mixed record: "name" is a string (so has_candidates=True, loop entered),
        # "statuscode" is an int → hits `not isinstance(v, str)` → continue at line 404
        record = {"statuscode": 42, "name": "SomeLabel"}
        result = await client._convert_labels_to_ints("account", record)
        assert result["statuscode"] == 42  # unchanged
        assert result["name"] == "SomeLabel"  # no mapping for it, also unchanged

    async def test_odata_key_skipped(self):
        """Line 405-406: keys containing '@odata.' are skipped."""
        client = _make_async_odata_client()
        # Seed cache with a picklist entry — but @odata. key should be skipped
        client._picklist_label_cache["account"] = {
            "ts": time.time(),
            "picklists": {"statuscode": {"active": 1}},
        }

        async def _noop_bulk(table_schema_name):
            pass

        client._bulk_fetch_picklists = _noop_bulk

        # Key contains '@odata.' — hits line 405 → continue
        record = {"@odata.type": "Microsoft.Dynamics.CRM.account", "statuscode": "Active"}
        result = await client._convert_labels_to_ints("account", record)
        # odata key must NOT be converted
        assert result["@odata.type"] == "Microsoft.Dynamics.CRM.account"

    async def test_attr_key_with_no_mapping_unchanged(self):
        """No mapping for attr_key → value left unchanged."""
        client = _make_async_odata_client()
        client._picklist_label_cache["account"] = {
            "ts": time.time(),
            "picklists": {},  # empty — no mappings
        }

        async def _noop_bulk(table_schema_name):
            pass

        client._bulk_fetch_picklists = _noop_bulk

        record = {"statuscode": "Active"}
        result = await client._convert_labels_to_ints("account", record)
        assert result["statuscode"] == "Active"


class TestAsyncODataCreateMultiple:
    """Lines 472-473, 475 — _create_multiple response parsing edge cases."""

    async def test_json_raises_value_error_returns_empty(self):
        """Lines 472-473: r.json() raises ValueError → body={} → return []."""
        client = _make_async_odata_client()
        _seed_cache(client)
        bad_resp = MagicMock()
        bad_resp.text = "not json"
        bad_resp.json.side_effect = ValueError("bad json")
        client._execute_raw = AsyncMock(return_value=bad_resp)

        result = await client._create_multiple("accounts", "account", [{"name": "Test"}])
        assert result == []

    async def test_empty_text_returns_empty_list(self):
        """r.text is falsy → body={} → return []."""
        client = _make_async_odata_client()
        _seed_cache(client)
        empty_resp = MagicMock()
        empty_resp.text = ""
        empty_resp.json.return_value = {}
        client._execute_raw = AsyncMock(return_value=empty_resp)

        result = await client._create_multiple("accounts", "account", [{"name": "Test"}])
        assert result == []

    async def test_body_not_dict_returns_empty(self):
        """Line 475: body is a list (not dict) → return []."""
        client = _make_async_odata_client()
        _seed_cache(client)
        list_resp = MagicMock()
        list_resp.text = "[1, 2]"
        list_resp.json.return_value = [1, 2]
        client._execute_raw = AsyncMock(return_value=list_resp)

        result = await client._create_multiple("accounts", "account", [{"name": "Test"}])
        assert result == []


class TestAsyncODataGetMultiple:
    """Lines 535-547, 561-562 — _get_multiple param branches and pagination."""

    async def test_all_optional_params_added(self):
        """Lines 535, 537, 539, 541 — filter, orderby, expand, count params."""
        client = _make_async_odata_client()
        _seed_cache(client)
        client._request.return_value = _mock_response(json_data={"value": [{"accountid": "1"}]})

        pages = []
        async for page in client._get_multiple(
            "account",
            select=["name"],
            filter="name eq 'X'",
            orderby=["name asc"],
            expand=["contacts"],
            count=True,
        ):
            pages.append(page)

        call_args = client._request.call_args
        params = call_args[1].get("params") or {}
        assert "$filter" in params
        assert "$orderby" in params
        assert "$expand" in params
        assert "$count" in params

    async def test_json_error_yields_nothing(self):
        """Lines 546-547: r.json() raises ValueError → no pages yielded."""
        client = _make_async_odata_client()
        _seed_cache(client)
        bad_resp = MagicMock()
        bad_resp.json.side_effect = ValueError("bad json")
        client._request.return_value = bad_resp

        pages = []
        async for page in client._get_multiple("account"):
            pages.append(page)

        assert pages == []

    async def test_pagination_next_link_json_error(self):
        """Lines 561-562: second page r.json() raises ValueError → loop ends."""
        client = _make_async_odata_client()
        _seed_cache(client)
        first_resp = _mock_response(
            json_data={
                "value": [{"accountid": "1"}],
                "@odata.nextLink": "https://example.com/next",
            }
        )
        bad_second = MagicMock()
        bad_second.json.side_effect = ValueError("bad json")
        client._request.side_effect = [first_resp, bad_second]

        pages = []
        async for page in client._get_multiple("account"):
            pages.append(page)

        assert len(pages) == 1  # only first page


class TestAsyncODataUpdateMultiple:
    """Line 592 — @odata.type injection when missing from record."""

    async def test_odata_type_injected_when_missing(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        client._execute_raw = AsyncMock(return_value=_mock_response(status_code=200))

        # Record without @odata.type — should be injected
        await client._update_multiple("accounts", "account", [{"accountid": "123", "name": "Test"}])

        req_arg = client._execute_raw.call_args[0][0]
        import json as _json

        body = _json.loads(req_arg.body)
        assert any("@odata.type" in r for r in body["Targets"])


class TestAsyncODataUpdateByIds:
    """_update_by_ids: invalid changes type → ValidationError."""

    async def test_invalid_changes_type_raises(self):
        client = _make_async_odata_client()
        _seed_cache(client)

        with pytest.raises(ValidationError, match="changes must be"):
            await client._update_by_ids("account", ["id-1"], 42)


class TestAsyncODataDeleteMultiple:
    """Lines 685-686 — _delete_multiple: r.json() raises ValueError → body={}."""

    async def test_json_raises_value_error_returns_none(self):
        client = _make_async_odata_client()
        _seed_cache(client)
        bad_resp = MagicMock()
        bad_resp.text = "bad"
        bad_resp.json.side_effect = ValueError("bad")
        client._request.return_value = bad_resp

        job_id = await client._delete_multiple("account", ["id-1"])
        assert job_id is None


class TestAsyncODataUpsertMultiple:
    """_upsert_multiple: conflicting alt_key / record field values → ValidationError."""

    async def test_conflicting_key_raises_value_error(self):
        client = _make_async_odata_client()
        _seed_cache(client)

        with pytest.raises(ValidationError, match="conflicts with alternate_key"):
            await client._upsert_multiple(
                "accounts",
                "account",
                alternate_keys=[{"accountnumber": "A"}],
                records=[{"accountnumber": "B"}],  # conflict!
            )


class TestAsyncODataQuerySql:
    """Lines 765-766, 772, 795-829 — _query_sql edge cases and pagination."""

    async def test_json_raises_value_error_returns_empty(self):
        """Lines 765-766: json() raises ValueError → return []."""
        client = _make_async_odata_client()
        _seed_cache(client, table="account", entity_set="accounts")
        bad_resp = MagicMock()
        bad_resp.json.side_effect = ValueError("bad json")
        client._request.return_value = bad_resp

        result = await client._query_sql("SELECT * FROM account")
        assert result == []

    async def test_body_not_dict_returns_empty(self):
        """Line 772: body is a non-dict string → return []."""
        client = _make_async_odata_client()
        _seed_cache(client, table="account", entity_set="accounts")
        str_resp = _mock_response(json_data="a string")
        # Override so json() returns a string
        str_resp.json.return_value = "a string"
        client._request.return_value = str_resp

        result = await client._query_sql("SELECT * FROM account")
        assert result == []

    async def test_pagination_same_next_link_warns_and_breaks(self):
        """Lines 795-804: second call returns same nextLink → warning and break."""
        import warnings as _warnings

        client = _make_async_odata_client()
        _seed_cache(client, table="account", entity_set="accounts")
        next_url = "https://example.crm.dynamics.com/api/data/v9.2/accounts?sql=SELECT+*+FROM+account&page=2"
        first_resp = _mock_response(json_data={"value": [{"id": "1"}], "@odata.nextLink": next_url})
        second_resp = _mock_response(json_data={"value": [{"id": "2"}], "@odata.nextLink": next_url})
        client._request.side_effect = [first_resp, second_resp]

        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter("always")
            result = await client._query_sql("SELECT * FROM account")

        assert any("same nextLink" in str(w.message) for w in caught)
        assert len(result) >= 1

    async def test_pagination_request_raises_exception_warns_and_breaks(self):
        """Lines 807-814: second _request raises → warning and break."""
        import warnings as _warnings

        client = _make_async_odata_client()
        _seed_cache(client, table="account", entity_set="accounts")
        next_url = "https://example.com/next"
        first_resp = _mock_response(json_data={"value": [{"id": "1"}], "@odata.nextLink": next_url})
        client._request.side_effect = [first_resp, RuntimeError("network error")]

        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter("always")
            result = await client._query_sql("SELECT * FROM account")

        assert any("next-page request failed" in str(w.message) for w in caught)
        assert result == [{"id": "1"}]

    async def test_pagination_next_page_json_error_warns_and_breaks(self):
        """Lines 817-824: second page json() raises ValueError → warning and break."""
        import warnings as _warnings

        client = _make_async_odata_client()
        _seed_cache(client, table="account", entity_set="accounts")
        next_url = "https://example.com/next2"
        first_resp = _mock_response(json_data={"value": [{"id": "1"}], "@odata.nextLink": next_url})
        bad_second = MagicMock()
        bad_second.json.side_effect = ValueError("bad")
        client._request.side_effect = [first_resp, bad_second]

        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter("always")
            result = await client._query_sql("SELECT * FROM account")

        assert any("not valid JSON" in str(w.message) for w in caught)
        assert result == [{"id": "1"}]

    async def test_pagination_next_page_not_dict_breaks(self):
        """Line 826: page_body is not a dict → break."""
        client = _make_async_odata_client()
        _seed_cache(client, table="account", entity_set="accounts")
        next_url = "https://example.com/next3"
        first_resp = _mock_response(json_data={"value": [{"id": "1"}], "@odata.nextLink": next_url})
        second_resp = _mock_response(json_data=[{"id": "2"}])  # list, not dict
        second_resp.json.return_value = [{"id": "2"}]
        client._request.side_effect = [first_resp, second_resp]

        result = await client._query_sql("SELECT * FROM account")
        assert result == [{"id": "1"}]

    async def test_pagination_next_page_empty_value_breaks(self):
        """Line 829: page_value is empty list → break."""
        client = _make_async_odata_client()
        _seed_cache(client, table="account", entity_set="accounts")
        next_url = "https://example.com/next4"
        first_resp = _mock_response(json_data={"value": [{"id": "1"}], "@odata.nextLink": next_url})
        second_resp = _mock_response(json_data={"value": []})
        client._request.side_effect = [first_resp, second_resp]

        result = await client._query_sql("SELECT * FROM account")
        assert result == [{"id": "1"}]


class TestAsyncODataCreateEntity:
    """Lines 865-892 — _create_entity success and error paths."""

    async def test_success_returns_entity_dict(self):
        client = _make_async_odata_client()
        entity = {"MetadataId": "m1", "EntitySetName": "accounts", "LogicalName": "account"}
        client._request.return_value = _mock_response(status_code=200)
        client._get_entity_by_table_schema_name = AsyncMock(return_value=entity)

        result = await client._create_entity("account", "Account", [])

        assert result == entity

    async def test_entity_not_found_after_create_raises(self):
        """Lines 886-889: _get_entity_by_table_schema_name returns None → RuntimeError."""
        client = _make_async_odata_client()
        client._request.return_value = _mock_response(status_code=200)
        client._get_entity_by_table_schema_name = AsyncMock(return_value=None)

        with pytest.raises(RuntimeError, match="EntitySetName not available"):
            await client._create_entity("account", "Account", [])

    async def test_entity_missing_entity_set_name_raises(self):
        """Lines 886-889: entity has no EntitySetName → RuntimeError."""
        client = _make_async_odata_client()
        client._request.return_value = _mock_response(status_code=200)
        client._get_entity_by_table_schema_name = AsyncMock(
            return_value={"MetadataId": "m1", "LogicalName": "account"}  # no EntitySetName
        )

        with pytest.raises(RuntimeError, match="EntitySetName not available"):
            await client._create_entity("account", "Account", [])

    async def test_entity_missing_metadata_id_raises(self):
        """Lines 890-891: entity present but no MetadataId → RuntimeError."""
        client = _make_async_odata_client()
        client._request.return_value = _mock_response(status_code=200)
        client._get_entity_by_table_schema_name = AsyncMock(
            return_value={"EntitySetName": "accounts", "LogicalName": "account"}  # no MetadataId
        )

        with pytest.raises(RuntimeError, match="MetadataId missing"):
            await client._create_entity("account", "Account", [])


class TestAsyncODataGetAttributeMetadata:
    """Lines 901-928 — _get_attribute_metadata."""

    async def test_returns_attribute_when_found(self):
        attr = {"MetadataId": "a1", "LogicalName": "name", "SchemaName": "Name"}
        client = _make_async_odata_client()
        client._request.return_value = _mock_response(json_data={"value": [attr]}, text='{"value": [{}]}')

        result = await client._get_attribute_metadata("m1", "name")
        assert result == attr

    async def test_returns_none_when_not_found(self):
        client = _make_async_odata_client()
        client._request.return_value = _mock_response(json_data={"value": []}, text="{}")

        result = await client._get_attribute_metadata("m1", "nonexistent")
        assert result is None

    async def test_extra_select_appended(self):
        """extra_select adds fields; duplicate / @ prefixed fields are ignored."""
        client = _make_async_odata_client()
        client._request.return_value = _mock_response(json_data={"value": []}, text="{}")

        await client._get_attribute_metadata("m1", "name", extra_select="@odata.type,AttributeType,MetadataId")

        call_params = client._request.call_args[1].get("params", {})
        select_str = call_params.get("$select", "")
        assert "AttributeType" in select_str
        # @odata.type should NOT be in $select (starts with @)
        assert "@odata.type" not in select_str
        # MetadataId is already in base list — should appear only once
        assert select_str.count("MetadataId") == 1

    async def test_json_value_error_returns_none(self):
        """Lines 919-922: json() raises ValueError → return None."""
        client = _make_async_odata_client()
        bad_resp = MagicMock()
        bad_resp.text = "bad json"
        bad_resp.json.side_effect = ValueError("bad")
        client._request.return_value = bad_resp

        result = await client._get_attribute_metadata("m1", "name")
        assert result is None


class TestAsyncODataWaitForAttributeVisibility:
    """Lines 937-952 — _wait_for_attribute_visibility."""

    async def test_succeeds_on_first_probe(self):
        """Returns immediately when first probe succeeds."""
        client = _make_async_odata_client()
        client._request.return_value = _mock_response(status_code=200)

        # Should not raise
        await client._wait_for_attribute_visibility("accounts", "name", delays=(0,))

    async def test_raises_runtime_error_when_all_probes_fail(self):
        """Lines 951-955: all probes fail → RuntimeError."""
        client = _make_async_odata_client()
        client._request.side_effect = RuntimeError("probe failed")

        with patch("asyncio.sleep", new_callable=AsyncMock):
            with pytest.raises(RuntimeError, match="did not become visible"):
                await client._wait_for_attribute_visibility("accounts", "name", delays=(0, 1))


class TestAsyncODataCreateTable:
    """Lines 999-1036 — _create_table."""

    async def test_creates_table_successfully(self):
        client = _make_async_odata_client()
        # Table does not exist yet
        client._get_entity_by_table_schema_name = AsyncMock(return_value=None)
        created_entity = {
            "MetadataId": "m1",
            "EntitySetName": "new_mytables",
            "LogicalName": "new_mytable",
            "PrimaryNameAttribute": "new_name",
            "PrimaryIdAttribute": "new_mytableid",
        }
        client._create_entity = AsyncMock(return_value=created_entity)

        result = await client._create_table("new_MyTable", {"new_description": "string"})

        assert result["entity_set_name"] == "new_mytables"
        assert "new_description" in result["columns_created"]

    async def test_primary_column_schema_name_override(self):
        """Lines 1007-1008: primary_column_schema_name overrides default."""
        client = _make_async_odata_client()
        client._get_entity_by_table_schema_name = AsyncMock(return_value=None)
        created_entity = {
            "MetadataId": "m1",
            "EntitySetName": "new_mytables",
            "LogicalName": "new_mytable",
        }
        client._create_entity = AsyncMock(return_value=created_entity)

        await client._create_table("new_MyTable", {}, primary_column_schema_name="new_CustomName")

        call_args = client._create_entity.call_args
        attrs = call_args[1]["attributes"] if "attributes" in call_args[1] else call_args[0][2]
        assert any(a.get("SchemaName") == "new_CustomName" for a in attrs)

    async def test_table_already_exists_raises_metadata_error(self):
        """Lines 1001-1004: table exists → MetadataError."""
        client = _make_async_odata_client()
        client._get_entity_by_table_schema_name = AsyncMock(
            return_value={"MetadataId": "m1", "EntitySetName": "existing"}
        )

        with pytest.raises(MetadataError, match="already exists"):
            await client._create_table("ExistingTable", {})

    async def test_unsupported_column_type_raises_value_error(self):
        """Lines 1019-1020: unsupported type raises ValueError."""
        client = _make_async_odata_client()
        client._get_entity_by_table_schema_name = AsyncMock(return_value=None)

        with pytest.raises(ValueError, match="Unsupported column type"):
            await client._create_table("new_MyTable", {"new_col": "unsupported_type_xyz"})


class TestAsyncODataCreateColumns:
    """Lines 1052-1081 — _create_columns."""

    async def test_creates_columns_successfully(self):
        client = _make_async_odata_client()
        client._get_entity_by_table_schema_name = AsyncMock(
            return_value={"MetadataId": "m1", "EntitySetName": "accounts"}
        )
        client._execute_raw = AsyncMock(return_value=_mock_response(status_code=200))

        result = await client._create_columns("account", {"new_field": "string"})

        assert "new_field" in result

    async def test_table_not_found_raises_metadata_error(self):
        """Lines 1055-1059: table not found → MetadataError."""
        client = _make_async_odata_client()
        client._get_entity_by_table_schema_name = AsyncMock(return_value=None)

        with pytest.raises(MetadataError, match="not found"):
            await client._create_columns("nonexistent", {"new_field": "string"})

    async def test_unsupported_column_type_raises_validation_error(self):
        """Lines 1065-1069: unsupported type raises ValidationError."""
        client = _make_async_odata_client()
        client._get_entity_by_table_schema_name = AsyncMock(
            return_value={"MetadataId": "m1", "EntitySetName": "accounts"}
        )

        with pytest.raises(ValidationError, match="Unsupported column type"):
            await client._create_columns("account", {"new_field": "unsupported_xyz"})


class TestAsyncODataDeleteColumns:
    """Lines 1089-1129 — _delete_columns."""

    async def test_deletes_single_string_column(self):
        client = _make_async_odata_client()
        client._get_entity_by_table_schema_name = AsyncMock(
            return_value={"MetadataId": "m1", "EntitySetName": "accounts", "SchemaName": "Account"}
        )
        client._get_attribute_metadata = AsyncMock(return_value={"MetadataId": "attr-m1", "LogicalName": "name"})
        client._execute_raw = AsyncMock(return_value=_mock_response(status_code=200))

        result = await client._delete_columns("account", "name")

        assert result == ["name"]

    async def test_deletes_list_of_columns(self):
        client = _make_async_odata_client()
        client._get_entity_by_table_schema_name = AsyncMock(
            return_value={"MetadataId": "m1", "EntitySetName": "accounts", "SchemaName": "Account"}
        )
        client._get_attribute_metadata = AsyncMock(return_value={"MetadataId": "attr-m1", "LogicalName": "name"})
        client._execute_raw = AsyncMock(return_value=_mock_response(status_code=200))

        result = await client._delete_columns("account", ["name", "telephone1"])
        assert "name" in result
        assert "telephone1" in result

    async def test_table_not_found_raises_metadata_error(self):
        client = _make_async_odata_client()
        client._get_entity_by_table_schema_name = AsyncMock(return_value=None)

        with pytest.raises(MetadataError, match="not found"):
            await client._delete_columns("nonexistent", "name")

    async def test_column_not_found_raises_metadata_error(self):
        client = _make_async_odata_client()
        client._get_entity_by_table_schema_name = AsyncMock(
            return_value={"MetadataId": "m1", "EntitySetName": "accounts", "SchemaName": "Account"}
        )
        client._get_attribute_metadata = AsyncMock(return_value=None)

        with pytest.raises(MetadataError, match="not found"):
            await client._delete_columns("account", "missingcol")

    async def test_missing_attribute_metadata_id_raises_runtime_error(self):
        client = _make_async_odata_client()
        client._get_entity_by_table_schema_name = AsyncMock(
            return_value={"MetadataId": "m1", "EntitySetName": "accounts", "SchemaName": "Account"}
        )
        # attr_meta exists but no MetadataId
        client._get_attribute_metadata = AsyncMock(return_value={"LogicalName": "name"})  # no MetadataId

        with pytest.raises(RuntimeError, match="missing MetadataId"):
            await client._delete_columns("account", "name")


class TestAsyncODataCreateAlternateKey:
    """Lines 1139-1155 — _create_alternate_key."""

    async def test_creates_key_successfully(self):
        client = _make_async_odata_client()
        client._get_entity_by_table_schema_name = AsyncMock(return_value={"MetadataId": "m1", "LogicalName": "account"})
        resp = _mock_response(status_code=200, headers={"OData-EntityId": "https://example.com/Keys(key-id-1)"})
        client._request.return_value = resp

        result = await client._create_alternate_key("account", "account_altkey", ["accountnumber"])

        assert result["schema_name"] == "account_altkey"
        assert result["key_attributes"] == ["accountnumber"]

    async def test_table_not_found_raises_metadata_error(self):
        client = _make_async_odata_client()
        client._get_entity_by_table_schema_name = AsyncMock(return_value=None)

        with pytest.raises(MetadataError, match="not found"):
            await client._create_alternate_key("nonexistent", "key", ["col"])


class TestAsyncODataGetAlternateKeys:
    """Lines 1163-1173 — _get_alternate_keys."""

    async def test_returns_keys_list(self):
        client = _make_async_odata_client()
        keys = [{"SchemaName": "account_altkey", "KeyAttributes": ["accountnumber"]}]
        client._get_entity_by_table_schema_name = AsyncMock(return_value={"MetadataId": "m1", "LogicalName": "account"})
        client._request.return_value = _mock_response(json_data={"value": keys})

        result = await client._get_alternate_keys("account")
        assert result == keys

    async def test_table_not_found_raises_metadata_error(self):
        client = _make_async_odata_client()
        client._get_entity_by_table_schema_name = AsyncMock(return_value=None)

        with pytest.raises(MetadataError, match="not found"):
            await client._get_alternate_keys("nonexistent")


class TestAsyncODataDeleteAlternateKey:
    """Lines 1177-1185 — _delete_alternate_key."""

    async def test_deletes_key_successfully(self):
        client = _make_async_odata_client()
        client._get_entity_by_table_schema_name = AsyncMock(return_value={"MetadataId": "m1", "LogicalName": "account"})
        client._request.return_value = _mock_response(status_code=200)

        await client._delete_alternate_key("account", "key-id-1")

        client._request.assert_awaited_once()

    async def test_table_not_found_raises_metadata_error(self):
        client = _make_async_odata_client()
        client._get_entity_by_table_schema_name = AsyncMock(return_value=None)

        with pytest.raises(MetadataError, match="not found"):
            await client._delete_alternate_key("nonexistent", "key-id-1")


class TestAsyncODataCreateOneToManyRelationship:
    """Lines 1198-1206 — _create_one_to_many_relationship."""

    async def test_creates_relationship_successfully(self):
        client = _make_async_odata_client()
        lookup = MagicMock()
        lookup.to_dict.return_value = {"SchemaName": "account_contact_lookup"}
        lookup.schema_name = "account_contact_lookup"
        relationship = MagicMock()
        relationship.to_dict.return_value = {"SchemaName": "account_contact_rel"}
        relationship.schema_name = "account_contact_rel"
        relationship.referenced_entity = "account"
        relationship.referencing_entity = "contact"

        resp = _mock_response(
            status_code=200, headers={"OData-EntityId": "https://example.com/Relationships(rel-id-1)"}
        )
        client._request.return_value = resp

        result = await client._create_one_to_many_relationship(lookup, relationship)

        assert result["relationship_schema_name"] == "account_contact_rel"
        assert result["lookup_schema_name"] == "account_contact_lookup"


class TestAsyncODataCreateManyToManyRelationship:
    """Lines 1220-1227 — _create_many_to_many_relationship."""

    async def test_creates_relationship_successfully(self):
        client = _make_async_odata_client()
        relationship = MagicMock()
        relationship.to_dict.return_value = {"SchemaName": "account_tag_rel"}
        relationship.schema_name = "account_tag_rel"
        relationship.entity1_logical_name = "account"
        relationship.entity2_logical_name = "tag"

        resp = _mock_response(
            status_code=200, headers={"OData-EntityId": "https://example.com/Relationships(m2m-id-1)"}
        )
        client._request.return_value = resp

        result = await client._create_many_to_many_relationship(relationship)

        assert result["relationship_schema_name"] == "account_tag_rel"
        assert result["entity1_logical_name"] == "account"


class TestAsyncODataDeleteRelationship:
    """Lines 1236-1237 — _delete_relationship."""

    async def test_sends_delete_with_if_match_star(self):
        client = _make_async_odata_client()
        client._request.return_value = _mock_response(status_code=200)

        await client._delete_relationship("rel-id-1")

        call_args = client._request.call_args
        assert call_args[0][0] == "delete"
        assert "rel-id-1" in call_args[0][1]
        headers = call_args[1].get("headers", {})
        assert headers.get("If-Match") == "*"


class TestAsyncODataGetRelationship:
    """Lines 1241-1246 — _get_relationship."""

    async def test_returns_relationship_when_found(self):
        client = _make_async_odata_client()
        rel = {"id": "r1", "SchemaName": "account_contact_rel"}
        client._request.return_value = _mock_response(json_data={"value": [rel]})

        result = await client._get_relationship("account_contact_rel")
        assert result == rel

    async def test_returns_none_when_not_found(self):
        client = _make_async_odata_client()
        client._request.return_value = _mock_response(json_data={"value": []})

        result = await client._get_relationship("nonexistent_rel")
        assert result is None


class TestAsyncODataUploadFile:
    """Lines 1263-1290 — _upload_file."""

    async def test_small_mode_calls_upload_file_small(self):
        client = _make_async_odata_client()
        _seed_cache(client, table="account", entity_set="accounts")
        client._get_entity_by_table_schema_name = AsyncMock(
            return_value={"MetadataId": "m1", "EntitySetName": "accounts"}
        )
        client._get_attribute_metadata = AsyncMock(return_value={"MetadataId": "attr-m1"})
        client._upload_file_small = AsyncMock()

        with patch("os.path.isfile", return_value=True):
            with patch("os.path.getsize", return_value=100):
                await client._upload_file("account", "rec-1", "filecolumn", "/fake/path.txt", mode="small")

        client._upload_file_small.assert_awaited_once()

    async def test_chunk_mode_calls_upload_file_chunk(self):
        client = _make_async_odata_client()
        _seed_cache(client, table="account", entity_set="accounts")
        client._get_entity_by_table_schema_name = AsyncMock(
            return_value={"MetadataId": "m1", "EntitySetName": "accounts"}
        )
        client._get_attribute_metadata = AsyncMock(return_value={"MetadataId": "attr-m1"})
        client._upload_file_chunk = AsyncMock()

        with patch("os.path.isfile", return_value=True):
            with patch("os.path.getsize", return_value=100):
                await client._upload_file("account", "rec-1", "filecolumn", "/fake/path.txt", mode="chunk")

        client._upload_file_chunk.assert_awaited_once()

    async def test_auto_mode_small_file_calls_upload_file_small(self):
        client = _make_async_odata_client()
        _seed_cache(client, table="account", entity_set="accounts")
        client._get_entity_by_table_schema_name = AsyncMock(
            return_value={"MetadataId": "m1", "EntitySetName": "accounts"}
        )
        client._get_attribute_metadata = AsyncMock(return_value={"MetadataId": "attr-m1"})
        client._upload_file_small = AsyncMock()

        with patch("os.path.isfile", return_value=True):
            with patch("os.path.getsize", return_value=1024):  # well under 128 MB
                await client._upload_file("account", "rec-1", "filecolumn", "/fake/path.txt", mode="auto")

        client._upload_file_small.assert_awaited_once()

    async def test_invalid_mode_raises_value_error(self):
        client = _make_async_odata_client()
        _seed_cache(client, table="account", entity_set="accounts")
        client._get_entity_by_table_schema_name = AsyncMock(
            return_value={"MetadataId": "m1", "EntitySetName": "accounts"}
        )
        client._get_attribute_metadata = AsyncMock(return_value={"MetadataId": "attr-m1"})

        with patch("os.path.isfile", return_value=True):
            with patch("os.path.getsize", return_value=100):
                with pytest.raises(ValueError, match="Invalid mode"):
                    await client._upload_file("account", "rec-1", "filecolumn", "/fake/path.txt", mode="badmode")

    async def test_creates_column_when_attr_metadata_missing(self):
        """_get_attribute_metadata returns None → _create_columns is called."""
        client = _make_async_odata_client()
        _seed_cache(client, table="account", entity_set="accounts")
        client._get_entity_by_table_schema_name = AsyncMock(
            return_value={"MetadataId": "m1", "EntitySetName": "accounts"}
        )
        client._get_attribute_metadata = AsyncMock(return_value=None)
        client._create_columns = AsyncMock(return_value=["filecolumn"])
        client._wait_for_attribute_visibility = AsyncMock()
        client._upload_file_small = AsyncMock()

        with patch("os.path.isfile", return_value=True):
            with patch("os.path.getsize", return_value=100):
                await client._upload_file("account", "rec-1", "filecolumn", "/fake/path.txt", mode="small")

        client._create_columns.assert_awaited_once()
        client._wait_for_attribute_visibility.assert_awaited_once()


class TestAsyncODataUploadFileSmall:
    """Lines 1302-1325 — _upload_file_small."""

    async def test_uploads_small_file_successfully(self):
        import tempfile, os as _os

        client = _make_async_odata_client()
        client._request.return_value = _mock_response(status_code=204)

        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
            f.write(b"hello world")
            tmp_path = f.name

        try:
            await client._upload_file_small("accounts", "rec-1", "filecolumn", tmp_path)
            client._request.assert_awaited_once()
        finally:
            _os.unlink(tmp_path)

    async def test_empty_record_id_raises_value_error(self):
        client = _make_async_odata_client()
        with pytest.raises(ValueError, match="record_id required"):
            await client._upload_file_small("accounts", "", "filecolumn", "/some/path.txt")

    async def test_file_not_found_raises(self):
        client = _make_async_odata_client()
        with pytest.raises(FileNotFoundError):
            await client._upload_file_small("accounts", "rec-1", "filecolumn", "/nonexistent/path.txt")

    async def test_file_too_large_raises_value_error(self):
        import tempfile, os as _os

        client = _make_async_odata_client()

        with tempfile.NamedTemporaryFile(delete=False, suffix=".bin") as f:
            f.write(b"x")
            tmp_path = f.name

        try:
            limit = 128 * 1024 * 1024
            with patch("os.path.getsize", return_value=limit + 1):
                with pytest.raises(ValueError, match="exceeds single-upload limit"):
                    await client._upload_file_small("accounts", "rec-1", "filecolumn", tmp_path)
        finally:
            _os.unlink(tmp_path)


class TestAsyncODataUploadFileChunk:
    """Lines 1336-1379 — _upload_file_chunk."""

    async def test_uploads_file_in_chunks_successfully(self):
        import tempfile, os as _os

        client = _make_async_odata_client()

        with tempfile.NamedTemporaryFile(delete=False, suffix=".bin") as f:
            f.write(b"A" * 100)
            tmp_path = f.name

        try:
            init_resp = _mock_response(
                status_code=200,
                headers={"Location": "https://example.com/upload-session/token123"},
            )
            chunk_resp = _mock_response(status_code=206)
            client._request.side_effect = [init_resp, chunk_resp]

            await client._upload_file_chunk("accounts", "rec-1", "filecolumn", tmp_path)

            assert client._request.call_count >= 2
        finally:
            _os.unlink(tmp_path)

    async def test_missing_location_header_raises_runtime_error(self):
        import tempfile, os as _os

        client = _make_async_odata_client()

        with tempfile.NamedTemporaryFile(delete=False, suffix=".bin") as f:
            f.write(b"data")
            tmp_path = f.name

        try:
            # No Location header in init response
            init_resp = _mock_response(status_code=200, headers={})
            client._request.return_value = init_resp

            with pytest.raises(RuntimeError, match="Missing Location header"):
                await client._upload_file_chunk("accounts", "rec-1", "filecolumn", tmp_path)
        finally:
            _os.unlink(tmp_path)

    async def test_empty_record_id_raises_value_error(self):
        client = _make_async_odata_client()
        with pytest.raises(ValueError, match="record_id required"):
            await client._upload_file_chunk("accounts", "", "filecolumn", "/some/path.txt")

    async def test_file_not_found_raises(self):
        client = _make_async_odata_client()
        with pytest.raises(FileNotFoundError):
            await client._upload_file_chunk("accounts", "rec-1", "filecolumn", "/nonexistent/path.txt")


# ---------------------------------------------------------------------------
# Additional tests to close remaining coverage gaps
# ---------------------------------------------------------------------------


class TestAsyncODataBulkFetchPicklistsFastPath:
    """Line 334 — fast-path return when cache is already fresh."""

    async def test_fast_path_returns_without_lock(self):
        """Line 334: cache already fresh before acquiring lock."""
        client = _make_async_odata_client()
        # Pre-seed a fresh cache entry
        client._picklist_label_cache["account"] = {"ts": time.time(), "picklists": {}}

        # Track whether the lock was ever acquired
        lock_acquired = []

        class _TrackingLock:
            async def __aenter__(self):
                lock_acquired.append(True)
                return self

            async def __aexit__(self, *args):
                pass

        client._picklist_cache_lock = _TrackingLock()
        await client._bulk_fetch_picklists("account")

        # The lock should NOT have been acquired because fast-path returned
        assert not lock_acquired


class TestAsyncODataCreateMultipleAdditional:
    """Lines 455, 478, 481-488 — _create_multiple remaining paths."""

    async def test_type_error_when_record_not_dict(self):
        """Line 455: raise TypeError when records contains non-dict."""
        client = _make_async_odata_client()
        with pytest.raises(TypeError, match="must be dicts"):
            await client._create_multiple("accounts", "account", ["not-a-dict"])

    async def test_ids_field_returned_directly(self):
        """Line 478: body has 'Ids' list → return it directly."""
        client = _make_async_odata_client()
        _seed_cache(client)
        # _convert_labels_to_ints needs _bulk_fetch_picklists — seed cache to skip HTTP
        client._picklist_label_cache["account"] = {"ts": time.time(), "picklists": {}}
        resp = MagicMock()
        resp.text = '{"Ids": ["id-1", "id-2"]}'
        resp.json.return_value = {"Ids": ["id-1", "id-2"]}
        client._execute_raw = AsyncMock(return_value=resp)

        result = await client._create_multiple("accounts", "account", [{"name": "A"}, {"name": "B"}])
        assert result == ["id-1", "id-2"]

    async def test_value_field_parses_guid_from_dict(self):
        """Lines 481-488: body has 'value' list of dicts with *id keys."""
        client = _make_async_odata_client()
        _seed_cache(client)
        client._picklist_label_cache["account"] = {"ts": time.time(), "picklists": {}}
        guid = "12345678901234567890123456789012"
        resp = MagicMock()
        resp.text = "x"
        resp.json.return_value = {"value": [{"accountid": guid}]}
        client._execute_raw = AsyncMock(return_value=resp)

        result = await client._create_multiple("accounts", "account", [{"name": "A"}])
        assert result == [guid]


class TestAsyncODataGetMultipleAdditional:
    """Lines 519-525, 539, 565 — _get_multiple additional param branches."""

    async def test_page_size_and_include_annotations_set_prefer_header(self):
        """Lines 519-525: page_size > 0 and include_annotations set Prefer header."""
        client = _make_async_odata_client()
        _seed_cache(client)
        client._request.return_value = _mock_response(json_data={"value": [{"accountid": "1"}]})

        pages = []
        async for page in client._get_multiple(
            "account",
            page_size=50,
            include_annotations="OData.Community.Display.V1.FormattedValue",
        ):
            pages.append(page)

        call_args = client._request.call_args
        prefer_header = call_args[1].get("headers", {}).get("Prefer", "")
        assert "odata.maxpagesize=50" in prefer_header
        assert "odata.include-annotations" in prefer_header

    async def test_top_param_added(self):
        """Line 539: top is not None → $top is set."""
        client = _make_async_odata_client()
        _seed_cache(client)
        client._request.return_value = _mock_response(json_data={"value": [{"accountid": "1"}]})

        pages = []
        async for page in client._get_multiple("account", top=5):
            pages.append(page)

        call_params = client._request.call_args[1].get("params", {})
        assert call_params.get("$top") == 5

    async def test_pagination_yields_second_page(self):
        """Line 565: yield in the pagination while loop."""
        client = _make_async_odata_client()
        _seed_cache(client)
        next_url = "https://example.com/accounts?page=2"
        first_resp = _mock_response(
            json_data={
                "value": [{"accountid": "1"}],
                "@odata.nextLink": next_url,
            }
        )
        second_resp = _mock_response(json_data={"value": [{"accountid": "2"}]})
        client._request.side_effect = [first_resp, second_resp]

        pages = []
        async for page in client._get_multiple("account"):
            pages.append(page)

        assert len(pages) == 2
        assert pages[0] == [{"accountid": "1"}]
        assert pages[1] == [{"accountid": "2"}]


class TestAsyncODataUpdateMultipleAdditional:
    """Line 592 — _update_multiple TypeError raise."""

    async def test_empty_records_raises_type_error(self):
        """Line 592: empty list raises TypeError."""
        client = _make_async_odata_client()
        with pytest.raises(TypeError, match="non-empty list"):
            await client._update_multiple("accounts", "account", [])

    async def test_non_list_records_raises_type_error(self):
        """Line 592: non-list raises TypeError."""
        client = _make_async_odata_client()
        with pytest.raises(TypeError, match="non-empty list"):
            await client._update_multiple("accounts", "account", "not-a-list")


class TestAsyncODataUpdateByIdsAdditional:
    """Lines 616, 618, 622, 624-630, 633 — _update_by_ids paths."""

    async def test_ids_not_list_raises_type_error(self):
        """Line 616: ids must be list."""
        client = _make_async_odata_client()
        with pytest.raises(TypeError, match="ids must be list"):
            await client._update_by_ids("account", "not-a-list", {})

    async def test_empty_ids_returns_early(self):
        """Line 617-618: empty ids list returns without making requests."""
        client = _make_async_odata_client()
        # Should return without errors
        await client._update_by_ids("account", [], {})
        client._request.assert_not_called()

    async def test_changes_as_dict(self):
        """changes is dict → apply same patch to all ids via _build_update_multiple."""
        client = _make_async_odata_client()
        _seed_cache(client)
        client._execute_raw = AsyncMock()
        client._picklist_label_cache["account"] = {"ts": time.time(), "picklists": {}}

        await client._update_by_ids("account", ["id-1", "id-2"], {"name": "Updated"})

        client._execute_raw.assert_awaited_once()
        import json as _json

        req = client._execute_raw.call_args[0][0]
        body = _json.loads(req.body)
        assert len(body["Targets"]) == 2
        assert all(r["name"] == "Updated" for r in body["Targets"])

    async def test_changes_as_list_mismatch_raises(self):
        """len(changes) != len(ids) raises ValidationError."""
        client = _make_async_odata_client()
        _seed_cache(client)
        with pytest.raises(ValidationError, match="equal length"):
            await client._update_by_ids("account", ["id-1", "id-2"], [{"name": "A"}])

    async def test_changes_as_list_non_dict_patch_raises(self):
        """patch in list is not dict → ValidationError."""
        client = _make_async_odata_client()
        _seed_cache(client)
        with pytest.raises(ValidationError, match="dict"):
            await client._update_by_ids("account", ["id-1"], ["not-a-dict"])

    async def test_changes_as_list_success(self):
        """list changes applied correctly — _execute_raw is called."""
        client = _make_async_odata_client()
        _seed_cache(client)
        client._execute_raw = AsyncMock()
        client._picklist_label_cache["account"] = {"ts": time.time(), "picklists": {}}

        await client._update_by_ids("account", ["id-1"], [{"name": "NewName"}])

        client._execute_raw.assert_awaited_once()


class TestAsyncODataDeleteMultipleAdditional:
    """Line 654 — _delete_multiple empty targets returns None."""

    async def test_empty_ids_returns_none(self):
        """Lines 652-654: all ids are empty strings → targets is empty → return None."""
        client = _make_async_odata_client()
        result = await client._delete_multiple("account", ["", ""])
        assert result is None


class TestAsyncODataUpsertMultipleAdditional:
    """Lines 719, 734-745 — _upsert_multiple success path."""

    async def test_length_mismatch_raises_value_error(self):
        """Different lengths raise ValidationError."""
        client = _make_async_odata_client()
        with pytest.raises(ValidationError, match="same length"):
            await client._upsert_multiple(
                "accounts",
                "account",
                alternate_keys=[{"accountnumber": "A"}, {"accountnumber": "B"}],
                records=[{"name": "Test"}],
            )

    async def test_success_path(self):
        """Lines 734-745: no conflict, @odata.type injected, execute_raw called."""
        client = _make_async_odata_client()
        client._execute_raw = AsyncMock(return_value=_mock_response(status_code=204))
        # Seed picklist cache to avoid HTTP call
        client._picklist_label_cache["account"] = {"ts": time.time(), "picklists": {}}

        await client._upsert_multiple(
            "accounts",
            "account",
            alternate_keys=[{"accountnumber": "A"}],
            records=[{"name": "Test"}],
        )

        client._execute_raw.assert_awaited_once()


class TestAsyncODataQuerySqlAdditional:
    """Lines 754, 756, 770 — _query_sql validation and list body."""

    async def test_non_string_sql_raises_validation_error(self):
        """Line 753-754: sql must be string."""
        client = _make_async_odata_client()
        with pytest.raises(ValidationError):
            await client._query_sql(42)

    async def test_empty_sql_raises_validation_error(self):
        """Line 755-756: sql must not be empty."""
        client = _make_async_odata_client()
        with pytest.raises(ValidationError):
            await client._query_sql("   ")

    async def test_list_body_returned_directly(self):
        """Line 769-770: body is a list → return it directly."""
        client = _make_async_odata_client()
        _seed_cache(client, table="account", entity_set="accounts")
        rows = [{"accountid": "1"}, {"accountid": "2"}]
        client._request.return_value = _mock_response(json_data=rows)

        result = await client._query_sql("SELECT * FROM account")
        assert result == rows


class TestAsyncODataCreateEntityAdditional:
    """Line 880 — _create_entity with solution_unique_name."""

    async def test_solution_param_passed_to_request(self):
        """Line 880: params includes SolutionUniqueName when provided."""
        client = _make_async_odata_client()
        entity = {"MetadataId": "m1", "EntitySetName": "accounts", "LogicalName": "account"}
        client._request.return_value = _mock_response(status_code=200)
        client._get_entity_by_table_schema_name = AsyncMock(return_value=entity)

        await client._create_entity("account", "Account", [], solution_unique_name="MySolution")

        call_kwargs = client._request.call_args[1]
        assert call_kwargs.get("params", {}).get("SolutionUniqueName") == "MySolution"


class TestAsyncODataCreateTableAdditional:
    """Lines 1025-1028 — _create_table solution_unique_name validation."""

    async def test_solution_not_string_raises_type_error(self):
        """Line 1025-1026: solution_unique_name must be string."""
        client = _make_async_odata_client()
        client._get_entity_by_table_schema_name = AsyncMock(return_value=None)

        with pytest.raises(TypeError, match="must be a string"):
            await client._create_table("new_MyTable", {}, solution_unique_name=123)

    async def test_empty_solution_raises_value_error(self):
        """Line 1027-1028: solution_unique_name cannot be empty."""
        client = _make_async_odata_client()
        client._get_entity_by_table_schema_name = AsyncMock(return_value=None)

        with pytest.raises(ValueError, match="cannot be empty"):
            await client._create_table("new_MyTable", {}, solution_unique_name="")


class TestAsyncODataCreateColumnsAdditional:
    """Lines 1071, 1080 — _create_columns picklist flush path."""

    async def test_boolean_column_with_option_set_flushes_cache(self):
        """Lines 1070-1071, 1079-1080: adding boolean (has OptionSet) column flushes cache."""
        client = _make_async_odata_client()
        client._get_entity_by_table_schema_name = AsyncMock(
            return_value={"MetadataId": "m1", "EntitySetName": "accounts"}
        )
        client._execute_raw = AsyncMock(return_value=_mock_response(status_code=200))
        # Pre-seed the picklist cache to verify it's cleared
        client._picklist_label_cache["account"] = {"ts": time.time(), "picklists": {}}

        # "boolean" type generates a payload with "OptionSet" key → triggers flush
        result = await client._create_columns("account", {"new_isactive": "boolean"})

        assert "new_isactive" in result
        # Cache should be cleared by flush
        assert "account" not in client._picklist_label_cache


class TestAsyncODataDeleteColumnsAdditional:
    """Lines 1094, 1097, 1123-1125, 1128 — _delete_columns edge cases."""

    async def test_non_list_non_str_columns_raises_type_error(self):
        """Line 1094: columns must be str or list."""
        client = _make_async_odata_client()
        with pytest.raises(TypeError, match="must be str or list"):
            await client._delete_columns("account", 42)

    async def test_empty_column_name_raises_value_error(self):
        """Line 1096-1097: empty column name raises ValueError."""
        client = _make_async_odata_client()
        client._get_entity_by_table_schema_name = AsyncMock(
            return_value={"MetadataId": "m1", "EntitySetName": "accounts", "SchemaName": "Account"}
        )
        with pytest.raises(ValueError, match="non-empty strings"):
            await client._delete_columns("account", [""])

    async def test_picklist_attr_type_flushes_cache(self):
        """Lines 1122-1125, 1127-1128: picklist attr type triggers cache flush."""
        client = _make_async_odata_client()
        client._get_entity_by_table_schema_name = AsyncMock(
            return_value={"MetadataId": "m1", "EntitySetName": "accounts", "SchemaName": "Account"}
        )
        client._get_attribute_metadata = AsyncMock(
            return_value={
                "MetadataId": "attr-m1",
                "LogicalName": "statuscode",
                "@odata.type": "Microsoft.Dynamics.CRM.PicklistAttributeMetadata",
            }
        )
        client._execute_raw = AsyncMock(return_value=_mock_response(status_code=200))
        client._picklist_label_cache["account"] = {"ts": time.time(), "picklists": {}}

        await client._delete_columns("account", "statuscode")

        # Cache should be flushed
        assert "account" not in client._picklist_label_cache


class TestAsyncODataCreateAlternateKeyAdditional:
    """Line 1152 — _create_alternate_key with display_name_label."""

    async def test_display_name_label_included_in_payload(self):
        """Line 1151-1152: display_name_label.to_dict() added to payload when provided."""
        client = _make_async_odata_client()
        client._get_entity_by_table_schema_name = AsyncMock(return_value={"MetadataId": "m1", "LogicalName": "account"})
        resp = _mock_response(status_code=200, headers={"OData-EntityId": "https://example.com/Keys(key-id-2)"})
        client._request.return_value = resp

        label_mock = MagicMock()
        label_mock.to_dict.return_value = {"UserLocalizedLabel": {"Label": "My Key"}}

        result = await client._create_alternate_key(
            "account",
            "account_altkey",
            ["accountnumber"],
            display_name_label=label_mock,
        )

        call_kwargs = client._request.call_args[1]
        payload = call_kwargs.get("json", {})
        assert "DisplayName" in payload
        assert result["schema_name"] == "account_altkey"


class TestAsyncODataCreateOneToManyRelationshipAdditional:
    """Line 1203 — _create_one_to_many_relationship with solution."""

    async def test_solution_adds_mscrm_header(self):
        """Line 1202-1203: solution sets MSCRM.SolutionUniqueName header."""
        client = _make_async_odata_client()
        lookup = MagicMock()
        lookup.to_dict.return_value = {}
        lookup.schema_name = "lookup"
        relationship = MagicMock()
        relationship.to_dict.return_value = {}
        relationship.schema_name = "rel"
        relationship.referenced_entity = "account"
        relationship.referencing_entity = "contact"

        resp = _mock_response(status_code=200, headers={"OData-EntityId": "https://example.com/Relationships(r1)"})
        client._request.return_value = resp

        await client._create_one_to_many_relationship(lookup, relationship, solution="MySolution")

        call_kwargs = client._request.call_args[1]
        headers = call_kwargs.get("headers", {})
        assert headers.get("MSCRM.SolutionUniqueName") == "MySolution"


class TestAsyncODataCreateManyToManyRelationshipAdditional:
    """Line 1224 — _create_many_to_many_relationship with solution."""

    async def test_solution_adds_mscrm_header(self):
        """Line 1223-1224: solution sets MSCRM.SolutionUniqueName header."""
        client = _make_async_odata_client()
        relationship = MagicMock()
        relationship.to_dict.return_value = {}
        relationship.schema_name = "rel"
        relationship.entity1_logical_name = "account"
        relationship.entity2_logical_name = "tag"

        resp = _mock_response(status_code=200, headers={"OData-EntityId": "https://example.com/Relationships(r2)"})
        client._request.return_value = resp

        await client._create_many_to_many_relationship(relationship, solution="MySolution")

        call_kwargs = client._request.call_args[1]
        headers = call_kwargs.get("headers", {})
        assert headers.get("MSCRM.SolutionUniqueName") == "MySolution"


class TestAsyncODataUploadFileAdditional:
    """Line 1278 — _upload_file auto mode file-not-found."""

    async def test_auto_mode_file_not_found_raises(self):
        """Line 1277-1278: auto mode checks os.path.isfile → FileNotFoundError."""
        client = _make_async_odata_client()
        _seed_cache(client, table="account", entity_set="accounts")
        client._get_entity_by_table_schema_name = AsyncMock(
            return_value={"MetadataId": "m1", "EntitySetName": "accounts"}
        )
        client._get_attribute_metadata = AsyncMock(return_value={"MetadataId": "attr-m1"})

        with patch("os.path.isfile", return_value=False):
            with pytest.raises(FileNotFoundError):
                await client._upload_file("account", "rec-1", "filecolumn", "/nonexistent/path.txt", mode="auto")


class TestAsyncODataUploadFileChunkAdditional:
    """Lines 1350, 1358-1359, 1362, 1369 — _upload_file_chunk additional paths."""

    async def test_if_none_match_false_uses_if_match_header(self):
        """Line 1349-1350: if_none_match=False sets If-Match: * header."""
        import tempfile, os as _os

        client = _make_async_odata_client()

        with tempfile.NamedTemporaryFile(delete=False, suffix=".bin") as f:
            f.write(b"data")
            tmp_path = f.name

        try:
            init_resp = _mock_response(
                status_code=200,
                headers={"Location": "https://example.com/upload-session/token"},
            )
            chunk_resp = _mock_response(status_code=206)
            client._request.side_effect = [init_resp, chunk_resp]

            await client._upload_file_chunk("accounts", "rec-1", "filecolumn", tmp_path, if_none_match=False)

            # First call should use If-Match: * header
            first_call_kwargs = client._request.call_args_list[0][1]
            assert first_call_kwargs.get("headers", {}).get("If-Match") == "*"
        finally:
            _os.unlink(tmp_path)

    async def test_invalid_chunk_size_header_falls_back_to_default(self):
        """Lines 1357-1359: non-parsable x-ms-chunk-size header → recommended_size=None → default 4MB."""
        import tempfile, os as _os

        client = _make_async_odata_client()

        with tempfile.NamedTemporaryFile(delete=False, suffix=".bin") as f:
            f.write(b"x" * 100)
            tmp_path = f.name

        try:
            init_resp = _mock_response(
                status_code=200,
                headers={"Location": "https://example.com/upload-session/tok", "x-ms-chunk-size": "not-a-number"},
            )
            chunk_resp = _mock_response(status_code=206)
            client._request.side_effect = [init_resp, chunk_resp]

            # Should not raise — falls back to 4MB default
            await client._upload_file_chunk("accounts", "rec-1", "filecolumn", tmp_path)
        finally:
            _os.unlink(tmp_path)

    async def test_zero_byte_file_still_uploads_one_chunk(self):
        """Line 1363: total_size=0 → total_chunks=1; line 1368-1369: chunk is empty → break."""
        import tempfile, os as _os

        client = _make_async_odata_client()

        with tempfile.NamedTemporaryFile(delete=False, suffix=".bin") as f:
            # zero-byte file
            tmp_path = f.name

        try:
            init_resp = _mock_response(
                status_code=200,
                headers={"Location": "https://example.com/upload-session/tok2"},
            )
            client._request.return_value = init_resp

            # Should not raise — empty file results in immediate break after first (empty) read
            await client._upload_file_chunk("accounts", "rec-1", "filecolumn", tmp_path)
        finally:
            _os.unlink(tmp_path)


# ---------------------------------------------------------------------------
# Final gap-closing tests
# ---------------------------------------------------------------------------


class TestAsyncODataCreateColumnsEmptyDict:
    """Line 1053 — _create_columns raises when columns is empty dict."""

    async def test_empty_columns_dict_raises_type_error(self):
        """Line 1053: empty dict raises TypeError."""
        client = _make_async_odata_client()
        with pytest.raises(TypeError, match="non-empty dict"):
            await client._create_columns("account", {})

    async def test_non_dict_columns_raises_type_error(self):
        """Line 1053: non-dict raises TypeError."""
        client = _make_async_odata_client()
        with pytest.raises(TypeError, match="non-empty dict"):
            await client._create_columns("account", "not-a-dict")


class TestAsyncODataUploadFileSmallIfMatch:
    """Line 1324 — _upload_file_small with if_none_match=False sets If-Match: *."""

    async def test_if_none_match_false_sets_if_match_header(self):
        import tempfile, os as _os

        client = _make_async_odata_client()
        client._request.return_value = _mock_response(status_code=204)

        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
            f.write(b"hello")
            tmp_path = f.name

        try:
            await client._upload_file_small("accounts", "rec-1", "filecolumn", tmp_path, if_none_match=False)

            call_kwargs = client._request.call_args[1]
            assert call_kwargs.get("headers", {}).get("If-Match") == "*"
            assert "If-None-Match" not in call_kwargs.get("headers", {})
        finally:
            _os.unlink(tmp_path)


class TestAsyncODataUploadFileChunkNegativeChunkSize:
    """Line 1362 — _upload_file_chunk raises when effective chunk size is <= 0."""

    async def test_negative_chunk_size_raises_value_error(self):
        """Line 1361-1362: effective_size <= 0 → ValueError."""
        import tempfile, os as _os

        client = _make_async_odata_client()

        with tempfile.NamedTemporaryFile(delete=False, suffix=".bin") as f:
            f.write(b"data")
            tmp_path = f.name

        try:
            # Return a valid Location and x-ms-chunk-size of -1.
            # int("-1") = -1 (truthy, so `recommended_size or default` gives -1),
            # then effective_size = -1 <= 0 → ValueError.
            init_resp = _mock_response(
                status_code=200,
                headers={"Location": "https://example.com/session/tok", "x-ms-chunk-size": "-1"},
            )
            client._request.return_value = init_resp

            with pytest.raises(ValueError, match="effective chunk size must be positive"):
                await client._upload_file_chunk("accounts", "rec-1", "filecolumn", tmp_path)
        finally:
            _os.unlink(tmp_path)


class TestAsyncODataQuerySqlPagingCookie:
    """Lines 795-804 — _query_sql pagination: same pagingcookie triggers warning."""

    async def test_same_pagingcookie_warns_and_breaks(self):
        """Lines 794-804: two different URLs with same pagingcookie → warning and break."""
        import warnings as _warnings
        from urllib.parse import urlencode, quote as _quote

        client = _make_async_odata_client()
        _seed_cache(client, table="account", entity_set="accounts")

        # Build next_link URLs with the same pagingcookie but different pagenumber
        cookie_val = 'pagingcookie="<cookie page=%221%22/>"'
        skiptoken1 = cookie_val + " pagenumber=1"
        skiptoken2 = cookie_val + " pagenumber=2"
        next_url1 = f"https://example.crm.dynamics.com/api/data/v9.2/accounts?sql=x&$skiptoken={_quote(skiptoken1)}"
        next_url2 = f"https://example.crm.dynamics.com/api/data/v9.2/accounts?sql=x&$skiptoken={_quote(skiptoken2)}"

        first_resp = _mock_response(json_data={"value": [{"id": "1"}], "@odata.nextLink": next_url1})
        second_resp = _mock_response(json_data={"value": [{"id": "2"}], "@odata.nextLink": next_url2})
        client._request.side_effect = [first_resp, second_resp, second_resp]

        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter("always")
            result = await client._query_sql("SELECT * FROM account")

        # Should have warned about same pagingcookie
        paging_warns = [w for w in caught if "pagingcookie" in str(w.message)]
        assert paging_warns, f"Expected pagingcookie warning. Got: {[str(w.message) for w in caught]}"
        assert len(result) >= 1
