# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Unit tests for AsyncRecordOperations (client.records namespace)."""

import pytest
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

from azure.core.credentials_async import AsyncTokenCredential

from PowerPlatform.Dataverse.aio.async_client import AsyncDataverseClient
from PowerPlatform.Dataverse.aio.operations.async_records import AsyncRecordOperations
from PowerPlatform.Dataverse.models.record import Record
from PowerPlatform.Dataverse.models.upsert import UpsertItem

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_client_with_mock_odata():
    """
    Return (client, mock_od).

    client._scoped_odata() is patched to yield mock_od without making any
    real HTTP or OData calls.
    """
    credential = AsyncMock(spec=AsyncTokenCredential)
    client = AsyncDataverseClient("https://example.crm.dynamics.com", credential)
    od = AsyncMock()

    @asynccontextmanager
    async def _fake_scoped_odata():
        yield od

    client._scoped_odata = _fake_scoped_odata
    return client, od


# ---------------------------------------------------------------------------
# Namespace
# ---------------------------------------------------------------------------


class TestAsyncRecordOperationsNamespace:
    """Tests that the records namespace is correctly exposed on the client."""

    def test_namespace_exists(self):
        """client.records exposes an AsyncRecordOperations instance."""
        credential = AsyncMock(spec=AsyncTokenCredential)
        client = AsyncDataverseClient("https://example.crm.dynamics.com", credential)
        assert isinstance(client.records, AsyncRecordOperations)


# ---------------------------------------------------------------------------
# create
# ---------------------------------------------------------------------------


class TestAsyncRecordCreate:
    """Tests for AsyncRecordOperations.create — single and bulk creation paths."""

    async def test_create_single_returns_guid(self):
        """create() with a single dict calls _create and returns the resulting GUID string."""
        client, od = _make_client_with_mock_odata()
        od._entity_set_from_schema_name.return_value = "accounts"
        od._create.return_value = "guid-123"

        result = await client.records.create("account", {"name": "Contoso"})

        od._entity_set_from_schema_name.assert_awaited_once_with("account")
        od._create.assert_awaited_once_with("accounts", "account", {"name": "Contoso"})
        assert result == "guid-123"
        assert isinstance(result, str)

    async def test_create_bulk_returns_list_of_guids(self):
        """create() with a list calls _create_multiple and returns the list of GUIDs."""
        client, od = _make_client_with_mock_odata()
        payloads = [{"name": "A"}, {"name": "B"}]
        od._entity_set_from_schema_name.return_value = "accounts"
        od._create_multiple.return_value = ["guid-1", "guid-2"]

        result = await client.records.create("account", payloads)

        od._create_multiple.assert_awaited_once_with("accounts", "account", payloads)
        assert result == ["guid-1", "guid-2"]
        assert isinstance(result, list)

    async def test_create_single_non_string_return_raises(self):
        """create() raises TypeError when the OData layer returns a non-string for a single create."""
        client, od = _make_client_with_mock_odata()
        od._entity_set_from_schema_name.return_value = "accounts"
        od._create.return_value = 12345  # not a str

        with pytest.raises(TypeError):
            await client.records.create("account", {"name": "Contoso"})

    async def test_create_bulk_non_list_return_raises(self):
        """create() raises TypeError when the OData layer returns a non-list for a bulk create."""
        client, od = _make_client_with_mock_odata()
        od._entity_set_from_schema_name.return_value = "accounts"
        od._create_multiple.return_value = "not-a-list"

        with pytest.raises(TypeError):
            await client.records.create("account", [{"name": "Contoso"}])

    async def test_create_invalid_data_type_raises(self):
        """create() raises TypeError when passed data that is neither a dict nor a list."""
        client, od = _make_client_with_mock_odata()
        od._entity_set_from_schema_name.return_value = "accounts"

        with pytest.raises(TypeError):
            await client.records.create("account", "bad-input")


# ---------------------------------------------------------------------------
# update
# ---------------------------------------------------------------------------


class TestAsyncRecordUpdate:
    """Tests for AsyncRecordOperations.update — single, broadcast, and paired update paths."""

    async def test_update_single(self):
        """update() with a scalar ID delegates to _update with that ID and the change dict."""
        client, od = _make_client_with_mock_odata()

        await client.records.update("account", "00000000-0000-0000-0000-000000000000", {"telephone1": "555-0199"})

        od._update.assert_awaited_once_with(
            "account", "00000000-0000-0000-0000-000000000000", {"telephone1": "555-0199"}
        )

    async def test_update_broadcast(self):
        """update() with a list of IDs and a single dict delegates to _update_by_ids (broadcast)."""
        client, od = _make_client_with_mock_odata()
        ids = ["id-1", "id-2"]
        changes = {"statecode": 1}

        await client.records.update("account", ids, changes)

        od._update_by_ids.assert_awaited_once_with("account", ids, changes)

    async def test_update_paired(self):
        """update() with parallel lists of IDs and change dicts delegates to _update_by_ids."""
        client, od = _make_client_with_mock_odata()
        ids = ["id-1", "id-2"]
        changes = [{"name": "A"}, {"name": "B"}]

        await client.records.update("account", ids, changes)

        od._update_by_ids.assert_awaited_once_with("account", ids, changes)

    async def test_update_single_non_dict_changes_raises(self):
        """update() raises TypeError when a scalar ID is combined with non-dict changes."""
        client, od = _make_client_with_mock_odata()

        with pytest.raises(TypeError):
            await client.records.update("account", "guid-1", ["not", "a", "dict"])

    async def test_update_invalid_ids_type_raises(self):
        """update() raises TypeError when ids is neither a string nor a list."""
        client, od = _make_client_with_mock_odata()

        with pytest.raises(TypeError):
            await client.records.update("account", 12345, {"name": "X"})


# ---------------------------------------------------------------------------
# delete
# ---------------------------------------------------------------------------


class TestAsyncRecordDelete:
    """Tests for AsyncRecordOperations.delete — single, bulk, and sequential delete paths."""

    async def test_delete_single_returns_none(self):
        """delete() with a scalar ID delegates to _delete and returns None."""
        client, od = _make_client_with_mock_odata()

        result = await client.records.delete("account", "guid-to-delete")

        od._delete.assert_awaited_once_with("account", "guid-to-delete")
        assert result is None

    async def test_delete_bulk_calls_delete_multiple(self):
        """delete() with a list of IDs delegates to _delete_multiple and returns the job GUID."""
        client, od = _make_client_with_mock_odata()
        od._delete_multiple.return_value = "job-guid-456"
        ids = ["id-1", "id-2", "id-3"]

        result = await client.records.delete("account", ids)

        od._delete_multiple.assert_awaited_once_with("account", ids)
        assert result == "job-guid-456"

    async def test_delete_bulk_sequential(self):
        """delete() with use_bulk_delete=False calls _delete once per ID sequentially."""
        client, od = _make_client_with_mock_odata()
        ids = ["id-1", "id-2", "id-3"]

        result = await client.records.delete("account", ids, use_bulk_delete=False)

        assert od._delete.await_count == 3
        od._delete.assert_any_await("account", "id-1")
        od._delete.assert_any_await("account", "id-2")
        od._delete.assert_any_await("account", "id-3")
        od._delete_multiple.assert_not_awaited()
        assert result is None

    async def test_delete_empty_list_returns_none(self):
        """delete() with an empty list makes no HTTP calls and returns None."""
        client, od = _make_client_with_mock_odata()

        result = await client.records.delete("account", [])

        od._delete.assert_not_awaited()
        od._delete_multiple.assert_not_awaited()
        assert result is None

    async def test_delete_invalid_ids_type_raises(self):
        """delete() raises TypeError when ids is neither a string nor a list."""
        client, od = _make_client_with_mock_odata()

        with pytest.raises(TypeError):
            await client.records.delete("account", 12345)

    async def test_delete_list_with_non_string_raises(self):
        """delete() raises TypeError when a list of IDs contains a non-string element."""
        client, od = _make_client_with_mock_odata()

        with pytest.raises(TypeError):
            await client.records.delete("account", ["valid-guid", 42])


# ---------------------------------------------------------------------------
# get — single
# ---------------------------------------------------------------------------


class TestAsyncRecordGetSingle:
    """Tests for AsyncRecordOperations.get() with a specific record_id."""

    async def test_get_single_returns_record(self):
        """get() with a record_id returns a Record hydrated from the raw OData response."""
        client, od = _make_client_with_mock_odata()
        raw = {"accountid": "guid-1", "name": "Contoso"}
        od._get.return_value = raw

        result = await client.records.get("account", "guid-1", select=["name"])

        od._get.assert_awaited_once_with("account", "guid-1", select=["name"])
        assert isinstance(result, Record)
        assert result.id == "guid-1"
        assert result.table == "account"
        assert result["name"] == "Contoso"

    async def test_get_single_with_query_params_raises(self):
        """get() raises ValueError when collection-level params like filter are used with a record_id."""
        client, od = _make_client_with_mock_odata()

        with pytest.raises(ValueError):
            await client.records.get("account", "guid-1", filter="statecode eq 0")

    async def test_get_non_string_record_id_raises(self):
        """get() raises TypeError when record_id is not a string."""
        client, od = _make_client_with_mock_odata()

        with pytest.raises(TypeError):
            await client.records.get("account", 12345)


# ---------------------------------------------------------------------------
# get — paginated
# ---------------------------------------------------------------------------


class TestAsyncRecordGetPaginated:
    """Tests for AsyncRecordOperations.get() without a record_id — paginated multi-record path."""

    async def test_get_paginated_yields_record_pages(self):
        """get() without a record_id returns an async generator that yields pages of Record objects."""
        client, od = _make_client_with_mock_odata()
        page_1 = [{"accountid": "1", "name": "A"}]
        page_2 = [{"accountid": "2", "name": "B"}]

        async def _mock_get_multiple(*args, **kwargs):
            for page in [page_1, page_2]:
                yield page

        od._get_multiple = _mock_get_multiple

        pages = await client.records.get("account")
        collected = []
        async for page in pages:
            collected.append(page)

        assert len(collected) == 2
        assert isinstance(collected[0][0], Record)
        assert collected[0][0]["name"] == "A"
        assert collected[1][0]["name"] == "B"

    async def test_get_paginated_passes_all_params(self):
        """get() forwards select, filter, orderby, top, expand, and page_size to _get_multiple."""
        client, od = _make_client_with_mock_odata()
        captured_kwargs: dict = {}

        async def _mock_get_multiple(*args, **kwargs):
            captured_kwargs.update(kwargs)
            return
            yield  # make it an async generator

        od._get_multiple = _mock_get_multiple

        pages = await client.records.get(
            "account",
            select=["name", "telephone1"],
            filter="statecode eq 0",
            orderby=["name asc"],
            top=50,
            expand=["primarycontactid"],
            page_size=25,
        )
        async for _ in pages:
            pass

        assert captured_kwargs["select"] == ["name", "telephone1"]
        assert captured_kwargs["filter"] == "statecode eq 0"
        assert captured_kwargs["orderby"] == ["name asc"]
        assert captured_kwargs["top"] == 50
        assert captured_kwargs["expand"] == ["primarycontactid"]
        assert captured_kwargs["page_size"] == 25
        assert captured_kwargs["count"] is False


# ---------------------------------------------------------------------------
# upsert
# ---------------------------------------------------------------------------


class TestAsyncRecordUpsert:
    """Tests for AsyncRecordOperations.upsert — single and multiple upsert paths."""

    async def test_upsert_single_upsert_item(self):
        """upsert() with one UpsertItem calls _upsert (not _upsert_multiple) and returns None."""
        client, od = _make_client_with_mock_odata()
        od._entity_set_from_schema_name.return_value = "accounts"
        item = UpsertItem(alternate_key={"accountnumber": "ACC-001"}, record={"name": "Contoso"})

        result = await client.records.upsert("account", [item])

        od._entity_set_from_schema_name.assert_awaited_once_with("account")
        od._upsert.assert_awaited_once_with("accounts", "account", {"accountnumber": "ACC-001"}, {"name": "Contoso"})
        od._upsert_multiple.assert_not_awaited()
        assert result is None

    async def test_upsert_single_dict(self):
        """upsert() accepts a dict with alternate_key/record keys and normalises it into an upsert call."""
        client, od = _make_client_with_mock_odata()
        od._entity_set_from_schema_name.return_value = "accounts"
        item = {"alternate_key": {"accountnumber": "ACC-001"}, "record": {"name": "Contoso"}}

        await client.records.upsert("account", [item])

        od._upsert.assert_awaited_once_with("accounts", "account", {"accountnumber": "ACC-001"}, {"name": "Contoso"})

    async def test_upsert_multiple_calls_upsert_multiple(self):
        """upsert() with two or more items calls _upsert_multiple with parallel key and record lists."""
        client, od = _make_client_with_mock_odata()
        od._entity_set_from_schema_name.return_value = "accounts"
        items = [
            UpsertItem(alternate_key={"accountnumber": "ACC-001"}, record={"name": "Contoso"}),
            UpsertItem(alternate_key={"accountnumber": "ACC-002"}, record={"name": "Fabrikam"}),
        ]

        await client.records.upsert("account", items)

        od._upsert.assert_not_awaited()
        od._upsert_multiple.assert_awaited_once_with(
            "accounts",
            "account",
            [{"accountnumber": "ACC-001"}, {"accountnumber": "ACC-002"}],
            [{"name": "Contoso"}, {"name": "Fabrikam"}],
        )

    async def test_upsert_empty_list_raises(self):
        """upsert() raises TypeError when passed an empty list."""
        client, od = _make_client_with_mock_odata()

        with pytest.raises(TypeError):
            await client.records.upsert("account", [])

    async def test_upsert_invalid_items_raises(self):
        """upsert() raises TypeError when passed a non-list."""
        client, od = _make_client_with_mock_odata()

        with pytest.raises(TypeError):
            await client.records.upsert("account", "not-a-list")

    async def test_upsert_invalid_item_element_raises(self):
        """upsert() raises TypeError when a list element has an unrecognised shape."""
        client, od = _make_client_with_mock_odata()

        with pytest.raises(TypeError):
            await client.records.upsert("account", [{"bad_key": "value"}])
