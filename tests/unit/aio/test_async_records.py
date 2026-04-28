# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import pytest
from unittest.mock import AsyncMock

from PowerPlatform.Dataverse.aio.operations.async_records import AsyncRecordOperations
from PowerPlatform.Dataverse.models.record import Record
from PowerPlatform.Dataverse.models.upsert import UpsertItem


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


class TestAsyncRecordGet:
    """Tests for AsyncRecordOperations.get."""

    async def test_get_single(self, async_client, mock_od):
        """get() with record_id returns a Record."""
        raw = {"accountid": "guid-1", "name": "Contoso"}
        mock_od._get.return_value = raw

        result = await async_client.records.get("account", "guid-1", select=["name"])

        mock_od._get.assert_called_once_with("account", "guid-1", select=["name"])
        assert isinstance(result, Record)
        assert result.table == "account"
        assert result["name"] == "Contoso"

    async def test_get_single_with_query_params_raises(self, async_client, mock_od):
        """get() with record_id and query params raises ValueError."""
        with pytest.raises(ValueError):
            await async_client.records.get("account", "guid-1", filter="statecode eq 0")

    async def test_get_non_string_record_id_raises(self, async_client, mock_od):
        """get() raises TypeError if record_id is not a string."""
        with pytest.raises(TypeError):
            await async_client.records.get("account", 12345)

    async def test_get_paginated_yields_pages(self, async_client, mock_od):
        """get() without record_id returns an async generator that yields Record pages."""
        page_1 = [{"accountid": "1", "name": "A"}]
        page_2 = [{"accountid": "2", "name": "B"}]

        async def _gen():
            yield page_1
            yield page_2

        # _get_multiple is a sync call that returns an async generator; use MagicMock so
        # calling od._get_multiple(...) returns the generator directly (not a coroutine).
        from unittest.mock import MagicMock

        mock_od._get_multiple = MagicMock(return_value=_gen())

        pages = []
        async for page in await async_client.records.get("account"):
            pages.append(page)

        assert len(pages) == 2
        assert isinstance(pages[0][0], Record)
        assert pages[0][0]["name"] == "A"
        assert isinstance(pages[1][0], Record)
        assert pages[1][0]["name"] == "B"

    async def test_get_paginated_passes_all_params(self, async_client, mock_od):
        """get() without record_id passes all query params to _get_multiple."""
        from unittest.mock import MagicMock

        async def _empty():
            return
            yield  # make it an async generator

        mock_od._get_multiple = MagicMock(return_value=_empty())

        async for _ in await async_client.records.get(
            "account",
            select=["name"],
            filter="statecode eq 0",
            orderby=["name asc"],
            top=50,
            expand=["primarycontactid"],
            page_size=25,
            count=True,
            include_annotations="*",
        ):
            pass

        mock_od._get_multiple.assert_called_once_with(
            "account",  # positional table arg
            select=["name"],
            filter="statecode eq 0",
            orderby=["name asc"],
            top=50,
            expand=["primarycontactid"],
            page_size=25,
            count=True,
            include_annotations="*",
        )


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
