# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Async record CRUD operations namespace for the Dataverse SDK."""

from __future__ import annotations

from typing import Any, AsyncIterable, Dict, List, Optional, Union, TYPE_CHECKING

from ..models.record import Record
from ..models.upsert import UpsertItem

if TYPE_CHECKING:
    from ..async_client import AsyncDataverseClient


__all__ = ["AsyncRecordOperations"]


class AsyncRecordOperations:
    """Async namespace for record-level CRUD operations.

    Accessed via ``client.records`` on
    :class:`~PowerPlatform.Dataverse.async_client.AsyncDataverseClient`.

    :param client: The parent async client instance.
    :type client: ~PowerPlatform.Dataverse.async_client.AsyncDataverseClient

    Example::

        async with AsyncDataverseClient(base_url, credential) as client:
            guid = await client.records.create("account", {"name": "Contoso Ltd"})
            record = await client.records.get("account", guid, select=["name"])
            await client.records.update("account", guid, {"telephone1": "555-0100"})
            await client.records.delete("account", guid)
    """

    def __init__(self, client: AsyncDataverseClient) -> None:
        self._client = client

    # ------------------------------------------------------------------ create

    async def create(
        self,
        table: str,
        data: Union[Dict[str, Any], List[Dict[str, Any]]],
    ) -> Union[str, List[str]]:
        """Create one or more records in a Dataverse table.

        When ``data`` is a single dictionary, creates one record and returns its
        GUID as a string. When ``data`` is a list of dictionaries, creates all
        records via the ``CreateMultiple`` action and returns a list of GUIDs.

        :param table: Schema name of the table (e.g. ``"account"`` or ``"new_MyTestTable"``).
        :type table: :class:`str`
        :param data: A single record dictionary or a list of record dictionaries.
        :type data: :class:`dict` or :class:`list` of :class:`dict`

        :return: A single GUID string for a single record, or a list of GUID strings for bulk.
        :rtype: :class:`str` or :class:`list` of :class:`str`

        :raises TypeError: If ``data`` is not a dict or list[dict].

        Example::

            guid = await client.records.create("account", {"name": "Contoso"})
            guids = await client.records.create("account", [{"name": "A"}, {"name": "B"}])
        """
        async with self._client._scoped_odata() as od:
            entity_set = await od._entity_set_from_schema_name(table)
            if isinstance(data, dict):
                rid = await od._create(entity_set, table, data)
                if not isinstance(rid, str):
                    raise TypeError("_create (single) did not return GUID string")
                return rid
            if isinstance(data, list):
                ids = await od._create_multiple(entity_set, table, data)
                if not isinstance(ids, list) or not all(isinstance(x, str) for x in ids):
                    raise TypeError("_create (multi) did not return list[str]")
                return ids
        raise TypeError("data must be dict or list[dict]")

    # ------------------------------------------------------------------ update

    async def update(
        self,
        table: str,
        ids: Union[str, List[str]],
        changes: Union[Dict[str, Any], List[Dict[str, Any]]],
    ) -> None:
        """Update one or more records in a Dataverse table.

        :param table: Schema name of the table.
        :type table: :class:`str`
        :param ids: A single GUID string, or a list of GUID strings.
        :type ids: :class:`str` or :class:`list` of :class:`str`
        :param changes: A dictionary of field changes (single/broadcast) or a list (paired).
        :type changes: :class:`dict` or :class:`list` of :class:`dict`

        Example::

            await client.records.update("account", guid, {"telephone1": "555-0100"})
            await client.records.update("account", [id1, id2], {"statecode": 1})
        """
        async with self._client._scoped_odata() as od:
            if isinstance(ids, str):
                if not isinstance(changes, dict):
                    raise TypeError("For single id, changes must be a dict")
                await od._update(table, ids, changes)
                return None
            if not isinstance(ids, list):
                raise TypeError("ids must be str or list[str]")
            await od._update_by_ids(table, ids, changes)
            return None

    # ------------------------------------------------------------------ delete

    async def delete(
        self,
        table: str,
        ids: Union[str, List[str]],
        *,
        use_bulk_delete: bool = True,
    ) -> Optional[str]:
        """Delete one or more records from a Dataverse table.

        :param table: Schema name of the table.
        :type table: :class:`str`
        :param ids: A single GUID string, or a list of GUID strings.
        :type ids: :class:`str` or :class:`list` of :class:`str`
        :param use_bulk_delete: When True (default) and ``ids`` is a list, use the
            BulkDelete action. When False, delete records one at a time.
        :type use_bulk_delete: :class:`bool`

        :return: The BulkDelete job ID when bulk-deleting; otherwise None.
        :rtype: :class:`str` or None

        Example::

            await client.records.delete("account", guid)
            job_id = await client.records.delete("account", [id1, id2, id3])
        """
        async with self._client._scoped_odata() as od:
            if isinstance(ids, str):
                await od._delete(table, ids)
                return None
            if not isinstance(ids, list):
                raise TypeError("ids must be str or list[str]")
            if not ids:
                return None
            if not all(isinstance(rid, str) for rid in ids):
                raise TypeError("ids must contain string GUIDs")
            if use_bulk_delete:
                return await od._delete_multiple(table, ids)
            for rid in ids:
                await od._delete(table, rid)
            return None

    # -------------------------------------------------------------------- get

    async def get(
        self,
        table: str,
        record_id: Optional[str] = None,
        *,
        select: Optional[List[str]] = None,
        filter: Optional[str] = None,
        orderby: Optional[List[str]] = None,
        top: Optional[int] = None,
        expand: Optional[List[str]] = None,
        page_size: Optional[int] = None,
    ) -> Union[Record, AsyncIterable[List[Record]]]:
        """Fetch a single record by ID, or fetch multiple records with async pagination.

        **Single record** -- pass ``record_id``; returns a
        :class:`~PowerPlatform.Dataverse.models.record.Record`.

        **Multiple records** -- omit ``record_id``; returns an async generator that
        yields one page (list of :class:`~PowerPlatform.Dataverse.models.record.Record`
        objects) at a time.

        :param table: Schema name of the table.
        :type table: :class:`str`
        :param record_id: GUID of a single record to retrieve, or None for multi-record fetch.
        :type record_id: :class:`str` or None
        :param select: Optional list of column logical names.
        :type select: :class:`list` of :class:`str` or None
        :param filter: Optional OData ``$filter`` expression (multi-record only).
        :type filter: :class:`str` or None
        :param orderby: Optional sort expressions (multi-record only).
        :type orderby: :class:`list` of :class:`str` or None
        :param top: Maximum total records (multi-record only).
        :type top: :class:`int` or None
        :param expand: Navigation properties to expand (multi-record only).
        :type expand: :class:`list` of :class:`str` or None
        :param page_size: Per-page size hint (multi-record only).
        :type page_size: :class:`int` or None

        Example::

            # Single record
            record = await client.records.get("account", guid, select=["name"])
            print(record["name"])

            # Multiple records (async iteration)
            async for page in await client.records.get("account", filter="statecode eq 0"):
                for rec in page:
                    print(rec["name"])
        """
        if record_id is not None:
            if not isinstance(record_id, str):
                raise TypeError("record_id must be str")
            if (
                filter is not None
                or orderby is not None
                or top is not None
                or expand is not None
                or page_size is not None
            ):
                raise ValueError(
                    "Cannot specify query parameters (filter, orderby, top, expand, page_size) "
                    "when fetching a single record by ID"
                )
            async with self._client._scoped_odata() as od:
                raw = await od._get(table, record_id, select=select)
                return Record.from_api_response(table, raw, record_id=record_id)

        async def _paged() -> AsyncIterable[List[Record]]:
            async with self._client._scoped_odata() as od:
                async for page in od._get_multiple(
                    table,
                    select=select,
                    filter=filter,
                    orderby=orderby,
                    top=top,
                    expand=expand,
                    page_size=page_size,
                ):
                    yield [Record.from_api_response(table, row) for row in page]

        return _paged()

    # ------------------------------------------------------------------ upsert

    async def upsert(self, table: str, items: List[Union[UpsertItem, Dict[str, Any]]]) -> None:
        """Upsert one or more records identified by alternate keys.

        :param table: Schema name of the table.
        :type table: :class:`str`
        :param items: Non-empty list of
            :class:`~PowerPlatform.Dataverse.models.upsert.UpsertItem` instances
            or dicts with ``"alternate_key"`` and ``"record"`` keys.
        :type items: list[UpsertItem | dict]

        Example::

            from PowerPlatform.Dataverse.models.upsert import UpsertItem

            await client.records.upsert("account", [
                UpsertItem(
                    alternate_key={"accountnumber": "ACC-001"},
                    record={"name": "Contoso Ltd"},
                )
            ])
        """
        if not isinstance(items, list) or not items:
            raise TypeError("items must be a non-empty list of UpsertItem or dicts")
        normalized: List[UpsertItem] = []
        for i in items:
            if isinstance(i, UpsertItem):
                normalized.append(i)
            elif isinstance(i, dict) and isinstance(i.get("alternate_key"), dict) and isinstance(i.get("record"), dict):
                normalized.append(UpsertItem(alternate_key=i["alternate_key"], record=i["record"]))
            else:
                raise TypeError("Each item must be a UpsertItem or a dict with 'alternate_key' and 'record' keys")
        async with self._client._scoped_odata() as od:
            entity_set = await od._entity_set_from_schema_name(table)
            if len(normalized) == 1:
                item = normalized[0]
                await od._upsert(entity_set, table, item.alternate_key, item.record)
            else:
                alternate_keys = [i.alternate_key for i in normalized]
                records = [i.record for i in normalized]
                await od._upsert_multiple(entity_set, table, alternate_keys, records)
        return None
