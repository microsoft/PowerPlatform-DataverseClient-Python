# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Async record CRUD operations namespace for the Dataverse SDK."""

from __future__ import annotations

from typing import Any, AsyncGenerator, Dict, List, Optional, Union, overload, TYPE_CHECKING

from ...core.errors import HttpError
from ...models.record import QueryResult, Record
from ...models.upsert import UpsertItem

if TYPE_CHECKING:
    from ...models.filters import FilterExpression
    from ..async_client import AsyncDataverseClient


__all__ = ["AsyncRecordOperations"]


class AsyncRecordOperations:
    """Async namespace for record-level CRUD operations.

    Accessed via ``client.records``. Provides create, update, delete, retrieve,
    list, and upsert operations on individual Dataverse records.

    :param client: The parent :class:`~PowerPlatform.Dataverse.aio.async_client.AsyncDataverseClient` instance.
    :type client: ~PowerPlatform.Dataverse.aio.async_client.AsyncDataverseClient

    Example::

        async with AsyncDataverseClient(base_url, credential) as client:

            # Create a single record
            guid = await client.records.create("account", {"name": "Contoso Ltd"})

            # Retrieve a record
            record = await client.records.retrieve("account", guid, select=["name"])

            # Update a record
            await client.records.update("account", guid, {"telephone1": "555-0100"})

            # Delete a record
            await client.records.delete("account", guid)
    """

    def __init__(self, client: "AsyncDataverseClient") -> None:
        self._client = client

    # ------------------------------------------------------------------ create

    @overload
    async def create(self, table: str, data: Dict[str, Any]) -> str: ...

    @overload
    async def create(self, table: str, data: List[Dict[str, Any]]) -> List[str]: ...

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
            Each dictionary maps column schema names to values.
        :type data: dict or list[dict]

        :return: A single GUID string for a single record, or a list of GUID
            strings for bulk creation.
        :rtype: str or list[str]

        :raises TypeError: If ``data`` is not a dict or list[dict].

        Example:
            Create a single record::

                guid = await client.records.create("account", {"name": "Contoso"})
                print(f"Created: {guid}")

            Create multiple records::

                guids = await client.records.create("account", [
                    {"name": "Contoso"},
                    {"name": "Fabrikam"},
                ])
                print(f"Created {len(guids)} accounts")
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

        Supports three usage patterns:

        1. **Single** -- ``update("account", "guid", {"name": "New"})``
        2. **Broadcast** -- ``update("account", [id1, id2], {"status": 1})``
           applies the same changes dict to every ID.
        3. **Paired** -- ``update("account", [id1, id2], [ch1, ch2])``
           applies each changes dict to its corresponding ID (lists must be
           equal length).

        :param table: Schema name of the table (e.g. ``"account"``).
        :type table: :class:`str`
        :param ids: A single GUID string, or a list of GUID strings.
        :type ids: str or list[str]
        :param changes: A dictionary of field changes (single/broadcast), or a
            list of dictionaries (paired, one per ID).
        :type changes: dict or list[dict]

        :raises TypeError: If ``ids`` is not str or list[str], or if ``changes``
            does not match the expected pattern.

        Example:
            Single update::

                await client.records.update("account", account_id, {"telephone1": "555-0100"})

            Broadcast update::

                await client.records.update("account", [id1, id2], {"statecode": 1})

            Paired update::

                await client.records.update(
                    "account",
                    [id1, id2],
                    [{"name": "Name A"}, {"name": "Name B"}],
                )
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

    @overload
    async def delete(self, table: str, ids: str) -> None: ...

    @overload
    async def delete(self, table: str, ids: List[str], *, use_bulk_delete: bool = True) -> Optional[str]: ...

    async def delete(
        self,
        table: str,
        ids: Union[str, List[str]],
        *,
        use_bulk_delete: bool = True,
    ) -> Optional[str]:
        """Delete one or more records from a Dataverse table.

        When ``ids`` is a single string, deletes that one record. When ``ids``
        is a list, either executes a BulkDelete action (returning the async job
        ID) or deletes each record sequentially depending on ``use_bulk_delete``.

        :param table: Schema name of the table (e.g. ``"account"``).
        :type table: :class:`str`
        :param ids: A single GUID string, or a list of GUID strings.
        :type ids: str or list[str]
        :param use_bulk_delete: When True (default) and ``ids`` is a list, use
            the BulkDelete action and return its async job ID. When False, delete
            records one at a time.
        :type use_bulk_delete: :class:`bool`

        :return: The BulkDelete job ID when bulk-deleting; otherwise None.
        :rtype: :class:`str` or None

        :raises TypeError: If ``ids`` is not str or list[str].

        Example:
            Delete a single record::

                await client.records.delete("account", account_id)

            Bulk delete::

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

    # --------------------------------------------------------------- retrieve

    async def retrieve(
        self,
        table: str,
        record_id: str,
        *,
        select: Optional[List[str]] = None,
        expand: Optional[List[str]] = None,
        include_annotations: Optional[str] = None,
    ) -> Optional[Record]:
        """Fetch a single record by its GUID, returning ``None`` if not found.

        Returns ``None`` instead of raising when the record does not exist (HTTP 404).

        :param table: Schema name of the table (e.g. ``"account"``).
        :type table: :class:`str`
        :param record_id: GUID of the record to retrieve.
        :type record_id: :class:`str`
        :param select: Optional list of column logical names to include.
        :type select: list[str] or None
        :param expand: Optional list of navigation properties to expand (e.g.
            ``["primarycontactid"]``). Navigation property names are
            case-sensitive and must match the entity's ``$metadata``.
        :type expand: list[str] or None
        :param include_annotations: OData annotation pattern for the
            ``Prefer: odata.include-annotations`` header (e.g. ``"*"`` or
            ``"OData.Community.Display.V1.FormattedValue"``), or ``None``.
        :type include_annotations: :class:`str` or None
        :return: Typed record, or ``None`` if not found.
        :rtype: :class:`~PowerPlatform.Dataverse.models.record.Record` or None

        Example::

            record = await client.records.retrieve(
                "account", account_id,
                select=["name", "statuscode"],
                expand=["primarycontactid"],
                include_annotations="OData.Community.Display.V1.FormattedValue",
            )
            if record is not None:
                contact = record.get("primarycontactid") or {}
                print(contact.get("fullname"))
        """
        async with self._client._scoped_odata() as od:
            try:
                raw = await od._get(
                    table, record_id, select=select, expand=expand, include_annotations=include_annotations
                )
            except HttpError as exc:
                if exc.status_code == 404:
                    return None
                raise
            return Record.from_api_response(table, raw, record_id=record_id)

    # -------------------------------------------------------------------- list

    async def list(
        self,
        table: str,
        *,
        filter: Optional[Union[str, "FilterExpression"]] = None,
        select: Optional[List[str]] = None,
        orderby: Optional[List[str]] = None,
        top: Optional[int] = None,
        expand: Optional[List[str]] = None,
        page_size: Optional[int] = None,
        count: bool = False,
        include_annotations: Optional[str] = None,
    ) -> QueryResult:
        """Fetch multiple records and return them as a :class:`QueryResult`.

        All pages are collected eagerly and returned as a single :class:`QueryResult`.

        :param table: Schema name of the table (e.g. ``"account"``).
        :type table: :class:`str`
        :param filter: Optional OData filter string or :class:`FilterExpression`.
        :type filter: str or FilterExpression or None
        :param select: Optional list of column logical names to include.
        :type select: list[str] or None
        :param orderby: Optional list of sort expressions (e.g. ``["name asc", "createdon desc"]``).
        :type orderby: list[str] or None
        :param top: Maximum total number of records to return.
        :type top: int or None
        :param expand: Optional list of navigation properties to expand.
        :type expand: list[str] or None
        :param page_size: Per-page size hint via ``Prefer: odata.maxpagesize``.
        :type page_size: int or None
        :param count: If ``True``, adds ``$count=true`` to include a total record count.
        :type count: bool
        :param include_annotations: OData annotation pattern for the
            ``Prefer: odata.include-annotations`` header, or ``None``.
        :type include_annotations: :class:`str` or None
        :return: All matching records collected into a :class:`QueryResult`.
        :rtype: :class:`~PowerPlatform.Dataverse.models.record.QueryResult`

        Example::

            from PowerPlatform.Dataverse import col

            result = await client.records.list(
                "account",
                filter=col("statecode") == 0,
                select=["name", "statuscode"],
                orderby=["name asc"],
                top=100,
                include_annotations="OData.Community.Display.V1.FormattedValue",
            )
            for record in result:
                print(record["name"], record.get("statuscode@OData.Community.Display.V1.FormattedValue"))
        """
        filter_str: Optional[str] = str(filter) if filter is not None else None
        all_records: List[Record] = []
        async with self._client._scoped_odata() as od:
            async for page in od._get_multiple(
                table,
                select=select,
                filter=filter_str,
                orderby=orderby,
                top=top,
                expand=expand,
                page_size=page_size,
                count=count,
                include_annotations=include_annotations,
            ):
                all_records.extend(Record.from_api_response(table, row) for row in page)
        return QueryResult(all_records)

    # --------------------------------------------------------------- list_pages

    async def list_pages(
        self,
        table: str,
        *,
        filter: Optional[Union[str, "FilterExpression"]] = None,
        select: Optional[List[str]] = None,
        orderby: Optional[List[str]] = None,
        top: Optional[int] = None,
        expand: Optional[List[str]] = None,
        page_size: Optional[int] = None,
        count: bool = False,
        include_annotations: Optional[str] = None,
    ) -> AsyncGenerator[QueryResult, None]:
        """Lazily yield one :class:`QueryResult` per HTTP page.

        Streaming counterpart to :meth:`list` — use when you want to process
        records page by page without loading all into memory. Each iteration
        triggers one network request via ``@odata.nextLink``. One-shot — do
        not iterate more than once.

        :param table: Schema name of the table (e.g. ``"account"``).
        :type table: :class:`str`
        :param filter: Optional OData filter string or :class:`FilterExpression`.
        :type filter: str or FilterExpression or None
        :param select: Optional list of column logical names to include.
        :type select: list[str] or None
        :param orderby: Optional list of sort expressions.
        :type orderby: list[str] or None
        :param top: Maximum total number of records to return.
        :type top: int or None
        :param expand: Optional list of navigation properties to expand.
        :type expand: list[str] or None
        :param page_size: Per-page size hint via ``Prefer: odata.maxpagesize``.
        :type page_size: int or None
        :param count: If ``True``, adds ``$count=true`` to include a total record count.
        :type count: bool
        :param include_annotations: OData annotation pattern for the
            ``Prefer: odata.include-annotations`` header, or ``None``.
        :type include_annotations: :class:`str` or None
        :return: Async generator of per-page :class:`QueryResult` objects.
        :rtype: AsyncGenerator[:class:`~PowerPlatform.Dataverse.models.record.QueryResult`, None]

        Example::

            async for page in client.records.list_pages(
                "account",
                filter="statecode eq 0",
                orderby=["name asc"],
                page_size=200,
            ):
                process(page.to_dataframe())
        """
        filter_str: Optional[str] = str(filter) if filter is not None else None
        async with self._client._scoped_odata() as od:
            async for page in od._get_multiple(
                table,
                select=select,
                filter=filter_str,
                orderby=orderby,
                top=top,
                expand=expand,
                page_size=page_size,
                count=count,
                include_annotations=include_annotations,
            ):
                yield QueryResult([Record.from_api_response(table, row) for row in page])

    # ------------------------------------------------------------------ upsert

    async def upsert(self, table: str, items: List[Union[UpsertItem, Dict[str, Any]]]) -> None:
        """Upsert one or more records identified by alternate keys.

        When ``items`` contains a single entry, performs a single upsert via PATCH
        using the alternate key in the URL. When ``items`` contains multiple entries,
        uses the ``UpsertMultiple`` bulk action.

        Each item must be either a :class:`~PowerPlatform.Dataverse.models.upsert.UpsertItem`
        or a plain ``dict`` with ``"alternate_key"`` and ``"record"`` keys (both dicts).

        :param table: Schema name of the table (e.g. ``"account"`` or ``"new_MyTestTable"``).
        :type table: str
        :param items: Non-empty list of :class:`~PowerPlatform.Dataverse.models.upsert.UpsertItem`
            instances or dicts with ``"alternate_key"`` and ``"record"`` keys.
        :type items: list[UpsertItem | dict]

        :return: ``None``
        :rtype: None

        :raises TypeError: If ``items`` is not a non-empty list, or if any element is
            neither a :class:`~PowerPlatform.Dataverse.models.upsert.UpsertItem` nor a
            dict with ``"alternate_key"`` and ``"record"`` keys.

        Example:
            Upsert a single record using ``UpsertItem``::

                from PowerPlatform.Dataverse.models.upsert import UpsertItem

                await client.records.upsert("account", [
                    UpsertItem(
                        alternate_key={"accountnumber": "ACC-001"},
                        record={"name": "Contoso Ltd", "description": "Primary account"},
                    )
                ])

            Upsert a single record using a plain dict::

                await client.records.upsert("account", [
                    {
                        "alternate_key": {"accountnumber": "ACC-001"},
                        "record": {"name": "Contoso Ltd", "description": "Primary account"},
                    },
                ])

            Upsert multiple records using ``UpsertItem``::

                from PowerPlatform.Dataverse.models.upsert import UpsertItem

                await client.records.upsert("account", [
                    UpsertItem(
                        alternate_key={"accountnumber": "ACC-001"},
                        record={"name": "Contoso Ltd", "description": "Primary account"},
                    ),
                    UpsertItem(
                        alternate_key={"accountnumber": "ACC-002"},
                        record={"name": "Fabrikam Inc", "description": "Partner account"},
                    ),
                ])

            Upsert multiple records using plain dicts::

                await client.records.upsert("account", [
                    {
                        "alternate_key": {"accountnumber": "ACC-001"},
                        "record": {"name": "Contoso Ltd", "description": "Primary account"},
                    },
                    {
                        "alternate_key": {"accountnumber": "ACC-002"},
                        "record": {"name": "Fabrikam Inc", "description": "Partner account"},
                    },
                ])

            The ``alternate_key`` dict may contain multiple columns when the configured
            alternate key is composite, e.g.
            ``{"accountnumber": "ACC-001", "address1_postalcode": "98052"}``.
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
                raise TypeError("Each item must be an UpsertItem or a dict with 'alternate_key' and 'record' keys")
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
