# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Async record CRUD operations namespace for the Dataverse SDK."""

from __future__ import annotations

from typing import Any, AsyncGenerator, Dict, List, Optional, Union, overload

from ...models.record import Record
from ...models.upsert import UpsertItem

__all__ = ["AsyncRecordOperations"]


class AsyncRecordOperations:
    """Async namespace for record-level CRUD operations.

    Accessed via ``client.records``.  Async counterpart of
    :class:`~PowerPlatform.Dataverse.operations.records.RecordOperations`.

    :param client: The parent :class:`~PowerPlatform.Dataverse.aio.AsyncDataverseClient`.
    """

    def __init__(self, client: Any) -> None:  # Any to avoid circular import
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

        When ``data`` is a single dictionary, creates one record and returns
        its GUID.  When ``data`` is a list, creates all records via
        ``CreateMultiple`` and returns a list of GUIDs.

        :param table: Schema name of the table (e.g. ``"account"``).
        :type table: :class:`str`
        :param data: A single record dict or a list of record dicts.
        :type data: dict or list[dict]

        :return: A single GUID string or a list of GUID strings.
        :rtype: str or list[str]

        :raises TypeError: If ``data`` is not a dict or list[dict].

        Example::

            guid = await client.records.create("account", {"name": "Contoso"})

            guids = await client.records.create("account", [
                {"name": "Contoso"},
                {"name": "Fabrikam"},
            ])
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
        """Update one or more records.

        Supports three patterns:

        1. **Single** — ``update("account", "guid", {"name": "New"})``
        2. **Broadcast** — ``update("account", [id1, id2], {"status": 1})``
        3. **Paired** — ``update("account", [id1, id2], [ch1, ch2])``

        :param table: Schema name of the table.
        :type table: :class:`str`
        :param ids: Single GUID string or list of GUID strings.
        :type ids: str or list[str]
        :param changes: Dict of changes (single/broadcast) or list of dicts (paired).
        :type changes: dict or list[dict]

        :raises TypeError: If ``ids`` is not str or list[str].

        Example::

            await client.records.update("account", account_id, {"telephone1": "555-0100"})
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
        """Delete one or more records.

        When ``ids`` is a single string, deletes that record.  When ``ids`` is
        a list, either executes a BulkDelete action (returning the async job
        ID) or deletes each record sequentially.

        :param table: Schema name of the table.
        :type table: :class:`str`
        :param ids: Single GUID string or list of GUID strings.
        :type ids: str or list[str]
        :param use_bulk_delete: When True (default) and ``ids`` is a list, use
            BulkDelete and return the job ID.
        :type use_bulk_delete: :class:`bool`

        :return: BulkDelete job ID when bulk-deleting; otherwise None.
        :rtype: :class:`str` or None

        :raises TypeError: If ``ids`` is not str or list[str].

        Example::

            await client.records.delete("account", account_id)

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

    @overload
    async def get(
        self,
        table: str,
        record_id: str,
        *,
        select: Optional[List[str]] = None,
    ) -> Record: ...

    @overload
    async def get(
        self,
        table: str,
        *,
        select: Optional[List[str]] = None,
        filter: Optional[str] = None,
        orderby: Optional[List[str]] = None,
        top: Optional[int] = None,
        expand: Optional[List[str]] = None,
        page_size: Optional[int] = None,
        count: bool = False,
        include_annotations: Optional[str] = None,
    ) -> AsyncGenerator[List[Record], None]: ...

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
        count: bool = False,
        include_annotations: Optional[str] = None,
    ) -> Union[Record, AsyncGenerator[List[Record], None]]:
        """Fetch a single record by ID, or fetch multiple records with pagination.

        **Fetch a single record** — pass ``record_id`` as a positional argument.
        Returns a :class:`~PowerPlatform.Dataverse.models.record.Record`.

        **Fetch multiple records** — omit ``record_id``.  Returns an
        ``AsyncGenerator`` that yields one page (list of Records) at a time.
        The caller must ``await`` the call to obtain the generator, then
        ``async for`` over it.

        :param table: Schema name of the table.
        :type table: :class:`str`
        :param record_id: GUID to retrieve a single record. When omitted,
            performs a paginated fetch.
        :type record_id: :class:`str` or None
        :param select: Optional list of column logical names.
        :type select: list[str] or None
        :param filter: Optional OData ``$filter`` expression.
        :type filter: :class:`str` or None
        :param orderby: Optional sort expressions.
        :type orderby: list[str] or None
        :param top: Optional maximum total number of records.
        :type top: :class:`int` or None
        :param expand: Optional navigation properties to expand (case-sensitive).
        :type expand: list[str] or None
        :param page_size: Optional per-page size hint.
        :type page_size: :class:`int` or None
        :param count: Include ``$count=true`` in the request.
        :type count: :class:`bool`
        :param include_annotations: OData annotation pattern for the
            ``Prefer: odata.include-annotations`` header.
        :type include_annotations: :class:`str` or None

        :return: A single record (``record_id`` given) or an async generator
            yielding pages of records.
        :rtype: Record or AsyncGenerator[list[Record], None]

        :raises TypeError: If ``record_id`` is provided but not a string.
        :raises ValueError: If query parameters are provided alongside ``record_id``.

        Example:
            Fetch a single record::

                record = await client.records.get(
                    "account", account_id, select=["name", "telephone1"]
                )
                print(record["name"])

            Fetch multiple records with pagination::

                pages = await client.records.get(
                    "account",
                    filter="statecode eq 0",
                    select=["name"],
                    page_size=50,
                )
                async for page in pages:
                    for record in page:
                        print(record["name"])
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
                or count is not False
                or include_annotations is not None
            ):
                raise ValueError(
                    "Cannot specify query parameters (filter, orderby, top, "
                    "expand, page_size, count, include_annotations) when "
                    "fetching a single record by ID"
                )
            async with self._client._scoped_odata() as od:
                raw = await od._get(table, record_id, select=select)
                return Record.from_api_response(table, raw, record_id=record_id)

        async def _paged() -> AsyncGenerator[List[Record], None]:
            async with self._client._scoped_odata() as od:
                async for page in od._get_multiple(
                    table,
                    select=select,
                    filter=filter,
                    orderby=orderby,
                    top=top,
                    expand=expand,
                    page_size=page_size,
                    count=count,
                    include_annotations=include_annotations,
                ):
                    yield [Record.from_api_response(table, row) for row in page]

        return _paged()

    # ------------------------------------------------------------------ upsert

    async def upsert(
        self,
        table: str,
        items: List[Union[UpsertItem, Dict[str, Any]]],
    ) -> None:
        """Upsert one or more records identified by alternate keys.

        :param table: Schema name of the table.
        :type table: :class:`str`
        :param items: Non-empty list of :class:`~PowerPlatform.Dataverse.models.upsert.UpsertItem`
            or dicts with ``"alternate_key"`` and ``"record"`` keys.
        :type items: list[UpsertItem | dict]

        :raises TypeError: If ``items`` is not a non-empty list or any element
            is invalid.

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
            elif (
                isinstance(i, dict)
                and isinstance(i.get("alternate_key"), dict)
                and isinstance(i.get("record"), dict)
            ):
                normalized.append(UpsertItem(alternate_key=i["alternate_key"], record=i["record"]))
            else:
                raise TypeError(
                    "Each item must be an UpsertItem or a dict with "
                    "'alternate_key' and 'record' keys"
                )
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
