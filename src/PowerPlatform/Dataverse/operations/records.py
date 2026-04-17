# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Record CRUD operations namespace for the Dataverse SDK."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Union, overload, TYPE_CHECKING

from ..models.record import Record
from ..models.upsert import UpsertItem
from ..models.entity import resolve_table, resolve_table_pair

if TYPE_CHECKING:
    from ..client import DataverseClient
    from ..models.entity import Entity


__all__ = ["RecordOperations"]


class RecordOperations:
    """Namespace for record-level CRUD operations.

    Accessed via ``client.records``. Provides create, update, delete, and get
    operations on individual Dataverse records.

    :param client: The parent :class:`~PowerPlatform.Dataverse.client.DataverseClient` instance.
    :type client: ~PowerPlatform.Dataverse.client.DataverseClient

    Example::

        client = DataverseClient(base_url, credential)

        # Create a single record
        guid = client.records.create("account", {"name": "Contoso Ltd"})

        # Get a record
        record = client.records.get("account", guid, select=["name"])

        # Update a record
        client.records.update("account", guid, {"telephone1": "555-0100"})

        # Delete a record
        client.records.delete("account", guid)
    """

    def __init__(self, client: DataverseClient) -> None:
        self._client = client

    # ------------------------------------------------------------------ create

    @overload
    def create(self, table: "Entity") -> str: ...

    @overload
    def create(self, table: "List[Entity]") -> List[str]: ...

    @overload
    def create(self, table: Union[str, "type[Entity]"], data: Dict[str, Any]) -> str: ...

    @overload
    def create(self, table: Union[str, "type[Entity]"], data: List[Dict[str, Any]]) -> List[str]: ...

    def create(
        self,
        table: Union[str, "type[Entity]", "Entity", "List[Entity]"],
        data: Union[Dict[str, Any], List[Dict[str, Any]], "Entity", "List[Entity]", None] = None,
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

                guid = client.records.create("account", {"name": "Contoso"})
                print(f"Created: {guid}")

            Create multiple records::

                guids = client.records.create("account", [
                    {"name": "Contoso"},
                    {"name": "Fabrikam"},
                ])
                print(f"Created {len(guids)} accounts")
        """
        from ..models.entity import Entity as _Entity
        # Entity-instance form: create(entity) or create([entity, ...])
        # Table is inferred from the instance type; no separate table arg needed.
        if isinstance(table, _Entity):
            data = table.to_dict()
            table = resolve_table(type(table))
        elif isinstance(table, list) and table and isinstance(table[0], _Entity):
            data = [e.to_dict() for e in table]
            table = resolve_table(type(table[0]))
        else:
            table = resolve_table(table)
            if isinstance(data, _Entity):
                data = data.to_dict()
            elif isinstance(data, list) and data and isinstance(data[0], _Entity):
                data = [e.to_dict() for e in data]

        with self._client._scoped_odata() as od:
            entity_set = od._entity_set_from_schema_name(table)
            if isinstance(data, dict):
                rid = od._create(entity_set, table, data)
                if not isinstance(rid, str):
                    raise TypeError("_create (single) did not return GUID string")
                return rid
            if isinstance(data, list):
                ids = od._create_multiple(entity_set, table, data)
                if not isinstance(ids, list) or not all(isinstance(x, str) for x in ids):
                    raise TypeError("_create (multi) did not return list[str]")
                return ids
        raise TypeError("data must be a dict, list[dict], Entity instance, or list[Entity]")

    # ------------------------------------------------------------------ update

    def update(
        self,
        table: Union[str, "type[Entity]"],
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

                client.records.update("account", account_id, {"telephone1": "555-0100"})

            Broadcast update::

                client.records.update("account", [id1, id2], {"statecode": 1})

            Paired update::

                client.records.update(
                    "account",
                    [id1, id2],
                    [{"name": "Name A"}, {"name": "Name B"}],
                )
        """
        from ..models.entity import Entity as _Entity
        table = resolve_table(table)
        if isinstance(changes, _Entity):
            changes = changes.to_dict()
        elif isinstance(changes, list) and changes and isinstance(changes[0], _Entity):
            changes = [c.to_dict() for c in changes]
        with self._client._scoped_odata() as od:
            if isinstance(ids, str):
                if not isinstance(changes, dict):
                    raise TypeError("For single id, changes must be a dict or Entity instance")
                od._update(table, ids, changes)
                return None
            if not isinstance(ids, list):
                raise TypeError("ids must be str or list[str]")
            od._update_by_ids(table, ids, changes)
            return None

    # ------------------------------------------------------------------ delete

    @overload
    def delete(self, table: Union[str, "type[Entity]"], ids: str) -> None: ...

    @overload
    def delete(self, table: Union[str, "type[Entity]"], ids: List[str], *, use_bulk_delete: bool = True) -> Optional[str]: ...

    def delete(
        self,
        table: Union[str, "type[Entity]"],
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

                client.records.delete("account", account_id)

            Bulk delete::

                job_id = client.records.delete("account", [id1, id2, id3])
        """
        table = resolve_table(table)
        with self._client._scoped_odata() as od:
            if isinstance(ids, str):
                od._delete(table, ids)
                return None
            if not isinstance(ids, list):
                raise TypeError("ids must be str or list[str]")
            if not ids:
                return None
            if not all(isinstance(rid, str) for rid in ids):
                raise TypeError("ids must contain string GUIDs")
            if use_bulk_delete:
                return od._delete_multiple(table, ids)
            for rid in ids:
                od._delete(table, rid)
            return None

    # -------------------------------------------------------------------- get

    @overload
    def get(
        self,
        table: str,
        record_id: str,
        *,
        select: Optional[List[str]] = None,
    ) -> Record:
        """Fetch a single record by its GUID (string table → Record)."""
        ...

    @overload
    def get(
        self,
        table: "type[Entity]",
        record_id: str,
        *,
        select: Optional[List[str]] = None,
    ) -> "Entity":
        """Fetch a single record by its GUID (Entity class → typed instance)."""
        ...

    @overload
    def get(
        self,
        table: Union[str, "type[Entity]"],
        *,
        select: Optional[List[str]] = None,
        filter: Optional[str] = None,
        orderby: Optional[List[str]] = None,
        top: Optional[int] = None,
        expand: Optional[List[str]] = None,
        page_size: Optional[int] = None,
        count: bool = False,
        include_annotations: Optional[str] = None,
    ) -> Iterable[List[Record]]:
        """Fetch multiple records from a Dataverse table with pagination.

        Returns a generator that yields one page (list of
        :class:`~PowerPlatform.Dataverse.models.record.Record` objects) at a
        time. Automatically follows ``@odata.nextLink`` for server-side paging.

        :param table: Schema name of the table (e.g. ``"account"`` or
            ``"new_MyTestTable"``).
        :type table: :class:`str`
        :param select: Optional list of column logical names to include.
            Column names are automatically lowercased.
        :type select: list[str] or None
        :param filter: Optional OData ``$filter`` expression (e.g.
            ``"name eq 'Contoso'"``). Column names in filter expressions must
            use exact lowercase logical names.
        :type filter: :class:`str` or None
        :param orderby: Optional list of sort expressions (e.g.
            ``["name asc", "createdon desc"]``). Column names are automatically
            lowercased.
        :type orderby: list[str] or None
        :param top: Optional maximum total number of records to return.
        :type top: :class:`int` or None
        :param expand: Optional list of navigation properties to expand (e.g.
            ``["primarycontactid"]``). Case-sensitive; must match server-defined
            names exactly.
        :type expand: list[str] or None
        :param page_size: Optional per-page size hint sent via
            ``Prefer: odata.maxpagesize``.
        :type page_size: :class:`int` or None
        :param count: If ``True``, adds ``$count=true`` to include a total
            record count in the response.
        :type count: :class:`bool`
        :param include_annotations: OData annotation pattern for the
            ``Prefer: odata.include-annotations`` header (e.g. ``"*"`` or
            ``"OData.Community.Display.V1.FormattedValue"``), or ``None``.
        :type include_annotations: :class:`str` or None

        :return: Generator yielding pages, where each page is a list of
            :class:`~PowerPlatform.Dataverse.models.record.Record` objects.
        :rtype: collections.abc.Iterable[list[~PowerPlatform.Dataverse.models.record.Record]]

        Example:
            Fetch with filtering and pagination::

                for page in client.records.get(
                    "account",
                    filter="statecode eq 0",
                    select=["name", "telephone1"],
                    page_size=50,
                ):
                    for record in page:
                        print(record["name"])
        """
        ...

    def get(
        self,
        table: Union[str, "type[Entity]"],
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
    ) -> Union[Record, Iterable[List[Record]]]:
        """Fetch a single record by ID, or fetch multiple records with pagination.

        This method has two usage patterns:

        **Fetch a single record** -- ``get(table, record_id, *, select=...)``

        Pass ``record_id`` as a positional argument to retrieve one record
        and get back a :class:`dict`. Query parameters (``filter``,
        ``orderby``, ``top``, ``expand``, ``page_size``) must not be provided.

        **Fetch multiple records** -- ``get(table, *, select=..., filter=..., ...)``

        Omit ``record_id`` to perform a paginated fetch and get back a
        generator that yields one page (list of record dicts) at a time.
        Automatically follows ``@odata.nextLink`` for server-side paging.

        :param table: Schema name of the table (e.g. ``"account"`` or
            ``"new_MyTestTable"``).
        :type table: :class:`str`
        :param record_id: GUID of the record to retrieve. When omitted,
            performs a multi-record fetch instead.
        :type record_id: :class:`str` or None
        :param select: Optional list of column logical names to include.
            Column names are automatically lowercased.
        :type select: list[str] or None
        :param filter: Optional OData ``$filter`` expression (e.g.
            ``"name eq 'Contoso'"``). Column names in filter expressions must
            use exact lowercase logical names. Only used for multi-record
            queries.
        :type filter: :class:`str` or None
        :param orderby: Optional list of sort expressions (e.g.
            ``["name asc", "createdon desc"]``). Column names are
            automatically lowercased. Only used for multi-record queries.
        :type orderby: list[str] or None
        :param top: Optional maximum total number of records to return. Only
            used for multi-record queries.
        :type top: :class:`int` or None
        :param expand: Optional list of navigation properties to expand (e.g.
            ``["primarycontactid"]``). Case-sensitive; must match
            server-defined names exactly. Only used for multi-record queries.
        :type expand: list[str] or None
        :param page_size: Optional per-page size hint sent via
            ``Prefer: odata.maxpagesize``. Only used for multi-record queries.
        :type page_size: :class:`int` or None
        :param count: If ``True``, adds ``$count=true`` to include a total
            record count in the response. Only used for multi-record queries.
        :type count: :class:`bool`
        :param include_annotations: OData annotation pattern for the
            ``Prefer: odata.include-annotations`` header (e.g. ``"*"`` or
            ``"OData.Community.Display.V1.FormattedValue"``), or ``None``.
            Only used for multi-record queries.
        :type include_annotations: :class:`str` or None

        :return: A single record dict when ``record_id`` is provided, or a
            generator yielding pages (lists of record dicts) when fetching
            multiple records.
        :rtype: dict or collections.abc.Iterable[list[dict]]

        :raises TypeError: If ``record_id`` is provided but not a string.
        :raises ValueError: If query parameters are provided alongside
            ``record_id``.

        Example:
            Fetch a single record::

                record = client.records.get(
                    "account", account_id, select=["name", "telephone1"]
                )
                print(record["name"])

            Fetch multiple records with pagination::

                for page in client.records.get(
                    "account",
                    filter="statecode eq 0",
                    select=["name", "telephone1"],
                    page_size=50,
                ):
                    for record in page:
                        print(record["name"])
        """
        table_str, entity_cls = resolve_table_pair(table)
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
            with self._client._scoped_odata() as od:
                raw = od._get(table_str, record_id, select=select)
                record = Record.from_api_response(table_str, raw, record_id=record_id)
                # When an entity class was passed, hydrate into a typed instance
                if entity_cls is not None:
                    return entity_cls.from_record(record)
                return record

        def _paged() -> Iterable[List[Record]]:
            with self._client._scoped_odata() as od:
                for page in od._get_multiple(
                    table_str,
                    select=select,
                    filter=filter,
                    orderby=orderby,
                    top=top,
                    expand=expand,
                    page_size=page_size,
                    count=count,
                    include_annotations=include_annotations,
                ):
                    yield [Record.from_api_response(table_str, row) for row in page]

        return _paged()

    # ------------------------------------------------------------------ upsert

    def upsert(self, table: Union[str, "type[Entity]"], items: List[Union[UpsertItem, Dict[str, Any]]]) -> None:
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

                client.records.upsert("account", [
                    UpsertItem(
                        alternate_key={"accountnumber": "ACC-001"},
                        record={"name": "Contoso Ltd", "description": "Primary account"},
                    )
                ])

            Upsert a single record using a plain dict::

                client.records.upsert("account", [
                    {
                        "alternate_key": {"accountnumber": "ACC-001"},
                        "record": {"name": "Contoso Ltd", "description": "Primary account"},
                    },
                ])

            Upsert multiple records using ``UpsertItem``::

                from PowerPlatform.Dataverse.models.upsert import UpsertItem

                client.records.upsert("account", [
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

                client.records.upsert("account", [
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
        table = resolve_table(table)
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
        with self._client._scoped_odata() as od:
            entity_set = od._entity_set_from_schema_name(table)
            if len(normalized) == 1:
                item = normalized[0]
                od._upsert(entity_set, table, item.alternate_key, item.record)
            else:
                alternate_keys = [i.alternate_key for i in normalized]
                records = [i.record for i in normalized]
                od._upsert_multiple(entity_set, table, alternate_keys, records)
        return None
