# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Record CRUD operations namespace for the Dataverse SDK."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Union, overload, TYPE_CHECKING

if TYPE_CHECKING:
    from ..client import DataverseClient


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
    def create(self, table: str, data: Dict[str, Any]) -> str: ...

    @overload
    def create(self, table: str, data: List[Dict[str, Any]]) -> List[str]: ...

    def create(
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
        :type data: :class:`dict` or :class:`list` of :class:`dict`

        :return: A single GUID string for a single record, or a list of GUID
            strings for bulk creation.
        :rtype: :class:`str` or :class:`list` of :class:`str`

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
        raise TypeError("data must be dict or list[dict]")

    # ------------------------------------------------------------------ update

    def update(
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
        :type ids: :class:`str` or :class:`list` of :class:`str`
        :param changes: A dictionary of field changes (single/broadcast), or a
            list of dictionaries (paired, one per ID).
        :type changes: :class:`dict` or :class:`list` of :class:`dict`

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
        with self._client._scoped_odata() as od:
            if isinstance(ids, str):
                if not isinstance(changes, dict):
                    raise TypeError("For single id, changes must be a dict")
                od._update(table, ids, changes)
                return None
            if not isinstance(ids, list):
                raise TypeError("ids must be str or list[str]")
            od._update_by_ids(table, ids, changes)
            return None

    # ------------------------------------------------------------------ delete

    @overload
    def delete(self, table: str, ids: str) -> None: ...

    @overload
    def delete(self, table: str, ids: List[str], *, use_bulk_delete: bool = True) -> Optional[str]: ...

    def delete(
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
        :type ids: :class:`str` or :class:`list` of :class:`str`
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
    ) -> Dict[str, Any]:
        """Fetch a single record by its GUID.

        :param table: Schema name of the table (e.g. ``"account"``).
        :type table: :class:`str`
        :param record_id: GUID of the record to retrieve.
        :type record_id: :class:`str`
        :param select: Optional list of column logical names to include in the
            response.
        :type select: :class:`list` of :class:`str` or None

        :return: Record dictionary with the requested attributes.
        :rtype: :class:`dict`

        :raises TypeError: If ``record_id`` is not a string.

        Example:
            Fetch a record with selected columns::

                record = client.records.get(
                    "account", account_id, select=["name", "telephone1"]
                )
                print(record["name"])
        """
        ...

    @overload
    def get(
        self,
        table: str,
        *,
        select: Optional[List[str]] = None,
        filter: Optional[str] = None,
        orderby: Optional[List[str]] = None,
        top: Optional[int] = None,
        expand: Optional[List[str]] = None,
        page_size: Optional[int] = None,
    ) -> Iterable[List[Dict[str, Any]]]:
        """Query multiple records from a Dataverse table with pagination.

        Returns a generator that yields one page (list of record dicts) at a
        time. Automatically follows ``@odata.nextLink`` for server-side paging.

        :param table: Schema name of the table (e.g. ``"account"`` or
            ``"new_MyTestTable"``).
        :type table: :class:`str`
        :param select: Optional list of column logical names to include.
            Column names are automatically lowercased.
        :type select: :class:`list` of :class:`str` or None
        :param filter: Optional OData ``$filter`` expression (e.g.
            ``"name eq 'Contoso'"``). Column names in filter expressions must
            use exact lowercase logical names.
        :type filter: :class:`str` or None
        :param orderby: Optional list of sort expressions (e.g.
            ``["name asc", "createdon desc"]``). Column names are automatically
            lowercased.
        :type orderby: :class:`list` of :class:`str` or None
        :param top: Optional maximum total number of records to return.
        :type top: :class:`int` or None
        :param expand: Optional list of navigation properties to expand (e.g.
            ``["primarycontactid"]``). Case-sensitive; must match server-defined
            names exactly.
        :type expand: :class:`list` of :class:`str` or None
        :param page_size: Optional per-page size hint sent via
            ``Prefer: odata.maxpagesize``.
        :type page_size: :class:`int` or None

        :return: Generator yielding pages, where each page is a list of record
            dictionaries.
        :rtype: :class:`collections.abc.Iterable` of :class:`list` of
            :class:`dict`

        Example:
            Query with filtering and pagination::

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
        table: str,
        record_id: Optional[str] = None,
        *,
        select: Optional[List[str]] = None,
        filter: Optional[str] = None,
        orderby: Optional[List[str]] = None,
        top: Optional[int] = None,
        expand: Optional[List[str]] = None,
        page_size: Optional[int] = None,
    ) -> Union[Dict[str, Any], Iterable[List[Dict[str, Any]]]]:
        """Fetch a single record by ID, or query multiple records with pagination.

        This method has two usage patterns:

        **Fetch a single record** -- ``get(table, record_id, *, select=...)``

        Pass ``record_id`` as a positional argument to retrieve one record
        and get back a :class:`dict`. Query parameters (``filter``,
        ``orderby``, ``top``, ``expand``, ``page_size``) must not be provided.

        **Query multiple records** -- ``get(table, *, select=..., filter=..., ...)``

        Omit ``record_id`` to perform a paginated OData query and get back a
        generator that yields one page (list of record dicts) at a time.
        Automatically follows ``@odata.nextLink`` for server-side paging.

        :param table: Schema name of the table (e.g. ``"account"`` or
            ``"new_MyTestTable"``).
        :type table: :class:`str`
        :param record_id: GUID of the record to retrieve. When omitted,
            performs a multi-record query instead.
        :type record_id: :class:`str` or None
        :param select: Optional list of column logical names to include.
            Column names are automatically lowercased.
        :type select: :class:`list` of :class:`str` or None
        :param filter: Optional OData ``$filter`` expression (e.g.
            ``"name eq 'Contoso'"``). Column names in filter expressions must
            use exact lowercase logical names. Only used for multi-record
            queries.
        :type filter: :class:`str` or None
        :param orderby: Optional list of sort expressions (e.g.
            ``["name asc", "createdon desc"]``). Column names are
            automatically lowercased. Only used for multi-record queries.
        :type orderby: :class:`list` of :class:`str` or None
        :param top: Optional maximum total number of records to return. Only
            used for multi-record queries.
        :type top: :class:`int` or None
        :param expand: Optional list of navigation properties to expand (e.g.
            ``["primarycontactid"]``). Case-sensitive; must match
            server-defined names exactly. Only used for multi-record queries.
        :type expand: :class:`list` of :class:`str` or None
        :param page_size: Optional per-page size hint sent via
            ``Prefer: odata.maxpagesize``. Only used for multi-record queries.
        :type page_size: :class:`int` or None

        :return: A single record dict when ``record_id`` is provided, or a
            generator yielding pages (lists of record dicts) when querying
            multiple records.
        :rtype: :class:`dict` or :class:`collections.abc.Iterable` of
            :class:`list` of :class:`dict`

        :raises TypeError: If ``record_id`` is provided but not a string.
        :raises ValueError: If query parameters are provided alongside
            ``record_id``.

        Example:
            Fetch a single record::

                record = client.records.get(
                    "account", account_id, select=["name", "telephone1"]
                )
                print(record["name"])

            Query multiple records with pagination::

                for page in client.records.get(
                    "account",
                    filter="statecode eq 0",
                    select=["name", "telephone1"],
                    page_size=50,
                ):
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
            ):
                raise ValueError(
                    "Cannot specify query parameters (filter, orderby, top, "
                    "expand, page_size) when fetching a single record by ID"
                )
            with self._client._scoped_odata() as od:
                return od._get(table, record_id, select=select)

        def _paged() -> Iterable[List[Dict[str, Any]]]:
            with self._client._scoped_odata() as od:
                yield from od._get_multiple(
                    table,
                    select=select,
                    filter=filter,
                    orderby=orderby,
                    top=top,
                    expand=expand,
                    page_size=page_size,
                )

        return _paged()
