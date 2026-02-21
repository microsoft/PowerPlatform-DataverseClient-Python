# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Record CRUD operations namespace for the Dataverse SDK."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Union, overload, TYPE_CHECKING

from ..models.upsert import UpsertItem

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
        if not isinstance(record_id, str):
            raise TypeError("record_id must be str")
        with self._client._scoped_odata() as od:
            return od._get(table, record_id, select=select)

    # ------------------------------------------------------------------ upsert

    def upsert(self, table: str, items: List[Union[UpsertItem, Dict[str, Any]]]) -> None:
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
