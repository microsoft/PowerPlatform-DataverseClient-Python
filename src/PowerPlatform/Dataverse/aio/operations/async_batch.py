# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Async batch operation namespaces for the Dataverse SDK."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

import pandas as pd

from ...core.errors import ValidationError
from ...core._error_codes import VALIDATION_SQL_EMPTY
from ...data._batch_base import (
    _ChangeSet,
    _RecordCreate,
    _RecordUpdate,
    _RecordDelete,
    _RecordGet,
    _RecordUpsert,
    _TableCreate,
    _TableDelete,
    _TableGet,
    _TableList,
    _TableAddColumns,
    _TableRemoveColumns,
    _TableCreateOneToMany,
    _TableCreateManyToMany,
    _TableDeleteRelationship,
    _TableGetRelationship,
    _TableCreateLookupField,
    _QuerySql,
)
from ..data._async_batch import _AsyncBatchClient
from ...models.batch import BatchResult
from ...models.upsert import UpsertItem
from ...models.relationship import (
    LookupAttributeMetadata,
    OneToManyRelationshipMetadata,
    ManyToManyRelationshipMetadata,
)
from ...common.constants import CASCADE_BEHAVIOR_REMOVE_LINK

if TYPE_CHECKING:
    from ..async_client import AsyncDataverseClient

__all__ = [
    "AsyncBatchRecordOperations",
    "AsyncBatchTableOperations",
    "AsyncBatchQueryOperations",
    "AsyncBatchDataFrameOperations",
    "AsyncBatchRequest",
    "AsyncBatchOperations",
    "AsyncChangeSet",
    "AsyncChangeSetRecordOperations",
]


# ---------------------------------------------------------------------------
# Changeset namespaces
# ---------------------------------------------------------------------------


class AsyncChangeSetRecordOperations:
    """
    Record write operations available inside an :class:`AsyncChangeSet`.

    Mirrors ``client.records`` but restricted to single-record forms (no bulk
    create/update/delete). Only write operations are allowed — GET is not
    permitted inside a changeset.

    Do not instantiate directly; use :attr:`AsyncChangeSet.records`.
    """

    def __init__(self, cs_internal: _ChangeSet) -> None:
        self._cs = cs_internal

    def create(self, table: str, data: Dict[str, Any]) -> str:
        """
        Add a single-record create to this changeset.

        :param table: Table schema name (e.g. ``"account"``).
        :type table: :class:`str`
        :param data: Column values for the new record.
        :type data: dict[str, typing.Any]
        :returns: A content-ID reference string (e.g. ``"$1"``) usable in
            subsequent operations within this changeset as a URI reference
            in ``@odata.bind`` fields or as ``record_id`` in
            :meth:`update` / :meth:`delete`.
        :rtype: :class:`str`

        Example::

            async with batch.changeset() as cs:
                lead_ref = cs.records.create("lead", {"firstname": "Ada"})
                cs.records.create("account", {
                    "name": "Babbage",
                    "originatingleadid@odata.bind": lead_ref,
                })
        """
        return self._cs.add_create(table, data)

    def update(self, table: str, record_id: str, changes: Dict[str, Any]) -> None:
        """
        Add a single-record update to this changeset.

        :param table: Table schema name. Ignored when ``record_id`` is a
            content-ID reference.
        :type table: :class:`str`
        :param record_id: GUID or a content-ID reference (e.g. ``"$1"``)
            returned by a prior :meth:`create` in this changeset.
        :type record_id: :class:`str`
        :param changes: Column values to update.
        :type changes: dict[str, typing.Any]
        """
        self._cs.add_update(table, record_id, changes)

    def delete(self, table: str, record_id: str) -> None:
        """
        Add a single-record delete to this changeset.

        :param table: Table schema name. Ignored when ``record_id`` is a
            content-ID reference.
        :type table: :class:`str`
        :param record_id: GUID or a content-ID reference (e.g. ``"$1"``).
        :type record_id: :class:`str`
        """
        self._cs.add_delete(table, record_id)


class AsyncChangeSet:
    """
    A transactional group of single-record write operations.

    All operations succeed or are rolled back together. Use as an async context
    manager or call :attr:`records` to add operations directly.

    Do not instantiate directly; use :meth:`AsyncBatchRequest.changeset`.

    Example::

        async with batch.changeset() as cs:
            ref = cs.records.create("contact", {"firstname": "Alice"})
            cs.records.update("account", account_id, {
                "primarycontactid@odata.bind": ref
            })
    """

    def __init__(self, internal: _ChangeSet) -> None:
        self._internal = internal
        self.records = AsyncChangeSetRecordOperations(internal)

    async def __aenter__(self) -> "AsyncChangeSet":
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        return None


# ---------------------------------------------------------------------------
# Batch request namespaces
# ---------------------------------------------------------------------------


class AsyncBatchRecordOperations:
    """
    Record operations on an :class:`AsyncBatchRequest`.

    Mirrors ``client.records`` exactly: same method names, same signatures.
    All methods return ``None``; results are available via
    :class:`~PowerPlatform.Dataverse.models.batch.BatchResult` after
    :meth:`AsyncBatchRequest.execute`.

    Do not instantiate directly; use ``batch.records``.
    """

    def __init__(self, batch: "AsyncBatchRequest") -> None:
        self._batch = batch

    def create(
        self,
        table: str,
        data: Union[Dict[str, Any], List[Dict[str, Any]]],
    ) -> None:
        """
        Add a create operation to the batch.

        A single dict creates one record (POST entity_set).
        A list of dicts creates all records via the ``CreateMultiple`` action
        (one batch item).

        :param table: Table schema name (e.g. ``"account"``).
        :type table: :class:`str`
        :param data: Single record dict or list of record dicts.
        :type data: dict or list[dict]
        """
        self._batch._items.append(_RecordCreate(table=table, data=data))

    def update(
        self,
        table: str,
        ids: Union[str, List[str]],
        changes: Union[Dict[str, Any], List[Dict[str, Any]]],
    ) -> None:
        """
        Add an update operation to the batch.

        - **Single** ``(table, "guid", {...})`` -> one PATCH request.
        - **Broadcast** ``(table, [id1, id2], {...})`` -> one ``UpdateMultiple`` POST.
        - **Paired** ``(table, [id1, id2], [{...}, {...}])`` -> one ``UpdateMultiple`` POST.

        :param table: Table schema name.
        :type table: :class:`str`
        :param ids: Single GUID or list of GUIDs.
        :type ids: str or list[str]
        :param changes: Single dict (single/broadcast) or list of dicts (paired).
        :type changes: dict or list[dict]
        """
        self._batch._items.append(_RecordUpdate(table=table, ids=ids, changes=changes))

    def delete(
        self,
        table: str,
        ids: Union[str, List[str]],
        *,
        use_bulk_delete: bool = True,
    ) -> None:
        """
        Add a delete operation to the batch.

        - **Single** ``(table, "guid")`` -> one DELETE request.
        - **List + use_bulk_delete=True** (default) -> one ``BulkDelete`` POST.
        - **List + use_bulk_delete=False** -> one DELETE per record.

        :param table: Table schema name.
        :type table: :class:`str`
        :param ids: Single GUID or list of GUIDs.
        :type ids: str or list[str]
        :param use_bulk_delete: When True (default) and ``ids`` is a list, use the
            BulkDelete action. When False, delete records individually.
        :type use_bulk_delete: :class:`bool`
        """
        self._batch._items.append(_RecordDelete(table=table, ids=ids, use_bulk_delete=use_bulk_delete))

    def get(
        self,
        table: str,
        record_id: str,
        *,
        select: Optional[List[str]] = None,
    ) -> None:
        """
        Add a single-record get operation to the batch.

        Only the single-record overload (``record_id`` provided) is supported.
        The paginated/multi-record overload is **not** supported in batch.

        :param table: Table schema name.
        :type table: :class:`str`
        :param record_id: GUID of the record to retrieve.
        :type record_id: :class:`str`
        :param select: Optional list of column names to include.
        :type select: list[str] or None
        """
        self._batch._items.append(_RecordGet(table=table, record_id=record_id, select=select))

    def upsert(
        self,
        table: str,
        items: List[Union[UpsertItem, Dict[str, Any]]],
    ) -> None:
        """
        Add an upsert operation to the batch.

        Each item must be a :class:`~PowerPlatform.Dataverse.models.upsert.UpsertItem`
        or a plain ``dict`` with ``"alternate_key"`` and ``"record"`` keys (both dicts).

        :param table: Table schema name (e.g. ``"account"``).
        :type table: :class:`str`
        :param items: Non-empty list of :class:`~PowerPlatform.Dataverse.models.upsert.UpsertItem`
            instances or equivalent dicts.
        :type items: list[~PowerPlatform.Dataverse.models.upsert.UpsertItem]

        :raises TypeError: If ``items`` is not a non-empty list, or if any element is invalid.
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
        self._batch._items.append(_RecordUpsert(table=table, items=normalized))


class AsyncBatchTableOperations:
    """
    Table metadata operations on an :class:`AsyncBatchRequest`.

    Mirrors ``client.tables`` exactly: same method names, same signatures.
    All methods return ``None``; results arrive via
    :class:`~PowerPlatform.Dataverse.models.batch.BatchResult`.

    .. note::
        ``tables.delete``, ``tables.add_columns``, and ``tables.remove_columns``
        require a metadata lookup (GET ``EntityDefinitions``) at
        :meth:`AsyncBatchRequest.execute` time to resolve the table's MetadataId.

    Do not instantiate directly; use ``batch.tables``.
    """

    def __init__(self, batch: "AsyncBatchRequest") -> None:
        self._batch = batch

    def create(
        self,
        table: str,
        columns: Dict[str, Any],
        *,
        solution: Optional[str] = None,
        primary_column: Optional[str] = None,
        display_name: Optional[str] = None,
    ) -> None:
        """
        Add a table-create operation to the batch.

        :param table: Schema name of the new table (e.g. ``"new_Product"``).
        :type table: :class:`str`
        :param columns: Mapping of column schema names to type strings or Enum subclasses.
        :type columns: dict[str, typing.Any]
        :param solution: Optional solution unique name.
        :type solution: str or None
        :param primary_column: Optional primary column schema name.
        :type primary_column: str or None
        :param display_name: Human-readable display name for the table.
        :type display_name: str or None
        """
        self._batch._items.append(
            _TableCreate(
                table=table,
                columns=columns,
                solution=solution,
                primary_column=primary_column,
                display_name=display_name,
            )
        )

    def delete(self, table: str) -> None:
        """
        Add a table-delete operation to the batch.

        The table's ``MetadataId`` is resolved via a GET request at execute time.

        :param table: Schema name of the table to delete.
        :type table: :class:`str`
        """
        self._batch._items.append(_TableDelete(table=table))

    def get(self, table: str) -> None:
        """
        Add a table-metadata-get operation to the batch.

        :param table: Schema name of the table.
        :type table: :class:`str`
        """
        self._batch._items.append(_TableGet(table=table))

    def list(
        self,
        *,
        filter: Optional[str] = None,
        select: Optional[List[str]] = None,
    ) -> None:
        """
        Add a list-all-tables operation to the batch.

        :param filter: Additional OData ``$filter`` expression.
        :type filter: str or None
        :param select: List of property names for ``$select``.
        :type select: list[str] or None
        """
        self._batch._items.append(_TableList(filter=filter, select=select))

    def add_columns(self, table: str, columns: Dict[str, Any]) -> None:
        """
        Add column-create operations to the batch (one per column).

        The table's ``MetadataId`` is resolved at execute time.

        :param table: Schema name of the target table.
        :type table: :class:`str`
        :param columns: Mapping of column schema names to type strings or Enum subclasses.
        :type columns: dict[str, typing.Any]
        """
        self._batch._items.append(_TableAddColumns(table=table, columns=columns))

    def remove_columns(self, table: str, columns: Union[str, List[str]]) -> None:
        """
        Add column-delete operations to the batch (one per column).

        The table's ``MetadataId`` and each column's ``MetadataId`` are resolved
        at execute time.

        :param table: Schema name of the target table.
        :type table: :class:`str`
        :param columns: Column schema name or list of column schema names to remove.
        :type columns: str or list[str]
        """
        self._batch._items.append(_TableRemoveColumns(table=table, columns=columns))

    def create_one_to_many_relationship(
        self,
        lookup: LookupAttributeMetadata,
        relationship: OneToManyRelationshipMetadata,
        *,
        solution: Optional[str] = None,
    ) -> None:
        """
        Add a one-to-many relationship creation to the batch.

        :param lookup: Lookup attribute metadata.
        :type lookup: ~PowerPlatform.Dataverse.models.relationship.LookupAttributeMetadata
        :param relationship: Relationship metadata.
        :type relationship: ~PowerPlatform.Dataverse.models.relationship.OneToManyRelationshipMetadata
        :param solution: Optional solution unique name.
        :type solution: str or None
        """
        self._batch._items.append(_TableCreateOneToMany(lookup=lookup, relationship=relationship, solution=solution))

    def create_many_to_many_relationship(
        self,
        relationship: ManyToManyRelationshipMetadata,
        *,
        solution: Optional[str] = None,
    ) -> None:
        """
        Add a many-to-many relationship creation to the batch.

        :param relationship: Relationship metadata.
        :type relationship: ~PowerPlatform.Dataverse.models.relationship.ManyToManyRelationshipMetadata
        :param solution: Optional solution unique name.
        :type solution: str or None
        """
        self._batch._items.append(_TableCreateManyToMany(relationship=relationship, solution=solution))

    def delete_relationship(self, relationship_id: str) -> None:
        """
        Add a relationship-delete operation to the batch.

        :param relationship_id: GUID of the relationship metadata to delete.
        :type relationship_id: :class:`str`
        """
        self._batch._items.append(_TableDeleteRelationship(relationship_id=relationship_id))

    def get_relationship(self, schema_name: str) -> None:
        """
        Add a relationship-metadata-get operation to the batch.

        :param schema_name: Schema name of the relationship.
        :type schema_name: :class:`str`
        """
        self._batch._items.append(_TableGetRelationship(schema_name=schema_name))

    def create_lookup_field(
        self,
        referencing_table: str,
        lookup_field_name: str,
        referenced_table: str,
        *,
        display_name: Optional[str] = None,
        description: Optional[str] = None,
        required: bool = False,
        cascade_delete: str = CASCADE_BEHAVIOR_REMOVE_LINK,
        solution: Optional[str] = None,
        language_code: int = 1033,
    ) -> None:
        """
        Add a lookup field creation to the batch (convenience wrapper for
        :meth:`create_one_to_many_relationship`).

        :param referencing_table: Logical name of the child (many) table.
        :type referencing_table: :class:`str`
        :param lookup_field_name: Schema name for the lookup field.
        :type lookup_field_name: :class:`str`
        :param referenced_table: Logical name of the parent (one) table.
        :type referenced_table: :class:`str`
        :param display_name: Display name for the lookup field.
        :type display_name: str or None
        :param description: Optional description.
        :type description: str or None
        :param required: Whether the lookup is required.
        :type required: :class:`bool`
        :param cascade_delete: Delete cascade behaviour.
        :type cascade_delete: :class:`str`
        :param solution: Optional solution unique name.
        :type solution: str or None
        :param language_code: Language code for labels (default 1033).
        :type language_code: :class:`int`
        """
        self._batch._items.append(
            _TableCreateLookupField(
                referencing_table=referencing_table,
                lookup_field_name=lookup_field_name,
                referenced_table=referenced_table,
                display_name=display_name,
                description=description,
                required=required,
                cascade_delete=cascade_delete,
                solution=solution,
                language_code=language_code,
            )
        )


# ---------------------------------------------------------------------------
# AsyncBatchQueryOperations
# ---------------------------------------------------------------------------


class AsyncBatchQueryOperations:
    """
    Query operations on an :class:`AsyncBatchRequest`.

    Mirrors ``client.query.sql`` exactly: same method name, same signature.
    Returns ``None``; results arrive via
    :class:`~PowerPlatform.Dataverse.models.batch.BatchResult`.

    Do not instantiate directly; use ``batch.query``.
    """

    def __init__(self, batch: "AsyncBatchRequest") -> None:
        self._batch = batch

    def sql(self, sql: str) -> None:
        """
        Add a SQL SELECT query to the batch.

        The entity set is resolved from the table name in the SQL statement at
        :meth:`AsyncBatchRequest.execute` time.

        :param sql: A single ``SELECT`` statement within the Dataverse-supported subset.
        :type sql: :class:`str`

        :raises ~PowerPlatform.Dataverse.core.errors.ValidationError:
            If ``sql`` is not a non-empty string.

        Example::

            batch.query.sql("SELECT accountid, name FROM account WHERE name = 'Contoso'")
        """
        if not isinstance(sql, str) or not sql.strip():
            raise ValidationError("sql must be a non-empty string", subcode=VALIDATION_SQL_EMPTY)
        self._batch._items.append(_QuerySql(sql=sql.strip()))


# ---------------------------------------------------------------------------
# DataFrame batch operations
# ---------------------------------------------------------------------------


class AsyncBatchDataFrameOperations:
    """Async DataFrame-oriented wrappers for batch record operations.

    Provides :meth:`create`, :meth:`update`, and :meth:`delete` that accept
    ``pandas.DataFrame`` / ``pandas.Series`` inputs and convert them to standard
    dicts before enqueueing on the batch.

    Accessed via ``batch.dataframe``.
    """

    def __init__(self, batch: "AsyncBatchRequest") -> None:
        self._batch = batch

    def create(self, table: str, records: pd.DataFrame) -> None:
        """Enqueue record creates from a pandas DataFrame.

        Each row becomes a record. All rows are bundled in a single
        ``CreateMultiple`` batch item (one HTTP request in the batch).

        :param table: Table schema name (e.g. ``"account"``).
        :type table: :class:`str`
        :param records: DataFrame where each row is a record to create.
        :type records: ~pandas.DataFrame

        :raises TypeError: If ``records`` is not a pandas DataFrame.
        :raises ValueError: If ``records`` is empty or any row has no non-null values.
        """
        if not isinstance(records, pd.DataFrame):
            raise TypeError("records must be a pandas DataFrame")
        if records.empty:
            raise ValueError("records must be a non-empty DataFrame")

        from ...utils._pandas import dataframe_to_records

        record_list = dataframe_to_records(records)
        empty_rows = [records.index[i] for i, r in enumerate(record_list) if not r]
        if empty_rows:
            raise ValueError(
                f"Records at index(es) {empty_rows} have no non-null values. "
                "All rows must contain at least one field to create."
            )
        self._batch.records.create(table, record_list)

    def update(
        self,
        table: str,
        changes: pd.DataFrame,
        id_column: str,
        clear_nulls: bool = False,
    ) -> None:
        """Enqueue record updates from a pandas DataFrame.

        :param table: Table schema name (e.g. ``"account"``).
        :type table: :class:`str`
        :param changes: DataFrame where each row contains a record GUID and the fields to update.
        :type changes: ~pandas.DataFrame
        :param id_column: Name of the DataFrame column containing record GUIDs.
        :type id_column: :class:`str`
        :param clear_nulls: When ``False`` (default), NaN/None values are skipped.
            When ``True``, NaN/None sends ``null`` to clear the field.
        :type clear_nulls: :class:`bool`

        :raises TypeError: If ``changes`` is not a pandas DataFrame.
        :raises ValueError: If ``changes`` is empty, ``id_column`` is missing, or IDs are invalid.
        """
        if not isinstance(changes, pd.DataFrame):
            raise TypeError("changes must be a pandas DataFrame")
        if changes.empty:
            raise ValueError("changes must be a non-empty DataFrame")
        if id_column not in changes.columns:
            raise ValueError(f"id_column '{id_column}' not found in DataFrame columns")

        raw_ids = changes[id_column].tolist()
        invalid = [changes.index[i] for i, v in enumerate(raw_ids) if not isinstance(v, str) or not v.strip()]
        if invalid:
            raise ValueError(
                f"id_column '{id_column}' contains invalid values at row index(es) {invalid}. "
                "All IDs must be non-empty strings."
            )
        ids = [v.strip() for v in raw_ids]

        change_columns = [c for c in changes.columns if c != id_column]
        if not change_columns:
            raise ValueError(
                "No columns to update. The DataFrame must contain at least one column besides the id_column."
            )

        from ...utils._pandas import dataframe_to_records

        change_list = dataframe_to_records(changes[change_columns], na_as_null=clear_nulls)
        paired = [(rid, patch) for rid, patch in zip(ids, change_list) if patch]
        if not paired:
            return
        ids_filtered = [p[0] for p in paired]
        change_filtered = [p[1] for p in paired]

        self._batch.records.update(table, ids_filtered, change_filtered)

    def delete(
        self,
        table: str,
        ids: pd.Series,
        use_bulk_delete: bool = True,
    ) -> None:
        """Enqueue record deletes from a pandas Series of GUIDs.

        :param table: Table schema name (e.g. ``"account"``).
        :type table: :class:`str`
        :param ids: Series of record GUIDs to delete.
        :type ids: ~pandas.Series
        :param use_bulk_delete: When ``True`` (default) and ``ids`` has multiple
            values, use the ``BulkDelete`` action.
        :type use_bulk_delete: :class:`bool`

        :raises TypeError: If ``ids`` is not a pandas Series.
        :raises ValueError: If ``ids`` contains invalid values.
        """
        if not isinstance(ids, pd.Series):
            raise TypeError("ids must be a pandas Series")
        raw_list = ids.tolist()
        if not raw_list:
            return
        invalid = [ids.index[i] for i, v in enumerate(raw_list) if not isinstance(v, str) or not v.strip()]
        if invalid:
            raise ValueError(f"ids contains invalid values at index(es) {invalid}. All IDs must be non-empty strings.")
        id_list = [v.strip() for v in raw_list]
        self._batch.records.delete(table, id_list, use_bulk_delete=use_bulk_delete)


# ---------------------------------------------------------------------------
# AsyncBatchRequest and AsyncBatchOperations
# ---------------------------------------------------------------------------


class AsyncBatchRequest:
    """
    Builder for constructing and executing a Dataverse OData ``$batch`` request.

    Obtain via :meth:`AsyncBatchOperations.new` (``client.batch.new()``). Add operations
    through :attr:`records`, :attr:`tables`, :attr:`query`, and :attr:`dataframe`,
    optionally group writes into a :meth:`changeset`, then call :meth:`execute`.

    Operations are executed sequentially in the order added. The resulting
    :class:`~PowerPlatform.Dataverse.models.batch.BatchResult` contains one
    :class:`~PowerPlatform.Dataverse.models.batch.BatchItemResponse` per HTTP
    request dispatched (some operations expand to multiple requests).

    .. note::
        Maximum 1000 HTTP operations per batch.

    Example::

        batch = client.batch.new()
        batch.records.create("account", {"name": "Contoso"})
        batch.tables.get("account")
        async with batch.changeset() as cs:
            ref = cs.records.create("contact", {"firstname": "Alice"})
            cs.records.update("account", account_id, {
                "primarycontactid@odata.bind": ref
            })
        result = await batch.execute()
    """

    def __init__(self, client: "AsyncDataverseClient") -> None:
        self._client = client
        self._items: List[Any] = []
        self._content_id_counter: List[int] = [1]  # shared across all changesets
        self.records = AsyncBatchRecordOperations(self)
        self.tables = AsyncBatchTableOperations(self)
        self.query = AsyncBatchQueryOperations(self)
        self.dataframe = AsyncBatchDataFrameOperations(self)

    def changeset(self) -> AsyncChangeSet:
        """
        Create a new :class:`AsyncChangeSet` attached to this batch.

        The changeset is added to the batch immediately. Operations added to
        the returned :class:`AsyncChangeSet` via ``cs.records.*`` execute atomically.

        :returns: A new :class:`AsyncChangeSet` ready to receive operations.

        Example::

            async with batch.changeset() as cs:
                cs.records.create("account", {"name": "ACME"})
                cs.records.create("contact", {"firstname": "Bob"})
        """
        internal = _ChangeSet(_counter=self._content_id_counter)
        self._items.append(internal)
        return AsyncChangeSet(internal)

    async def execute(self, *, continue_on_error: bool = False) -> BatchResult:
        """
        Submit the batch to Dataverse and return all responses.

        :param continue_on_error: When False (default), Dataverse stops at the
            first failure and returns that operation's error as a 4xx response.
            When True, ``Prefer: odata.continue-on-error`` is sent and all
            operations are attempted.
        :returns: :class:`~PowerPlatform.Dataverse.models.batch.BatchResult`
            with one entry per HTTP operation in submission order.
        :raises ValidationError: If the batch exceeds 1000 operations or an
            unsupported column type is specified.
        :raises MetadataError: If metadata pre-resolution fails (table or
            column not found) for ``tables.delete``, ``tables.add_columns``,
            or ``tables.remove_columns``.
        :raises HttpError: On HTTP-level failures (auth, server error, etc.)
            that prevent the batch from executing.
        """
        async with self._client._scoped_odata() as od:
            return await _AsyncBatchClient(od).execute(self._items, continue_on_error=continue_on_error)


class AsyncBatchOperations:
    """
    Async namespace for batch operations (``client.batch``).

    Accessed via ``client.batch``. Use :meth:`new` to create an
    :class:`AsyncBatchRequest` builder.

    :param client: The parent :class:`~PowerPlatform.Dataverse.aio.async_client.AsyncDataverseClient` instance.

    Example::

        batch = client.batch.new()
        batch.records.create("account", {"name": "Fabrikam"})
        result = await batch.execute()
    """

    def __init__(self, client: "AsyncDataverseClient") -> None:
        self._client = client

    def new(self) -> AsyncBatchRequest:
        """
        Create a new empty :class:`AsyncBatchRequest` builder.

        :returns: An empty :class:`AsyncBatchRequest`.
        """
        return AsyncBatchRequest(self._client)
