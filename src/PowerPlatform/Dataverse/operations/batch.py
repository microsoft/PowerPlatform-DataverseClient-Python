# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Batch operation namespaces for the Dataverse SDK."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from ..core.errors import ValidationError
from ..data._batch import (
    _BatchClient,
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
from ..models.batch import BatchResult
from ..models.upsert import UpsertItem
from ..models.relationship import (
    LookupAttributeMetadata,
    OneToManyRelationshipMetadata,
    ManyToManyRelationshipMetadata,
)
from ..common.constants import CASCADE_BEHAVIOR_REMOVE_LINK

if TYPE_CHECKING:
    from ..client import DataverseClient

__all__ = []


# ---------------------------------------------------------------------------
# Changeset namespaces
# ---------------------------------------------------------------------------


class ChangeSetRecordOperations:
    """
    Record write operations available inside a :class:`ChangeSet`.

    Mirrors ``client.records`` but restricted to single-record forms (no bulk
    create/update/delete). Only write operations are allowed — GET is not
    permitted inside a changeset.

    Do not instantiate directly; use :attr:`ChangeSet.records`.
    """

    def __init__(self, cs_internal: _ChangeSet) -> None:
        self._cs = cs_internal

    def create(self, table: str, data: Dict[str, Any]) -> str:
        """
        Add a single-record create to this changeset.

        :param table: Table schema name (e.g. ``"account"``).
        :param data: Column values for the new record.
        :returns: A content-ID reference string (e.g. ``"$1"``) usable in
            subsequent operations within this changeset as a URI reference
            in ``@odata.bind`` fields or as ``record_id`` in
            :meth:`update` / :meth:`delete`.
        :rtype: :class:`str`

        Example::

            with batch.changeset() as cs:
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
        :param record_id: GUID or a content-ID reference (e.g. ``"$1"``)
            returned by a prior :meth:`create` in this changeset.
        :param changes: Column values to update.
        """
        self._cs.add_update(table, record_id, changes)

    def delete(self, table: str, record_id: str) -> None:
        """
        Add a single-record delete to this changeset.

        :param table: Table schema name. Ignored when ``record_id`` is a
            content-ID reference.
        :param record_id: GUID or a content-ID reference (e.g. ``"$1"``).
        """
        self._cs.add_delete(table, record_id)


class ChangeSet:
    """
    A transactional group of single-record write operations.

    All operations succeed or are rolled back together. Use as a context
    manager or call :attr:`records` to add operations directly.

    Do not instantiate directly; use :meth:`BatchRequest.changeset`.

    Example::

        with batch.changeset() as cs:
            ref = cs.records.create("contact", {"firstname": "Alice"})
            cs.records.update("account", account_id, {
                "primarycontactid@odata.bind": ref
            })
    """

    def __init__(self, internal: _ChangeSet) -> None:
        self._internal = internal
        self.records = ChangeSetRecordOperations(internal)

    def __enter__(self) -> "ChangeSet":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        return None


# ---------------------------------------------------------------------------
# Batch request namespaces
# ---------------------------------------------------------------------------


class BatchRecordOperations:
    """
    Record operations on a :class:`BatchRequest`.

    Mirrors ``client.records`` exactly: same method names, same signatures.
    All methods return ``None``; results are available via
    :class:`~PowerPlatform.Dataverse.models.batch.BatchResult` after
    :meth:`BatchRequest.execute`.

    Do not instantiate directly; use ``batch.records``.
    """

    def __init__(self, batch: "BatchRequest") -> None:
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
        :param data: Single record dict or list of record dicts.
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
        :param ids: Single GUID or list of GUIDs.
        :param changes: Single dict (single/broadcast) or list of dicts (paired).
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
          The async job ID will be available in ``BatchItemResponse.data["JobId"]``.
        - **List + use_bulk_delete=False** -> one DELETE per record.

        :param table: Table schema name.
        :param ids: Single GUID or list of GUIDs.
        :param use_bulk_delete: When True (default) and ``ids`` is a list, use the
            BulkDelete action. When False, delete records individually.
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
        The paginated/multi-record overload of ``client.records.get()``
        (``filter``, ``orderby``, etc., without ``record_id``) is **not**
        supported in batch — pagination requires following
        ``@odata.nextLink`` across multiple round-trips, which is
        incompatible with a single batch request.

        The response body will be available in
        :attr:`~PowerPlatform.Dataverse.models.batch.BatchItemResponse.data`
        after :meth:`BatchRequest.execute`.

        :param table: Table schema name.
        :param record_id: GUID of the record to retrieve.
        :param select: Optional list of column names to include.
        """
        self._batch._items.append(_RecordGet(table=table, record_id=record_id, select=select))

    def upsert(
        self,
        table: str,
        items: List[Union[UpsertItem, Dict[str, Any]]],
    ) -> None:
        """
        Add an upsert operation to the batch.

        Mirrors :meth:`~PowerPlatform.Dataverse.operations.records.RecordOperations.upsert`:
        a single item becomes a PATCH request using the alternate key; multiple items
        become one ``UpsertMultiple`` POST.

        Each item must be a :class:`~PowerPlatform.Dataverse.models.upsert.UpsertItem`
        or a plain ``dict`` with ``"alternate_key"`` and ``"record"`` keys (both dicts).

        :param table: Table schema name (e.g. ``"account"``).
        :param items: Non-empty list of :class:`~PowerPlatform.Dataverse.models.upsert.UpsertItem`
            instances or equivalent dicts.

        :raises TypeError: If ``items`` is not a non-empty list, or if any element is
            neither a :class:`~PowerPlatform.Dataverse.models.upsert.UpsertItem` nor a
            dict with ``"alternate_key"`` and ``"record"`` keys.

        Example::

            from PowerPlatform.Dataverse.models.upsert import UpsertItem

            batch.records.upsert("account", [
                UpsertItem(
                    alternate_key={"accountnumber": "ACC-001"},
                    record={"name": "Contoso Ltd"},
                ),
                UpsertItem(
                    alternate_key={"accountnumber": "ACC-002"},
                    record={"name": "Fabrikam Inc"},
                ),
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
                raise TypeError("Each item must be an UpsertItem or a dict with 'alternate_key' and 'record' keys")
        self._batch._items.append(_RecordUpsert(table=table, items=normalized))


class BatchTableOperations:
    """
    Table metadata operations on a :class:`BatchRequest`.

    Mirrors ``client.tables`` exactly: same method names, same signatures.
    All methods return ``None``; results arrive via
    :class:`~PowerPlatform.Dataverse.models.batch.BatchResult`.

    .. note::
        ``tables.delete``, ``tables.add_columns``, and ``tables.remove_columns``
        require a metadata lookup (GET ``EntityDefinitions``) at
        :meth:`BatchRequest.execute` time to resolve the table's MetadataId.
        This lookup is transparent to the caller.

    .. note::
        ``tables.add_columns`` and ``tables.remove_columns`` each produce one
        batch item per column, so they contribute multiple entries to
        :attr:`~PowerPlatform.Dataverse.models.batch.BatchResult.responses`.

    Do not instantiate directly; use ``batch.tables``.
    """

    def __init__(self, batch: "BatchRequest") -> None:
        self._batch = batch

    def create(
        self,
        table: str,
        columns: Dict[str, Any],
        *,
        solution: Optional[str] = None,
        primary_column: Optional[str] = None,
    ) -> None:
        """
        Add a table-create operation to the batch.

        .. note::
            The pre-existence check performed by ``client.tables.create`` is skipped
            in batch mode. If the table already exists the server returns an error
            in the corresponding :class:`~PowerPlatform.Dataverse.models.batch.BatchItemResponse`.

        :param table: Schema name of the new table (e.g. ``"new_Product"``).
        :param columns: Mapping of column schema names to type strings or Enum subclasses.
        :param solution: Optional solution unique name.
        :param primary_column: Optional primary column schema name.
        """
        self._batch._items.append(
            _TableCreate(
                table=table,
                columns=columns,
                solution=solution,
                primary_column=primary_column,
            )
        )

    def delete(self, table: str) -> None:
        """
        Add a table-delete operation to the batch.

        The table's ``MetadataId`` is resolved via a GET request at execute time.

        :param table: Schema name of the table to delete.
        """
        self._batch._items.append(_TableDelete(table=table))

    def get(self, table: str) -> None:
        """
        Add a table-metadata-get operation to the batch.

        The response will be in ``BatchItemResponse.data`` after execute.

        :param table: Schema name of the table.
        """
        self._batch._items.append(_TableGet(table=table))

    def list(self) -> None:
        """
        Add a list-all-tables operation to the batch.

        The response will be in ``BatchItemResponse.data`` after execute.
        """
        self._batch._items.append(_TableList())

    def add_columns(self, table: str, columns: Dict[str, Any]) -> None:
        """
        Add column-create operations to the batch (one per column).

        The table's ``MetadataId`` is resolved at execute time. Each column
        produces one entry in :attr:`BatchResult.responses`.

        :param table: Schema name of the target table.
        :param columns: Mapping of column schema names to type strings or Enum subclasses.
        """
        self._batch._items.append(_TableAddColumns(table=table, columns=columns))

    def remove_columns(self, table: str, columns: Union[str, List[str]]) -> None:
        """
        Add column-delete operations to the batch (one per column).

        The table's ``MetadataId`` and each column's ``MetadataId`` are resolved
        at execute time. Each column produces one entry in
        :attr:`BatchResult.responses`.

        :param table: Schema name of the target table.
        :param columns: Column schema name or list of column schema names to remove.
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
        :param relationship: Relationship metadata.
        :param solution: Optional solution unique name.
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
        :param solution: Optional solution unique name.
        """
        self._batch._items.append(_TableCreateManyToMany(relationship=relationship, solution=solution))

    def delete_relationship(self, relationship_id: str) -> None:
        """
        Add a relationship-delete operation to the batch.

        :param relationship_id: GUID of the relationship metadata to delete.
        """
        self._batch._items.append(_TableDeleteRelationship(relationship_id=relationship_id))

    def get_relationship(self, schema_name: str) -> None:
        """
        Add a relationship-metadata-get operation to the batch.

        The response will be in ``BatchItemResponse.data`` after execute.

        :param schema_name: Schema name of the relationship.
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
        :param lookup_field_name: Schema name for the lookup field.
        :param referenced_table: Logical name of the parent (one) table.
        :param display_name: Display name for the lookup field.
        :param description: Optional description.
        :param required: Whether the lookup is required.
        :param cascade_delete: Delete cascade behaviour.
        :param solution: Optional solution unique name.
        :param language_code: Language code for labels (default 1033).
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
# BatchQueryOperations
# ---------------------------------------------------------------------------


class BatchQueryOperations:
    """
    Query operations on a :class:`BatchRequest`.

    Mirrors ``client.query`` exactly: same method names, same signatures.
    All methods return ``None``; results arrive via
    :class:`~PowerPlatform.Dataverse.models.batch.BatchResult`.

    Do not instantiate directly; use ``batch.query``.
    """

    def __init__(self, batch: "BatchRequest") -> None:
        self._batch = batch

    def sql(self, sql: str) -> None:
        """
        Add a SQL SELECT query to the batch.

        Mirrors :meth:`~PowerPlatform.Dataverse.operations.query.QueryOperations.sql`.
        The entity set is resolved from the table name in the SQL statement at
        :meth:`BatchRequest.execute` time.

        :param sql: A single ``SELECT`` statement within the Dataverse-supported subset.
        :type sql: ``str``

        Example::

            batch.query.sql("SELECT accountid, name FROM account WHERE name = 'Contoso'")
        """
        if not isinstance(sql, str) or not sql.strip():
            raise ValidationError("sql must be a non-empty string", subcode="VALIDATION_SQL_EMPTY")
        self._batch._items.append(_QuerySql(sql=sql.strip()))


# ---------------------------------------------------------------------------
# BatchRequest and BatchOperations
# ---------------------------------------------------------------------------


class BatchRequest:
    """
    Builder for constructing and executing a Dataverse OData ``$batch`` request.

    Obtain via :meth:`BatchOperations.new` (``client.batch.new()``). Add operations
    through :attr:`records`, :attr:`tables`, and :attr:`query`, optionally group writes
    into a :meth:`changeset`, then call :meth:`execute`.

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
        with batch.changeset() as cs:
            ref = cs.records.create("contact", {"firstname": "Alice"})
            cs.records.update("account", account_id, {
                "primarycontactid@odata.bind": ref
            })
        result = batch.execute()
    """

    def __init__(self, client: "DataverseClient") -> None:
        self._client = client
        self._items: list = []
        self.records = BatchRecordOperations(self)
        self.tables = BatchTableOperations(self)
        self.query = BatchQueryOperations(self)

    def changeset(self) -> ChangeSet:
        """
        Create a new :class:`ChangeSet` attached to this batch.

        The changeset is added to the batch immediately. Operations added to
        the returned :class:`ChangeSet` via ``cs.records.*`` execute atomically.

        :returns: A new :class:`ChangeSet` ready to receive operations.

        Example::

            with batch.changeset() as cs:
                cs.records.create("account", {"name": "ACME"})
                cs.records.create("contact", {"firstname": "Bob"})
        """
        internal = _ChangeSet()
        self._items.append(internal)
        return ChangeSet(internal)

    def execute(self, *, continue_on_error: bool = False) -> BatchResult:
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
        with self._client._scoped_odata() as od:
            return _BatchClient(od).execute(self._items, continue_on_error=continue_on_error)


class BatchOperations:
    """
    Namespace for batch operations (``client.batch``).

    Accessed via ``client.batch``. Use :meth:`new` to create a
    :class:`BatchRequest` builder.

    :param client: The parent :class:`~PowerPlatform.Dataverse.client.DataverseClient` instance.

    Example::

        batch = client.batch.new()
        batch.records.create("account", {"name": "Fabrikam"})
        result = batch.execute()
    """

    def __init__(self, client: "DataverseClient") -> None:
        self._client = client

    def new(self) -> BatchRequest:
        """
        Create a new empty :class:`BatchRequest` builder.

        :returns: An empty :class:`BatchRequest`.
        """
        return BatchRequest(self._client)
