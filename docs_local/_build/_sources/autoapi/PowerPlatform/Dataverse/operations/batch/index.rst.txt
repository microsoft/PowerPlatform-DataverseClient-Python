PowerPlatform.Dataverse.operations.batch
========================================

.. py:module:: PowerPlatform.Dataverse.operations.batch

.. autoapi-nested-parse::

   Batch operation namespaces for the Dataverse SDK.



Classes
-------

.. autoapisummary::

   PowerPlatform.Dataverse.operations.batch.ChangeSetRecordOperations
   PowerPlatform.Dataverse.operations.batch.ChangeSet
   PowerPlatform.Dataverse.operations.batch.BatchRecordOperations
   PowerPlatform.Dataverse.operations.batch.BatchTableOperations
   PowerPlatform.Dataverse.operations.batch.BatchQueryOperations
   PowerPlatform.Dataverse.operations.batch.BatchDataFrameOperations
   PowerPlatform.Dataverse.operations.batch.BatchRequest
   PowerPlatform.Dataverse.operations.batch.BatchOperations


Module Contents
---------------

.. py:class:: ChangeSetRecordOperations(cs_internal: PowerPlatform.Dataverse.data._batch._ChangeSet)

   Record write operations available inside a :class:`ChangeSet`.

   Mirrors ``client.records`` but restricted to single-record forms (no bulk
   create/update/delete). Only write operations are allowed — GET is not
   permitted inside a changeset.

   Do not instantiate directly; use ``ChangeSet.records``.


   .. py:method:: create(table: str, data: Dict[str, Any]) -> str

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

          with batch.changeset() as cs:
              lead_ref = cs.records.create("lead", {"firstname": "Ada"})
              cs.records.create("account", {
                  "name": "Babbage",
                  "originatingleadid@odata.bind": lead_ref,
              })



   .. py:method:: update(table: str, record_id: str, changes: Dict[str, Any]) -> None

      Add a single-record update to this changeset.

      :param table: Table schema name. Ignored when ``record_id`` is a
          content-ID reference.
      :type table: :class:`str`
      :param record_id: GUID or a content-ID reference (e.g. ``"$1"``)
          returned by a prior :meth:`create` in this changeset.
      :type record_id: :class:`str`
      :param changes: Column values to update.
      :type changes: dict[str, typing.Any]



   .. py:method:: delete(table: str, record_id: str) -> None

      Add a single-record delete to this changeset.

      :param table: Table schema name. Ignored when ``record_id`` is a
          content-ID reference.
      :type table: :class:`str`
      :param record_id: GUID or a content-ID reference (e.g. ``"$1"``).
      :type record_id: :class:`str`



.. py:class:: ChangeSet(internal: PowerPlatform.Dataverse.data._batch._ChangeSet)

   A transactional group of single-record write operations.

   All operations succeed or are rolled back together. Use as a context
   manager or call ``records`` to add operations directly.

   Do not instantiate directly; use :meth:`BatchRequest.changeset`.

   Example::

       with batch.changeset() as cs:
           ref = cs.records.create("contact", {"firstname": "Alice"})
           cs.records.update("account", account_id, {
               "primarycontactid@odata.bind": ref
           })


   .. py:attribute:: records


.. py:class:: BatchRecordOperations(batch: _BatchContext)

   Record operations on a :class:`BatchRequest`.

   Mirrors ``client.records``: same method names, same signatures.
   All methods return ``None``; results are available via
   :class:`~PowerPlatform.Dataverse.models.batch.BatchResult` after
   :meth:`BatchRequest.execute`.

   GA methods: :meth:`retrieve` (single record) and :meth:`list` (multi-record,
   single page). :meth:`get` is deprecated — use :meth:`retrieve` instead.

   Do not instantiate directly; use ``batch.records``.


   .. py:method:: create(table: str, data: Union[Dict[str, Any], List[Dict[str, Any]]]) -> None

      Add a create operation to the batch.

      A single dict creates one record (POST entity_set).
      A list of dicts creates all records via the ``CreateMultiple`` action
      (one batch item).

      :param table: Table schema name (e.g. ``"account"``).
      :type table: :class:`str`
      :param data: Single record dict or list of record dicts.
      :type data: dict or list[dict]



   .. py:method:: update(table: str, ids: Union[str, List[str]], changes: Union[Dict[str, Any], List[Dict[str, Any]]]) -> None

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



   .. py:method:: delete(table: str, ids: Union[str, List[str]], *, use_bulk_delete: bool = True) -> None

      Add a delete operation to the batch.

      - **Single** ``(table, "guid")`` -> one DELETE request.
      - **List + use_bulk_delete=True** (default) -> one ``BulkDelete`` POST.
        The async job ID will be available in ``BatchItemResponse.data["JobId"]``.
      - **List + use_bulk_delete=False** -> one DELETE per record.

      :param table: Table schema name.
      :type table: :class:`str`
      :param ids: Single GUID or list of GUIDs.
      :type ids: str or list[str]
      :param use_bulk_delete: When True (default) and ``ids`` is a list, use the
          BulkDelete action. When False, delete records individually.
      :type use_bulk_delete: :class:`bool`



   .. py:method:: get(table: str, record_id: str, *, select: Optional[List[str]] = None) -> None

      Add a single-record get operation to the batch.

      .. deprecated::
          Use :meth:`retrieve` instead. ``batch.records.get()`` is deprecated
          and will be removed in a future release.

      :param table: Table schema name.
      :type table: :class:`str`
      :param record_id: GUID of the record to retrieve.
      :type record_id: :class:`str`
      :param select: Optional list of column names to include.
      :type select: list[str] or None



   .. py:method:: upsert(table: str, items: List[Union[PowerPlatform.Dataverse.models.upsert.UpsertItem, Dict[str, Any]]]) -> None

      Add an upsert operation to the batch.

      Mirrors :meth:`~PowerPlatform.Dataverse.operations.records.RecordOperations.upsert`:
      a single item becomes a PATCH request using the alternate key; multiple items
      become one ``UpsertMultiple`` POST.

      Each item must be a :class:`~PowerPlatform.Dataverse.models.upsert.UpsertItem`
      or a plain ``dict`` with ``"alternate_key"`` and ``"record"`` keys (both dicts).

      :param table: Table schema name (e.g. ``"account"``).
      :type table: :class:`str`
      :param items: Non-empty list of :class:`~PowerPlatform.Dataverse.models.upsert.UpsertItem`
          instances or equivalent dicts.
      :type items: list[~PowerPlatform.Dataverse.models.upsert.UpsertItem]

      :raises TypeError: If ``items`` is not a non-empty list, or if any element is
          neither a :class:`~PowerPlatform.Dataverse.models.upsert.UpsertItem` nor a
          dict with ``"alternate_key"`` and ``"record"`` keys.

      Example::

          from PowerPlatform.Dataverse.models import UpsertItem

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



   .. py:method:: retrieve(table: str, record_id: str, *, select: Optional[List[str]] = None, expand: Optional[List[str]] = None, include_annotations: Optional[str] = None) -> None

      Add a single-record retrieve operation to the batch.

      GA replacement for the deprecated :meth:`get`. Enqueues a GET request
      for one record by its GUID. The response body will be available in
      :attr:`~PowerPlatform.Dataverse.models.batch.BatchItemResponse.data`
      after :meth:`BatchRequest.execute`.

      :param table: Table schema name (e.g. ``"account"``).
      :type table: :class:`str`
      :param record_id: GUID of the record to retrieve.
      :type record_id: :class:`str`
      :param select: Optional list of column logical names to include.
      :type select: list[str] or None
      :param expand: Optional list of navigation properties to expand.
          Navigation property names are case-sensitive and must match the
          entity's ``$metadata``.
      :type expand: list[str] or None
      :param include_annotations: OData annotation pattern for the
          ``Prefer: odata.include-annotations`` header (e.g. ``"*"`` or
          ``"OData.Community.Display.V1.FormattedValue"``), or ``None``.
      :type include_annotations: :class:`str` or None

      Example::

          batch = client.batch.new()
          batch.records.retrieve(
              "account", account_id,
              select=["name", "statuscode"],
              expand=["primarycontactid"],
              include_annotations="OData.Community.Display.V1.FormattedValue",
          )
          result = batch.execute()
          record = result.responses[0].data
          contact = (record.get("primarycontactid") or {})
          print(contact.get("fullname"))



   .. py:method:: list(table: str, *, filter: Optional[Union[str, FilterExpression]] = None, select: Optional[List[str]] = None, orderby: Optional[List[str]] = None, top: Optional[int] = None, expand: Optional[List[str]] = None, page_size: Optional[int] = None, count: bool = False, include_annotations: Optional[str] = None) -> None

      Add a multi-record list operation to the batch (single page, no pagination).

      Enqueues a GET request for multiple records. Because batch requests are
      a single HTTP round-trip, pagination (``@odata.nextLink``) is **not**
      supported — use ``top`` to bound the result size, or rely on the
      server's default page limit.

      The response body (``{"value": [...]}`` JSON) will be available in
      :attr:`~PowerPlatform.Dataverse.models.batch.BatchItemResponse.data`
      after :meth:`BatchRequest.execute`.

      :param table: Table schema name (e.g. ``"account"``).
      :type table: :class:`str`
      :param filter: Optional OData ``$filter`` expression or :class:`~PowerPlatform.Dataverse.models.filters.FilterExpression`.
      :type filter: str or FilterExpression or None
      :param select: Optional list of column logical names to include.
      :type select: list[str] or None
      :param orderby: Optional list of sort expressions (e.g. ``["name asc"]``).
      :type orderby: list[str] or None
      :param top: Maximum number of records to return.
      :type top: int or None
      :param expand: Optional list of navigation properties to expand.
      :type expand: list[str] or None
      :param page_size: Per-page size hint via ``Prefer: odata.maxpagesize``.
      :type page_size: int or None
      :param count: If ``True``, adds ``$count=true`` to the request.
      :type count: bool
      :param include_annotations: OData annotation pattern for the
          ``Prefer: odata.include-annotations`` header, or ``None``.
      :type include_annotations: :class:`str` or None

      Example::

          batch = client.batch.new()
          batch.records.list(
              "account",
              filter="statecode eq 0",
              select=["name", "statuscode"],
              orderby=["name asc"],
              top=50,
              include_annotations="OData.Community.Display.V1.FormattedValue",
          )
          result = batch.execute()
          records = result.responses[0].data.get("value", [])



.. py:class:: BatchTableOperations(batch: _BatchContext)

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


   .. py:method:: create(table: str, columns: Dict[str, Any], *, solution: Optional[str] = None, primary_column: Optional[str] = None, display_name: Optional[str] = None) -> None

      Add a table-create operation to the batch.

      .. note::
          The pre-existence check performed by ``client.tables.create`` is skipped
          in batch mode. If the table already exists the server returns an error
          in the corresponding :class:`~PowerPlatform.Dataverse.models.batch.BatchItemResponse`.

      :param table: Schema name of the new table (e.g. ``"new_Product"``).
      :type table: :class:`str`
      :param columns: Mapping of column schema names to type strings or Enum subclasses.
      :type columns: dict[str, typing.Any]
      :param solution: Optional solution unique name.
      :type solution: str or None
      :param primary_column: Optional primary column schema name.
      :type primary_column: str or None
      :param display_name: Human-readable display name for the table.
          When omitted, defaults to the table schema name.
      :type display_name: str or None



   .. py:method:: delete(table: str) -> None

      Add a table-delete operation to the batch.

      The table's ``MetadataId`` is resolved via a GET request at execute time.

      :param table: Schema name of the table to delete.
      :type table: :class:`str`



   .. py:method:: get(table: str) -> None

      Add a table-metadata-get operation to the batch.

      The response will be in ``BatchItemResponse.data`` after execute.

      :param table: Schema name of the table.
      :type table: :class:`str`



   .. py:method:: list(*, filter: Optional[str] = None, select: Optional[List[str]] = None) -> None

      Add a list-all-tables operation to the batch.

      Mirrors ``client.tables.list()``.  Supply an optional OData
      ``$filter`` expression to further narrow the results (combined with
      ``IsPrivate eq false`` using ``and``).  ``select`` projects
      specific property names via ``$select``.

      The response will be in ``BatchItemResponse.data`` after execute.

      :param filter: Additional OData ``$filter`` expression.
      :type filter: str or None
      :param select: List of property names for ``$select``.
      :type select: list[str] or None



   .. py:method:: add_columns(table: str, columns: Dict[str, Any]) -> None

      Add column-create operations to the batch (one per column).

      The table's ``MetadataId`` is resolved at execute time. Each column
      produces one entry in :attr:`~PowerPlatform.Dataverse.models.batch.BatchResult.responses`.

      :param table: Schema name of the target table.
      :type table: :class:`str`
      :param columns: Mapping of column schema names to type strings or Enum subclasses.
      :type columns: dict[str, typing.Any]



   .. py:method:: remove_columns(table: str, columns: Union[str, List[str]]) -> None

      Add column-delete operations to the batch (one per column).

      The table's ``MetadataId`` and each column's ``MetadataId`` are resolved
      at execute time. Each column produces one entry in
      :attr:`~PowerPlatform.Dataverse.models.batch.BatchResult.responses`.

      :param table: Schema name of the target table.
      :type table: :class:`str`
      :param columns: Column schema name or list of column schema names to remove.
      :type columns: str or list[str]



   .. py:method:: create_one_to_many_relationship(lookup: PowerPlatform.Dataverse.models.relationship.LookupAttributeMetadata, relationship: PowerPlatform.Dataverse.models.relationship.OneToManyRelationshipMetadata, *, solution: Optional[str] = None) -> None

      Add a one-to-many relationship creation to the batch.

      :param lookup: Lookup attribute metadata.
      :type lookup: ~PowerPlatform.Dataverse.models.relationship.LookupAttributeMetadata
      :param relationship: Relationship metadata.
      :type relationship: ~PowerPlatform.Dataverse.models.relationship.OneToManyRelationshipMetadata
      :param solution: Optional solution unique name.
      :type solution: str or None



   .. py:method:: create_many_to_many_relationship(relationship: PowerPlatform.Dataverse.models.relationship.ManyToManyRelationshipMetadata, *, solution: Optional[str] = None) -> None

      Add a many-to-many relationship creation to the batch.

      :param relationship: Relationship metadata.
      :type relationship: ~PowerPlatform.Dataverse.models.relationship.ManyToManyRelationshipMetadata
      :param solution: Optional solution unique name.
      :type solution: str or None



   .. py:method:: delete_relationship(relationship_id: str) -> None

      Add a relationship-delete operation to the batch.

      :param relationship_id: GUID of the relationship metadata to delete.
      :type relationship_id: :class:`str`



   .. py:method:: get_relationship(schema_name: str) -> None

      Add a relationship-metadata-get operation to the batch.

      The response will be in ``BatchItemResponse.data`` after execute.

      :param schema_name: Schema name of the relationship.
      :type schema_name: :class:`str`



   .. py:method:: create_lookup_field(referencing_table: str, lookup_field_name: str, referenced_table: str, *, display_name: Optional[str] = None, description: Optional[str] = None, required: bool = False, cascade_delete: str = CASCADE_BEHAVIOR_REMOVE_LINK, solution: Optional[str] = None, language_code: int = 1033) -> None

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



.. py:class:: BatchQueryOperations(batch: _BatchContext)

   Query operations on a :class:`BatchRequest`.

   Mirrors ``client.query`` exactly: same method names, same signatures.
   All methods return ``None``; results arrive via
   :class:`~PowerPlatform.Dataverse.models.batch.BatchResult`.

   Do not instantiate directly; use ``batch.query``.


   .. py:method:: sql(sql: str) -> None

      Add a SQL SELECT query to the batch.

      Mirrors :meth:`~PowerPlatform.Dataverse.operations.query.QueryOperations.sql`.
      The entity set is resolved from the table name in the SQL statement at
      :meth:`BatchRequest.execute` time.

      :param sql: A single ``SELECT`` statement within the Dataverse-supported subset.
      :type sql: :class:`str`

      :raises ~PowerPlatform.Dataverse.core.errors.ValidationError:
          If ``sql`` is not a non-empty string.

      Example::

          batch.query.sql("SELECT accountid, name FROM account WHERE name = 'Contoso'")



.. py:class:: BatchDataFrameOperations(batch: _BatchContext)

   DataFrame-oriented wrappers for batch record operations.

   Provides :meth:`create`, :meth:`update`, and :meth:`delete` that accept
   ``pandas.DataFrame`` / ``pandas.Series`` inputs and convert them to standard
   dicts before enqueueing on the batch.  This lets data-science callers feed
   DataFrames directly into a batch without manual conversion.

   Accessed via ``batch.dataframe``.

   Example::

       import pandas as pd

       batch = client.batch.new()
       df = pd.DataFrame([
           {"name": "Contoso", "telephone1": "555-0100"},
           {"name": "Fabrikam", "telephone1": "555-0200"},
       ])
       batch.dataframe.create("account", df)
       result = batch.execute()


   .. py:method:: create(table: str, records: pandas.DataFrame) -> None

      Enqueue record creates from a pandas DataFrame.

      Each row becomes a record. All rows are bundled in a single
      ``CreateMultiple`` batch item (one HTTP request in the batch).

      :param table: Table schema name (e.g. ``"account"``).
      :type table: :class:`str`
      :param records: DataFrame where each row is a record to create.
      :type records: ~pandas.DataFrame

      :raises TypeError: If ``records`` is not a pandas DataFrame.
      :raises ValueError: If ``records`` is empty or any row has no non-null values.

      Example::

          df = pd.DataFrame([{"name": "Contoso"}, {"name": "Fabrikam"}])
          batch.dataframe.create("account", df)



   .. py:method:: update(table: str, changes: pandas.DataFrame, id_column: str, clear_nulls: bool = False) -> None

      Enqueue record updates from a pandas DataFrame.

      Each row represents an update. The ``id_column`` specifies which
      column contains the record GUIDs.

      :param table: Table schema name (e.g. ``"account"``).
      :type table: :class:`str`
      :param changes: DataFrame where each row contains a record GUID and
          the fields to update.
      :type changes: ~pandas.DataFrame
      :param id_column: Name of the DataFrame column containing record GUIDs.
      :type id_column: :class:`str`
      :param clear_nulls: When ``False`` (default), NaN/None values are
          skipped. When ``True``, NaN/None sends ``null`` to clear the field.
      :type clear_nulls: :class:`bool`

      :raises TypeError: If ``changes`` is not a pandas DataFrame.
      :raises ValueError: If ``changes`` is empty, ``id_column`` is missing,
          or IDs are invalid.

      Example::

          df = pd.DataFrame([
              {"accountid": "guid-1", "telephone1": "555-0100"},
              {"accountid": "guid-2", "telephone1": "555-0200"},
          ])
          batch.dataframe.update("account", df, id_column="accountid")



   .. py:method:: delete(table: str, ids: pandas.Series, use_bulk_delete: bool = True) -> None

      Enqueue record deletes from a pandas Series of GUIDs.

      :param table: Table schema name (e.g. ``"account"``).
      :type table: :class:`str`
      :param ids: Series of record GUIDs to delete.
      :type ids: ~pandas.Series
      :param use_bulk_delete: When ``True`` (default) and ``ids`` has multiple
          values, use the ``BulkDelete`` action.
      :type use_bulk_delete: :class:`bool`

      :raises TypeError: If ``ids`` is not a pandas Series.
      :raises ValueError: If ``ids`` contains invalid values.

      Example::

          ids_series = pd.Series(["guid-1", "guid-2", "guid-3"])
          batch.dataframe.delete("account", ids_series)



.. py:class:: BatchRequest(client: PowerPlatform.Dataverse.client.DataverseClient)

   Builder for constructing and executing a Dataverse OData ``$batch`` request.

   Obtain via :meth:`BatchOperations.new` (``client.batch.new()``). Add operations
   through ``records``, ``tables``, ``query``, and ``dataframe``,
   optionally group writes
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


   .. py:attribute:: records


   .. py:attribute:: tables


   .. py:attribute:: query


   .. py:attribute:: dataframe


   .. py:method:: changeset() -> ChangeSet

      Create a new :class:`ChangeSet` attached to this batch.

      The changeset is added to the batch immediately. Operations added to
      the returned :class:`ChangeSet` via ``cs.records.*`` execute atomically.

      :returns: A new :class:`ChangeSet` ready to receive operations.

      Example::

          with batch.changeset() as cs:
              cs.records.create("account", {"name": "ACME"})
              cs.records.create("contact", {"firstname": "Bob"})



   .. py:method:: execute(*, continue_on_error: bool = False) -> PowerPlatform.Dataverse.models.batch.BatchResult

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



.. py:class:: BatchOperations(client: PowerPlatform.Dataverse.client.DataverseClient)

   Namespace for batch operations (``client.batch``).

   Accessed via ``client.batch``. Use :meth:`new` to create a
   :class:`BatchRequest` builder.

   :param client: The parent :class:`~PowerPlatform.Dataverse.client.DataverseClient` instance.

   Example::

       batch = client.batch.new()
       batch.records.create("account", {"name": "Fabrikam"})
       result = batch.execute()


   .. py:method:: new() -> BatchRequest

      Create a new empty :class:`BatchRequest` builder.

      :returns: An empty :class:`BatchRequest`.



