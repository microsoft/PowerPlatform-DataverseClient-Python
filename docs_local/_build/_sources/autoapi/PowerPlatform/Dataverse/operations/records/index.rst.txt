PowerPlatform.Dataverse.operations.records
==========================================

.. py:module:: PowerPlatform.Dataverse.operations.records

.. autoapi-nested-parse::

   Record CRUD operations namespace for the Dataverse SDK.



Classes
-------

.. autoapisummary::

   PowerPlatform.Dataverse.operations.records.RecordOperations


Module Contents
---------------

.. py:class:: RecordOperations(client: PowerPlatform.Dataverse.client.DataverseClient)

   Namespace for record-level CRUD operations.

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


   .. py:method:: create(table: str, data: Dict[str, Any]) -> str
                  create(table: str, data: List[Dict[str, Any]]) -> List[str]

      Create one or more records in a Dataverse table.

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

      .. rubric:: Example

      Create a single record::

          guid = client.records.create("account", {"name": "Contoso"})
          print(f"Created: {guid}")

      Create multiple records::

          guids = client.records.create("account", [
              {"name": "Contoso"},
              {"name": "Fabrikam"},
          ])
          print(f"Created {len(guids)} accounts")



   .. py:method:: update(table: str, ids: Union[str, List[str]], changes: Union[Dict[str, Any], List[Dict[str, Any]]]) -> None

      Update one or more records in a Dataverse table.

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

      .. rubric:: Example

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



   .. py:method:: delete(table: str, ids: str) -> None
                  delete(table: str, ids: List[str], *, use_bulk_delete: bool = True) -> Optional[str]

      Delete one or more records from a Dataverse table.

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

      .. rubric:: Example

      Delete a single record::

          client.records.delete("account", account_id)

      Bulk delete::

          job_id = client.records.delete("account", [id1, id2, id3])



   .. py:method:: get(table: str, record_id: str, *, select: Optional[List[str]] = None) -> PowerPlatform.Dataverse.models.record.Record
                  get(table: str, *, select: Optional[List[str]] = None, filter: Optional[str] = None, orderby: Optional[List[str]] = None, top: Optional[int] = None, expand: Optional[List[str]] = None, page_size: Optional[int] = None, count: bool = False, include_annotations: Optional[str] = None) -> Iterable[List[PowerPlatform.Dataverse.models.record.Record]]

      Fetch a single record by ID, or fetch multiple records with pagination.

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

      :return: A single :class:`~PowerPlatform.Dataverse.models.record.Record`
          when ``record_id`` is provided, or a generator yielding pages
          (lists of :class:`~PowerPlatform.Dataverse.models.record.Record`)
          when fetching multiple records.
      :rtype: ~PowerPlatform.Dataverse.models.record.Record or
          collections.abc.Iterable[list[~PowerPlatform.Dataverse.models.record.Record]]

      :raises TypeError: If ``record_id`` is provided but not a string.
      :raises ValueError: If query parameters are provided alongside
          ``record_id``.

      .. rubric:: Example

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



   .. py:method:: retrieve(table: str, record_id: str, *, select: Optional[List[str]] = None, expand: Optional[List[str]] = None, include_annotations: Optional[str] = None) -> Optional[PowerPlatform.Dataverse.models.record.Record]

      Fetch a single record by its GUID, returning ``None`` if not found.

      GA replacement for ``records.get(table, record_id)``. Returns ``None``
      instead of raising when the record does not exist (HTTP 404).

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

          record = client.records.retrieve(
              "account", account_id,
              select=["name", "statuscode"],
              expand=["primarycontactid"],
              include_annotations="OData.Community.Display.V1.FormattedValue",
          )
          if record is not None:
              contact = record.get("primarycontactid") or {}
              print(contact.get("fullname"))



   .. py:method:: list(table: str, *, filter: Optional[Union[str, PowerPlatform.Dataverse.models.filters.FilterExpression]] = None, select: Optional[List[str]] = None, orderby: Optional[List[str]] = None, top: Optional[int] = None, expand: Optional[List[str]] = None, page_size: Optional[int] = None, count: bool = False, include_annotations: Optional[str] = None) -> PowerPlatform.Dataverse.models.record.QueryResult

      Fetch multiple records and return them as a :class:`~PowerPlatform.Dataverse.models.record.QueryResult`.

      GA replacement for ``records.get(table, filter=...)``. All pages are
      collected eagerly and returned as a single :class:`~PowerPlatform.Dataverse.models.record.QueryResult`.

      :param table: Schema name of the table (e.g. ``"account"``).
      :type table: :class:`str`
      :param filter: Optional OData filter string or :class:`~PowerPlatform.Dataverse.models.filters.FilterExpression`.
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
      :return: All matching records collected into a :class:`~PowerPlatform.Dataverse.models.record.QueryResult`.
      :rtype: :class:`~PowerPlatform.Dataverse.models.record.QueryResult`

      Example::

          from PowerPlatform.Dataverse import col

          result = client.records.list(
              "account",
              filter=col("statecode") == 0,
              select=["name", "statuscode"],
              orderby=["name asc"],
              top=100,
              include_annotations="OData.Community.Display.V1.FormattedValue",
          )
          for record in result:
              print(record["name"], record.get("statuscode@OData.Community.Display.V1.FormattedValue"))



   .. py:method:: list_pages(table: str, *, filter: Optional[Union[str, PowerPlatform.Dataverse.models.filters.FilterExpression]] = None, select: Optional[List[str]] = None, orderby: Optional[List[str]] = None, top: Optional[int] = None, expand: Optional[List[str]] = None, page_size: Optional[int] = None, count: bool = False, include_annotations: Optional[str] = None) -> Iterator[PowerPlatform.Dataverse.models.record.QueryResult]

      Lazily yield one :class:`~PowerPlatform.Dataverse.models.record.QueryResult` per HTTP page.

      Streaming counterpart to :meth:`list`. Each iteration triggers one
      network request via ``@odata.nextLink``. One-shot — do not iterate
      more than once.

      :param table: Schema name of the table (e.g. ``"account"``).
      :type table: :class:`str`
      :param filter: Optional OData filter string or :class:`~PowerPlatform.Dataverse.models.filters.FilterExpression`.
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
      :return: Iterator of per-page :class:`~PowerPlatform.Dataverse.models.record.QueryResult` objects.
      :rtype: Iterator[:class:`~PowerPlatform.Dataverse.models.record.QueryResult`]

      Example::

          for page in client.records.list_pages(
              "account",
              filter="statecode eq 0",
              orderby=["name asc"],
              page_size=200,
          ):
              process(page.to_dataframe())



   .. py:method:: upsert(table: str, items: List[Union[PowerPlatform.Dataverse.models.upsert.UpsertItem, Dict[str, Any]]]) -> None

      Upsert one or more records identified by alternate keys.

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

      .. rubric:: Example

      Upsert a single record using ``UpsertItem``::

          from PowerPlatform.Dataverse.models import UpsertItem

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

          from PowerPlatform.Dataverse.models import UpsertItem

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



