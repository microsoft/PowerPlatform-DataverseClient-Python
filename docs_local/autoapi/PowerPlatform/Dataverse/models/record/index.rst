PowerPlatform.Dataverse.models.record
=====================================

.. py:module:: PowerPlatform.Dataverse.models.record

.. autoapi-nested-parse::

   Record data model for Dataverse entities.



Classes
-------

.. autoapisummary::

   PowerPlatform.Dataverse.models.record.Record
   PowerPlatform.Dataverse.models.record.QueryResult


Module Contents
---------------

.. py:class:: Record

   Strongly-typed Dataverse record with dict-like backward compatibility.

   Wraps raw OData response data into a structured object while preserving
   ``result["key"]`` access patterns for existing code.

   :param id: Record GUID. Empty string if not extracted (e.g. paginated
       results, SQL queries).
   :type id: :class:`str`
   :param table: Table schema name used in the request.
   :type table: :class:`str`
   :param data: Record field data as key-value pairs.
   :type data: :class:`dict`
   :param etag: ETag for optimistic concurrency, extracted from
       ``@odata.etag`` in the API response.
   :type etag: :class:`str` or None

   Example::

       record = client.records.get("account", account_id, select=["name"])
       print(record.id)          # structured access
       print(record["name"])     # dict-like access (backward compat)


   .. py:attribute:: id
      :type:  str
      :value: ''



   .. py:attribute:: table
      :type:  str
      :value: ''



   .. py:attribute:: data
      :type:  Dict[str, Any]


   .. py:attribute:: etag
      :type:  Optional[str]
      :value: None



   .. py:method:: get(key: str, default: Any = None) -> Any

      Return value for *key*, or *default* if not present.



   .. py:method:: keys() -> KeysView[str]

      Return data keys.



   .. py:method:: values() -> ValuesView[Any]

      Return data values.



   .. py:method:: items() -> ItemsView[str, Any]

      Return data items.



   .. py:method:: from_api_response(table: str, response_data: Dict[str, Any], *, record_id: str = '') -> Record
      :classmethod:


      Create a :class:`Record` from a raw OData API response.

      Strips ``@odata.*`` annotation keys from the data and extracts the
      ``@odata.etag`` value if present.

      :param table: Table schema name.
      :type table: :class:`str`
      :param response_data: Raw JSON dict from the OData response.
      :type response_data: :class:`dict`
      :param record_id: Known record GUID. Pass explicitly when available
          (e.g. single-record get). Defaults to empty string.
      :type record_id: :class:`str`
      :rtype: :class:`Record`



   .. py:method:: to_dict() -> Dict[str, Any]

      Return a plain dict copy of the record data (excludes metadata).



.. py:class:: QueryResult(records: List[Record])

   Iterable wrapper around a list of :class:`Record` objects.

   Returned by :meth:`~PowerPlatform.Dataverse.models.query_builder.QueryBuilder.execute`
   (flat mode) and :meth:`~PowerPlatform.Dataverse.operations.records.RecordOperations.list`.

   Backward-compatible: ``for r in result`` continues to work without change.

   :param records: Collected records from all pages.
   :type records: list[:class:`Record`]


   .. py:attribute:: records
      :type:  List[Record]


   .. py:method:: first() -> Optional[Record]

      Return the first record, or ``None`` if the result is empty.



   .. py:method:: to_dataframe() -> Any

      Return all records as a pandas DataFrame.

      :raises ImportError: If pandas is not installed.
      :rtype: ~pandas.DataFrame



