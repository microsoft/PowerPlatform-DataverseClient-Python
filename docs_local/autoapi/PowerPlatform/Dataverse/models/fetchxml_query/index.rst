PowerPlatform.Dataverse.models.fetchxml_query
=============================================

.. py:module:: PowerPlatform.Dataverse.models.fetchxml_query

.. autoapi-nested-parse::

   FetchXmlQuery — inert query object returned by QueryOperations.fetchxml().



Classes
-------

.. autoapisummary::

   PowerPlatform.Dataverse.models.fetchxml_query.FetchXmlQuery


Module Contents
---------------

.. py:class:: FetchXmlQuery(xml: str, entity_name: str, client: PowerPlatform.Dataverse.client.DataverseClient)

   Inert FetchXML query object. No HTTP request is made until
   :meth:`execute` or :meth:`execute_pages` is called.

   Obtained via ``client.query.fetchxml(xml)``.

   :param xml: Stripped, well-formed FetchXML string.
   :param entity_name: Entity schema name from the ``<entity>`` element.
   :param client: Parent :class:`~PowerPlatform.Dataverse.client.DataverseClient`.


   .. py:method:: execute() -> PowerPlatform.Dataverse.models.record.QueryResult

      Execute the FetchXML query and return all results as a :class:`~PowerPlatform.Dataverse.models.record.QueryResult`.

      Blocking — fetches all pages upfront and holds every record in memory before
      returning. Simple for small-to-medium result sets; use :meth:`execute_pages`
      when the result set may be large or you want to process records as they arrive.

      :return: All matching records across all pages.
      :rtype: :class:`~PowerPlatform.Dataverse.models.record.QueryResult`

      Example::

          rows = client.query.fetchxml(xml).execute()
          df = rows.to_dataframe()



   .. py:method:: execute_pages() -> Iterator[PowerPlatform.Dataverse.models.record.QueryResult]

      Lazily yield one :class:`~PowerPlatform.Dataverse.models.record.QueryResult` per HTTP page.

      Streaming — each iteration fires one HTTP request and yields one page.
      Prefer over :meth:`execute` when:

      - The result set may be large and you do not want all records in memory at once.
      - You want early exit: stop iterating once you find what you need and the
        remaining HTTP round-trips are skipped automatically.
      - You need per-page progress reporting or batched downstream writes.

      One-shot — do not iterate more than once.

      :return: Iterator of per-page :class:`~PowerPlatform.Dataverse.models.record.QueryResult` objects.
      :rtype: Iterator[:class:`~PowerPlatform.Dataverse.models.record.QueryResult`]

      Example::

          for page in client.query.fetchxml(xml).execute_pages():
              process(page.to_dataframe())



