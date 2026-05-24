PowerPlatform.Dataverse.models.query_builder
============================================

.. py:module:: PowerPlatform.Dataverse.models.query_builder

.. autoapi-nested-parse::

   Fluent query builder for constructing OData queries.

   Provides a type-safe, discoverable interface for building complex queries
   against Dataverse tables with method chaining.

   Example::

       # Via client (recommended) -- flat iteration over records
       from PowerPlatform.Dataverse.models import col

       for record in (client.query.builder("account")
                      .select("name", "revenue")
                      .where(col("statecode") == 0)
                      .where(col("revenue") > 1_000_000)
                      .order_by("revenue", descending=True)
                      .top(100)
                      .execute()):
           print(record["name"])

       # With composable expression tree
       from PowerPlatform.Dataverse.models import col, raw

       for record in (client.query.builder("account")
                      .select("name", "revenue")
                      .where((col("statecode") == 0) | (col("statecode") == 1))
                      .where(col("revenue") > 100000)
                      .top(100)
                      .execute()):
           print(record["name"])

       # Lazy paged iteration (one QueryResult per HTTP page)
       for page in (client.query.builder("account")
                    .select("name")
                    .execute_pages()):
           process_batch(page)

       # Get results as a pandas DataFrame
       df = (client.query.builder("account")
             .select("name", "telephone1")
             .where(col("statecode") == 0)
             .top(100)
             .execute()
             .to_dataframe())



Classes
-------

.. autoapisummary::

   PowerPlatform.Dataverse.models.query_builder.QueryParams
   PowerPlatform.Dataverse.models.query_builder.ExpandOption
   PowerPlatform.Dataverse.models.query_builder.QueryBuilder


Module Contents
---------------

.. py:class:: QueryParams

   Bases: :py:obj:`TypedDict`


   Typed dictionary returned by ``QueryBuilder.build()``.

   Provides IDE autocomplete when passing build results to
   ``client.records.list()`` manually.

   Initialize self.  See help(type(self)) for accurate signature.


   .. py:attribute:: table
      :type:  str


   .. py:attribute:: select
      :type:  List[str]


   .. py:attribute:: filter
      :type:  str


   .. py:attribute:: orderby
      :type:  List[str]


   .. py:attribute:: expand
      :type:  List[str]


   .. py:attribute:: top
      :type:  int


   .. py:attribute:: page_size
      :type:  int


   .. py:attribute:: count
      :type:  bool


   .. py:attribute:: include_annotations
      :type:  str


.. py:class:: ExpandOption(relation: str)

   Structured options for an ``$expand`` navigation property.

   Allows specifying nested ``$select``, ``$filter``, ``$orderby``, and
   ``$top`` options for a single navigation property expansion, following
   the OData ``$expand`` syntax.

   :param relation: Navigation property name (case-sensitive).
   :type relation: str

   Example::

       # Expand Account_Tasks with nested options
       opt = (ExpandOption("Account_Tasks")
              .select("subject", "createdon")
              .filter("contains(subject,'Task')")
              .order_by("createdon", descending=True)
              .top(5))

       query = (client.query.builder("account")
                .select("name")
                .expand(opt)
                .execute())


   .. py:attribute:: relation


   .. py:method:: select(*columns: str) -> ExpandOption

      Select specific columns from the expanded entity.

      :param columns: Column names to select.
      :return: Self for method chaining.



   .. py:method:: filter(filter_str: str) -> ExpandOption

      Filter the expanded collection.

      :param filter_str: OData ``$filter`` expression.
      :return: Self for method chaining.



   .. py:method:: order_by(column: str, descending: bool = False) -> ExpandOption

      Sort the expanded collection.

      :param column: Column name to sort by.
      :param descending: Sort descending if ``True``.
      :return: Self for method chaining.



   .. py:method:: top(count: int) -> ExpandOption

      Limit expanded results.

      :param count: Maximum number of expanded records.
      :return: Self for method chaining.



   .. py:method:: to_odata() -> str

      Compile to OData ``$expand`` syntax.

      :return: OData expand string like ``"Nav($select=col1,col2;$filter=...)"``
      :rtype: str



.. py:class:: QueryBuilder(table: str)

   Bases: :py:obj:`_QueryBuilderBase`


   Fluent interface for building and executing OData queries against a sync client.

   Provides method chaining for constructing complex queries with
   composable filter expressions. Can be used standalone (via ``build()``)
   or bound to a client (via :meth:`execute`).

   :param table: Table schema name to query.
   :type table: str
   :raises ValueError: If ``table`` is empty.

   .. rubric:: Example

   Standalone query construction::

       from PowerPlatform.Dataverse.models import col

       query = (QueryBuilder("account")
                .select("name")
                .where(col("statecode") == 0)
                .top(10))
       params = query.build()
       # {"table": "account", "select": ["name"],
       #  "filter": "statecode eq 0", "top": 10}


   .. py:method:: execute(*, by_page=_BY_PAGE_UNSET) -> Union[PowerPlatform.Dataverse.models.record.QueryResult, Iterator[PowerPlatform.Dataverse.models.record.QueryResult]]

      Execute the query and return results.

      Returns a :class:`~PowerPlatform.Dataverse.models.record.QueryResult`
      with all pages collected. Use :meth:`execute_pages` for lazy per-page
      iteration.

      This method is only available when the QueryBuilder was created
      via ``client.query.builder(table)``.  Standalone ``QueryBuilder``
      instances should use ``build()`` to get parameters and pass them
      to ``client.records.list()`` manually.

      At least one of ``select()``, ``where()``, or ``top()`` must be
      called before ``execute()``; otherwise a :class:`ValueError` is
      raised to prevent accidental full-table scans.

      .. deprecated::
          The ``by_page`` parameter is deprecated. Use :meth:`execute_pages`
          for lazy per-page iteration, or plain ``execute()`` (no flag) for
          the default eager result.

      :return: :class:`~PowerPlatform.Dataverse.models.record.QueryResult`
          with all pages collected (default), or page iterator (deprecated
          ``by_page=True``).
      :rtype: QueryResult or Iterator[QueryResult]
      :raises ValueError: If no ``select``, ``where``, or ``top``
          constraint has been set.
      :raises RuntimeError: If the query was not created via
          ``client.query.builder()``.

      Example::

          from PowerPlatform.Dataverse.models import col

          for record in (client.query.builder("account")
                         .select("name")
                         .where(col("statecode") == 0)
                         .execute()):
              print(record["name"])



   .. py:method:: execute_pages() -> Iterator[PowerPlatform.Dataverse.models.record.QueryResult]

      Lazily yield one :class:`~PowerPlatform.Dataverse.models.record.QueryResult`
      per HTTP page.

      Each iteration triggers a network request via ``@odata.nextLink``.
      One-shot — do not iterate more than once.

      At least one of ``select()``, ``where()``, or ``top()`` must be
      called before ``execute_pages()``; otherwise a :class:`ValueError` is
      raised to prevent accidental full-table scans.

      :return: Iterator of per-page :class:`~PowerPlatform.Dataverse.models.record.QueryResult`.
      :rtype: Iterator[QueryResult]
      :raises ValueError: If no ``select``, ``where``, or ``top``
          constraint has been set.
      :raises RuntimeError: If the query was not created via
          ``client.query.builder()``.

      Example::

          from PowerPlatform.Dataverse.models import col

          for page in (client.query.builder("account")
                       .select("name")
                       .where(col("statecode") == 0)
                       .execute_pages()):
              process(page.to_dataframe())



   .. py:method:: to_dataframe() -> pandas.DataFrame

      Execute the query and return results as a pandas DataFrame.

      .. deprecated::
          Use ``QueryBuilder.execute().to_dataframe()`` instead.
          ``QueryBuilder.to_dataframe()`` will be removed in a future release.

      All pages are consolidated into a single DataFrame.

      This method is only available when the QueryBuilder was created
      via ``client.query.builder(table)``.

      At least one of ``select()``, ``where()``, or ``top()`` must be
      called before ``to_dataframe()``; otherwise a :class:`ValueError`
      is raised to prevent accidental full-table scans.

      :return: DataFrame containing all matching records. Returns an empty
          DataFrame when no records match.
      :rtype: ~pandas.DataFrame
      :raises ValueError: If no ``select``, ``where``, or ``top``
          constraint has been set.
      :raises RuntimeError: If the query was not created via
          ``client.query.builder()``.

      Example::

          from PowerPlatform.Dataverse.models import col

          df = (client.query.builder("account")
                .select("name", "telephone1")
                .where(col("statecode") == 0)
                .top(100)
                .execute()
                .to_dataframe())



