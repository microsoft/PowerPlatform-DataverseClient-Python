PowerPlatform.Dataverse.operations.dataframe
============================================

.. py:module:: PowerPlatform.Dataverse.operations.dataframe

.. autoapi-nested-parse::

   DataFrame CRUD operations namespace for the Dataverse SDK.



Classes
-------

.. autoapisummary::

   PowerPlatform.Dataverse.operations.dataframe.DataFrameOperations


Module Contents
---------------

.. py:class:: DataFrameOperations(client: PowerPlatform.Dataverse.client.DataverseClient)

   Namespace for pandas DataFrame CRUD operations.

   Accessed via ``client.dataframe``. Provides DataFrame-oriented wrappers
   around the record-level CRUD operations.

   :param client: The parent :class:`~PowerPlatform.Dataverse.client.DataverseClient` instance.
   :type client: ~PowerPlatform.Dataverse.client.DataverseClient

   Example::

       import pandas as pd

       client = DataverseClient(base_url, credential)

       # Query records as a DataFrame
       df = client.dataframe.get("account", select=["name"], top=100)

       # Create records from a DataFrame
       new_df = pd.DataFrame([{"name": "Contoso"}, {"name": "Fabrikam"}])
       new_df["accountid"] = client.dataframe.create("account", new_df)

       # Update records
       new_df["telephone1"] = ["555-0100", "555-0200"]
       client.dataframe.update("account", new_df, id_column="accountid")

       # Delete records
       client.dataframe.delete("account", new_df["accountid"])


   .. py:method:: sql(sql: str) -> pandas.DataFrame

      Execute a SQL query and return the results as a pandas DataFrame.

      Delegates to :meth:`~PowerPlatform.Dataverse.operations.query.QueryOperations.sql`
      and converts the list of records into a single DataFrame.

      :param sql: Supported SQL SELECT statement.
      :type sql: :class:`str`

      :return: DataFrame containing all result rows. Returns an empty
          DataFrame when no rows match.
      :rtype: ~pandas.DataFrame

      :raises ~PowerPlatform.Dataverse.core.errors.ValidationError:
          If ``sql`` is not a string or is empty.

      .. rubric:: Example

      SQL query to DataFrame::

          df = client.dataframe.sql(
              "SELECT TOP 100 name, revenue FROM account "
              "WHERE statecode = 0 ORDER BY revenue"
          )
          print(f"Got {len(df)} rows")
          print(df.head())

      Aggregate query to DataFrame::

          df = client.dataframe.sql(
              "SELECT a.name, COUNT(c.contactid) as cnt "
              "FROM account a "
              "JOIN contact c ON a.accountid = c.parentcustomerid "
              "GROUP BY a.name"
          )



   .. py:method:: get(table: str, record_id: Optional[str] = None, select: Optional[List[str]] = None, filter: Optional[str] = None, orderby: Optional[List[str]] = None, top: Optional[int] = None, expand: Optional[List[str]] = None, page_size: Optional[int] = None, count: bool = False, include_annotations: Optional[str] = None) -> pandas.DataFrame

      Fetch records and return as a single pandas DataFrame.

      When ``record_id`` is provided, returns a single-row DataFrame.
      When ``record_id`` is None, internally iterates all pages and returns one
      consolidated DataFrame.

      :param table: Schema name of the table (e.g. ``"account"`` or ``"new_MyTestTable"``).
      :type table: :class:`str`
      :param record_id: Optional GUID to fetch a specific record. If None, queries multiple records.
      :type record_id: :class:`str` or None
      :param select: Optional list of attribute logical names to retrieve.
      :type select: list[str] or None
      :param filter: Optional OData filter string. Column names must use exact lowercase logical names.
      :type filter: :class:`str` or None
      :param orderby: Optional list of attributes to sort by.
      :type orderby: list[str] or None
      :param top: Optional maximum number of records to return.
      :type top: :class:`int` or None
      :param expand: Optional list of navigation properties to expand (case-sensitive).
      :type expand: list[str] or None
      :param page_size: Optional number of records per page for pagination.
      :type page_size: :class:`int` or None
      :param count: If ``True``, adds ``$count=true`` to include a total
          record count in the response.
      :type count: :class:`bool`
      :param include_annotations: OData annotation pattern for the
          ``Prefer: odata.include-annotations`` header (e.g. ``"*"`` or
          ``"OData.Community.Display.V1.FormattedValue"``), or ``None``.
      :type include_annotations: :class:`str` or None

      :return: DataFrame containing all matching records. Returns an empty DataFrame
          when no records match.
      :rtype: ~pandas.DataFrame

      :raises ValueError: If ``record_id`` is not a non-empty string, or if
          query parameters (``filter``, ``orderby``, ``top``, ``expand``,
          ``page_size``) are provided alongside ``record_id``.

      .. tip::
          For large tables, use ``top`` or ``filter`` to limit the result set.

      .. rubric:: Example

      Fetch a single record as a DataFrame::

          df = client.dataframe.get("account", record_id=account_id, select=["name", "telephone1"])
          print(df)

      Query with filtering::

          df = client.dataframe.get("account", filter="statecode eq 0", select=["name"])
          print(f"Got {len(df)} active accounts")

      Limit result size::

          df = client.dataframe.get("account", select=["name"], top=100)



   .. py:method:: create(table: str, records: pandas.DataFrame) -> pandas.Series

      Create records from a pandas DataFrame.

      :param table: Schema name of the table (e.g. ``"account"`` or ``"new_MyTestTable"``).
      :type table: :class:`str`
      :param records: DataFrame where each row is a record to create.
      :type records: ~pandas.DataFrame

      :return: Series of created record GUIDs, aligned with the input DataFrame index.
      :rtype: ~pandas.Series

      :raises TypeError: If ``records`` is not a pandas DataFrame.
      :raises ValueError: If ``records`` is empty or the number of returned
          IDs does not match the number of input rows.

      .. tip::
          All rows are sent in a single ``CreateMultiple`` request. For very
          large DataFrames, consider splitting into smaller batches to avoid
          request timeouts.

      .. rubric:: Example

      Create records from a DataFrame::

          import pandas as pd

          df = pd.DataFrame([
              {"name": "Contoso", "telephone1": "555-0100"},
              {"name": "Fabrikam", "telephone1": "555-0200"},
          ])
          df["accountid"] = client.dataframe.create("account", df)



   .. py:method:: update(table: str, changes: pandas.DataFrame, id_column: str, clear_nulls: bool = False) -> None

      Update records from a pandas DataFrame.

      Each row in the DataFrame represents an update. The ``id_column`` specifies which
      column contains the record GUIDs.

      :param table: Schema name of the table (e.g. ``"account"`` or ``"new_MyTestTable"``).
      :type table: :class:`str`
      :param changes: DataFrame where each row contains a record GUID and the fields to update.
      :type changes: ~pandas.DataFrame
      :param id_column: Name of the DataFrame column containing record GUIDs.
      :type id_column: :class:`str`
      :param clear_nulls: When ``False`` (default), missing values (NaN/None) are skipped
          (the field is left unchanged on the server). When ``True``, missing values are sent
          as ``null`` to Dataverse, clearing the field. Use ``True`` only when you intentionally
          want NaN/None values to clear fields.
      :type clear_nulls: :class:`bool`

      :raises TypeError: If ``changes`` is not a pandas DataFrame.
      :raises ValueError: If ``changes`` is empty, ``id_column`` is not found in the
          DataFrame, ``id_column`` contains invalid (non-string, empty, or whitespace-only)
          values, or no updatable columns exist besides ``id_column``.
          When ``clear_nulls`` is ``False`` (default), rows where all change values
          are NaN/None produce empty patches and are silently skipped. If all rows
          are skipped, the method returns without making an API call. When
          ``clear_nulls`` is ``True``, NaN/None values become explicit nulls, so
          rows are never skipped.

      .. tip::
          All rows are sent in a single ``UpdateMultiple`` request (or a
          single PATCH for one row). For very large DataFrames, consider
          splitting into smaller batches to avoid request timeouts.

      .. rubric:: Example

      Update records with different values per row::

          import pandas as pd

          df = pd.DataFrame([
              {"accountid": "guid-1", "telephone1": "555-0100"},
              {"accountid": "guid-2", "telephone1": "555-0200"},
          ])
          client.dataframe.update("account", df, id_column="accountid")

      Broadcast the same change to all records::

          df = pd.DataFrame({"accountid": ["guid-1", "guid-2", "guid-3"]})
          df["websiteurl"] = "https://example.com"
          client.dataframe.update("account", df, id_column="accountid")

      Clear a field by setting clear_nulls=True::

          df = pd.DataFrame([{"accountid": "guid-1", "websiteurl": None}])
          client.dataframe.update("account", df, id_column="accountid", clear_nulls=True)



   .. py:method:: delete(table: str, ids: pandas.Series, use_bulk_delete: bool = True) -> Optional[str]

      Delete records by passing a pandas Series of GUIDs.

      :param table: Schema name of the table (e.g. ``"account"`` or ``"new_MyTestTable"``).
      :type table: :class:`str`
      :param ids: Series of record GUIDs to delete.
      :type ids: ~pandas.Series
      :param use_bulk_delete: When ``True`` (default) and ``ids`` contains multiple values, execute the BulkDelete
          action and return its async job identifier. When ``False`` each record is deleted sequentially.
      :type use_bulk_delete: :class:`bool`

      :raises TypeError: If ``ids`` is not a pandas Series.
      :raises ValueError: If ``ids`` contains invalid (non-string, empty, or
          whitespace-only) values.

      :return: BulkDelete job ID when deleting multiple records via BulkDelete;
          ``None`` when deleting a single record, using sequential deletion, or
          when ``ids`` is empty.
      :rtype: :class:`str` or None

      .. rubric:: Example

      Delete records using a Series::

          import pandas as pd

          ids = pd.Series(["guid-1", "guid-2", "guid-3"])
          client.dataframe.delete("account", ids)



