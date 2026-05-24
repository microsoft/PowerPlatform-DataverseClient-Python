PowerPlatform.Dataverse.client
==============================

.. py:module:: PowerPlatform.Dataverse.client


Classes
-------

.. autoapisummary::

   PowerPlatform.Dataverse.client.DataverseClient


Module Contents
---------------

.. py:class:: DataverseClient(base_url: str, credential: azure.core.credentials.TokenCredential, config: Optional[PowerPlatform.Dataverse.core.config.DataverseConfig] = None, *, context: Optional[PowerPlatform.Dataverse.core.config.OperationContext] = None)

   High-level client for Microsoft Dataverse operations.

   This client provides a simple, stable interface for interacting with Dataverse environments
   through the Web API. It handles authentication via Azure Identity and delegates HTTP operations
   to an internal OData client.

   Key capabilities:
       - OData CRUD operations: create, read, update, delete records
       - SQL queries: execute read-only SQL via Web API ``?sql`` parameter
       - Table metadata: create, inspect, and delete custom tables; create and delete columns
       - File uploads: upload files to file columns with chunking support

   :param base_url: Your Dataverse environment URL, for example
       ``"https://org.crm.dynamics.com"``. Trailing slash is automatically removed.
   :type base_url: :class:`str`
   :param credential: Azure Identity credential for authentication.
   :type credential: ~azure.core.credentials.TokenCredential
   :param config: Optional configuration for language, timeouts, and retries.
       If not provided, defaults are loaded from :meth:`~PowerPlatform.Dataverse.core.config.DataverseConfig.from_env`.
   :type config: ~PowerPlatform.Dataverse.core.config.DataverseConfig or None
   :param context: Optional caller-defined context object appended to the
       outbound ``User-Agent`` header for plugin/tool attribution. Cannot be used
       together with ``config`` -- pass the context via
       :class:`~PowerPlatform.Dataverse.core.config.DataverseConfig` instead.
   :type context: ~PowerPlatform.Dataverse.core.config.OperationContext or None

   :raises ValueError: If ``base_url`` is missing or empty after trimming.
   :raises ValueError: If both ``config`` and ``context`` are provided.

   .. note::
       The client lazily initializes its internal OData client on first use, allowing lightweight construction without immediate network calls.

   .. note::
       All methods that communicate with the Dataverse Web API may raise
       :class:`~PowerPlatform.Dataverse.core.errors.HttpError` on non-successful
       HTTP responses (e.g. 401, 403, 404, 429, 500). Individual method
       docstrings document only domain-specific exceptions.

   Operations are organized into namespaces:

   - ``client.records`` -- create, update, delete, and get records (single or paginated queries)
   - ``client.query`` -- query and search operations
   - ``client.tables`` -- table and column metadata management
   - ``client.files`` -- file upload operations
   - ``client.dataframe`` -- pandas DataFrame wrappers for record CRUD
   - ``client.batch`` -- batch multiple operations into a single HTTP request

   The client supports Python's context manager protocol for automatic resource
   cleanup and HTTP connection pooling:

   .. rubric:: Example

   **Recommended -- context manager** (enables HTTP connection pooling)::

       from azure.identity import InteractiveBrowserCredential
       from PowerPlatform.Dataverse.client import DataverseClient

       credential = InteractiveBrowserCredential()

       with DataverseClient("https://org.crm.dynamics.com", credential) as client:
           record_id = client.records.create("account", {"name": "Contoso Ltd"})
           client.records.update("account", record_id, {"telephone1": "555-0100"})
       # Session closed, caches cleared automatically

   **Manual lifecycle**::

       client = DataverseClient("https://org.crm.dynamics.com", credential)
       try:
           record_id = client.records.create("account", {"name": "Contoso Ltd"})
       finally:
           client.close()


   .. py:attribute:: auth


   .. py:attribute:: records


   .. py:attribute:: query


   .. py:attribute:: tables


   .. py:attribute:: files


   .. py:attribute:: dataframe


   .. py:attribute:: batch


   .. py:method:: close() -> None

      Close the client and release resources.

      Closes the HTTP session (if any), clears internal caches, and
      marks the client as closed. Safe to call multiple times. After
      closing, any operation will raise :class:`RuntimeError`.

      Called automatically when using the client as a context manager.

      Example::

          client = DataverseClient(base_url, credential)
          try:
              client.records.create("account", {"name": "Contoso"})
          finally:
              client.close()



   .. py:method:: flush_cache(kind) -> int

      Flush cached client metadata or state.

      :param kind: Cache kind to flush. Currently supported values:

          - ``"picklist"``: Clears picklist label cache used for label-to-integer conversion

          Future kinds (e.g. ``"entityset"``, ``"primaryid"``) may be added without
          breaking this signature.
      :type kind: :class:`str`

      :return: Number of cache entries removed.
      :rtype: :class:`int`

      .. rubric:: Example

      Clear the picklist cache::

          removed = client.flush_cache("picklist")
          print(f"Cleared {removed} cached picklist entries")



