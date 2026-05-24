PowerPlatform.Dataverse.models.batch
====================================

.. py:module:: PowerPlatform.Dataverse.models.batch

.. autoapi-nested-parse::

   Public result types for batch operations.



Classes
-------

.. autoapisummary::

   PowerPlatform.Dataverse.models.batch.BatchItemResponse
   PowerPlatform.Dataverse.models.batch.BatchResult


Module Contents
---------------

.. py:class:: BatchItemResponse

   Response from a single operation within a batch request.

   Responses are returned in submission order. For operations added to a
   changeset, responses appear in the changeset's position in that order.

   :param status_code: HTTP status code for this operation (e.g. 204, 200, 400).
   :param content_id: ``Content-ID`` value from the changeset response part, if any.
   :param entity_id: GUID extracted from the ``OData-EntityId`` response header.
       Set for successful create (POST) operations.
   :param data: Parsed JSON response body (e.g. for GET operations).
   :param error_message: Error message when the operation failed.
   :param error_code: Service error code when the operation failed.

   Example::

       for item in result.responses:
           if item.is_success:
               print(f"[OK] {item.status_code} entity_id={item.entity_id}")
           else:
               print(f"[ERR] {item.status_code}: {item.error_message}")


   .. py:attribute:: status_code
      :type:  int


   .. py:attribute:: content_id
      :type:  Optional[str]
      :value: None



   .. py:attribute:: entity_id
      :type:  Optional[str]
      :value: None



   .. py:attribute:: data
      :type:  Optional[Dict[str, Any]]
      :value: None



   .. py:attribute:: error_message
      :type:  Optional[str]
      :value: None



   .. py:attribute:: error_code
      :type:  Optional[str]
      :value: None



   .. py:property:: is_success
      :type: bool


      Return True when status_code is 2xx.


.. py:class:: BatchResult

   Result of executing a batch request.

   Contains one :class:`BatchItemResponse` per HTTP operation submitted.
   Operations that expand to multiple HTTP requests (e.g. ``add_columns``
   with three columns) contribute three entries.

   :param responses: All responses in submission order.

   Example::

       result = client.batch.new().execute()
       print(f"Succeeded: {len(result.succeeded)}, Failed: {len(result.failed)}")
       for guid in result.entity_ids:
           print(f"[OK] entity_id: {guid}")


   .. py:attribute:: responses
      :type:  List[BatchItemResponse]
      :value: []



   .. py:property:: succeeded
      :type: List[BatchItemResponse]


      Responses with 2xx status codes.


   .. py:property:: failed
      :type: List[BatchItemResponse]


      Responses with non-2xx status codes.


   .. py:property:: has_errors
      :type: bool


      True when any response has a non-2xx status code.


   .. py:property:: entity_ids
      :type: List[str]


      GUIDs extracted from ``OData-EntityId`` headers of successful responses.

      Returns entity IDs from any successful (2xx) response that includes an
      ``OData-EntityId`` header.  Both individual ``POST`` (create) and
      ``PATCH`` (update) operations return this header with the record's GUID.
      ``GET`` and ``DELETE`` operations do not.

      .. note::
          ``CreateMultiple`` and ``UpsertMultiple`` action responses do **not**
          return per-record ``OData-EntityId`` headers.  Their IDs are in the
          JSON response body (``data["Ids"]``).  Access them via::

              for resp in result.succeeded:
                  if resp.data and "Ids" in resp.data:
                      bulk_ids = resp.data["Ids"]


