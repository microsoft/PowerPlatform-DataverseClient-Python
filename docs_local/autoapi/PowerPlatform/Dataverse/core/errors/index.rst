PowerPlatform.Dataverse.core.errors
===================================

.. py:module:: PowerPlatform.Dataverse.core.errors

.. autoapi-nested-parse::

   Structured Dataverse exception hierarchy.

   This module provides :class:`~PowerPlatform.Dataverse.core.errors.DataverseError` and
   specialized :class:`~PowerPlatform.Dataverse.core.errors.ValidationError`,
   :class:`~PowerPlatform.Dataverse.core.errors.MetadataError`,
   :class:`~PowerPlatform.Dataverse.core.errors.SQLParseError`, and
   :class:`~PowerPlatform.Dataverse.core.errors.HttpError` for validation, metadata,
   SQL parsing, and Web API HTTP failures.



Exceptions
----------

.. autoapisummary::

   PowerPlatform.Dataverse.core.errors.DataverseError
   PowerPlatform.Dataverse.core.errors.ValidationError
   PowerPlatform.Dataverse.core.errors.MetadataError
   PowerPlatform.Dataverse.core.errors.SQLParseError
   PowerPlatform.Dataverse.core.errors.HttpError


Module Contents
---------------

.. py:exception:: DataverseError(message: str, code: str, subcode: Optional[str] = None, status_code: Optional[int] = None, details: Optional[Dict[str, Any]] = None, source: Optional[str] = None, is_transient: bool = False)

   Bases: :py:obj:`Exception`


   Base structured exception for the Dataverse SDK.

   :param message: Human-readable error message.
   :type message: :class:`str`
   :param code: Error category code (e.g. ``"validation_error"``, ``"http_error"``).
   :type code: :class:`str`
   :param subcode: Optional subcategory or specific error identifier.
   :type subcode: :class:`str` | None
   :param status_code: Optional HTTP status code if the error originated from an HTTP response.
   :type status_code: :class:`int` | None
   :param details: Optional dictionary containing additional diagnostic information.
   :type details: :class:`dict` | None
   :param source: Error source, either ``"client"`` or ``"server"``.
   :type source: :class:`str`
   :param is_transient: Whether the error is potentially transient and may succeed on retry.
   :type is_transient: :class:`bool`

   Initialize self.  See help(type(self)) for accurate signature.


   .. py:attribute:: message


   .. py:attribute:: code


   .. py:attribute:: subcode
      :value: None



   .. py:attribute:: status_code
      :value: None



   .. py:attribute:: details


   .. py:attribute:: source
      :value: 'client'



   .. py:attribute:: is_transient
      :value: False



   .. py:attribute:: timestamp


   .. py:method:: to_dict() -> Dict[str, Any]

      Convert the error to a dictionary representation.

      :return: Dictionary containing all error properties.
      :rtype: :class:`dict`



.. py:exception:: ValidationError(message: str, *, subcode: Optional[str] = None, details: Optional[Dict[str, Any]] = None)

   Bases: :py:obj:`DataverseError`


   Exception raised for client-side validation failures.

   :param message: Human-readable validation error message.
   :type message: :class:`str`
   :param subcode: Optional specific validation error identifier.
   :type subcode: :class:`str` | None
   :param details: Optional dictionary with additional validation context.
   :type details: :class:`dict` | None

   Initialize self.  See help(type(self)) for accurate signature.


.. py:exception:: MetadataError(message: str, *, subcode: Optional[str] = None, details: Optional[Dict[str, Any]] = None)

   Bases: :py:obj:`DataverseError`


   Exception raised for metadata operation failures.

   :param message: Human-readable metadata error message.
   :type message: :class:`str`
   :param subcode: Optional specific metadata error identifier.
   :type subcode: :class:`str` | None
   :param details: Optional dictionary with additional metadata context.
   :type details: :class:`dict` | None

   Initialize self.  See help(type(self)) for accurate signature.


.. py:exception:: SQLParseError(message: str, *, subcode: Optional[str] = None, details: Optional[Dict[str, Any]] = None)

   Bases: :py:obj:`DataverseError`


   Exception raised for SQL query parsing failures.

   :param message: Human-readable SQL parsing error message.
   :type message: :class:`str`
   :param subcode: Optional specific SQL parsing error identifier.
   :type subcode: :class:`str` | None
   :param details: Optional dictionary with SQL query context and parse information.
   :type details: :class:`dict` | None

   Initialize self.  See help(type(self)) for accurate signature.


.. py:exception:: HttpError(message: str, status_code: int, is_transient: bool = False, subcode: Optional[str] = None, service_error_code: Optional[str] = None, correlation_id: Optional[str] = None, client_request_id: Optional[str] = None, service_request_id: Optional[str] = None, traceparent: Optional[str] = None, body_excerpt: Optional[str] = None, retry_after: Optional[int] = None, details: Optional[Dict[str, Any]] = None)

   Bases: :py:obj:`DataverseError`


   Exception raised for HTTP request failures from the Dataverse Web API.

   :param message: Human-readable HTTP error message, typically from the API error response.
   :type message: :class:`str`
   :param status_code: HTTP status code (e.g. 400, 404, 500).
   :type status_code: :class:`int`
   :param is_transient: Whether the error is transient (429, 503, 504) and may succeed on retry.
   :type is_transient: :class:`bool`
   :param subcode: Optional HTTP status category (e.g. ``"4xx"``, ``"5xx"``).
   :type subcode: :class:`str` | None
   :param service_error_code: Optional Dataverse-specific error code from the API response.
   :type service_error_code: :class:`str` | None
   :param correlation_id: Optional client-generated correlation ID for tracking requests within an SDK call.
   :type correlation_id: :class:`str` | None
   :param client_request_id: Optional client-generated request ID injected into outbound headers.
   :type client_request_id: :class:`str` | None
   :param service_request_id: Optional ``x-ms-service-request-id`` value returned by Dataverse servers.
   :type service_request_id: :class:`str` | None
   :param traceparent: Optional W3C trace context for distributed tracing.
   :type traceparent: :class:`str` | None
   :param body_excerpt: Optional excerpt of the response body for diagnostics.
   :type body_excerpt: :class:`str` | None
   :param retry_after: Optional number of seconds to wait before retrying (from Retry-After header).
   :type retry_after: :class:`int` | None
   :param details: Optional additional diagnostic details.
   :type details: :class:`dict` | None

   Initialize self.  See help(type(self)) for accurate signature.


