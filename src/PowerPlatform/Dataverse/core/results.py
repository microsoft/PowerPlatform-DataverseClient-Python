# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Result types for Dataverse SDK operations.

This module provides structured result types that wrap operation outcomes with
request metadata for debugging and tracing:

**Core Types (New API):**

- :class:`RequestMetadata`: HTTP request/response metadata for diagnostics
- :class:`DataverseResponse`: Standard response object with result and telemetry
- :class:`FluentResult`: Wrapper enabling fluent ``.with_detail_response()`` pattern

**Legacy Types (Backward Compatible):**

- :class:`OperationResult`: Base result with request IDs for any operation
- :class:`CreateResult`: Result from create operations containing record GUIDs
- :class:`UpdateResult`: Result from update operations
- :class:`DeleteResult`: Result from delete operations (may include bulk job ID)
- :class:`GetResult`: Result from single-record fetch operations
- :class:`PagedResult`: Result for paginated queries, yielded per page

The new :class:`FluentResult` wrapper enables a fluent API pattern where operations
return values that act like their underlying results by default, but can optionally
return detailed telemetry via ``.with_detail_response()``.

Example::

    # Default behavior - acts like the result directly
    ids = client.create("account", [{"name": "A"}, {"name": "B"}])
    print(ids[0])  # Works via __getitem__

    # Detailed response with telemetry
    response = client.create("account", [{"name": "A"}]).with_detail_response()
    print(response.result)  # ['guid-123']
    print(response.telemetry['timing_ms'])  # 150
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, TypeVar, Generic, Iterator, Union


@dataclass(frozen=True)
class OperationResult:
    """
    Base result containing request metadata for any Dataverse operation.

    All result types inherit from this to provide consistent access to
    request tracking IDs for debugging and distributed tracing.

    :param client_request_id: Client-generated request ID sent in ``x-ms-client-request-id`` header.
    :type client_request_id: :class:`str` | None
    :param correlation_id: Client-generated correlation ID sent in ``x-ms-correlation-id`` header,
        shared across all HTTP requests within a single SDK call scope.
    :type correlation_id: :class:`str` | None
    :param service_request_id: Server-returned ``x-ms-service-request-id`` (if available).
        Typically only populated on error responses.
    :type service_request_id: :class:`str` | None
    """

    client_request_id: Optional[str] = None
    correlation_id: Optional[str] = None
    service_request_id: Optional[str] = None


@dataclass(frozen=True)
class CreateResult(OperationResult):
    """
    Result from a create operation containing the created record GUID(s).

    :param ids: List of created record GUIDs. Single-element list for single creates.
    :type ids: :class:`list` of :class:`str`

    Example:
        Create a single record::

            result = client.create("account", {"name": "Contoso"})
            print(f"Created: {result.ids[0]}")
            print(f"Request ID: {result.client_request_id}")

        Create multiple records::

            result = client.create("account", [{"name": "A"}, {"name": "B"}])
            print(f"Created {len(result.ids)} records")
    """

    ids: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class UpdateResult(OperationResult):
    """
    Result from an update operation.

    Update operations don't return data, but the result provides request
    metadata for debugging.

    Example:
        Update a record::

            result = client.update("account", account_id, {"name": "New Name"})
            print(f"Update completed, request ID: {result.client_request_id}")
    """

    pass


@dataclass(frozen=True)
class DeleteResult(OperationResult):
    """
    Result from a delete operation.

    :param bulk_job_id: Async job ID when using BulkDelete for multiple records.
        ``None`` for single deletes or sequential multi-delete.
    :type bulk_job_id: :class:`str` | None

    Example:
        Delete multiple records via BulkDelete::

            result = client.delete("account", [id1, id2, id3])
            if result.bulk_job_id:
                print(f"Bulk delete job: {result.bulk_job_id}")
    """

    bulk_job_id: Optional[str] = None


@dataclass(frozen=True)
class GetResult(OperationResult):
    """
    Result from fetching a single record by ID.

    :param record: The retrieved record as a dictionary.
    :type record: :class:`dict`

    Example:
        Fetch a single record::

            result = client.get("account", record_id=account_id)
            print(f"Name: {result.record['name']}")
            print(f"Request ID: {result.client_request_id}")
    """

    record: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PagedResult(OperationResult):
    """
    Result for a single page of a paginated query.

    Yielded by ``get()`` when querying multiple records. Each page carries
    its own request metadata from the HTTP request that fetched it.

    :param records: List of record dictionaries in this page.
    :type records: :class:`list` of :class:`dict`
    :param page_number: 1-based page number (first page is 1).
    :type page_number: :class:`int`
    :param has_more: Whether more pages are available after this one.
    :type has_more: :class:`bool`

    Example:
        Iterate through pages::

            for page in client.get("account", filter="statecode eq 0"):
                print(f"Page {page.page_number}: {len(page.records)} records")
                print(f"Request ID: {page.client_request_id}")
                for record in page.records:
                    print(record["name"])
    """

    records: List[Dict[str, Any]] = field(default_factory=list)
    page_number: int = 0
    has_more: bool = False


# =============================================================================
# New Fluent API Types (Phase 1 Implementation)
# =============================================================================

# Type variable for generic result types
T = TypeVar('T')


@dataclass(frozen=True)
class RequestMetadata:
    """
    HTTP request/response metadata for diagnostics and tracing.

    This dataclass captures metadata from HTTP requests for debugging,
    monitoring, and distributed tracing scenarios.

    :param client_request_id: Client-generated request ID sent in
        ``x-ms-client-request-id`` header.
    :type client_request_id: :class:`str` | None
    :param correlation_id: Client-generated correlation ID sent in
        ``x-ms-correlation-id`` header, shared across all HTTP requests
        within a single SDK call scope.
    :type correlation_id: :class:`str` | None
    :param service_request_id: Server-returned ``x-ms-service-request-id``
        header value (if available).
    :type service_request_id: :class:`str` | None
    :param http_status_code: HTTP response status code.
    :type http_status_code: :class:`int` | None
    :param timing_ms: Operation duration in milliseconds.
    :type timing_ms: :class:`float` | None

    Example::

        metadata = RequestMetadata(
            client_request_id="abc-123",
            correlation_id="corr-456",
            http_status_code=201,
            timing_ms=150.5
        )
    """

    client_request_id: Optional[str] = None
    correlation_id: Optional[str] = None
    service_request_id: Optional[str] = None
    http_status_code: Optional[int] = None
    timing_ms: Optional[float] = None


@dataclass
class DataverseResponse(Generic[T]):
    """
    Standard response object for all Dataverse operations.

    This class provides a consistent structure for operation responses,
    combining the operation result with telemetry data for monitoring
    and debugging.

    :param result: The operation result (IDs, records, etc.). The type
        depends on the operation:

        - Create single: ``str`` (record ID)
        - Create bulk: ``list[str]`` (record IDs)
        - Update: ``None``
        - Delete single: ``None``
        - Delete bulk: ``str | None`` (bulk job ID)
        - Get single: ``dict`` (record)
        - Query: ``list[dict]`` (records)

    :type result: T
    :param telemetry: Dictionary containing telemetry data:

        - ``client_request_id``: Client request ID
        - ``correlation_id``: Correlation ID for the operation scope
        - ``service_request_id``: Server-side request ID
        - ``http_status_code``: HTTP response status code
        - ``timing_ms``: Operation duration in milliseconds
        - ``batch_info``: Batch details for bulk operations (optional)

    :type telemetry: :class:`dict`

    Example::

        response = client.create("account", [{"name": "A"}]).with_detail_response()
        print(response.result)  # ['guid-123']
        print(response.telemetry['timing_ms'])  # 150.5
        print(response.telemetry['http_status_code'])  # 200
    """

    result: T
    telemetry: Dict[str, Any] = field(default_factory=dict)


class FluentResult(Generic[T]):
    """
    Wrapper enabling fluent ``.with_detail_response()`` pattern.

    This class wraps operation results to provide a fluent API where:

    - **Default behavior**: Acts like the result directly (supports iteration,
      indexing, string conversion, equality, etc.)
    - **Detailed behavior**: Call ``.with_detail_response()`` to get a
      :class:`DataverseResponse` with telemetry data

    This pattern allows existing code to continue working unchanged while
    enabling new code to access detailed telemetry when needed.

    :param result: The operation result value.
    :type result: T
    :param metadata: HTTP request/response metadata.
    :type metadata: :class:`RequestMetadata`
    :param batch_info: Optional batch information for bulk operations.
    :type batch_info: :class:`dict` | None

    Example::

        # Default behavior - acts like the result directly
        ids = client.create("account", [{"name": "A"}, {"name": "B"}])
        print(ids[0])  # Works via __getitem__
        for id in ids:  # Works via __iter__
            print(id)

        # Detailed response with telemetry
        response = client.create("account", [{"name": "A"}]).with_detail_response()
        print(response.result)  # ['guid-123']
        print(response.telemetry['timing_ms'])  # 150
        print(response.telemetry['batch_info'])  # {'total': 1, 'success': 1}
    """

    __slots__ = ('_result', '_metadata', '_batch_info')

    def __init__(
        self,
        result: T,
        metadata: RequestMetadata,
        batch_info: Optional[Dict[str, Any]] = None
    ) -> None:
        self._result = result
        self._metadata = metadata
        self._batch_info = batch_info

    @property
    def value(self) -> T:
        """
        Direct access to the result value.

        This property provides explicit access to the underlying result
        when the magic methods are not sufficient.

        :return: The operation result.
        :rtype: T
        """
        return self._result

    def with_detail_response(self) -> DataverseResponse[T]:
        """
        Return detailed response with telemetry.

        Converts this fluent result into a :class:`DataverseResponse` that
        includes both the operation result and telemetry data.

        :return: A DataverseResponse containing result and telemetry.
        :rtype: :class:`DataverseResponse`

        Example::

            response = client.create("account", {"name": "A"}).with_detail_response()
            print(response.result)  # 'guid-123'
            print(response.telemetry['timing_ms'])  # 150.5
        """
        telemetry: Dict[str, Any] = {
            "client_request_id": self._metadata.client_request_id,
            "correlation_id": self._metadata.correlation_id,
            "service_request_id": self._metadata.service_request_id,
            "http_status_code": self._metadata.http_status_code,
            "timing_ms": self._metadata.timing_ms,
        }
        if self._batch_info is not None:
            telemetry["batch_info"] = self._batch_info
        return DataverseResponse(result=self._result, telemetry=telemetry)

    # -------------------------------------------------------------------------
    # Magic methods for transparent default behavior
    # -------------------------------------------------------------------------

    def __iter__(self) -> Iterator:
        """
        Support iteration for default behavior.

        Allows iterating over the result when it's a sequence.
        For non-sequence results, yields the single result.

        Example::

            ids = client.create("account", [{"name": "A"}, {"name": "B"}])
            for id in ids:
                print(id)
        """
        if isinstance(self._result, (list, tuple)):
            return iter(self._result)
        return iter([self._result])

    def __getitem__(self, key: Any) -> Any:
        """
        Support indexing for default behavior.

        Allows accessing elements by index or key depending on the result type.

        Example::

            ids = client.create("account", [{"name": "A"}, {"name": "B"}])
            print(ids[0])  # First ID
        """
        return self._result[key]  # type: ignore

    def __len__(self) -> int:
        """
        Return the length of the result.

        For sequences, returns the number of elements.
        For non-sequences, returns 1.

        Example::

            ids = client.create("account", [{"name": "A"}, {"name": "B"}])
            print(len(ids))  # 2
        """
        if isinstance(self._result, (list, tuple, dict)):
            return len(self._result)
        return 1

    def __str__(self) -> str:
        """Return string representation of the result."""
        return str(self._result)

    def __repr__(self) -> str:
        """Return detailed representation for debugging."""
        return f"FluentResult({self._result!r})"

    def __eq__(self, other: object) -> bool:
        """
        Compare for equality.

        Compares against both FluentResult instances and raw values.

        Example::

            result1 = client.create("account", {"name": "A"})
            result2 = client.create("account", {"name": "A"})
            print(result1 == result2)  # True if same IDs
        """
        if isinstance(other, FluentResult):
            return self._result == other._result
        return self._result == other

    def __bool__(self) -> bool:
        """
        Return truthiness of the result.

        Example::

            result = client.delete("account", "guid-123")
            if result:  # True if result is truthy
                print("Deleted")
        """
        return bool(self._result)

    def __contains__(self, item: Any) -> bool:
        """
        Support ``in`` operator for sequences.

        Example::

            ids = client.create("account", [{"name": "A"}, {"name": "B"}])
            if "guid-123" in ids:
                print("Found!")
        """
        if isinstance(self._result, (list, tuple, dict, str)):
            return item in self._result
        return item == self._result

    def __hash__(self) -> int:
        """
        Return hash for hashable results.

        Note: Only works if the underlying result is hashable.
        """
        if isinstance(self._result, (list, dict)):
            # Lists and dicts are not hashable
            raise TypeError(f"unhashable type: 'FluentResult' with {type(self._result).__name__}")
        return hash(self._result)


__all__ = [
    # Legacy types (backward compatible)
    "OperationResult",
    "CreateResult",
    "UpdateResult",
    "DeleteResult",
    "GetResult",
    "PagedResult",
    # New fluent API types
    "RequestMetadata",
    "DataverseResponse",
    "FluentResult",
]
