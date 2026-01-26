# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Result types for the Dataverse SDK.

This module provides the foundational result types that enable the fluent
`.with_response_details()` API pattern for accessing telemetry data.

Classes:
    RequestMetadata: Immutable HTTP request/response metadata for diagnostics.
    DataverseResponse: Standard response object with result and telemetry.
    OperationResult: Wrapper enabling fluent .with_response_details() pattern.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, TypeVar, Generic, Iterator, Dict, Optional

T = TypeVar("T")


@dataclass(frozen=True)
class RequestTelemetryData:
    """
    Telemetry data from HTTP requests for diagnostics.

    This immutable dataclass captures correlation IDs from HTTP requests
    for debugging and distributed tracing purposes.

    :param client_request_id: Client-generated request ID sent in outbound headers.
    :type client_request_id: :class:`str` | None
    :param correlation_id: Client-generated correlation ID for tracking requests within an SDK call.
    :type correlation_id: :class:`str` | None
    :param service_request_id: Server-side request ID from Dataverse (x-ms-service-request-id).
    :type service_request_id: :class:`str` | None
    """

    client_request_id: Optional[str] = None
    correlation_id: Optional[str] = None
    service_request_id: Optional[str] = None


@dataclass
class DataverseResponse(Generic[T]):
    """
    Standard response object for all Dataverse operations.

    Provides consistent structure with operation result and telemetry data.

    :param result: The operation result (record IDs, records, tables, etc.)
    :type result: T
    :param telemetry: Dictionary containing request correlation IDs for diagnostics.
        Keys include: ``service_request_id``, ``client_request_id``, ``correlation_id``.
    :type telemetry: :class:`dict`

    Example:
        Access result and telemetry after a create operation::

            response = client.create("account", [{"name": "A"}]).with_response_details()
            print(response.result)  # ['guid-123']
            print(response.telemetry['service_request_id'])  # 'abc-123...'
            print(response.telemetry['client_request_id'])  # 'xyz-456...'
    """

    result: T
    telemetry: Dict[str, Any] = field(default_factory=dict)


class OperationResult(Generic[T]):
    """
    Wrapper enabling fluent .with_response_details() pattern.

    By default, ``OperationResult`` acts like the underlying result value,
    supporting iteration, indexing, equality comparison, and string conversion.
    Call ``.with_response_details()`` to get a ``DataverseResponse`` with telemetry.

    :param result: The operation result value.
    :type result: T
    :param telemetry_data: Request telemetry data containing correlation IDs.
    :type telemetry_data: :class:`RequestTelemetryData`

    Example:
        Default behavior (acts like the result)::

            # Returns OperationResult[List[str]]
            ids = client.create("account", [{"name": "A"}, {"name": "B"}])
            print(ids[0])  # Works via __getitem__
            for id in ids:  # Works via __iter__
                print(id)

        Detailed response with telemetry::

            response = client.create("account", [{"name": "A"}]).with_response_details()
            print(response.telemetry["client_request_id"])
    """

    def __init__(self, result: T, telemetry_data: RequestTelemetryData) -> None:
        """
        Initialize an OperationResult.

        :param result: The operation result value.
        :type result: T
        :param telemetry_data: Request telemetry data containing correlation IDs.
        :type telemetry_data: :class:`RequestTelemetryData`
        """
        self._result = result
        self._telemetry_data = telemetry_data

    @property
    def value(self) -> T:
        """
        Direct access to the result value.

        :return: The underlying result value.
        :rtype: T
        """
        return self._result

    def with_response_details(self) -> DataverseResponse[T]:
        """
        Return detailed response with telemetry.

        :return: A DataverseResponse containing the result and telemetry dictionary.
        :rtype: :class:`DataverseResponse`

        Example:
            >>> response = result.with_response_details()
            >>> print(response.result)
            >>> print(response.telemetry['service_request_id'])
        """
        telemetry = {
            "client_request_id": self._telemetry_data.client_request_id,
            "correlation_id": self._telemetry_data.correlation_id,
            "service_request_id": self._telemetry_data.service_request_id,
        }
        return DataverseResponse(result=self._result, telemetry=telemetry)

    # Dunder methods for transparent default behavior

    def __iter__(self) -> Iterator:
        """
        Support iteration over the result.

        If the result is a list or tuple, iterates over its elements.
        Otherwise, iterates over a single-element list containing the result.

        :return: Iterator over the result.
        :rtype: :class:`Iterator`
        """
        if isinstance(self._result, (list, tuple)):
            return iter(self._result)
        return iter([self._result])

    def __getitem__(self, key: Any) -> Any:
        """
        Support indexing into the result.

        :param key: Index or key to access.
        :return: Element at the specified index/key.
        """
        return self._result[key]  # type: ignore[index]

    def __len__(self) -> int:
        """
        Return the length of the result.

        For lists, tuples, and dicts, returns their length.
        For other types, returns 1.

        :return: Length of the result.
        :rtype: :class:`int`
        """
        if isinstance(self._result, (list, tuple, dict)):
            return len(self._result)
        return 1

    def __str__(self) -> str:
        """
        Return string representation of the result.

        :return: String representation.
        :rtype: :class:`str`
        """
        return str(self._result)

    def __repr__(self) -> str:
        """
        Return detailed string representation.

        :return: Detailed representation.
        :rtype: :class:`str`
        """
        return f"OperationResult({self._result!r})"

    def __eq__(self, other: object) -> bool:
        """
        Compare equality with another value.

        If comparing with another OperationResult, compares the underlying results.
        Otherwise, compares the result directly with the other value.

        :param other: Value to compare with.
        :return: True if equal, False otherwise.
        :rtype: :class:`bool`
        """
        if isinstance(other, OperationResult):
            return self._result == other._result
        return self._result == other

    def __bool__(self) -> bool:
        """
        Return truthiness of the result.

        :return: True if the result is truthy, False otherwise.
        :rtype: :class:`bool`
        """
        return bool(self._result)

    def __getattr__(self, name: str) -> Any:
        """
        Delegate attribute access to the underlying result.

        This enables calling methods like .get() on dict results transparently.

        :param name: Attribute name to access.
        :return: Attribute from the underlying result.
        :raises AttributeError: If the result doesn't have the attribute.
        """
        return getattr(self._result, name)

    def __contains__(self, item: Any) -> bool:
        """
        Support 'in' operator for membership testing.

        :param item: Item to check for membership.
        :return: True if item is in the result.
        :rtype: :class:`bool`
        """
        return item in self._result  # type: ignore[operator]

    def __add__(self, other: Any) -> Any:
        """
        Support concatenation with + operator.

        When combining OperationResults (e.g., concatenating batches), returns
        the raw combined result since there's no meaningful single telemetry
        to preserve for the combined value.

        :param other: Value to concatenate with.
        :return: Combined result (raw value, not wrapped in OperationResult).
        """
        if isinstance(other, OperationResult):
            return self._result + other._result  # type: ignore[operator]
        return self._result + other  # type: ignore[operator]

    def __radd__(self, other: Any) -> Any:
        """
        Support right-hand concatenation (e.g., [] + result).

        :param other: Left-hand value to concatenate with.
        :return: Combined result (raw value, not wrapped in OperationResult).
        """
        return other + self._result  # type: ignore[operator]


__all__ = ["RequestTelemetryData", "DataverseResponse", "OperationResult"]
