# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Public result types for batch operations."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

__all__ = ["BatchItemResponse", "BatchResult"]


@dataclass
class BatchItemResponse:
    """
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
    """

    status_code: int
    content_id: Optional[str] = None
    entity_id: Optional[str] = None
    data: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    error_code: Optional[str] = None

    @property
    def is_success(self) -> bool:
        """Return True when status_code is 2xx."""
        return 200 <= self.status_code < 300


@dataclass
class BatchResult:
    """
    Result of executing a batch request.

    Contains one :class:`BatchItemResponse` per HTTP operation submitted.
    Operations that expand to multiple HTTP requests (e.g. ``add_columns``
    with three columns) contribute three entries.

    :param responses: All responses in submission order.

    Example::

        result = client.batch.new().execute()
        print(f"Succeeded: {len(result.succeeded)}, Failed: {len(result.failed)}")
        for guid in result.created_ids:
            print(f"[OK] Created: {guid}")
    """

    responses: List[BatchItemResponse] = field(default_factory=list)

    @property
    def succeeded(self) -> List[BatchItemResponse]:
        """Responses with 2xx status codes."""
        return [r for r in self.responses if r.is_success]

    @property
    def failed(self) -> List[BatchItemResponse]:
        """Responses with non-2xx status codes."""
        return [r for r in self.responses if not r.is_success]

    @property
    def has_errors(self) -> bool:
        """True when any response has a non-2xx status code."""
        return any(not r.is_success for r in self.responses)

    @property
    def created_ids(self) -> List[str]:
        """GUIDs extracted from ``OData-EntityId`` headers of successful responses.

        Returns entity IDs from any successful (2xx) response that includes an
        ``OData-EntityId`` header.  Individual ``POST`` creates return this
        header with the new record's GUID.

        .. note::
            ``CreateMultiple`` and ``UpsertMultiple`` action responses do **not**
            return per-record ``OData-EntityId`` headers.  Their IDs are in the
            JSON response body (``data["Ids"]``).  Access them via::

                for resp in result.succeeded:
                    if resp.data and "Ids" in resp.data:
                        bulk_ids = resp.data["Ids"]
        """
        return [r.entity_id for r in self.succeeded if r.entity_id is not None]
