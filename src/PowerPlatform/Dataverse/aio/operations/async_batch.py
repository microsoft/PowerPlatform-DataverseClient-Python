# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Async batch operation namespaces for the Dataverse SDK."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, List

from ...data._batch_base import _ChangeSet
from ...operations.batch import (
    BatchDataFrameOperations,
    BatchQueryOperations,
    BatchRecordOperations,
    BatchTableOperations,
    ChangeSetRecordOperations,
)
from ..data._async_batch import _AsyncBatchClient
from ...models.batch import BatchResult

if TYPE_CHECKING:
    from ..async_client import AsyncDataverseClient

__all__ = [
    "AsyncBatchRequest",
    "AsyncBatchOperations",
    "AsyncChangeSet",
]


# ---------------------------------------------------------------------------
# Changeset
# ---------------------------------------------------------------------------


class AsyncChangeSet:
    """
    A transactional group of single-record write operations.

    All operations succeed or are rolled back together. Use as an async context
    manager or call :attr:`records` to add operations directly.

    Do not instantiate directly; use :meth:`AsyncBatchRequest.changeset`.

    Example::

        async with batch.changeset() as cs:
            ref = cs.records.create("contact", {"firstname": "Alice"})
            cs.records.update("account", account_id, {
                "primarycontactid@odata.bind": ref
            })
    """

    def __init__(self, internal: _ChangeSet) -> None:
        self._internal = internal
        self.records = ChangeSetRecordOperations(internal)

    async def __aenter__(self) -> "AsyncChangeSet":
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        return None


# ---------------------------------------------------------------------------
# AsyncBatchRequest and AsyncBatchOperations
# ---------------------------------------------------------------------------


class AsyncBatchRequest:
    """
    Builder for constructing and executing a Dataverse OData ``$batch`` request.

    Obtain via :meth:`AsyncBatchOperations.new` (``client.batch.new()``). Add operations
    through :attr:`records`, :attr:`tables`, :attr:`query`, and :attr:`dataframe`,
    optionally group writes into a :meth:`changeset`, then call :meth:`execute`.

    Operations are executed sequentially in the order added. The resulting
    :class:`~PowerPlatform.Dataverse.models.batch.BatchResult` contains one
    :class:`~PowerPlatform.Dataverse.models.batch.BatchItemResponse` per HTTP
    request dispatched (some operations expand to multiple requests).

    .. note::
        Maximum 1000 HTTP operations per batch.

    Example::

        batch = client.batch.new()
        batch.records.create("account", {"name": "Contoso"})
        batch.tables.get("account")
        async with batch.changeset() as cs:
            ref = cs.records.create("contact", {"firstname": "Alice"})
            cs.records.update("account", account_id, {
                "primarycontactid@odata.bind": ref
            })
        result = await batch.execute()
    """

    def __init__(self, client: "AsyncDataverseClient") -> None:
        self._client = client
        self._items: List[Any] = []
        self._content_id_counter: List[int] = [1]  # shared across all changesets
        self.records = BatchRecordOperations(self)  # type: ignore[arg-type]
        self.tables = BatchTableOperations(self)  # type: ignore[arg-type]
        self.query = BatchQueryOperations(self)  # type: ignore[arg-type]
        self.dataframe = BatchDataFrameOperations(self)  # type: ignore[arg-type]

    def changeset(self) -> AsyncChangeSet:
        """
        Create a new :class:`AsyncChangeSet` attached to this batch.

        The changeset is added to the batch immediately. Operations added to
        the returned :class:`AsyncChangeSet` via ``cs.records.*`` execute atomically.

        :returns: A new :class:`AsyncChangeSet` ready to receive operations.

        Example::

            async with batch.changeset() as cs:
                cs.records.create("account", {"name": "ACME"})
                cs.records.create("contact", {"firstname": "Bob"})
        """
        internal = _ChangeSet(_counter=self._content_id_counter)
        self._items.append(internal)
        return AsyncChangeSet(internal)

    async def execute(self, *, continue_on_error: bool = False) -> BatchResult:
        """
        Submit the batch to Dataverse and return all responses.

        :param continue_on_error: When False (default), Dataverse stops at the
            first failure and returns that operation's error as a 4xx response.
            When True, ``Prefer: odata.continue-on-error`` is sent and all
            operations are attempted.
        :returns: :class:`~PowerPlatform.Dataverse.models.batch.BatchResult`
            with one entry per HTTP operation in submission order.
        :raises ValidationError: If the batch exceeds 1000 operations or an
            unsupported column type is specified.
        :raises MetadataError: If metadata pre-resolution fails (table or
            column not found) for ``tables.delete``, ``tables.add_columns``,
            or ``tables.remove_columns``.
        :raises HttpError: On HTTP-level failures (auth, server error, etc.)
            that prevent the batch from executing.
        """
        async with self._client._scoped_odata() as od:
            return await _AsyncBatchClient(od).execute(self._items, continue_on_error=continue_on_error)


class AsyncBatchOperations:
    """
    Async namespace for batch operations (``client.batch``).

    Accessed via ``client.batch``. Use :meth:`new` to create an
    :class:`AsyncBatchRequest` builder.

    :param client: The parent :class:`~PowerPlatform.Dataverse.aio.async_client.AsyncDataverseClient` instance.

    Example::

        batch = client.batch.new()
        batch.records.create("account", {"name": "Fabrikam"})
        result = await batch.execute()
    """

    def __init__(self, client: "AsyncDataverseClient") -> None:
        self._client = client

    def new(self) -> AsyncBatchRequest:
        """
        Create a new empty :class:`AsyncBatchRequest` builder.

        :returns: An empty :class:`AsyncBatchRequest`.
        """
        return AsyncBatchRequest(self._client)
