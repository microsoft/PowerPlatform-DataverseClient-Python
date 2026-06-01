"""CRUD operations for FinOps OData entities (``/data/{EntitySet}``).

This is **Step 1** of the FinOps SDK roadmap captured in
``FinOps-SDK-Plan.docx``. It implements only the four basic CRUD verbs:

============  =========================================  =====================================
Verb          HTTP                                       SDK call
============  =========================================  =====================================
Create        ``POST   /data/{EntitySet}``               ``client.records.create(...)``
Retrieve      ``GET    /data/{EntitySet}({key})``        ``client.records.get(...)``
Update        ``PATCH  /data/{EntitySet}({key})``        ``client.records.update(...)``
Delete        ``DELETE /data/{EntitySet}({key})``        ``client.records.delete(...)``
============  =========================================  =====================================

These verbs are backed by ``AxODataController`` in the FinOps Platform
(``Source/Platform/Integration/Services/OData/Sources/AxODataController.cs``),
which exposes standard OData v4 over the ``/data`` path.

Notes
-----
* **Composite keys.** Many FinOps entities are keyed by the company partition
  (``dataAreaId``) plus one or more business identifiers. This module accepts
  either a scalar ``key`` (single-valued) or a ``Mapping[str, Any]`` (composite),
  and serializes it into the canonical OData key syntax
  ``Set(name1=value1,name2=value2)``.
* **Optimistic concurrency.** Update / Delete accept an optional ``etag``;
  it's emitted as ``If-Match``. Defaults to ``*`` (overwrite) so the basic
  CRUD path 'just works' for the v0.1 spike.
* The Create call returns the URL of the newly created entity (extracted from
  the ``OData-EntityId`` header) when the server returns a Location header.
  When it does not, the deserialized response body is returned instead.
"""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Iterator, Mapping, Optional, Sequence, Union
from urllib.parse import quote

from ..errors import FinOpsError

if TYPE_CHECKING:  # pragma: no cover
    from ..client import FinOpsClient


KeyType = Union[str, int, Mapping[str, Any]]


class RecordOperations:
    """CRUD operations on FinOps OData entity sets.

    Obtain via ``FinOpsClient.records`` — do not instantiate directly.
    """

    def __init__(self, client: "FinOpsClient") -> None:
        self._client = client

    # ------------------------------------------------------------------ #
    # CREATE                                                             #
    # ------------------------------------------------------------------ #
    def create(self, entity_set: str, data: Mapping[str, Any]) -> Union[str, dict]:
        """``POST /data/{entity_set}`` — create one record.

        Returns
        -------
        str
            The fully-qualified URL of the newly-created entity, taken
            from the ``OData-EntityId`` response header, when present.
        dict
            Otherwise, the deserialized JSON response body.
        """
        if not isinstance(data, Mapping):
            raise TypeError("data must be a mapping of column -> value")
        url = self._collection_url(entity_set)
        resp = self._client._http.request(
            "POST",
            url,
            json=dict(data),
            headers={"Content-Type": "application/json"},
            expected=(200, 201, 204),
        )
        # FinOps (like Dataverse) returns the new entity's URL in OData-EntityId.
        loc = resp.headers.get("OData-EntityId") or resp.headers.get("Location")
        if loc:
            return loc
        if resp.content:
            return resp.json()
        return {}

    # ------------------------------------------------------------------ #
    # RETRIEVE                                                           #
    # ------------------------------------------------------------------ #
    def get(
        self,
        entity_set: str,
        key: KeyType,
        *,
        select: Optional[list] = None,
        expand: Optional[list] = None,
    ) -> dict:
        """``GET /data/{entity_set}({key})`` — read one record.

        Parameters
        ----------
        entity_set:
            FinOps OData entity set name (e.g. ``"CustomersV3"``).
        key:
            Single-value key or a mapping for composite keys.
        select:
            Optional list of column names for the OData ``$select`` projection.
        expand:
            Optional list of navigation properties for ``$expand``.
        """
        url = self._record_url(entity_set, key)
        params: dict = {}
        if select:
            params["$select"] = ",".join(select)
        if expand:
            params["$expand"] = ",".join(expand)
        resp = self._client._http.request("GET", url, params=params or None, expected=(200,))
        return resp.json()

    # ------------------------------------------------------------------ #
    # LIST (paginated)                                                   #
    # ------------------------------------------------------------------ #
    def list(
        self,
        entity_set: str,
        *,
        filter: Optional[str] = None,
        select: Optional[Sequence[str]] = None,
        expand: Optional[Sequence[str]] = None,
        orderby: Optional[Union[str, Sequence[str]]] = None,
        top: Optional[int] = None,
        page_size: Optional[int] = None,
        cross_company: bool = False,
    ) -> Iterator[dict]:
        """``GET /data/{entity_set}`` — yield rows lazily across all pages.

        Transparently follows the ``@odata.nextLink`` continuation token
        emitted by FinOps OData and yields one row dict at a time. Stops
        after ``top`` rows when given (server is told via ``$top``; client
        also caps just in case the server ignores it).

        Parameters
        ----------
        entity_set:
            FinOps OData entity set name (e.g. ``"CustomersV3"``).
        filter:
            Raw OData ``$filter`` expression. Callers are responsible for
            quoting; the SDK will not try to parse it. A typed query builder
            is on the roadmap (see ``FinOps-SDK-Plan.docx`` §8 Phase 2).
        select:
            Column names for ``$select``.
        expand:
            Navigation properties for ``$expand``.
        orderby:
            Either a single OData ordering clause (``"CreatedDateTime desc"``)
            or a list of them.
        top:
            Hard cap on rows. Sent server-side as ``$top`` and enforced
            client-side as a defensive stop.
        page_size:
            Optional ``Prefer: odata.maxpagesize=N`` hint to ask the server
            for smaller pages — useful when the dataset is large and the
            caller is paging memory-sensitively.
        cross_company:
            When ``True``, sends the FinOps-specific ``cross-company=true``
            query parameter so rows from every legal entity (``dataAreaId``)
            are returned. Default is ``False``, which mirrors the FinOps
            OData default of scoping to the caller's default company.

        Yields
        ------
        dict
            One OData row at a time.
        """
        params: dict = {}
        if cross_company:
            params["cross-company"] = "true"
        if filter:
            params["$filter"] = filter
        if select:
            params["$select"] = ",".join(select)
        if expand:
            params["$expand"] = ",".join(expand)
        if orderby:
            params["$orderby"] = orderby if isinstance(orderby, str) else ",".join(orderby)
        if top is not None:
            if top <= 0:
                return
            params["$top"] = str(top)

        headers: Optional[dict] = None
        if page_size is not None:
            if page_size <= 0:
                raise ValueError("page_size must be positive")
            headers = {"Prefer": f"odata.maxpagesize={int(page_size)}"}

        url: Optional[str] = self._collection_url(entity_set)
        request_params: Optional[dict] = params or None
        yielded = 0
        while url:
            resp = self._client._http.request(
                "GET", url, params=request_params, headers=headers, expected=(200,)
            )
            payload = resp.json()
            for row in payload.get("value", []):
                if top is not None and yielded >= top:
                    return
                yield row
                yielded += 1
            url = payload.get("@odata.nextLink")
            # The nextLink already encodes all of the original $-options.
            request_params = None


    def update(
        self,
        entity_set: str,
        key: KeyType,
        changes: Mapping[str, Any],
        *,
        etag: str = "*",
    ) -> None:
        """``PATCH /data/{entity_set}({key})`` — partial update.

        ``etag`` is emitted as the ``If-Match`` header (defaults to ``*`` —
        unconditional overwrite). Pass a real ETag string to enable optimistic
        concurrency; the call will raise :class:`FinOpsConcurrencyError` (412)
        on mismatch.
        """
        if not isinstance(changes, Mapping):
            raise TypeError("changes must be a mapping of column -> value")
        if not changes:
            raise ValueError("changes is empty — nothing to update")
        url = self._record_url(entity_set, key)
        self._client._http.request(
            "PATCH",
            url,
            json=dict(changes),
            headers={"Content-Type": "application/json", "If-Match": etag},
            expected=(200, 204),
        )

    # ------------------------------------------------------------------ #
    # DELETE                                                             #
    # ------------------------------------------------------------------ #
    def delete(
        self,
        entity_set: str,
        key: KeyType,
        *,
        etag: str = "*",
    ) -> None:
        """``DELETE /data/{entity_set}({key})`` — delete one record.

        ``etag`` is emitted as ``If-Match`` (defaults to ``*``).
        """
        url = self._record_url(entity_set, key)
        self._client._http.request(
            "DELETE",
            url,
            headers={"If-Match": etag},
            expected=(200, 204),
        )

    # ------------------------------------------------------------------ #
    # URL helpers                                                        #
    # ------------------------------------------------------------------ #
    def _collection_url(self, entity_set: str) -> str:
        return f"{self._client._data_url}/{_safe_segment(entity_set)}"

    def _record_url(self, entity_set: str, key: KeyType) -> str:
        return f"{self._collection_url(entity_set)}({_format_key(key)})"


# ---------------------------------------------------------------------- #
# OData key formatting                                                   #
# ---------------------------------------------------------------------- #


def _safe_segment(name: str) -> str:
    if not name or "/" in name:
        raise FinOpsError(f"invalid entity set name: {name!r}")
    return quote(name, safe="")


def _format_key(key: KeyType) -> str:
    """Render ``key`` in canonical OData key syntax."""
    if isinstance(key, Mapping):
        if not key:
            raise ValueError("composite key mapping cannot be empty")
        parts = [f"{_safe_segment(k)}={_format_value(v)}" for k, v in key.items()]
        return ",".join(parts)
    return _format_value(key)


def _format_value(v: Any) -> str:
    """OData v4 primitive literal serialization (subset sufficient for keys)."""
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, Decimal)):
        return str(v)
    if isinstance(v, float):
        return repr(v)
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, str):
        # Single quotes inside values are doubled in OData literals.
        escaped = v.replace("'", "''")
        return f"'{escaped}'"
    raise TypeError(f"unsupported OData key value type: {type(v).__name__}")
