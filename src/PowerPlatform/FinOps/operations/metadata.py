"""Metadata read operations for the FinOps SDK.

This is **Step 2** of the FinOps SDK roadmap captured in
``FinOps-SDK-Plan.docx``. It wraps the read-only metadata surface exposed
by the FinOps Platform under ``/metadata/...``:

==============================  ==============================================
HTTP                            SDK call
==============================  ==============================================
``GET /metadata/DataEntities``        ``client.metadata.list_data_entities(...)``
``GET /metadata/DataEntities('N')``   ``client.metadata.get_data_entity('N')``
``GET /metadata/PublicEntities``      ``client.metadata.list_public_entities(...)``
``GET /metadata/PublicEntities('N')`` ``client.metadata.get_public_entity('N')``
``GET /metadata/PublicEnumerations`` ``client.metadata.list_public_enumerations()``
==============================  ==============================================

These verbs are backed by ``DataEntitiesController`` and sibling controllers in
the FinOps Platform under
``Source/Platform/Integration/Services/WebApi/Metadata/Source/Controllers/``.

These endpoints are read-only by design — there is no runtime metadata-write
API in FinOps (schema is authored in X++ and built into the model layer; see
``FinOps-SDK-Plan.docx`` §7).
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Iterator, Optional
from urllib.parse import quote

from ..errors import FinOpsError

if TYPE_CHECKING:  # pragma: no cover
    from ..client import FinOpsClient


class MetadataOperations:
    """Read-only metadata operations on the FinOps ``/metadata`` surface.

    Obtain via ``FinOpsClient.metadata`` — do not instantiate directly.
    """

    def __init__(self, client: "FinOpsClient") -> None:
        self._client = client

    # ------------------------------------------------------------------ #
    # /metadata/DataEntities                                             #
    # ------------------------------------------------------------------ #
    def list_data_entities(
        self,
        *,
        filter: Optional[str] = None,
        top: Optional[int] = None,
    ) -> Iterator[dict]:
        """``GET /metadata/DataEntities`` — yield every public data entity descriptor.

        Returns one row at a time, transparently following the
        ``@odata.nextLink`` continuation token.

        .. note::
           The metadata controllers do **not** support ``$select`` (the server
           replies HTTP 400). ``$top`` is accepted but currently ignored
           server-side, so this SDK enforces ``top`` as a client-side cap.
        """
        yield from self._paginate("DataEntities", filter=filter, top=top)

    def get_data_entity(self, name: str) -> dict:
        """``GET /metadata/DataEntities('Name')`` — single entity descriptor."""
        if not name:
            raise ValueError("entity name is required")
        url = f"{self._metadata_url()}/DataEntities('{_escape(name)}')"
        return self._client._http.request("GET", url, expected=(200,)).json()

    # ------------------------------------------------------------------ #
    # /metadata/PublicEntities                                           #
    # ------------------------------------------------------------------ #
    def list_public_entities(
        self,
        *,
        filter: Optional[str] = None,
        top: Optional[int] = None,
    ) -> Iterator[dict]:
        """``GET /metadata/PublicEntities`` — yield every public entity (with column metadata).

        See :meth:`list_data_entities` for ``$select``/``$top`` caveats.
        """
        yield from self._paginate("PublicEntities", filter=filter, top=top)

    def get_public_entity(self, name: str) -> dict:
        """``GET /metadata/PublicEntities('Name')`` — single entity with column metadata."""
        if not name:
            raise ValueError("entity name is required")
        url = f"{self._metadata_url()}/PublicEntities('{_escape(name)}')"
        return self._client._http.request("GET", url, expected=(200,)).json()

    # ------------------------------------------------------------------ #
    # /metadata/PublicEnumerations                                       #
    # ------------------------------------------------------------------ #
    def list_public_enumerations(
        self,
        *,
        filter: Optional[str] = None,
        top: Optional[int] = None,
    ) -> Iterator[dict]:
        """``GET /metadata/PublicEnumerations`` — yield every public enum descriptor."""
        yield from self._paginate("PublicEnumerations", filter=filter, top=top)

    def get_public_enumeration(self, name: str) -> dict:
        """``GET /metadata/PublicEnumerations('Name')`` — single enum descriptor."""
        if not name:
            raise ValueError("enumeration name is required")
        url = f"{self._metadata_url()}/PublicEnumerations('{_escape(name)}')"
        return self._client._http.request("GET", url, expected=(200,)).json()

    # ------------------------------------------------------------------ #
    # internals                                                          #
    # ------------------------------------------------------------------ #
    def _metadata_url(self) -> str:
        return f"{self._client.environment_url}/metadata"

    def _paginate(
        self,
        collection: str,
        *,
        filter: Optional[str] = None,
        top: Optional[int] = None,
    ) -> Iterator[dict]:
        params: dict = {}
        if filter:
            params["$filter"] = filter
        if top is not None:
            if top <= 0:
                return
            # Server currently ignores $top on /metadata/* but sending it is
            # harmless and lets us upgrade transparently if/when it lands.
            params["$top"] = str(top)

        url: Optional[str] = f"{self._metadata_url()}/{collection}"
        request_params: Optional[dict] = params or None
        yielded = 0
        while url:
            resp = self._client._http.request(
                "GET", url, params=request_params, expected=(200,)
            )
            payload = resp.json()
            for row in payload.get("value", []):
                if top is not None and yielded >= top:
                    return
                yield row
                yielded += 1
            url = payload.get("@odata.nextLink")
            request_params = None


def _escape(name: str) -> str:
    if "'" in name:
        # OData v4 string literal escape: single quotes are doubled.
        return name.replace("'", "''")
    return name
