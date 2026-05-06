# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""FetchXmlQuery — inert query object returned by QueryOperations.fetchxml()."""

from __future__ import annotations

import warnings
import xml.etree.ElementTree as _ET
from typing import Iterator, List, TYPE_CHECKING
from urllib.parse import unquote as _url_unquote, quote as _url_quote

from ..core.errors import ValidationError
from .record import QueryResult, Record

if TYPE_CHECKING:
    from ..client import DataverseClient


__all__ = ["FetchXmlQuery"]

_PREFER_HEADER = (
    "odata.include-annotations=" '"Microsoft.Dynamics.CRM.fetchxmlpagingcookie,' 'Microsoft.Dynamics.CRM.morerecords"'
)

# Documented Dataverse GET request URL limit. See:
# learn.microsoft.com/power-apps/developer/data-platform/webapi/compose-http-requests-handle-errors#maximum-url-length
# FetchXML queries with many attributes or conditions are the most common way to reach it.
# $batch POST doubles this to 64 KB.
_MAX_URL_LENGTH = 32_768
# Guards against infinite paging loops caused by a bug in cookie propagation or an
# unexpected server response. At the default Dataverse page size of 5,000 rows this
# cap allows up to 50 million records before raising; it is not a practical record
# limit but a circuit-breaker against runaway iteration.
_MAX_PAGES = 10_000


class FetchXmlQuery:
    """Inert FetchXML query object. No HTTP request is made until
    :meth:`execute` or :meth:`execute_pages` is called.

    Obtained via ``client.query.fetchxml(xml)``.

    :param xml: Stripped, well-formed FetchXML string.
    :param entity_name: Entity schema name from the ``<entity>`` element.
    :param client: Parent :class:`~PowerPlatform.Dataverse.client.DataverseClient`.
    """

    def __init__(self, xml: str, entity_name: str, client: "DataverseClient") -> None:
        self._xml = xml
        self._entity_name = entity_name
        self._client = client

    def execute(self) -> QueryResult:
        """Execute the FetchXML query and return all results as a :class:`QueryResult`.

        Blocking — fetches all pages upfront and holds every record in memory before
        returning. Simple for small-to-medium result sets; use :meth:`execute_pages`
        when the result set may be large or you want to process records as they arrive.

        :return: All matching records across all pages.
        :rtype: :class:`~PowerPlatform.Dataverse.models.record.QueryResult`

        Example::

            rows = client.query.fetchxml(xml).execute()
            df = rows.to_dataframe()
        """
        all_records: List[Record] = []
        for page in self.execute_pages():
            all_records.extend(page.records)
        return QueryResult(all_records)

    def execute_pages(self) -> Iterator[QueryResult]:
        """Lazily yield one :class:`QueryResult` per HTTP page.

        Streaming — each iteration fires one HTTP request and yields one page.
        Prefer over :meth:`execute` when:

        - The result set may be large and you do not want all records in memory at once.
        - You want early exit: stop iterating once you find what you need and the
          remaining HTTP round-trips are skipped automatically.
        - You need per-page progress reporting or batched downstream writes.

        One-shot — do not iterate more than once.

        :return: Iterator of per-page :class:`QueryResult` objects.
        :rtype: Iterator[:class:`~PowerPlatform.Dataverse.models.record.QueryResult`]

        Example::

            for page in client.query.fetchxml(xml).execute_pages():
                process(page.to_dataframe())
        """
        current_xml = self._xml
        page_num = 1
        page_count = 0

        with self._client._scoped_odata() as od:
            entity_set = od._entity_set_from_schema_name(self._entity_name)
            base_url = f"{od.api}/{entity_set}"

            while True:
                page_count += 1
                if page_count > _MAX_PAGES:
                    raise ValidationError(
                        f"FetchXML paging exceeded {_MAX_PAGES} pages. "
                        "This may indicate a runaway query or a bug in paging cookie propagation."
                    )

                encoded_len = len(base_url) + len("?fetchXml=") + len(_url_quote(current_xml, safe=""))
                if encoded_len > _MAX_URL_LENGTH:
                    raise ValidationError(
                        f"FetchXML request URL exceeds {_MAX_URL_LENGTH} characters after encoding. "
                        "Simplify the query or reduce attributes/conditions."
                    )

                r = od._request(
                    "get",
                    base_url,
                    headers={"Prefer": _PREFER_HEADER},
                    params={"fetchXml": current_xml},
                )
                data = r.json() if hasattr(r, "json") else {}
                items = data.get("value") if isinstance(data, dict) else None
                page_records: List[Record] = []
                if isinstance(items, list):
                    for item in items:
                        if isinstance(item, dict):
                            page_records.append(Record.from_api_response(self._entity_name, item))

                yield QueryResult(page_records)

                more_raw = data.get("@Microsoft.Dynamics.CRM.morerecords", False) if isinstance(data, dict) else False
                more = more_raw is True or (isinstance(more_raw, str) and more_raw.lower() == "true")
                if not more:
                    break

                raw_cookie = (
                    data.get("@Microsoft.Dynamics.CRM.fetchxmlpagingcookie", "") if isinstance(data, dict) else ""
                )

                _cookie_parse_error = False
                if raw_cookie:
                    try:
                        cookie_el = _ET.fromstring(raw_cookie)
                        inner_encoded = cookie_el.get("pagingcookie", "")
                        if inner_encoded:
                            cookie = _url_unquote(_url_unquote(inner_encoded))
                            page_num = int(cookie_el.get("pagenumber", str(page_num + 1)))
                            fetch_el = _ET.fromstring(current_xml)
                            fetch_el.set("paging-cookie", cookie)
                            fetch_el.set("page", str(page_num))
                            current_xml = _ET.tostring(fetch_el, encoding="unicode")
                            continue
                    except (_ET.ParseError, ValueError) as exc:
                        warnings.warn(
                            f"FetchXML paging cookie could not be parsed ({exc}); "
                            "falling back to simple paging.",
                            UserWarning,
                            stacklevel=2,
                        )
                        _cookie_parse_error = True

                # Simple paging fallback: server returned morerecords=true but no paging
                # cookie. Dataverse omits the cookie when the query cannot use cookie-based
                # paging (e.g. FetchXML ordered by a link-entity column). We continue with
                # page-number-only paging rather than truncating, but warn because simple
                # paging has a 50,000-record server cap and performance degrades at high page
                # numbers. The caller may be able to avoid this by reordering on the root
                # entity instead.
                if not _cookie_parse_error:
                    warnings.warn(
                        "Dataverse did not return a paging cookie; falling back to simple paging "
                        "(page-number increment only). Simple paging is capped at 50,000 records "
                        "and degrades in performance at high page numbers. Consider reordering on "
                        "a root-entity column to enable cookie-based paging.",
                        UserWarning,
                        stacklevel=2,
                    )
                page_num += 1
                fetch_el = _ET.fromstring(current_xml)
                fetch_el.set("page", str(page_num))
                current_xml = _ET.tostring(fetch_el, encoding="unicode")
