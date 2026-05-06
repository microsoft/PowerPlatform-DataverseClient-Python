# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""FetchXmlQuery — inert query object returned by QueryOperations.fetch_xml()."""

from __future__ import annotations

import xml.etree.ElementTree as _ET
from typing import Iterator, List, TYPE_CHECKING
from urllib.parse import unquote as _url_unquote

from .record import QueryResult, Record

if TYPE_CHECKING:
    from ..client import DataverseClient


__all__ = ["FetchXmlQuery"]

_PREFER_HEADER = (
    "odata.include-annotations=" '"Microsoft.Dynamics.CRM.fetchxmlpagingcookie,' 'Microsoft.Dynamics.CRM.morerecords"'
)


class FetchXmlQuery:
    """Inert FetchXML query object. No HTTP request is made until
    :meth:`execute` or :meth:`execute_pages` is called.

    Obtained via ``client.query.fetch_xml(xml)``.

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

            rows = client.query.fetch_xml(xml).execute()
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

            for page in client.query.fetch_xml(xml).execute_pages():
                process(page.to_dataframe())
        """
        current_xml = self._xml
        page_num = 1

        with self._client._scoped_odata() as od:
            entity_set = od._entity_set_from_schema_name(self._entity_name)
            base_url = f"{od.api}/{entity_set}"

            while True:
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

                more = bool(data.get("@Microsoft.Dynamics.CRM.morerecords", False)) if isinstance(data, dict) else False
                if not more:
                    break

                raw_cookie = (
                    data.get("@Microsoft.Dynamics.CRM.fetchxmlpagingcookie", "") if isinstance(data, dict) else ""
                )
                if not raw_cookie:
                    break

                cookie = _url_unquote(_url_unquote(raw_cookie))
                page_num += 1
                fetch_el = _ET.fromstring(current_xml)
                fetch_el.set("paging-cookie", cookie)
                fetch_el.set("page", str(page_num))
                current_xml = _ET.tostring(fetch_el, encoding="unicode")
