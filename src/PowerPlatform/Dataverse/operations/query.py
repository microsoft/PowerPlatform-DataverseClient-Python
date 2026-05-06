# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Query operations namespace for the Dataverse SDK."""

from __future__ import annotations

import warnings
import xml.etree.ElementTree as _ET
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from ..core.errors import MetadataError
from ..models.fetch_xml_query import FetchXmlQuery
from ..models.record import Record
from ..models.query_builder import QueryBuilder

if TYPE_CHECKING:
    from ..client import DataverseClient


__all__ = ["QueryOperations"]


class QueryOperations:
    """Namespace for query operations.

    Accessed via ``client.query``. Provides query and search operations
    against Dataverse tables.

    :param client: The parent :class:`~PowerPlatform.Dataverse.client.DataverseClient` instance.
    :type client: ~PowerPlatform.Dataverse.client.DataverseClient

    Example::

        client = DataverseClient(base_url, credential)

        # Fluent query builder (recommended)
        for record in (client.query.builder("account")
                       .select("name", "revenue")
                       .filter_eq("statecode", 0)
                       .order_by("revenue", descending=True)
                       .top(100)
                       .execute()):
            print(record["name"])

        # SQL query
        rows = client.query.sql("SELECT TOP 10 name FROM account ORDER BY name")
        for row in rows:
            print(row["name"])
    """

    def __init__(self, client: DataverseClient) -> None:
        self._client = client

    # ----------------------------------------------------------------- builder

    def builder(self, table: str) -> QueryBuilder:
        """Create a fluent query builder for the specified table.

        Returns a :class:`~PowerPlatform.Dataverse.models.query_builder.QueryBuilder`
        that can be chained with filter, select, and order methods, then
        executed directly via ``.execute()``.

        :param table: Table schema name (e.g. ``"account"``).
        :type table: :class:`str`
        :return: A QueryBuilder instance bound to this client.
        :rtype: ~PowerPlatform.Dataverse.models.query_builder.QueryBuilder

        Example:
            Build and execute a query fluently::

                for record in (client.query.builder("account")
                               .select("name", "revenue")
                               .filter_eq("statecode", 0)
                               .filter_gt("revenue", 1000000)
                               .order_by("revenue", descending=True)
                               .top(100)
                               .page_size(50)
                               .execute()):
                    print(record["name"])

            With composable expression tree::

                from PowerPlatform.Dataverse.models.filters import eq, gt

                for record in (client.query.builder("account")
                               .where((eq("statecode", 0) | eq("statecode", 1))
                                      & gt("revenue", 100000))
                               .execute()):
                    print(record["name"])
        """
        qb = QueryBuilder(table)
        qb._query_ops = self
        return qb

    # -------------------------------------------------------------------- sql

    def sql(self, sql: str) -> List[Record]:
        """Execute a read-only SQL query using the Dataverse Web API.

        The Dataverse SQL endpoint supports a broad subset of T-SQL::

            SELECT / SELECT DISTINCT / SELECT TOP N (0-5000)
            FROM table [alias]
            INNER JOIN / LEFT JOIN (multi-table, no depth limit)
            WHERE (=, !=, >, <, >=, <=, LIKE, IN, NOT IN, IS NULL,
                   IS NOT NULL, BETWEEN, AND, OR, nested parentheses)
            GROUP BY column
            ORDER BY column [ASC|DESC]
            OFFSET n ROWS FETCH NEXT m ROWS ONLY
            COUNT(*), SUM(), AVG(), MIN(), MAX()

        ``SELECT *`` is not supported -- specify column names explicitly.
        Use :meth:`sql_columns` to discover available column names for a table.

        Not supported: SELECT *, subqueries, CTE, HAVING, UNION,
        RIGHT/FULL/CROSS JOIN, CASE, COALESCE, window functions,
        string/date/math functions, INSERT/UPDATE/DELETE. For writes, use
        ``client.records`` methods.

        :param sql: Supported SQL SELECT statement.
        :type sql: :class:`str`

        :return: List of :class:`~PowerPlatform.Dataverse.models.record.Record`
            objects. Returns an empty list when no rows match.
        :rtype: list[~PowerPlatform.Dataverse.models.record.Record]

        :raises ~PowerPlatform.Dataverse.core.errors.ValidationError:
            If ``sql`` is not a string or is empty.

        Example:
            Basic query::

                rows = client.query.sql(
                    "SELECT TOP 10 name FROM account ORDER BY name"
                )

            JOIN with aggregation::

                rows = client.query.sql(
                    "SELECT a.name, COUNT(c.contactid) as cnt "
                    "FROM account a "
                    "JOIN contact c ON a.accountid = c.parentcustomerid "
                    "GROUP BY a.name"
                )

        """
        with self._client._scoped_odata() as od:
            rows = od._query_sql(sql)
            return [Record.from_api_response("", row) for row in rows]

    # --------------------------------------------------------------- fetch_xml

    def fetch_xml(self, xml: str) -> FetchXmlQuery:
        """Return an inert :class:`~PowerPlatform.Dataverse.models.fetch_xml_query.FetchXmlQuery` object.

        No HTTP request is made until
        :meth:`~PowerPlatform.Dataverse.models.fetch_xml_query.FetchXmlQuery.execute`
        or
        :meth:`~PowerPlatform.Dataverse.models.fetch_xml_query.FetchXmlQuery.execute_pages`
        is called on the returned object.

        Use for SQL-JOIN scenarios, aggregate queries, or other operations that
        the OData builder endpoint cannot express.

        :param xml: Well-formed FetchXML query string. The root ``<entity name="...">``
            element determines the entity set endpoint.
        :type xml: :class:`str`
        :return: Inert query object with ``.execute()`` and ``.execute_pages()`` methods.
        :rtype: :class:`~PowerPlatform.Dataverse.models.fetch_xml_query.FetchXmlQuery`
        :raises ValueError: If the FetchXML is missing a root ``<entity>`` element
            or the entity ``name`` attribute.

        Example::

            query = client.query.fetch_xml(\"\"\"
              <fetch top="50">
                <entity name="account">
                  <attribute name="name" />
                  <link-entity name="contact" from="parentcustomerid"
                               to="accountid" alias="c" link-type="inner">
                    <attribute name="fullname" />
                  </link-entity>
                </entity>
              </fetch>
            \"\"\")

            # Eager — collect all pages:
            result = query.execute()
            df = result.to_dataframe()

            # Lazy — process one page at a time:
            for page in query.execute_pages():
                process(page.to_dataframe())
        """
        root_el = _ET.fromstring(xml.strip())
        entity_el = root_el.find("entity")
        if entity_el is None:
            raise ValueError("FetchXML must contain an <entity> child element")
        entity_name = entity_el.get("name", "")
        if not entity_name:
            raise ValueError("FetchXML <entity> element must have a 'name' attribute")

        return FetchXmlQuery(xml.strip(), entity_name, self._client)

    # --------------------------------------------------------------- sql_columns

    def sql_columns(
        self,
        table: str,
        *,
        include_system: bool = False,
    ) -> List[Dict[str, Any]]:
        """Return a simplified list of SQL-usable columns for a table.

        Each dict contains ``name`` (logical name for SQL), ``type``
        (Dataverse attribute type), ``is_pk`` (primary key flag), and
        ``label`` (display name).  Virtual columns are always excluded
        because the SQL endpoint cannot query them.

        :param table: Schema name of the table (e.g. ``"account"``).
        :type table: :class:`str`
        :param include_system: When ``False`` (default), columns that end
            with common system suffixes (``_base``, ``versionnumber``,
            ``timezoneruleversionnumber``, ``utcconversiontimezonecode``,
            ``importsequencenumber``, ``overriddencreatedon``) are excluded.
        :type include_system: :class:`bool`

        :return: List of column metadata dicts.
        :rtype: list[dict[str, typing.Any]]

        Example::

            cols = client.query.sql_columns("account")
            for c in cols:
                print(f"{c['name']:30s} {c['type']:20s} PK={c['is_pk']}")
        """
        _SYSTEM_SUFFIXES = (
            "_base",
            "versionnumber",
            "timezoneruleversionnumber",
            "utcconversiontimezonecode",
            "importsequencenumber",
            "overriddencreatedon",
        )

        raw = self._client.tables.list_columns(
            table,
            select=[
                "LogicalName",
                "SchemaName",
                "AttributeType",
                "IsPrimaryId",
                "IsPrimaryName",
                "DisplayName",
                "AttributeOf",
            ],
            filter="AttributeType ne 'Virtual'",
        )
        result: List[Dict[str, Any]] = []
        for c in raw:
            name = c.get("LogicalName", "")
            if not name:
                continue
            if not include_system and any(name.endswith(s) for s in _SYSTEM_SUFFIXES):
                continue
            # Skip computed display-name columns (AttributeOf is set, meaning
            # they are auto-generated from a lookup column)
            if c.get("AttributeOf"):
                continue
            # Extract display label
            label = ""
            dn = c.get("DisplayName")
            if isinstance(dn, dict):
                ul = dn.get("UserLocalizedLabel")
                if isinstance(ul, dict):
                    label = ul.get("Label", "")
            result.append(
                {
                    "name": name,
                    "type": c.get("AttributeType", ""),
                    "is_pk": bool(c.get("IsPrimaryId")),
                    "is_name": bool(c.get("IsPrimaryName")),
                    "label": label,
                }
            )
        result.sort(key=lambda x: (not x["is_pk"], not x["is_name"], x["name"]))
        return result

    # ===========================================================
    # OData helpers -- eliminate friction for records.get() users
    # ===========================================================

    # -------------------------------------------------------- odata_select

    def odata_select(
        self,
        table: str,
        *,
        include_system: bool = False,
    ) -> List[str]:
        """Return a list of column logical names suitable for ``$select``.

        Can be passed directly to ``client.records.get(table, select=...)``.

        :param table: Schema name of the table (e.g. ``"account"``).
        :type table: :class:`str`
        :param include_system: Include system columns (default ``False``).
        :type include_system: :class:`bool`

        :return: List of lowercase column logical names.
        :rtype: list[str]

        Example::

            cols = client.query.odata_select("account")
            for page in client.records.get("account", select=cols, top=10):
                for r in page:
                    print(r)
        """
        warnings.warn(
            "'odata_select' is deprecated; use the typed builder (1.x) "
            "or client.query.sql_columns() to discover columns.",
            DeprecationWarning,
            stacklevel=2,
        )
        columns = self.sql_columns(table, include_system=include_system)
        return [c["name"] for c in columns]

    # ------------------------------------------------------- odata_expands

    def odata_expands(
        self,
        table: str,
    ) -> List[Dict[str, Any]]:
        """Discover all ``$expand`` navigation properties from a table.

        Returns entries for each outgoing lookup (single-valued navigation
        property).  Each entry contains the exact PascalCase navigation
        property name needed for ``$expand`` and ``@odata.bind``, plus
        the target entity set name.

        :param table: Schema name of the table (e.g. ``"contact"``).
        :type table: :class:`str`

        :return: List of dicts, each with:

            - ``nav_property`` -- PascalCase navigation property for $expand
            - ``target_table`` -- target entity logical name
            - ``target_entity_set`` -- target entity set (for @odata.bind)
            - ``lookup_attribute`` -- the lookup column logical name
            - ``relationship`` -- relationship schema name

        :rtype: list[dict[str, typing.Any]]

        Example::

            expands = client.query.odata_expands("contact")
            for e in expands:
                print(f"expand={e['nav_property']}  -> {e['target_table']}")

            # Use in a query
            e = next(e for e in expands if e['target_table'] == 'account')
            for page in client.records.get("contact",
                                           select=["fullname"],
                                           expand=[e['nav_property']]):
                ...
        """
        table_lower = table.lower()
        rels = self._client.tables.list_table_relationships(table)

        result: List[Dict[str, Any]] = []
        for r in rels:
            ref_entity = (r.get("ReferencingEntity") or "").lower()
            if ref_entity != table_lower:
                continue
            nav_prop = r.get("ReferencingEntityNavigationPropertyName", "")
            target = r.get("ReferencedEntity", "")
            lookup_attr = r.get("ReferencingAttribute", "")
            schema = r.get("SchemaName", "")
            if not nav_prop or not target:
                continue

            # Resolve entity set name for @odata.bind
            target_set = ""
            try:
                with self._client._scoped_odata() as od:
                    target_set = od._entity_set_from_schema_name(target)
            except (KeyError, AttributeError, ValueError, MetadataError):
                pass  # Entity set resolution failed; target_set stays empty

            result.append(
                {
                    "nav_property": nav_prop,
                    "target_table": target,
                    "target_entity_set": target_set,
                    "lookup_attribute": lookup_attr,
                    "relationship": schema,
                }
            )

        result.sort(key=lambda x: (x["target_table"], x["nav_property"]))
        return result

    # -------------------------------------------------------- odata_expand

    def odata_expand(
        self,
        from_table: str,
        to_table: str,
    ) -> str:
        """Return the navigation property name to ``$expand`` from one table to another.

        Discovers via relationship metadata. Returns the exact PascalCase
        string for the ``expand=`` parameter.

        :param from_table: Schema name of the source table (e.g. ``"contact"``).
        :type from_table: :class:`str`
        :param to_table: Schema name of the target table (e.g. ``"account"``).
        :type to_table: :class:`str`

        :return: The navigation property name (PascalCase).
        :rtype: :class:`str`

        :raises ValueError: If no navigation property found for the target.

        Example::

            nav = client.query.odata_expand("contact", "account")
            # Returns e.g. "parentcustomerid_account"
            for page in client.records.get("contact",
                                           select=["fullname"],
                                           expand=[nav],
                                           top=5):
                for r in page:
                    acct = r.get(nav) or {}
                    print(f"{r['fullname']} -> {acct.get('name', 'N/A')}")
        """
        warnings.warn(
            "'odata_expand' is deprecated; use the typed builder (1.x) "
            "with .expand() or client.query.odata_expands() to discover navigation properties.",
            DeprecationWarning,
            stacklevel=2,
        )
        to_lower = to_table.lower()
        expands = self.odata_expands(from_table)
        match = [e for e in expands if e["target_table"].lower() == to_lower]
        if not match:
            raise ValueError(
                f"No navigation property found from '{from_table}' to "
                f"'{to_table}'. Use client.query.odata_expands('{from_table}') "
                f"to see available targets."
            )
        return match[0]["nav_property"]

    # --------------------------------------------------------- odata_bind

    def odata_bind(
        self,
        from_table: str,
        to_table: str,
        target_id: str,
    ) -> Dict[str, str]:
        """Build an ``@odata.bind`` entry for setting a lookup field.

        Auto-discovers the navigation property name and entity set name
        from metadata.  Returns a single-entry dict that can be merged
        into a create or update payload.

        :param from_table: Schema name of the entity being created/updated.
        :type from_table: :class:`str`
        :param to_table: Schema name of the target entity the lookup points to.
        :type to_table: :class:`str`
        :param target_id: GUID of the target record.
        :type target_id: :class:`str`

        :return: A dict like ``{"NavProp@odata.bind": "/entityset(guid)"}``.
        :rtype: dict[str, str]

        :raises ValueError: If no relationship found between the tables.

        Example::

            # Instead of manually constructing:
            #   {"parentcustomerid_account@odata.bind": "/accounts(guid)"}
            # Just do:
            bind = client.query.odata_bind("contact", "account", acct_id)
            client.records.create("contact", {
                "firstname": "Jane",
                "lastname": "Doe",
                **bind,
            })
        """
        warnings.warn(
            "'odata_bind' is deprecated; use the typed builder (1.x) " "or pass the @odata.bind dict manually.",
            DeprecationWarning,
            stacklevel=2,
        )
        to_lower = to_table.lower()
        expands = self.odata_expands(from_table)
        match = [e for e in expands if e["target_table"].lower() == to_lower and e["target_entity_set"]]
        if not match:
            raise ValueError(
                f"No relationship found from '{from_table}' to '{to_table}'. "
                f"Use client.query.odata_expands('{from_table}') to see options."
            )

        e = match[0]
        key = f"{e['nav_property']}@odata.bind"
        value = f"/{e['target_entity_set']}({target_id})"
        return {key: value}
