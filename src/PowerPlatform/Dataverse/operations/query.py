# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Query operations namespace for the Dataverse SDK."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, TYPE_CHECKING

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
            INNER JOIN / LEFT JOIN (multi-table, up to 6+ tables)
            WHERE (=, !=, >, <, >=, <=, LIKE, IN, NOT IN, IS NULL,
                   IS NOT NULL, BETWEEN, AND, OR, nested parentheses)
            GROUP BY column
            ORDER BY column [ASC|DESC]
            OFFSET n ROWS FETCH NEXT m ROWS ONLY
            COUNT(*), SUM(), AVG(), MIN(), MAX()

        ``SELECT *`` is automatically expanded into explicit column names
        by the SDK (the server rejects ``*`` directly).

        Not supported: subqueries, CTE, HAVING, UNION, RIGHT/FULL/CROSS
        JOIN, CASE, COALESCE, window functions, string/date/math functions,
        INSERT/UPDATE/DELETE. For writes, use ``client.records`` methods.

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

            SELECT * (auto-expanded by SDK)::

                rows = client.query.sql(
                    "SELECT * FROM account"
                )
        """
        with self._client._scoped_odata() as od:
            rows = od._query_sql(sql)
            return [Record.from_api_response("", row) for row in rows]

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
            select=["LogicalName", "SchemaName", "AttributeType", "IsPrimaryId", "IsPrimaryName", "DisplayName"],
            filter="AttributeType ne 'Virtual'",
        )
        result: List[Dict[str, Any]] = []
        for c in raw:
            name = c.get("LogicalName", "")
            if not name:
                continue
            if not include_system and any(name.endswith(s) for s in _SYSTEM_SUFFIXES):
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

    # --------------------------------------------------------------- sql_select

    def sql_select(
        self,
        table: str,
        *,
        include_system: bool = False,
    ) -> str:
        """Return a comma-separated column list for use in SQL SELECT.

        Excludes virtual columns and optionally system columns. The result
        can be embedded directly in a SQL query string.

        :param table: Schema name of the table (e.g. ``"account"``).
        :type table: :class:`str`
        :param include_system: Include system columns (default ``False``).
        :type include_system: :class:`bool`

        :return: Comma-separated column names.
        :rtype: :class:`str`

        Example::

            cols = client.query.sql_select("account")
            sql = f"SELECT TOP 10 {cols} FROM account"
            df = client.dataframe.sql(sql)
        """
        columns = self.sql_columns(table, include_system=include_system)
        return ", ".join(c["name"] for c in columns)

    # --------------------------------------------------------------- sql_joins

    def sql_joins(
        self,
        table: str,
    ) -> List[Dict[str, Any]]:
        """Discover all possible SQL JOINs from a table.

        Returns one entry per outgoing lookup relationship, with the
        exact column names needed for SQL ``JOIN ... ON`` clauses.

        For **polymorphic** lookups (e.g. ``customerid`` targeting both
        ``account`` and ``contact``), multiple entries are returned with
        the same ``column`` but different ``target`` values.

        :param table: Schema name of the table (e.g. ``"contact"``).
        :type table: :class:`str`

        :return: List of JOIN metadata dicts, each containing:

            - ``column`` -- the lookup attribute on this table (use in ON clause)
            - ``target`` -- the referenced entity name
            - ``target_pk`` -- the referenced entity's primary key column
            - ``relationship`` -- the schema name of the relationship
            - ``join_clause`` -- a ready-to-use ``JOIN ... ON ...`` fragment

        :rtype: list[dict[str, typing.Any]]

        Example::

            joins = client.query.sql_joins("contact")
            for j in joins:
                print(f"{j['column']:30s} -> {j['target']}.{j['target_pk']}")
                print(f"  {j['join_clause']}")

            # Use in a query
            j = next(j for j in joins if j['target'] == 'account')
            sql = f"SELECT TOP 10 c.fullname, a.name FROM contact c {j['join_clause']}"
        """
        table_lower = table.lower()
        rels = self._client.tables.list_table_relationships(table)

        result: List[Dict[str, Any]] = []
        for r in rels:
            ref_entity = (r.get("ReferencingEntity") or "").lower()
            if ref_entity != table_lower:
                continue
            col = r.get("ReferencingAttribute", "")
            target = r.get("ReferencedEntity", "")
            target_pk = r.get("ReferencedAttribute", "")
            schema = r.get("SchemaName", "")
            if not all([col, target, target_pk]):
                continue

            # Generate a short alias for the target table
            alias = target[0] if target else "j"
            join_clause = f"JOIN {target} {alias} " f"ON {table_lower}.{col} = {alias}.{target_pk}"

            result.append(
                {
                    "column": col,
                    "target": target,
                    "target_pk": target_pk,
                    "relationship": schema,
                    "join_clause": join_clause,
                }
            )

        result.sort(key=lambda x: (x["target"], x["column"]))
        return result

    # --------------------------------------------------------------- sql_join

    def sql_join(
        self,
        from_table: str,
        to_table: str,
        *,
        from_alias: Optional[str] = None,
        to_alias: Optional[str] = None,
    ) -> str:
        """Generate a SQL JOIN clause between two tables.

        Discovers the relationship automatically via metadata. If multiple
        relationships exist (e.g. polymorphic lookups), picks the first
        match. Use :meth:`sql_joins` to see all options.

        :param from_table: Schema name of the FROM table (e.g. ``"contact"``).
        :type from_table: :class:`str`
        :param to_table: Schema name of the target table (e.g. ``"account"``).
        :type to_table: :class:`str`
        :param from_alias: Optional alias for the FROM table in the JOIN
            clause. If ``None``, uses the full table name.
        :type from_alias: :class:`str` or None
        :param to_alias: Optional alias for the target table. If ``None``,
            uses the first letter of the target table name.
        :type to_alias: :class:`str` or None

        :return: A ready-to-use ``JOIN ... ON ...`` clause.
        :rtype: :class:`str`

        :raises ValueError: If no relationship is found between the tables.

        Example::

            j = client.query.sql_join("contact", "account", from_alias="c", to_alias="a")
            # Returns: "JOIN account a ON c.parentcustomerid = a.accountid"
            sql = f"SELECT TOP 10 c.fullname, a.name FROM contact c {j}"
            df = client.dataframe.sql(sql)
        """
        to_lower = to_table.lower()
        joins = self.sql_joins(from_table)
        match = [j for j in joins if j["target"].lower() == to_lower]
        if not match:
            raise ValueError(
                f"No relationship found from '{from_table}' to '{to_table}'. "
                f"Use client.query.sql_joins('{from_table}') to see available targets."
            )

        j = match[0]
        src = from_alias or from_table.lower()
        tgt = to_alias or to_lower[0]
        return f"JOIN {to_lower} {tgt} " f"ON {src}.{j['column']} = {tgt}.{j['target_pk']}"

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
            except Exception:
                pass

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
