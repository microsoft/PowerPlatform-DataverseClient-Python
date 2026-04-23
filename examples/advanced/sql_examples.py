# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
End-to-end SQL query examples -- pure SQL workflows in Dataverse.

This example demonstrates everything a SQL developer can do through the
Python SDK's ``client.query.sql()`` and ``client.dataframe.sql()`` methods,
based on extensive testing of the Dataverse SQL endpoint (353 test queries).

Capabilities PROVEN to work:
- SELECT with specific columns
- INNER JOIN, LEFT JOIN (up to 6+ tables)
- COUNT(*), SUM(), AVG(), MIN(), MAX() aggregates
- GROUP BY, DISTINCT, DISTINCT TOP
- WHERE (=, !=, >, <, >=, <=, LIKE, IN, NOT IN, IS NULL, IS NOT NULL, BETWEEN)
- TOP N (0-5000), ORDER BY col [ASC|DESC]
- OFFSET ... FETCH NEXT (server-side pagination)
- Table and column aliases
- Polymorphic lookups (ownerid, customerid) via separate JOINs
- Audit trail (createdby, modifiedby) via systemuser JOINs
- SQL read -> DataFrame transform -> SDK write-back (full round-trip)
- AND/OR, NOT IN, NOT LIKE boolean logic
- Deep JOINs (5-8 tables) with no server depth limit
- SQL helper functions: sql_columns, sql_select, sql_joins, sql_join
- OData helper functions: odata_select, odata_expands, odata_expand, odata_bind
- SQL vs OData side-by-side comparison

Not supported (server rejects):
- INSERT/UPDATE/DELETE (read-only) -> use client.dataframe.create/update/delete
- Subqueries, CTE, HAVING, UNION
- RIGHT JOIN, FULL OUTER JOIN, CROSS JOIN
- CASE, COALESCE, CAST, string/date/math functions
- Window functions (ROW_NUMBER, RANK)

Prerequisites:
- pip install PowerPlatform-Dataverse-Client azure-identity
"""

import sys
import json
import time
from collections import defaultdict
from enum import IntEnum

import pandas as pd
from azure.identity import InteractiveBrowserCredential
from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.core.errors import MetadataError
import requests

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def log_call(description):
    print(f"\n-> {description}")


def heading(section_num, title):
    print(f"\n{'=' * 80}")
    print(f"{section_num}. {title}")
    print("=" * 80)


def backoff(op, *, delays=(0, 2, 5, 10, 20, 20)):
    """Retry an operation with exponential back-off."""
    last = None
    total_delay = 0
    attempts = 0
    for d in delays:
        if d:
            time.sleep(d)
            total_delay += d
        attempts += 1
        try:
            result = op()
            if attempts > 1:
                print(f"   [INFO] Backoff succeeded after {attempts - 1} " f"retry(s); waited {total_delay}s total.")
            return result
        except Exception as ex:
            last = ex
            continue
    if last:
        if attempts:
            print(
                f"   [WARN] Backoff exhausted after {max(attempts - 1, 0)} retry(s); waited {total_delay}s total."
                f"\n   [ERROR] {last}"
            )
        raise last


class Region(IntEnum):
    NORTH = 1
    SOUTH = 2
    EAST = 3
    WEST = 4


def main():
    print("=" * 80)
    print("Dataverse SDK -- SQL End-to-End (Pure SQL Workflows)")
    print("=" * 80)

    heading(1, "Setup & Authentication")
    base_url = input("Enter Dataverse org URL (e.g. https://yourorg.crm.dynamics.com): ").strip()
    if not base_url:
        print("No URL entered; exiting.")
        sys.exit(1)
    base_url = base_url.rstrip("/")

    log_call("InteractiveBrowserCredential()")
    credential = InteractiveBrowserCredential()

    log_call(f"DataverseClient(base_url='{base_url}', credential=...)")
    with DataverseClient(base_url=base_url, credential=credential) as client:
        print(f"[OK] Connected to: {base_url}")
        _run_examples(client)


def _run_examples(client):
    parent_table = "new_SQLDemoTeam"
    child_table = "new_SQLDemoTask"

    # ==================================================================
    # 2. Seed demo data (SDK writes -- SQL is read-only)
    # ==================================================================
    heading(2, "Seed Demo Data (SDK Writes -- SQL Is Read-Only)")
    print(
        "[INFO] SQL is read-only (no INSERT/UPDATE/DELETE). We use the SDK's\n"
        "records namespace to seed data, then query it all via SQL."
    )

    log_call(f"client.tables.get('{parent_table}')")
    info = backoff(lambda: client.tables.get(parent_table))
    if info:
        print(f"[OK] Table already exists: {parent_table}")
    else:
        log_call(f"client.tables.create('{parent_table}', ...)")
        info = backoff(
            lambda: client.tables.create(
                parent_table,
                {
                    "new_Code": "string",
                    "new_Region": Region,
                    "new_Budget": "decimal",
                    "new_Active": "bool",
                },
            )
        )
        print(f"[OK] Created table: {parent_table}")

    log_call(f"client.tables.get('{child_table}')")
    info2 = backoff(lambda: client.tables.get(child_table))
    if info2:
        print(f"[OK] Table already exists: {child_table}")
    else:
        log_call(f"client.tables.create('{child_table}', ...)")
        info2 = backoff(
            lambda: client.tables.create(
                child_table,
                {
                    "new_Title": "string",
                    "new_Hours": "int",
                    "new_Done": "bool",
                    "new_Priority": "int",
                },
            )
        )
        print(f"[OK] Created table: {child_table}")

    # Create lookup so tasks reference teams via JOIN
    print("\n[INFO] Creating lookup field so tasks reference teams via JOIN...")
    try:
        backoff(
            lambda: client.tables.create_lookup_field(
                referencing_table=child_table,
                lookup_field_name="new_TeamId",
                referenced_table=parent_table,
                display_name="Team",
            )
        )
        print("[OK] Created lookup: new_TeamId on tasks -> teams")
    except Exception as e:
        msg = str(e).lower()
        if "already exists" in msg or "duplicate" in msg:
            print("[OK] Lookup already exists (skipped)")
        else:
            raise

    log_call(f"client.records.create('{parent_table}', [...])")
    teams = [
        {"new_Code": "ALPHA", "new_Region": Region.NORTH, "new_Budget": 50000, "new_Active": True},
        {"new_Code": "BRAVO", "new_Region": Region.SOUTH, "new_Budget": 75000, "new_Active": True},
        {"new_Code": "CHARLIE", "new_Region": Region.EAST, "new_Budget": 30000, "new_Active": False},
        {"new_Code": "DELTA", "new_Region": Region.WEST, "new_Budget": 90000, "new_Active": True},
        {"new_Code": "ECHO", "new_Region": Region.NORTH, "new_Budget": 42000, "new_Active": True},
    ]
    team_ids = backoff(lambda: client.records.create(parent_table, teams))
    print(f"[OK] Seeded {len(team_ids)} teams")

    parent_logical = parent_table.lower()
    parent_set = f"{parent_logical}s"
    try:
        tinfo = client.tables.get(parent_table)
        if tinfo:
            parent_set = tinfo.get("entity_set_name", parent_set)
    except Exception:
        pass

    log_call(f"client.records.create('{child_table}', [...])")
    tasks = [
        {
            "new_Title": "Design mockups",
            "new_Hours": 8,
            "new_Done": True,
            "new_Priority": 2,
            f"new_TeamId@odata.bind": f"/{parent_set}({team_ids[0]})",
        },
        {
            "new_Title": "Write unit tests",
            "new_Hours": 12,
            "new_Done": False,
            "new_Priority": 3,
            f"new_TeamId@odata.bind": f"/{parent_set}({team_ids[0]})",
        },
        {
            "new_Title": "Code review",
            "new_Hours": 3,
            "new_Done": True,
            "new_Priority": 1,
            f"new_TeamId@odata.bind": f"/{parent_set}({team_ids[1]})",
        },
        {
            "new_Title": "Deploy to staging",
            "new_Hours": 5,
            "new_Done": False,
            "new_Priority": 3,
            f"new_TeamId@odata.bind": f"/{parent_set}({team_ids[1]})",
        },
        {
            "new_Title": "Update docs",
            "new_Hours": 4,
            "new_Done": True,
            "new_Priority": 1,
            f"new_TeamId@odata.bind": f"/{parent_set}({team_ids[2]})",
        },
        {
            "new_Title": "Performance tuning",
            "new_Hours": 10,
            "new_Done": False,
            "new_Priority": 2,
            f"new_TeamId@odata.bind": f"/{parent_set}({team_ids[3]})",
        },
        {
            "new_Title": "Security audit",
            "new_Hours": 6,
            "new_Done": False,
            "new_Priority": 3,
            f"new_TeamId@odata.bind": f"/{parent_set}({team_ids[4]})",
        },
    ]
    task_ids = backoff(lambda: client.records.create(child_table, tasks))
    print(f"[OK] Seeded {len(task_ids)} tasks (with team lookups)")

    parent_id_col = f"{parent_logical}id"

    try:
        # ==============================================================
        # 3. Schema discovery
        # ==============================================================
        heading(3, "Schema Discovery Before Writing SQL")
        log_call(f"client.tables.list_columns('{parent_table}', select=[...])")
        columns = backoff(
            lambda: client.tables.list_columns(
                parent_table,
                select=["LogicalName", "SchemaName", "AttributeType"],
            )
        )
        custom_cols = [c for c in columns if c.get("LogicalName", "").startswith("new_")]
        print(f"[OK] Custom columns on {parent_table}:")
        for col in custom_cols:
            print(f"  {col['LogicalName']:30s}  Type: {col.get('AttributeType', 'N/A')}")

        log_call(f"client.tables.list_table_relationships('{child_table}', ...)")
        rels = backoff(
            lambda: client.tables.list_table_relationships(
                child_table,
                select=["SchemaName"],
            )
        )
        print(f"[OK] Relationships on {child_table}: {len(rels)}")

        # ==============================================================
        # 4. Basic SELECT
        # ==============================================================
        heading(4, "Basic SQL -- SELECT Specific Columns")
        sql = f"SELECT new_code, new_budget, new_active FROM {parent_table}"
        log_call(f'client.query.sql("{sql}")')
        results = backoff(lambda: client.query.sql(sql))
        print(f"[OK] {len(results)} rows:")
        for r in results:
            print(f"  {r.get('new_code', ''):<12s}  Budget={r.get('new_budget')}  Active={r.get('new_active')}")

        # ==============================================================
        # 5. SELECT * -- Rejected by Design
        # ==============================================================
        heading(5, "SELECT * -- Rejected by Design")
        print(
            "SELECT * is deliberately rejected -- not a server workaround,\n"
            "but an intentional design decision. Wide entities (e.g. account\n"
            "has 307 columns) make SELECT * extremely expensive on shared\n"
            "infrastructure. Specify columns explicitly instead.\n"
            "Use client.query.sql_columns('account') to discover column names."
        )
        from PowerPlatform.Dataverse.core.errors import ValidationError as _VE

        try:
            client.query.sql(f"SELECT * FROM {parent_table}")
            print("[UNEXPECTED] SELECT * did not raise -- check SDK version")
        except _VE as exc:
            print(f"[OK] ValidationError raised as expected: {exc}")

        # ==============================================================
        # 6. WHERE clause
        # ==============================================================
        heading(6, "SQL -- WHERE (=, >, <, IN, IS NULL, BETWEEN)")
        sql = f"SELECT new_code, new_budget FROM {parent_table} WHERE new_budget > 40000"
        log_call(f'client.query.sql("{sql}")')
        results = backoff(lambda: client.query.sql(sql))
        print(f"[OK] budget > 40000: {len(results)} rows")

        sql = f"SELECT new_code FROM {parent_table} WHERE new_code IN ('ALPHA', 'DELTA')"
        results = backoff(lambda: client.query.sql(sql))
        print(f"[OK] IN clause: {[r.get('new_code') for r in results]}")

        sql = f"SELECT new_title FROM {child_table} WHERE new_priority IS NOT NULL"
        results = backoff(lambda: client.query.sql(sql))
        print(f"[OK] IS NOT NULL: {len(results)} tasks")

        # ==============================================================
        # 7. LIKE
        # ==============================================================
        heading(7, "SQL -- LIKE Pattern Matching")
        sql = f"SELECT new_title FROM {child_table} WHERE new_title LIKE '%test%'"
        log_call(f'client.query.sql("{sql}")')
        results = backoff(lambda: client.query.sql(sql))
        print(f"[OK] LIKE '%test%': {len(results)} matches")

        # ==============================================================
        # 8. TOP + ORDER BY
        # ==============================================================
        heading(8, "SQL -- TOP N + ORDER BY")
        sql = f"SELECT TOP 3 new_code, new_budget FROM {parent_table} ORDER BY new_budget DESC"
        log_call(f'client.query.sql("{sql}")')
        results = backoff(lambda: client.query.sql(sql))
        print(f"[OK] Top 3 by budget:")
        for r in results:
            print(f"  {r.get('new_code', ''):<12s}  Budget={r.get('new_budget')}")

        # ==============================================================
        # 9. Aliases
        # ==============================================================
        heading(9, "SQL -- Table and Column Aliases")
        sql = (
            f"SELECT t.new_code AS team_code, t.new_budget AS budget "
            f"FROM {parent_table} AS t WHERE t.new_active = 1"
        )
        log_call(f'client.query.sql("{sql}")')
        results = backoff(lambda: client.query.sql(sql))
        print(f"[OK] Aliased results: {len(results)} rows")

        # ==============================================================
        # 10. DISTINCT
        # ==============================================================
        heading(10, "SQL -- DISTINCT")
        sql = f"SELECT DISTINCT new_region FROM {parent_table}"
        log_call(f'client.query.sql("{sql}")')
        results = backoff(lambda: client.query.sql(sql))
        print(f"[OK] Distinct regions: {[r.get('new_region') for r in results]}")

        sql = f"SELECT DISTINCT TOP 2 new_region FROM {parent_table}"
        results = backoff(lambda: client.query.sql(sql))
        print(f"[OK] DISTINCT TOP 2: {[r.get('new_region') for r in results]}")

        # ==============================================================
        # 11. Aggregates: COUNT, SUM, AVG, MIN, MAX
        # ==============================================================
        heading(11, "SQL -- Aggregates (All Run on Server)")
        sql = (
            f"SELECT COUNT(*) as cnt, SUM(new_budget) as total, "
            f"AVG(new_budget) as avg_b, MIN(new_budget) as min_b, "
            f"MAX(new_budget) as max_b FROM {parent_table}"
        )
        log_call('client.query.sql("SELECT COUNT, SUM, AVG, MIN, MAX...")')
        results = backoff(lambda: client.query.sql(sql))
        if results:
            print(f"[OK] {json.dumps(dict(results[0]), indent=2)}")

        # ==============================================================
        # 12. GROUP BY
        # ==============================================================
        heading(12, "SQL -- GROUP BY (Server-Side)")
        sql = (
            f"SELECT new_region, COUNT(*) as team_count, "
            f"SUM(new_budget) as total_budget "
            f"FROM {parent_table} GROUP BY new_region"
        )
        log_call(f'client.query.sql("{sql}")')
        results = backoff(lambda: client.query.sql(sql))
        print(f"[OK] {len(results)} groups:")
        for r in results:
            print(f"  Region={r.get('new_region')}  Count={r.get('team_count')}  Total={r.get('total_budget')}")

        # ==============================================================
        # 13. INNER JOIN
        # ==============================================================
        heading(13, "SQL -- INNER JOIN")
        print("Use the lookup attribute's logical name (e.g. new_teamid) for JOINs.")

        # Use sql_join() to auto-discover the relationship and build
        # the JOIN clause with proper aliases.
        lookup_col = "new_teamid"  # Lookup logical name, NOT _..._value
        join_clause = client.query.sql_join(
            from_table=child_table,
            to_table=parent_table,
            from_alias="tk",
            to_alias="t",
        )
        print(f"[INFO] Lookup column: {lookup_col}")
        print(f"[INFO] Generated JOIN: {join_clause}")

        sql = f"SELECT t.new_code, tk.new_title, tk.new_hours " f"FROM {child_table} tk " f"{join_clause}"
        log_call('client.query.sql("...INNER JOIN...")')
        try:
            results = backoff(lambda: client.query.sql(sql))
            print(f"[OK] JOIN: {len(results)} rows")
            for r in results[:5]:
                print(
                    f"  Team={r.get('new_code', ''):<10s}  Task={r.get('new_title', ''):<25s}  Hours={r.get('new_hours')}"
                )
        except Exception as e:
            print(f"[WARN] JOIN failed: {e}")

        # ==============================================================
        # 14. LEFT JOIN
        # ==============================================================
        heading(14, "SQL -- LEFT JOIN")
        sql = (
            f"SELECT t.new_code, tk.new_title "
            f"FROM {parent_table} t "
            f"LEFT JOIN {child_table} tk ON t.{parent_id_col} = tk.{lookup_col}"
        )  # lookup_col = logical name, NOT _..._value
        log_call('client.query.sql("...LEFT JOIN...")')
        try:
            results = backoff(lambda: client.query.sql(sql))
            print(f"[OK] LEFT JOIN: {len(results)} rows")
        except Exception as e:
            print(f"[WARN] LEFT JOIN failed: {e}")

        # ==============================================================
        # 15. JOIN + GROUP BY + aggregates
        # ==============================================================
        heading(15, "SQL -- JOIN + GROUP BY + Aggregates")
        sql = (
            f"SELECT t.new_code, COUNT(tk.new_sqldemotaskid) as task_count, "
            f"SUM(tk.new_hours) as total_hours "
            f"FROM {parent_table} t "
            f"JOIN {child_table} tk ON t.{parent_id_col} = tk.{lookup_col} "  # logical name
            f"GROUP BY t.new_code"
        )
        log_call('client.query.sql("...JOIN...GROUP BY...")')
        try:
            results = backoff(lambda: client.query.sql(sql))
            print(f"[OK] {len(results)} groups:")
            for r in results:
                print(f"  Team={r.get('new_code', ''):<10s}  Tasks={r.get('task_count')}  Hours={r.get('total_hours')}")
        except Exception as e:
            print(f"[WARN] JOIN+GROUP BY failed: {e}")

        # ==============================================================
        # 16. OFFSET FETCH (server-side pagination)
        # ==============================================================
        heading(16, "SQL -- OFFSET FETCH (Server-Side Pagination)")
        page_size = 3
        for pg in range(1, 4):
            offset = (pg - 1) * page_size
            sql = (
                f"SELECT new_title, new_hours FROM {child_table} "
                f"ORDER BY new_hours "
                f"OFFSET {offset} ROWS FETCH NEXT {page_size} ROWS ONLY"
            )
            log_call(f"Page {pg}: OFFSET {offset} FETCH NEXT {page_size}")
            results = backoff(lambda sql=sql: client.query.sql(sql))
            print(f"  Page {pg}: {len(results)} rows")
            for r in results:
                print(f"    {r.get('new_title', ''):<25s}  Hours={r.get('new_hours')}")
            if len(results) < page_size:
                break

        # ==============================================================
        # 17. SQL to DataFrame
        # ==============================================================
        heading(17, "SQL to DataFrame (client.dataframe.sql)")
        print("Get SQL results directly as a pandas DataFrame.")
        sql = f"SELECT new_code, new_budget, new_region " f"FROM {parent_table} ORDER BY new_budget DESC"
        log_call(f'client.dataframe.sql("{sql}")')
        df = backoff(lambda: client.dataframe.sql(sql))
        print(f"[OK] DataFrame: {len(df)} rows x {len(df.columns)} columns")
        print(df.to_string(index=False))
        print(f"\n  Mean budget: {df['new_budget'].mean():,.2f}")
        print(f"  Budget by region:\n{df.groupby('new_region')['new_budget'].sum()}")

        # ==============================================================
        # 18. SQL to DataFrame with JOINs
        # ==============================================================
        heading(18, "SQL to DataFrame -- JOIN Query")
        sql = (
            f"SELECT t.new_code, tk.new_title, tk.new_hours "
            f"FROM {child_table} tk "
            f"JOIN {parent_table} t ON tk.{lookup_col} = t.{parent_id_col}"
        )
        log_call('client.dataframe.sql("...JOIN...")')
        try:
            df_j = backoff(lambda: client.dataframe.sql(sql))
            print(f"[OK] {len(df_j)} rows")
            print(df_j.to_string(index=False))
            print("\n-- Pivot: hours by team --")
            print(df_j.groupby("new_code")["new_hours"].agg(["sum", "mean", "count"]).to_string())
        except Exception as e:
            print(f"[WARN] {e}")

        # ==============================================================
        # 19. Built-in table JOINs
        # ==============================================================
        heading(19, "Built-In Table JOINs (account -> contact)")
        sql = "SELECT a.name, c.fullname FROM account a " "INNER JOIN contact c ON a.accountid = c.parentcustomerid"
        log_call('client.query.sql("...account JOIN contact...")')
        try:
            results = backoff(lambda: client.query.sql(sql))
            print(f"[OK] {len(results)} rows")
            for r in results[:5]:
                print(f"  Account={r.get('name', ''):<25s}  Contact={r.get('fullname', '')}")
        except Exception as e:
            print(f"[INFO] {e}")

        # ==============================================================
        # 20. LIMITATION: Writes require SDK
        # ==============================================================
        heading(20, "LIMITATION: Writes Require SDK (Read-Only SQL)")
        sql = f"SELECT new_sqldemotaskid, new_title " f"FROM {child_table} WHERE new_done = 0"
        incomplete = backoff(lambda: client.query.sql(sql))
        print(f"[OK] SQL found {len(incomplete)} incomplete tasks")
        if incomplete:
            fid = incomplete[0].get("new_sqldemotaskid")
            if fid:
                backoff(lambda: client.records.update(child_table, fid, {"new_Done": True}))
                print(f"[OK] Updated via SDK: '{incomplete[0].get('new_title')}'")

        # ==============================================================
        # 21. LIMITATION: No subqueries
        # ==============================================================
        heading(21, "LIMITATION: No Subqueries -- Chain SQL Calls")
        sql1 = f"SELECT {parent_id_col} FROM {parent_table} WHERE new_budget > 50000"
        big = backoff(lambda: client.query.sql(sql1))
        big_ids = [r.get(parent_id_col) for r in big if r.get(parent_id_col)]
        print(f"[OK] Step 1: {len(big_ids)} teams with budget > 50000")
        if big_ids:
            id_list = ", ".join(f"'{i}'" for i in big_ids)
            sql2 = f"SELECT new_title FROM {child_table} " f"WHERE {lookup_col} IN ({id_list})"
            tasks_r = backoff(lambda: client.query.sql(sql2))
            print(f"[OK] Step 2: {len(tasks_r)} tasks for big-budget teams")

        # ==============================================================
        # 22. LIMITATION: No functions
        # ==============================================================
        heading(22, "LIMITATION: No Functions -- Post-Process in Python")
        sql = f"SELECT new_code, new_budget FROM {parent_table}"
        rows = backoff(lambda: client.query.sql(sql))
        print("[OK] Post-processing (CASE equivalent):")
        for r in rows:
            b = float(r.get("new_budget") or 0)
            tier = "HIGH" if b > 60000 else "MEDIUM" if b > 35000 else "LOW"
            print(f"  {r.get('new_code', ''):<12s}  Budget={b:>10,.2f}  Tier={tier}")

        # ==============================================================
        # 23. Polymorphic lookups via SQL (ownerid, customerid)
        # ==============================================================
        heading(23, "Polymorphic Lookups via SQL (ownerid, customerid)")
        print(
            "Some Dataverse lookup columns are POLYMORPHIC -- the GUID can\n"
            "point to different entity types (e.g. ownerid -> systemuser OR\n"
            "team, customerid -> account OR contact).\n"
            "\n"
            "SQL pattern: INNER JOIN acts as both a join AND a type filter.\n"
            "If the GUID points to a different type, the JOIN simply returns\n"
            "no row -- so you get exactly the records of the type you joined."
        )

        # 23a. Discover lookup columns on a table
        print("\n-- 23a. Discover lookup columns on account --")
        log_call("client.tables.list_columns('account', filter=Lookup)")
        try:
            acct_cols = backoff(
                lambda: client.tables.list_columns(
                    "account",
                    select=["LogicalName", "AttributeType"],
                    filter="AttributeType eq 'Lookup' or AttributeType eq 'Owner' or AttributeType eq 'Customer'",
                )
            )
            lookup_names = sorted(c.get("LogicalName", "") for c in acct_cols if c.get("LogicalName", ""))
            print(f"[OK] Lookup columns on account ({len(lookup_names)} found):")
            for ln in lookup_names[:10]:
                print(f"  {ln}")
            if len(lookup_names) > 10:
                print(f"  ... and {len(lookup_names) - 10} more")
        except Exception as e:
            print(f"[INFO] Lookup discovery skipped: {e}")

        # 23b. Discover polymorphic targets via relationship metadata
        print("\n-- 23b. Discover which entities a polymorphic lookup targets --")
        log_call("client.tables.list_table_relationships('account', ...)")
        try:
            acct_rels = backoff(lambda: client.tables.list_table_relationships("account"))
            by_attr = defaultdict(list)
            for rel in acct_rels:
                attr = rel.get("ReferencingAttribute", "")
                ref = rel.get("ReferencedEntity", "")
                if attr and ref and rel.get("ReferencingEntity", "").lower() == "account":
                    by_attr[attr].append(ref)
            print("[OK] Lookup targets on account:")
            for attr, targets in sorted(by_attr.items()):
                tag = "POLYMORPHIC" if len(targets) > 1 else "regular"
                print(f"  {attr:<35s} -> {', '.join(targets):<30s}  [{tag}]")
        except Exception as e:
            print(f"[INFO] Relationship discovery skipped: {e}")

        # 23c. Resolve ownerid (polymorphic: systemuser or team)
        print("\n-- 23c. Resolve ownerid via SQL JOINs --")
        print("ownerid is polymorphic (systemuser or team). Use separate\n" "JOINs and combine in a DataFrame.")
        try:
            # Records owned by users
            log_call("SQL: account JOIN systemuser ON ownerid")
            df_user_owned = backoff(
                lambda: client.dataframe.sql(
                    "SELECT TOP 5 a.name, su.fullname as owner_name "
                    "FROM account a "
                    "INNER JOIN systemuser su ON a.ownerid = su.systemuserid"
                )
            )
            df_user_owned["owner_type"] = "User"

            # Records owned by teams
            log_call("SQL: account JOIN team ON ownerid")
            df_team_owned = backoff(
                lambda: client.dataframe.sql(
                    "SELECT TOP 5 a.name, t.name as owner_name "
                    "FROM account a "
                    "INNER JOIN team t ON a.ownerid = t.teamid"
                )
            )
            df_team_owned["owner_type"] = "Team"

            df_owners = pd.concat([df_user_owned, df_team_owned], ignore_index=True)
            print(f"[OK] Owner resolution: {len(df_owners)} rows")
            print(f"  User-owned:  {len(df_user_owned)}")
            print(f"  Team-owned:  {len(df_team_owned)}")
            if not df_owners.empty:
                print(df_owners.to_string(index=False))
        except Exception as e:
            print(f"[INFO] Owner resolution skipped (may have no data): {e}")

        # 23d. Track created-by and modified-by (common audit pattern)
        print("\n-- 23d. Audit trail: who created/modified records (via SQL) --")
        try:
            log_call("SQL: account JOIN systemuser (createdby + modifiedby)")
            results = backoff(
                lambda: client.query.sql(
                    "SELECT TOP 5 a.name, "
                    "creator.fullname as created_by, "
                    "modifier.fullname as modified_by "
                    "FROM account a "
                    "JOIN systemuser creator ON a.createdby = creator.systemuserid "
                    "JOIN systemuser modifier ON a.modifiedby = modifier.systemuserid"
                )
            )
            print(f"[OK] Audit trail: {len(results)} rows")
            for r in results[:5]:
                print(
                    f"  {r.get('name', ''):<25s}  "
                    f"Created: {r.get('created_by', ''):<20s}  "
                    f"Modified: {r.get('modified_by', '')}"
                )
        except Exception as e:
            print(f"[INFO] Audit trail skipped: {e}")

        # ==============================================================
        # 24. SQL Read -> DataFrame Transform -> SDK Write-Back
        # ==============================================================
        heading(24, "SQL Read -> DataFrame Transform -> SDK Write-Back")
        print(
            "The full bidirectional workflow for SQL users:\n"
            "  1. SQL query  ->  DataFrame (read)\n"
            "  2. pandas     ->  Transform (compute)\n"
            "  3. DataFrame  ->  SDK write-back (create/update/delete)\n"
            "\n"
            "This is how SQL developers do end-to-end work without\n"
            "learning OData or the Web API."
        )

        # Read current state via SQL
        sql = f"SELECT new_sqldemotaskid, new_title, new_hours, new_done " f"FROM {child_table}"
        log_call(f'client.dataframe.sql("{sql}")')
        df_tasks = backoff(lambda: client.dataframe.sql(sql))
        print(f"[OK] Read {len(df_tasks)} tasks via SQL")
        print(df_tasks.to_string(index=False))

        # Transform: bump hours by 1 for incomplete tasks
        mask = df_tasks["new_done"] == False  # noqa: E712
        original_hours = df_tasks.loc[mask, "new_hours"].copy()
        df_tasks.loc[mask, "new_hours"] = df_tasks.loc[mask, "new_hours"] + 1
        changed = mask.sum()
        print(f"\n[OK] Bumped hours +1 for {changed} incomplete tasks (in DataFrame)")

        # Write back via SDK
        if changed > 0:
            updates = df_tasks.loc[mask, ["new_sqldemotaskid", "new_hours"]]
            log_call(f"client.dataframe.update('{child_table}', ..., id_column='new_sqldemotaskid')")
            backoff(lambda: client.dataframe.update(child_table, updates, id_column="new_sqldemotaskid"))
            print(f"[OK] Wrote back {len(updates)} updated rows via DataFrame")

            # Verify with SQL
            verify = backoff(
                lambda: client.dataframe.sql(f"SELECT new_title, new_hours FROM {child_table} WHERE new_done = 0")
            )
            print(f"[OK] Verified via SQL -- incomplete tasks now:")
            print(verify.to_string(index=False))

            # Restore original values
            df_tasks.loc[mask, "new_hours"] = original_hours
            restore = df_tasks.loc[mask, ["new_sqldemotaskid", "new_hours"]]
            backoff(lambda: client.dataframe.update(child_table, restore, id_column="new_sqldemotaskid"))
            print("[OK] Restored original hours")

        # ==============================================================
        # 25. SQL-driven bulk create from query results
        # ==============================================================
        heading(25, "SQL-Driven Bulk Create (Query -> Transform -> Insert)")
        print(
            "Pattern: query existing data with SQL, transform it,\n"
            "then create new records via DataFrame -- all without\n"
            "learning OData syntax."
        )

        # Read teams via SQL
        sql = f"SELECT new_code, new_budget FROM {parent_table} WHERE new_active = 1"
        log_call(f'client.dataframe.sql("{sql}")')
        df_active = backoff(lambda: client.dataframe.sql(sql))
        print(f"[OK] Read {len(df_active)} active teams via SQL")

        # Transform: create a new task for each active team
        new_tasks = pd.DataFrame(
            {
                "new_Title": [f"Review budget for {code}" for code in df_active["new_code"]],
                "new_Hours": [2] * len(df_active),
                "new_Done": [False] * len(df_active),
                "new_Priority": [1] * len(df_active),
            }
        )
        log_call(f"client.dataframe.create('{child_table}', DataFrame({len(new_tasks)} rows))")
        new_ids = backoff(lambda: client.dataframe.create(child_table, new_tasks))
        print(f"[OK] Created {len(new_ids)} new tasks from SQL query results")

        # Verify with SQL
        verify_sql = f"SELECT new_title, new_hours FROM {child_table} " f"WHERE new_title LIKE 'Review budget%'"
        created_tasks = backoff(lambda: client.query.sql(verify_sql))
        print(f"[OK] Verified via SQL: {len(created_tasks)} 'Review budget' tasks")

        # Clean up the created tasks
        backoff(lambda: client.dataframe.delete(child_table, new_ids))
        print(f"[OK] Cleaned up {len(new_ids)} demo tasks")

        # ==============================================================
        # 26. SQL-driven bulk delete
        # ==============================================================
        heading(26, "SQL-Driven Bulk Delete (Query -> Filter -> Delete)")
        print("Pattern: find records with SQL, filter in pandas,\n" "then delete via DataFrame -- pure SQL thinking.")

        # Create some temp records to demonstrate
        temp = pd.DataFrame(
            {
                "new_Title": ["TEMP: delete me 1", "TEMP: delete me 2", "TEMP: keep me"],
                "new_Hours": [1, 2, 3],
                "new_Done": [False, False, False],
                "new_Priority": [1, 1, 1],
            }
        )
        temp_ids = backoff(lambda: client.dataframe.create(child_table, temp))
        print(f"[OK] Created {len(temp_ids)} temp records")

        # SQL to find, pandas to filter, SDK to delete
        sql = f"SELECT new_sqldemotaskid, new_title FROM {child_table} WHERE new_title LIKE 'TEMP:%'"
        df_temp = backoff(lambda: client.dataframe.sql(sql))
        print(f"[OK] SQL found {len(df_temp)} TEMP records")

        # Filter in pandas: only delete the "delete me" ones
        to_delete = df_temp[df_temp["new_title"].str.contains("delete me")]
        print(f"[OK] Pandas filtered to {len(to_delete)} records to delete")

        if not to_delete.empty:
            log_call("client.dataframe.delete(...)")
            backoff(lambda: client.dataframe.delete(child_table, to_delete["new_sqldemotaskid"]))
            print(f"[OK] Deleted {len(to_delete)} records via DataFrame")

        # Verify the "keep me" record survived
        remaining = backoff(
            lambda: client.query.sql(f"SELECT new_title FROM {child_table} WHERE new_title LIKE 'TEMP:%'")
        )
        print(f"[OK] Remaining TEMP records: {len(remaining)}")
        for r in remaining:
            print(f"  {r.get('new_title')}")

        # Clean up the surviving temp record
        keep_ids = [
            r.get("new_sqldemotaskid")
            for r in backoff(
                lambda: client.query.sql(f"SELECT new_sqldemotaskid FROM {child_table} WHERE new_title LIKE 'TEMP:%'")
            )
            if r.get("new_sqldemotaskid")
        ]
        for kid in keep_ids:
            backoff(lambda kid=kid: client.records.delete(child_table, kid))

        # ==============================================================
        # 27. AND/OR, NOT IN, NOT LIKE
        # ==============================================================
        heading(27, "SQL -- AND/OR, NOT IN, NOT LIKE")
        sql = f"SELECT new_code, new_budget FROM {parent_table} " f"WHERE new_active = 1 AND new_budget > 40000"
        log_call(f'client.query.sql("{sql}")')
        results = backoff(lambda: client.query.sql(sql))
        print(f"[OK] AND: {len(results)} rows")

        sql = f"SELECT new_code FROM {parent_table} " f"WHERE new_code = 'ALPHA' OR new_code = 'DELTA'"
        results = backoff(lambda: client.query.sql(sql))
        print(f"[OK] OR: {[r.get('new_code') for r in results]}")

        sql = (
            f"SELECT new_code FROM {parent_table} "
            f"WHERE new_active = 1 AND (new_budget > 80000 OR new_budget < 45000)"
        )
        results = backoff(lambda: client.query.sql(sql))
        print(f"[OK] AND + OR with parens: {len(results)} rows")

        sql = f"SELECT new_code FROM {parent_table} WHERE new_code NOT IN ('ALPHA')"
        results = backoff(lambda: client.query.sql(sql))
        print(f"[OK] NOT IN: {[r.get('new_code') for r in results]}")

        sql = f"SELECT new_title FROM {child_table} WHERE new_title NOT LIKE 'Design%'"
        results = backoff(lambda: client.query.sql(sql))
        print(f"[OK] NOT LIKE: {len(results)} rows")

        # ==============================================================
        # 28. Deep JOINs (5-8 tables)
        # ==============================================================
        heading(28, "Deep JOINs (5+ Tables) -- No Depth Limit")
        print(
            "SQL JOINs have no server-imposed depth limit (tested up to 15\n"
            "tables). Each JOIN uses indexed foreign key lookups, so\n"
            "performance stays consistent. Most real-world queries use\n"
            "2-4 tables; deeper JOINs are available when needed."
        )

        sql = (
            "SELECT TOP 3 a.name, c.fullname, o.name as opp, "
            "su.fullname as owner, bu.name as bu "
            "FROM account a "
            "JOIN contact c ON a.accountid = c.parentcustomerid "
            "JOIN opportunity o ON a.accountid = o.parentaccountid "
            "JOIN systemuser su ON a.ownerid = su.systemuserid "
            "JOIN businessunit bu ON su.businessunitid = bu.businessunitid"
        )
        log_call("5-table JOIN")
        try:
            results = backoff(lambda: client.query.sql(sql))
            print(f"[OK] 5-table JOIN: {len(results)} rows")
        except Exception as e:
            print(f"[INFO] {e}")

        sql = (
            "SELECT TOP 3 a.name, c.fullname, o.name as opp, "
            "su.fullname as owner, bu.name as bu, t.name as team, "
            "cr.fullname as creator, md.fullname as modifier "
            "FROM account a "
            "JOIN contact c ON a.accountid = c.parentcustomerid "
            "JOIN opportunity o ON a.accountid = o.parentaccountid "
            "JOIN systemuser su ON a.ownerid = su.systemuserid "
            "JOIN businessunit bu ON su.businessunitid = bu.businessunitid "
            "JOIN team t ON bu.businessunitid = t.businessunitid "
            "JOIN systemuser cr ON a.createdby = cr.systemuserid "
            "JOIN systemuser md ON a.modifiedby = md.systemuserid"
        )
        log_call("8-table JOIN")
        try:
            results = backoff(lambda: client.query.sql(sql))
            print(f"[OK] 8-table JOIN: {len(results)} rows")
        except Exception as e:
            print(f"[INFO] {e}")

        # ==============================================================
        # 29. SQL Helper Functions
        # ==============================================================
        heading(29, "SQL Helper Functions (query.sql_*)")
        print(
            "The SDK provides helper functions that auto-discover column\n"
            "names and JOIN clauses from metadata -- no guessing needed."
        )

        # sql_columns
        log_call(f"client.query.sql_columns('{parent_table}')")
        cols = client.query.sql_columns(parent_table)
        print(f"[OK] {len(cols)} columns:")
        for c in cols[:5]:
            print(f"  {c['name']:30s} Type: {c['type']:15s} PK={c['is_pk']}")

        # sql_select
        log_call(f"client.query.sql_select('{parent_table}')")
        select_str = client.query.sql_select(parent_table)
        print(f"[OK] SELECT list: {select_str[:60]}...")

        # sql_joins
        log_call(f"client.query.sql_joins('{child_table}')")
        joins = client.query.sql_joins(child_table)
        print(f"[OK] {len(joins)} possible JOINs:")
        for j in joins[:5]:
            print(f"  {j['column']:25s} -> {j['target']}.{j['target_pk']}")

        # sql_join (auto-generate JOIN clause)
        log_call(f"client.query.sql_join('{child_table}', '{parent_table}', ...)")
        try:
            join_clause = client.query.sql_join(child_table, parent_table, from_alias="tk", to_alias="t")
            print(f"[OK] {join_clause}")

            sql = f"SELECT TOP 3 tk.new_title, t.new_code FROM {child_table} tk {join_clause}"
            results = backoff(lambda: client.query.sql(sql))
            print(f"[OK] Live query with sql_join(): {len(results)} rows")
        except Exception as e:
            print(f"[WARN] {e}")

        # ==============================================================
        # 30. OData Helper Functions
        # ==============================================================
        heading(30, "OData Helper Functions (query.odata_*)")
        print(
            "Parallel helpers for OData/records.get() users -- auto-discover\n"
            "navigation properties and build @odata.bind payloads."
        )

        # odata_select
        log_call(f"client.query.odata_select('{parent_table}')")
        odata_cols = client.query.odata_select(parent_table)
        print(f"[OK] {len(odata_cols)} columns for $select: {odata_cols[:5]}...")

        # odata_expands
        log_call(f"client.query.odata_expands('{child_table}')")
        try:
            expands = client.query.odata_expands(child_table)
            print(f"[OK] {len(expands)} expand targets:")
            for e in expands[:5]:
                print(f"  nav={e['nav_property']:30s} -> {e['target_table']}")
        except Exception as e:
            print(f"[WARN] {e}")

        # odata_expand (single target)
        try:
            nav = client.query.odata_expand(child_table, parent_table)
            print(f"\n[OK] odata_expand('{child_table}', '{parent_table}') = '{nav}'")
            print("  Usage: client.records.get('" + child_table + "', expand=['" + nav + "'])")
        except Exception as e:
            print(f"[WARN] {e}")

        # odata_bind
        log_call("client.query.odata_bind(...)")
        try:
            bind = client.query.odata_bind(child_table, parent_table, team_ids[0])
            print(f"[OK] {bind}")
            print("  Merge into create/update payload: {{'new_Title': 'X', **bind}}")
        except Exception as e:
            print(f"[WARN] {e}")

        # ==============================================================
        # 31. SQL vs OData Comparison
        # ==============================================================
        heading(31, "SQL vs OData -- Side-by-Side Comparison")
        print("Both SQL and OData can query Dataverse. Here's how they compare.")

        print("""
+-------------------------------+------------------------+------------------------+
| Capability                    | SQL (client.query.sql) | OData (records.get)    |
+-------------------------------+------------------------+------------------------+
| Read data                     | YES                    | YES                    |
| Write data                    | NO (read-only)         | YES (create/update/del)|
| JOIN depth                    | No limit (tested 15)   | $expand 10-level max   |
| JOIN types                    | INNER, LEFT            | $expand (single-valued)|
| Aggregates (COUNT, SUM, etc.) | YES (server-side)      | Limited ($apply)       |
| GROUP BY                      | YES (server-side)      | Via $apply (complex)   |
| DISTINCT                      | YES                    | Not directly           |
| Pagination                    | OFFSET FETCH           | @odata.nextLink        |
| Max results                   | 5000 per query         | 5000 per page          |
| Column discovery              | sql_columns/sql_select | odata_select           |
| JOIN discovery                | sql_joins/sql_join     | odata_expands/expand   |
| Lookup binding                | N/A (read-only)        | odata_bind             |
| SELECT *                      | YES (SDK auto-expands) | Not applicable         |
| Polymorphic lookups           | Separate JOINs         | $expand by nav prop    |
| Return format                 | list[Record] / DF      | pages of Record / DF   |
| Subqueries                    | NO (chain SQL calls)   | NO ($filter only)      |
| Functions (CASE, CAST, etc.)  | NO                     | NO                     |
+-------------------------------+------------------------+------------------------+

When to use SQL:
  - Complex JOINs across 3+ tables
  - Aggregates and GROUP BY
  - DISTINCT queries
  - Familiar SQL syntax preferred
  - Read-only analysis / reporting

When to use OData (records.get):
  - Need to write data (create/update/delete)
  - Simple single-table or 1-level expand queries
  - Need automatic paging (nextLink)
  - Prefer typed QueryBuilder API
""")

        # Live comparison: same query via SQL and OData
        print("-- Live comparison: account + contact --")
        import time as _time

        # SQL version
        t0 = _time.time()
        try:
            sql_rows = backoff(
                lambda: client.query.sql(
                    "SELECT TOP 5 a.name, c.fullname "
                    "FROM account a "
                    "JOIN contact c ON a.accountid = c.parentcustomerid"
                )
            )
            sql_time = _time.time() - t0
            print(f"  SQL JOIN: {len(sql_rows)} rows in {sql_time:.2f}s")
        except Exception as e:
            sql_time = _time.time() - t0
            print(f"  SQL JOIN: error ({sql_time:.2f}s): {e}")

        # OData version (expand)
        t0 = _time.time()
        try:
            odata_rows = []
            for page in backoff(
                lambda: client.records.get(
                    "account",
                    select=["name"],
                    expand=["contact_customer_accounts"],
                    top=5,
                )
            ):
                odata_rows.extend(page)
            odata_time = _time.time() - t0
            print(f"  OData $expand: {len(odata_rows)} rows in {odata_time:.2f}s")
        except Exception as e:
            odata_time = _time.time() - t0
            print(f"  OData $expand: error ({odata_time:.2f}s): {e}")

        # ==============================================================
        # 32. Anti-Patterns & Best Practices
        # ==============================================================
        heading(32, "IMPORTANT: Anti-Patterns & Best Practices")
        print("""
=== ANTI-PATTERNS (avoid these -- they hurt shared database performance) ===

1. CARTESIAN PRODUCTS (FROM table1, table2 without ON)
   BAD:  SELECT a.name, c.fullname FROM account a, contact c
   WHY:  Produces rows_a * rows_b intermediate rows. With 5000-row tables,
         that's 25 MILLION rows the server must process before capping at 5000.
   FIX:  Always use explicit JOIN with ON clause.

2. LEADING-WILDCARD LIKE (LIKE '%value')
   BAD:  SELECT name FROM account WHERE name LIKE '%corp'
   WHY:  Forces a FULL TABLE SCAN -- cannot use indexes. On tables with
         millions of rows, this monopolizes shared database resources and
         slows down OTHER users' queries on the same database.
   FIX:  Use trailing wildcards: LIKE 'corp%' (uses indexes efficiently).
         If you must search mid-string, add TOP to limit scan scope.

3. NO FILTER ON LARGE SYSTEM TABLES
   BAD:  SELECT name FROM role
   WHY:  System tables (role, asyncoperation, sdkmessageprocessingstep)
         can have 5000+ rows. Unfiltered queries return max rows.
   FIX:  Always add WHERE filters and TOP when querying system tables.

4. SELECT * (BLOCKED -- ValidationError)
   BAD:  SELECT * FROM account
   WHY:  SELECT * is intentionally rejected -- not a technical limitation.
         Wide entities (account has 307 columns) make wildcard selects
         extremely expensive on shared database infrastructure.
   FIX:  List only the columns you need: SELECT name, revenue FROM account
         Or discover columns first:
           cols = client.query.sql_columns("account")
         For JOINs, always qualify columns from each table:
           SELECT a.name, c.fullname FROM account a JOIN contact c ON ...

5. DEEP JOINS WITHOUT TOP
   OK:   SELECT TOP 100 a.name, ... FROM account a JOIN ... (15 tables)
   BAD:  SELECT a.name, ... FROM account a JOIN ... (15 tables, no TOP)
   WHY:  Deep JOINs are safe with proper FK relationships and TOP.
         Without TOP, the server processes up to 5000 rows across all joins.
   FIX:  Always include TOP N for multi-table JOINs.

SDK guardrails:
  - Patterns #1 (writes), unsupported syntax (CROSS/RIGHT/FULL JOIN,
    UNION, HAVING, CTE, subqueries), and #4 (SELECT *)
    -> ValidationError (blocked).
  - Pattern #2 (cartesian FROM a, b) -> UserWarning (advisory).
  - Server enforces 5000-row cap on all queries (#3, #5).
  - Use sql_columns() or sql_select() to discover valid column names.
  - Use sql_joins() or sql_join() to discover valid JOIN clauses.
""")

        # ==============================================================
        # 33. Summary
        # ==============================================================
        heading(33, "Summary -- SQL Capabilities Reference")
        print("""
+-------------------------------+----------+----------------------------------------+
| Feature                       | SQL      | Notes / SDK Fallback                   |
+-------------------------------+----------+----------------------------------------+
| SELECT col1, col2             | YES      | Use LogicalName (lowercase)            |
| SELECT *                      | NO       | Specify columns explicitly             |
| WHERE =, !=, >, <, LIKE, IN   | YES      |                                        |
| AND, OR, parentheses          | YES      | Full boolean logic                     |
| NOT IN, NOT LIKE              | YES      |                                        |
| IS NULL, IS NOT NULL, BETWEEN | YES      |                                        |
| TOP N (0-5000)                | YES      | Max 5000 per query                     |
| ORDER BY col [ASC|DESC]       | YES      | Multiple columns supported             |
| OFFSET n FETCH NEXT m         | YES      | Server-side pagination                 |
| Table/Column aliases          | YES      |                                        |
| DISTINCT / DISTINCT TOP       | YES      | Works with JOINs too                   |
| COUNT, SUM, AVG, MIN, MAX     | YES      | All 5 standard aggregates              |
| GROUP BY                      | YES      | Server-side grouping                   |
| INNER JOIN                    | YES      | 15+ tables tested (no depth limit)     |
| LEFT JOIN                     | YES      |                                        |
| Self JOIN                     | YES      | Same table with different aliases      |
| SQL -> DataFrame              | YES      | client.dataframe.sql(query)            |
| Polymorphic lookups           | YES      | Separate JOINs per target type         |
| Nested polymorphic chains     | YES      | e.g. opp -> acct -> contact -> owner   |
| Audit trail (createdby, etc.) | YES      | JOIN to systemuser                     |
| SQL read -> DF write-back     | YES      | dataframe.sql() + .update()/.create()  |
| SQL column discovery          | YES      | query.sql_columns() / sql_select()     |
| SQL JOIN discovery            | YES      | query.sql_joins() / sql_join()         |
| OData column discovery        | YES      | query.odata_select()                   |
| OData expand discovery        | YES      | query.odata_expands() / odata_expand() |
| OData bind builder            | YES      | query.odata_bind()                     |
+-------------------------------+----------+----------------------------------------+
| HAVING                        | NO       | Filter before GROUP BY                 |
| Subqueries / CTE              | NO       | Chain multiple SQL calls               |
| RIGHT/FULL OUTER/CROSS JOIN   | NO       | Rewrite as LEFT/INNER JOIN             |
| UNION / UNION ALL             | NO       | Separate queries + pd.concat()         |
| CASE, COALESCE, CAST          | NO       | Post-process in Python/pandas          |
| String/Date/Math functions    | NO       | Post-process in Python/pandas          |
| Window fns (ROW_NUMBER, RANK) | NO       | Post-process in Python/pandas          |
| INSERT / UPDATE / DELETE      | NO       | dataframe.create/update/delete()       |
+-------------------------------+----------+----------------------------------------+

SQL-First Workflow (no OData knowledge needed):
  1. Discover schema:  cols = client.query.sql_columns("account")
  2. Discover JOINs:   joins = client.query.sql_joins("contact")
  3. Build JOIN:        j = client.query.sql_join("contact", "account", from_alias="c", to_alias="a")
  4. Query with SQL:    df = client.dataframe.sql(f"SELECT c.fullname, a.name FROM contact c {j}")
  5. Transform:         df["col"] = df["col"] * 1.1
  6. Write back:        client.dataframe.update("account", df, id_column="accountid")
  7. Verify:            df2 = client.dataframe.sql("SELECT ...")
""")

    finally:
        heading(34, "Cleanup")
        for tbl in [child_table, parent_table]:
            log_call(f"client.tables.delete('{tbl}')")
            try:
                backoff(lambda tbl=tbl: client.tables.delete(tbl))
                print(f"[OK] Deleted table: {tbl}")
            except Exception as ex:
                code = getattr(getattr(ex, "response", None), "status_code", None)
                if isinstance(ex, (requests.exceptions.HTTPError, MetadataError)) and code == 404:
                    print(f"[OK] Table already removed: {tbl}")
                else:
                    print(f"[WARN] Could not delete {tbl}: {ex}")

    print("\n" + "=" * 80)
    print("SQL Examples Complete!")
    print("=" * 80)


if __name__ == "__main__":
    main()
