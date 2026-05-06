# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
End-to-end FetchXML examples for Dataverse.

Demonstrates ``client.query.fetch_xml()`` across the scenarios where FetchXML
is required or preferred over OData/SQL:

- Basic attribute queries
- <condition> operators (eq, like, in, null, not-null, between)
- <link-entity> (inner and outer joins)
- Ordering
- Top N with automatic paging-cookie propagation
- Aggregate queries (count, sum, avg, min, max, group-by)
- Built-in system tables (account → contact join)

FetchXML is the right tool when:
- You need a JOIN type OData $expand cannot express (many-to-many, outer link)
- You need server-side aggregates (count, sum, avg) without GROUP BY SQL
- You need ``<condition>`` operators unavailable in OData ($filter)

Prerequisites:
- pip install PowerPlatform-Dataverse-Client azure-identity
"""

import sys
import time

from azure.identity import InteractiveBrowserCredential
from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.core.errors import MetadataError
import requests

# ---------------------------------------------------------------------------
# Helpers (same pattern as sql_examples.py)
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
                print(f"   [INFO] Backoff succeeded after {attempts - 1} retry(s); waited {total_delay}s total.")
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


def main():
    print("=" * 80)
    print("Dataverse SDK -- FetchXML End-to-End Examples")
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
    project_table = "new_FXDemoProject"
    task_table = "new_FXDemoTask"

    # ===================================================================
    # 2. Create tables and seed data
    # ===================================================================
    heading(2, "Create Tables & Seed Data")

    log_call(f"client.tables.get('{project_table}')")
    if client.tables.get(project_table):
        print(f"[OK] Table already exists: {project_table}")
    else:
        log_call(f"client.tables.create('{project_table}', ...)")
        try:
            backoff(
                lambda: client.tables.create(
                    project_table,
                    {
                        "new_Code": "string",
                        "new_Budget": "decimal",
                        "new_Active": "bool",
                        "new_Region": "int",
                    },
                )
            )
            print(f"[OK] Created table: {project_table}")
        except Exception as e:
            if "already exists" in str(e).lower() or "not unique" in str(e).lower():
                print(f"[OK] Table already exists: {project_table} (skipped)")
            else:
                raise

    log_call(f"client.tables.get('{task_table}')")
    if client.tables.get(task_table):
        print(f"[OK] Table already exists: {task_table}")
    else:
        log_call(f"client.tables.create('{task_table}', ...)")
        try:
            backoff(
                lambda: client.tables.create(
                    task_table,
                    {
                        "new_Title": "string",
                        "new_Hours": "int",
                        "new_Done": "bool",
                        "new_Priority": "int",
                    },
                )
            )
            print(f"[OK] Created table: {task_table}")
        except Exception as e:
            if "already exists" in str(e).lower() or "not unique" in str(e).lower():
                print(f"[OK] Table already exists: {task_table} (skipped)")
            else:
                raise

    print("\n[INFO] Creating lookup field: tasks → projects ...")
    try:
        client.tables.create_lookup_field(
            referencing_table=task_table,
            lookup_field_name="new_ProjectId",
            referenced_table=project_table,
            display_name="Project",
        )
        print("[OK] Created lookup: new_ProjectId on tasks → projects")
    except Exception as e:
        msg = str(e).lower()
        if "already exists" in msg or "duplicate" in msg or "not unique" in msg:
            print("[OK] Lookup already exists (skipped)")
        else:
            raise

    # Resolve entity set name for @odata.bind
    project_set = f"{project_table.lower()}s"
    try:
        tinfo = client.tables.get(project_table)
        if tinfo:
            project_set = tinfo.get("entity_set_name", project_set)
    except Exception:
        pass

    log_call(f"client.records.create('{project_table}', [...])")
    projects = [
        {"new_Code": "ALPHA", "new_Budget": 50000, "new_Active": True, "new_Region": 1},
        {"new_Code": "BRAVO", "new_Budget": 75000, "new_Active": True, "new_Region": 2},
        {"new_Code": "CHARLIE", "new_Budget": 30000, "new_Active": False, "new_Region": 3},
        {"new_Code": "DELTA", "new_Budget": 90000, "new_Active": True, "new_Region": 1},
        {"new_Code": "ECHO", "new_Budget": 42000, "new_Active": True, "new_Region": 2},
    ]
    project_ids = backoff(lambda: client.records.create(project_table, projects))
    print(f"[OK] Seeded {len(project_ids)} projects")

    log_call(f"client.records.create('{task_table}', [...])")
    tasks = [
        {
            "new_Title": "Design mockups",
            "new_Hours": 8,
            "new_Done": True,
            "new_Priority": 2,
            "new_ProjectId@odata.bind": f"/{project_set}({project_ids[0]})",
        },
        {
            "new_Title": "Write unit tests",
            "new_Hours": 12,
            "new_Done": False,
            "new_Priority": 3,
            "new_ProjectId@odata.bind": f"/{project_set}({project_ids[0]})",
        },
        {
            "new_Title": "Code review",
            "new_Hours": 3,
            "new_Done": True,
            "new_Priority": 1,
            "new_ProjectId@odata.bind": f"/{project_set}({project_ids[1]})",
        },
        {
            "new_Title": "Deploy to staging",
            "new_Hours": 5,
            "new_Done": False,
            "new_Priority": 3,
            "new_ProjectId@odata.bind": f"/{project_set}({project_ids[1]})",
        },
        {
            "new_Title": "Update docs",
            "new_Hours": 4,
            "new_Done": True,
            "new_Priority": 1,
            "new_ProjectId@odata.bind": f"/{project_set}({project_ids[2]})",
        },
        {
            "new_Title": "Performance tuning",
            "new_Hours": 10,
            "new_Done": False,
            "new_Priority": 2,
            "new_ProjectId@odata.bind": f"/{project_set}({project_ids[3]})",
        },
        {
            "new_Title": "Security audit",
            "new_Hours": 6,
            "new_Done": False,
            "new_Priority": 3,
            "new_ProjectId@odata.bind": f"/{project_set}({project_ids[4]})",
        },
    ]
    task_ids = backoff(lambda: client.records.create(task_table, tasks))
    print(f"[OK] Seeded {len(task_ids)} tasks")

    project_logical = project_table.lower()  # new_fxdemoproject
    task_logical = task_table.lower()  # new_fxdemotask
    project_pk = f"{project_logical}id"  # new_fxdemoprojectid
    task_pk = f"{task_logical}id"  # new_fxdemoproject id
    lookup_attr = "new_projectid"  # lookup logical name on task

    try:
        # ===============================================================
        # 3. Basic attribute query
        # ===============================================================
        heading(3, "Basic Attribute Query")
        xml = f"""
        <fetch>
          <entity name="{project_logical}">
            <attribute name="new_code" />
            <attribute name="new_budget" />
            <attribute name="new_active" />
          </entity>
        </fetch>
        """
        log_call("client.query.fetch_xml(basic attribute query)")
        result = backoff(lambda: client.query.fetch_xml(xml).execute())
        print(f"[OK] {len(result)} projects:")
        for r in result:
            print(f"  {r.get('new_code', ''):<10s}  Budget={r.get('new_budget')}  Active={r.get('new_active')}")

        # ===============================================================
        # 4. <condition> operators: eq, like, in, null, not-null, between
        # ===============================================================
        heading(4, "<condition> Operators")

        # eq
        xml = f"""
        <fetch>
          <entity name="{project_logical}">
            <attribute name="new_code" />
            <filter>
              <condition attribute="new_code" operator="eq" value="ALPHA" />
            </filter>
          </entity>
        </fetch>
        """
        log_call('operator="eq" value="ALPHA"')
        r = backoff(lambda: client.query.fetch_xml(xml).execute())
        print(f"[OK] eq: {[x.get('new_code') for x in r]}")

        # like
        xml = f"""
        <fetch>
          <entity name="{task_logical}">
            <attribute name="new_title" />
            <filter>
              <condition attribute="new_title" operator="like" value="%test%" />
            </filter>
          </entity>
        </fetch>
        """
        log_call('operator="like" value="%test%"')
        r = backoff(lambda: client.query.fetch_xml(xml).execute())
        print(f"[OK] like: {len(r)} matches -> {[x.get('new_title') for x in r]}")

        # in
        xml = f"""
        <fetch>
          <entity name="{project_logical}">
            <attribute name="new_code" />
            <filter>
              <condition attribute="new_code" operator="in">
                <value>ALPHA</value>
                <value>DELTA</value>
              </condition>
            </filter>
          </entity>
        </fetch>
        """
        log_call('operator="in" values=[ALPHA, DELTA]')
        r = backoff(lambda: client.query.fetch_xml(xml).execute())
        print(f"[OK] in: {[x.get('new_code') for x in r]}")

        # null / not-null
        xml = f"""
        <fetch>
          <entity name="{task_logical}">
            <attribute name="new_title" />
            <filter>
              <condition attribute="new_priority" operator="not-null" />
            </filter>
          </entity>
        </fetch>
        """
        log_call('operator="not-null"')
        r = backoff(lambda: client.query.fetch_xml(xml).execute())
        print(f"[OK] not-null: {len(r)} tasks have priority set")

        # between
        xml = f"""
        <fetch>
          <entity name="{project_logical}">
            <attribute name="new_code" />
            <attribute name="new_budget" />
            <filter>
              <condition attribute="new_budget" operator="between">
                <value>40000</value>
                <value>80000</value>
              </condition>
            </filter>
          </entity>
        </fetch>
        """
        log_call('operator="between" 40000 and 80000')
        r = backoff(lambda: client.query.fetch_xml(xml).execute())
        print(f"[OK] between: {len(r)} projects -> {[(x.get('new_code'), x.get('new_budget')) for x in r]}")

        # ===============================================================
        # 5. <link-entity> — inner join (tasks → projects)
        # ===============================================================
        heading(5, "<link-entity> Inner Join (Tasks → Projects)")
        xml = f"""
        <fetch>
          <entity name="{task_logical}">
            <attribute name="new_title" />
            <attribute name="new_hours" />
            <link-entity name="{project_logical}"
                         from="{project_pk}"
                         to="{lookup_attr}"
                         alias="p"
                         link-type="inner">
              <attribute name="new_code" />
              <attribute name="new_budget" />
            </link-entity>
          </entity>
        </fetch>
        """
        log_call("client.query.fetch_xml(link-entity inner join)")
        try:
            result = backoff(lambda: client.query.fetch_xml(xml).execute())
            print(f"[OK] {len(result)} rows:")
            for r in result:
                print(
                    f"  Task={r.get('new_title', ''):<25s}  "
                    f"Hours={r.get('new_hours')}  "
                    f"Project={r.get('p.new_code', '')}  "
                    f"Budget={r.get('p.new_budget')}"
                )
        except Exception as e:
            print(f"[WARN] link-entity join failed: {e}")

        # ===============================================================
        # 6. <link-entity> — outer join (projects with or without tasks)
        # ===============================================================
        heading(6, "<link-entity> Outer Join (Projects With or Without Tasks)")
        xml = f"""
        <fetch>
          <entity name="{project_logical}">
            <attribute name="new_code" />
            <link-entity name="{task_logical}"
                         from="{lookup_attr}"
                         to="{project_pk}"
                         alias="t"
                         link-type="outer">
              <attribute name="new_title" />
            </link-entity>
          </entity>
        </fetch>
        """
        log_call("client.query.fetch_xml(link-entity outer join)")
        try:
            result = backoff(lambda: client.query.fetch_xml(xml).execute())
            print(f"[OK] {len(result)} rows (includes projects with no tasks):")
            for r in result[:8]:
                print(f"  Project={r.get('new_code', ''):<10s}  Task={r.get('t.new_title', '(none)')}")
        except Exception as e:
            print(f"[WARN] outer join failed: {e}")

        # ===============================================================
        # 7. Ordering
        # ===============================================================
        heading(7, "Ordering (<order> element)")

        xml = f"""
        <fetch>
          <entity name="{task_logical}">
            <attribute name="new_title" />
            <attribute name="new_hours" />
            <order attribute="new_hours" descending="true" />
          </entity>
        </fetch>
        """
        log_call("client.query.fetch_xml(order by hours DESC)")
        result = backoff(lambda: client.query.fetch_xml(xml).execute())
        print(f"[OK] Tasks by hours DESC:")
        for r in result:
            print(f"  {r.get('new_title', ''):<25s}  Hours={r.get('new_hours')}")

        # ===============================================================
        # 8. Top N + paging-cookie propagation
        # ===============================================================
        heading(8, "Paging-Cookie Propagation")
        print(
            "[INFO] 'count' sets the page size in FetchXML (not 'top' — 'top' is a total-result limit).\n"
            "With count='2' and 7 seeded tasks the server returns pages of 2, 2, 2, 1.\n"
            ".execute() collects all pages eagerly; .execute_pages() yields one QueryResult per HTTP page."
        )
        xml_paged = f"""
        <fetch count="2">
          <entity name="{task_logical}">
            <attribute name="new_title" />
            <attribute name="new_hours" />
            <order attribute="new_hours" />
          </entity>
        </fetch>
        """
        log_call("client.query.fetch_xml(xml).execute()  — eager, all pages collected")
        result = backoff(lambda: client.query.fetch_xml(xml_paged).execute())
        print(f"[OK] execute(): {len(result)} total tasks across all pages (seeded {len(task_ids)}):")
        for r in result:
            print(f"  {r.get('new_title', ''):<25s}  Hours={r.get('new_hours')}")

        log_call("client.query.fetch_xml(xml).execute_pages()  — lazy, one QueryResult per HTTP page")
        page_num = 0
        page_record_count = 0
        for page in backoff(lambda: client.query.fetch_xml(xml_paged).execute_pages()):
            page_num += 1
            page_record_count += len(page)
            print(f"  Page {page_num}: {len(page)} record(s) — {[r.get('new_title') for r in page]}")
        print(f"[OK] execute_pages(): {page_record_count} total tasks across {page_num} page(s)")

        # ===============================================================
        # 9. Aggregates (count, sum, avg, min, max)
        # ===============================================================
        heading(9, "Aggregate Queries (<fetch aggregate='true'>)")

        # Global aggregates
        xml = f"""
        <fetch aggregate="true">
          <entity name="{task_logical}">
            <attribute name="new_hours" aggregate="count" alias="task_count" />
            <attribute name="new_hours" aggregate="sum"   alias="total_hours" />
            <attribute name="new_hours" aggregate="avg"   alias="avg_hours" />
            <attribute name="new_hours" aggregate="min"   alias="min_hours" />
            <attribute name="new_hours" aggregate="max"   alias="max_hours" />
          </entity>
        </fetch>
        """
        log_call("client.query.fetch_xml(aggregate: count, sum, avg, min, max)")
        try:
            result = backoff(lambda: client.query.fetch_xml(xml).execute())
            if result:
                row = result[0]
                print(
                    f"[OK] count={row.get('task_count')}  sum={row.get('total_hours')}  "
                    f"avg={row.get('avg_hours')}  min={row.get('min_hours')}  max={row.get('max_hours')}"
                )
        except Exception as e:
            print(f"[WARN] aggregate failed: {e}")

        # Group-by aggregate: total hours per project
        xml = f"""
        <fetch aggregate="true">
          <entity name="{task_logical}">
            <attribute name="new_hours" aggregate="sum" alias="total_hours" />
            <attribute name="new_hours" aggregate="count" alias="task_count" />
            <link-entity name="{project_logical}"
                         from="{project_pk}"
                         to="{lookup_attr}"
                         alias="p"
                         link-type="inner">
              <attribute name="new_code" groupby="true" alias="project_code" />
            </link-entity>
          </entity>
        </fetch>
        """
        log_call("client.query.fetch_xml(aggregate group-by project)")
        try:
            result = backoff(lambda: client.query.fetch_xml(xml).execute())
            print(f"[OK] Hours per project ({len(result)} groups):")
            for r in result:
                print(
                    f"  {r.get('project_code', ''):<10s}  "
                    f"Tasks={r.get('task_count')}  "
                    f"Hours={r.get('total_hours')}"
                )
        except Exception as e:
            print(f"[WARN] group-by aggregate failed: {e}")

        # ===============================================================
        # 10. Built-in system tables (account → contact)
        # ===============================================================
        heading(10, "Built-In System Tables (account → contact Join)")
        xml = """
        <fetch top="5">
          <entity name="account">
            <attribute name="name" />
            <link-entity name="contact"
                         from="parentcustomerid"
                         to="accountid"
                         alias="c"
                         link-type="inner">
              <attribute name="fullname" />
            </link-entity>
          </entity>
        </fetch>
        """
        log_call("client.query.fetch_xml(account → contact inner join)")
        try:
            result = backoff(lambda: client.query.fetch_xml(xml).execute())
            print(f"[OK] {len(result)} account-contact pairs:")
            for r in result:
                print(f"  Account={r.get('name', ''):<25s}  Contact={r.get('c.fullname', '')}")
        except Exception as e:
            print(f"[INFO] No account-contact data in this org: {e}")

    finally:
        heading(11, "Cleanup")
        for tbl in [task_table, project_table]:
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
    print("FetchXML Examples Complete!")
    print("=" * 80)


if __name__ == "__main__":
    main()
