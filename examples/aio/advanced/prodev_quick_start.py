# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
PowerPlatform Dataverse Client - Async Pro-Dev Quick Start

Async equivalent of examples/advanced/prodev_quick_start.py.

A developer-focused example that demonstrates the full async SDK lifecycle:
install, authenticate, create a system with 4 related tables, populate
data, query it, and clean up -- all in a single script.

What this example covers:
    1) SDK installation and authentication
    2) Create 4 custom tables concurrently with asyncio.gather()
    3) Create columns and relationships between tables
    4) Populate with sample data using async DataFrame CRUD
    5) Query and join data across tables
    6) Clean up (delete tables)

Prerequisites:
    pip install PowerPlatform-Dataverse-Client
    pip install azure-identity
"""

import asyncio
import sys
import uuid
import warnings
from pathlib import Path

# Suppress MSAL advisory about response_mode (third-party library, not actionable here)
warnings.filterwarnings("ignore", message="response_mode=.*form_post", category=UserWarning)

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _auth import AsyncInteractiveBrowserCredential

from PowerPlatform.Dataverse.aio.async_client import AsyncDataverseClient
from PowerPlatform.Dataverse.models.filters import col

# -- Table schema names --
SUFFIX = uuid.uuid4().hex[:6]
TABLE_CUSTOMER = f"new_DemoCustomer{SUFFIX}"
TABLE_PROJECT = f"new_DemoProject{SUFFIX}"
TABLE_TASK = f"new_DemoTask{SUFFIX}"
TABLE_TIMEENTRY = f"new_DemoTimeEntry{SUFFIX}"

# -- Output folder for exported data --
_SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = _SCRIPT_DIR / "prodev_output"


async def main():
    """Entry point."""
    print("=" * 60)
    print("  DATAVERSE PYTHON SDK -- ASYNC PRO-DEV QUICK START")
    print("=" * 60)
    print()

    base_url = input("Enter Dataverse org URL (e.g. https://yourorg.crm.dynamics.com): ").strip()
    if not base_url:
        print("[ERR] No URL entered; exiting.")
        sys.exit(1)
    base_url = base_url.rstrip("/")

    print("[INFO] Authenticating via browser (Azure Identity)...")
    credential = AsyncInteractiveBrowserCredential()
    try:
        async with AsyncDataverseClient(base_url, credential) as client:
            try:
                await run_demo(client)
            except Exception as e:
                print(f"\n[ERR] {e}")
                print("[INFO] Attempting cleanup...")
                await cleanup(client)
                raise
    finally:
        await credential.close()


async def run_demo(client):
    """Run the full async pro-dev demo pipeline."""
    OUTPUT_DIR.mkdir(exist_ok=True)
    print(f"[INFO] Output folder: {OUTPUT_DIR.resolve()}")

    # -- Step 1: Create 4 tables concurrently --
    primary_name_col, primary_id_col = await step1_create_tables(client)

    # -- Step 2: Create relationships --
    await step2_create_relationships(client)

    # -- Step 3: Populate with sample data --
    customer_ids, project_ids, task_ids = await step3_populate_data(client, primary_name_col)

    # -- Step 4: Query and analyze --
    await step4_query_and_analyze(client, customer_ids, primary_name_col, primary_id_col)

    # -- Step 5: Update and delete --
    await step5_update_and_delete(client, task_ids, primary_name_col, primary_id_col)

    # -- Step 6: Cleanup --
    do_cleanup = input("\n6. Delete demo tables and cleanup? (Y/n): ").strip() or "y"
    if do_cleanup.lower() in ("y", "yes"):
        await cleanup(client)
    else:
        print("[INFO] Tables kept for inspection.")

    print("\n" + "=" * 60)
    print("[OK] Async pro-dev quick start demo complete!")
    print("=" * 60)


# ================================================================
# Step 1: Create tables (concurrently with asyncio.gather)
# ================================================================


async def step1_create_tables(client):
    """Create 4 custom tables sequentially.

    Note: Dataverse holds a metadata customization lock for the duration of
    each table-creation request.  Concurrent creates (asyncio.gather) trigger
    a CustomizationLockException on the server, so tables must be created one
    at a time.
    """
    print("\n" + "-" * 60)
    print("STEP 1: Create 4 custom tables (sequentially)")
    print("-" * 60)

    customer_result = await client.tables.create(
        TABLE_CUSTOMER,
        {
            f"{TABLE_CUSTOMER}_Email": "string",
            f"{TABLE_CUSTOMER}_Industry": "string",
            f"{TABLE_CUSTOMER}_Revenue": "money",
        },
    )
    await client.tables.create(
        TABLE_PROJECT,
        {
            f"{TABLE_PROJECT}_Budget": "money",
            f"{TABLE_PROJECT}_Status": "string",
            f"{TABLE_PROJECT}_StartDate": "datetime",
        },
    )
    await client.tables.create(
        TABLE_TASK,
        {
            f"{TABLE_TASK}_Priority": "integer",
            f"{TABLE_TASK}_Status": "string",
            f"{TABLE_TASK}_EstimatedHours": "decimal",
        },
    )
    await client.tables.create(
        TABLE_TIMEENTRY,
        {
            f"{TABLE_TIMEENTRY}_Hours": "decimal",
            f"{TABLE_TIMEENTRY}_Date": "datetime",
            f"{TABLE_TIMEENTRY}_Description": "string",
        },
    )

    primary_name_col = customer_result.primary_name_attribute
    primary_id_col = customer_result.primary_id_attribute
    print(f"[OK] Created table: {TABLE_CUSTOMER} (name: {primary_name_col}, id: {primary_id_col})")
    print(f"[OK] Created table: {TABLE_PROJECT}")
    print(f"[OK] Created table: {TABLE_TASK}")
    print(f"[OK] Created table: {TABLE_TIMEENTRY}")
    print(f"[OK] All 4 tables created (suffix: {SUFFIX})")

    return primary_name_col, primary_id_col


# ================================================================
# Step 2: Create relationships
# ================================================================


async def step2_create_relationships(client):
    """Create relationships between the 4 tables using lookup fields."""
    print("\n" + "-" * 60)
    print("STEP 2: Create relationships (lookup fields)")
    print("-" * 60)

    # Relationships must be created sequentially -- Dataverse rejects
    # concurrent metadata writes to related tables.
    await client.tables.create_lookup_field(
        referencing_table=TABLE_PROJECT.lower(),
        lookup_field_name=f"{TABLE_PROJECT}_CustomerId",
        referenced_table=TABLE_CUSTOMER.lower(),
        display_name="Customer",
    )
    print(f"[OK] {TABLE_CUSTOMER} 1:N {TABLE_PROJECT}")

    await client.tables.create_lookup_field(
        referencing_table=TABLE_TASK.lower(),
        lookup_field_name=f"{TABLE_TASK}_ProjectId",
        referenced_table=TABLE_PROJECT.lower(),
        display_name="Project",
    )
    print(f"[OK] {TABLE_PROJECT} 1:N {TABLE_TASK}")

    await client.tables.create_lookup_field(
        referencing_table=TABLE_TIMEENTRY.lower(),
        lookup_field_name=f"{TABLE_TIMEENTRY}_TaskId",
        referenced_table=TABLE_TASK.lower(),
        display_name="Task",
    )
    print(f"[OK] {TABLE_TASK} 1:N {TABLE_TIMEENTRY}")
    print("[OK] 3 lookup relationships created (Customer -> Project -> Task -> TimeEntry)")


# ================================================================
# Step 3: Populate with sample data
# ================================================================


async def step3_populate_data(client, primary_name_col):
    """Create sample records using client.dataframe.create()."""
    print("\n" + "-" * 60)
    print("STEP 3: Populate with sample data (async DataFrame CRUD)")
    print("-" * 60)

    # -- Customers --
    name_col = primary_name_col
    customers_df = pd.DataFrame(
        [
            {
                name_col: "Contoso Ltd",
                f"{TABLE_CUSTOMER}_Email": "info@contoso.com",
                f"{TABLE_CUSTOMER}_Industry": "Technology",
                f"{TABLE_CUSTOMER}_Revenue": 5000000,
            },
            {
                name_col: "Fabrikam Inc",
                f"{TABLE_CUSTOMER}_Email": "contact@fabrikam.com",
                f"{TABLE_CUSTOMER}_Industry": "Manufacturing",
                f"{TABLE_CUSTOMER}_Revenue": 12000000,
            },
            {
                name_col: "Northwind Traders",
                f"{TABLE_CUSTOMER}_Email": "sales@northwind.com",
                f"{TABLE_CUSTOMER}_Industry": "Retail",
                f"{TABLE_CUSTOMER}_Revenue": 3000000,
            },
        ]
    )
    customer_ids = await client.dataframe.create(TABLE_CUSTOMER, customers_df)
    customers_df["id"] = customer_ids
    print(f"[OK] Created {len(customers_df)} customers")

    # -- Projects (linked to customers via lookup) --
    customer_lookup = f"{TABLE_PROJECT}_CustomerId@odata.bind"
    customer_info = await client.tables.get(TABLE_CUSTOMER)
    customer_set = customer_info.get("entity_set_name") if customer_info else TABLE_CUSTOMER.lower() + "s"
    projects_df = pd.DataFrame(
        [
            {
                name_col: "Cloud Migration",
                f"{TABLE_PROJECT}_Budget": 250000,
                f"{TABLE_PROJECT}_Status": "Active",
                f"{TABLE_PROJECT}_StartDate": pd.Timestamp("2026-01-15"),
                customer_lookup: f"/{customer_set}({customer_ids.iloc[0]})",
            },
            {
                name_col: "ERP Upgrade",
                f"{TABLE_PROJECT}_Budget": 500000,
                f"{TABLE_PROJECT}_Status": "Active",
                f"{TABLE_PROJECT}_StartDate": pd.Timestamp("2026-02-01"),
                customer_lookup: f"/{customer_set}({customer_ids.iloc[1]})",
            },
            {
                name_col: "POS Modernization",
                f"{TABLE_PROJECT}_Budget": 150000,
                f"{TABLE_PROJECT}_Status": "Planning",
                f"{TABLE_PROJECT}_StartDate": pd.Timestamp("2026-03-01"),
                customer_lookup: f"/{customer_set}({customer_ids.iloc[2]})",
            },
            {
                name_col: "Data Analytics Platform",
                f"{TABLE_PROJECT}_Budget": 180000,
                f"{TABLE_PROJECT}_Status": "Active",
                f"{TABLE_PROJECT}_StartDate": pd.Timestamp("2026-01-20"),
                customer_lookup: f"/{customer_set}({customer_ids.iloc[0]})",
            },
        ]
    )
    project_ids = await client.dataframe.create(TABLE_PROJECT, projects_df)
    projects_df["id"] = project_ids
    print(f"[OK] Created {len(projects_df)} projects across 3 customers")

    # -- Tasks (linked to projects) --
    task_names = [
        ("Infrastructure Setup", 1, "In Progress", 40),
        ("Data Assessment", 2, "Not Started", 20),
        ("Testing & QA", 1, "Not Started", 60),
        ("Requirements Gathering", 1, "Complete", 30),
        ("Development Sprint 1", 1, "In Progress", 80),
        ("User Training", 3, "Not Started", 16),
    ]
    project_assignment = [0, 0, 0, 1, 1, 2]

    project_info = await client.tables.get(TABLE_PROJECT)
    project_set = project_info.get("entity_set_name") if project_info else TABLE_PROJECT.lower() + "s"
    project_lookup = f"{TABLE_TASK}_ProjectId@odata.bind"

    tasks_data = [
        {
            name_col: task_name,
            f"{TABLE_TASK}_Priority": priority,
            f"{TABLE_TASK}_Status": status,
            f"{TABLE_TASK}_EstimatedHours": hours,
            project_lookup: f"/{project_set}({project_ids.iloc[project_assignment[i]]})",
        }
        for i, (task_name, priority, status, hours) in enumerate(task_names)
    ]

    tasks_df = pd.DataFrame(tasks_data)
    task_ids = await client.dataframe.create(TABLE_TASK, tasks_df)
    tasks_df["id"] = task_ids
    print(f"[OK] Created {len(tasks_df)} tasks across 4 projects")

    print(
        f"\n  Total records: {len(customers_df) + len(projects_df) + len(tasks_df)} "
        f"({len(customers_df)} customers, {len(projects_df)} projects, {len(tasks_df)} tasks)"
    )

    return customer_ids, project_ids, task_ids


# ================================================================
# Step 4: Query and analyze data
# ================================================================


async def step4_query_and_analyze(client, customer_ids, primary_name_col, primary_id_col):
    """Query data and demonstrate DataFrame analysis."""
    print("\n" + "-" * 60)
    print("STEP 4: Query and analyze data")
    print("-" * 60)

    name_attr = primary_name_col

    # Query projects and tasks concurrently
    project_result, task_result = await asyncio.gather(
        client.query.builder(TABLE_PROJECT)
        .select(name_attr, f"{TABLE_PROJECT}_Budget", f"{TABLE_PROJECT}_Status")
        .execute(),
        client.query.builder(TABLE_TASK)
        .select(name_attr, f"{TABLE_TASK}_Priority", f"{TABLE_TASK}_Status", f"{TABLE_TASK}_EstimatedHours")
        .execute(),
    )

    projects = project_result.to_dataframe()
    tasks = task_result.to_dataframe()

    print(f"\n  All projects ({len(projects)} rows):")
    print(f"{projects.to_string(index=False)}")

    print(f"\n  All tasks ({len(tasks)} rows):")
    print(f"{tasks.to_string(index=False)}")

    # -- DataFrame analysis --
    hours_col = f"{TABLE_TASK}_EstimatedHours"
    status_col = f"{TABLE_TASK}_Status"
    budget_col = f"{TABLE_PROJECT}_Budget"

    if hours_col in tasks.columns:
        print(f"\n  Task hours summary:")
        print(f"    Total estimated hours: {tasks[hours_col].sum():.0f}")
        print(f"    Average per task:      {tasks[hours_col].mean():.1f}")
        print(f"    Max single task:       {tasks[hours_col].max():.0f}")

    if status_col in tasks.columns:
        print(f"\n  Tasks by status:")
        for status, count in tasks[status_col].value_counts().items():
            print(f"    {status}: {count}")

    if budget_col in projects.columns:
        print(f"\n  Project budget summary:")
        print(f"    Total budget:   ${projects[budget_col].sum():,.0f}")
        print(f"    Average budget: ${projects[budget_col].mean():,.0f}")

    # Fetch single customer record by ID
    first_id = customer_ids.iloc[0]
    single_result = await client.query.builder(TABLE_CUSTOMER).where(col(primary_id_col) == first_id).execute()
    single = single_result.to_dataframe()
    print(f"\n  Single customer record (by ID):")
    print(f"{single.to_string(index=False)}")

    # -- Export query results to CSV --
    projects.to_csv(OUTPUT_DIR / "projects.csv", index=False)
    tasks.to_csv(OUTPUT_DIR / "tasks.csv", index=False)
    single.to_csv(OUTPUT_DIR / "single_customer.csv", index=False)
    print(f"\n[OK] Exported query results to {OUTPUT_DIR}/")


# ================================================================
# Step 5: Update and delete records
# ================================================================


async def step5_update_and_delete(client, task_ids, primary_name_col, primary_id_col):
    """Demonstrate update and delete with DataFrames."""
    print("\n" + "-" * 60)
    print("STEP 5: Update and delete records")
    print("-" * 60)

    status_col = f"{TABLE_TASK}_Status"

    # Update: mark first two tasks as "Complete"
    update_df = pd.DataFrame(
        {
            primary_id_col: [task_ids.iloc[0], task_ids.iloc[1]],
            status_col: ["Complete", "Complete"],
        }
    )
    await client.dataframe.update(TABLE_TASK, update_df, id_column=primary_id_col)
    print(f"[OK] Updated 2 tasks to 'Complete'")

    # Delete: remove the last task
    delete_ids = pd.Series([task_ids.iloc[-1]])
    await client.dataframe.delete(TABLE_TASK, delete_ids)
    print(f"[OK] Deleted 1 task")

    # Verify
    result = await client.query.builder(TABLE_TASK).select(primary_name_col, status_col).execute()
    remaining = result.to_dataframe()
    print(f"\n  Remaining tasks ({len(remaining)}):")
    print(f"{remaining.to_string(index=False)}")


# ================================================================
# Cleanup
# ================================================================


async def cleanup(client):
    """Delete all demo tables.

    Tables must be deleted leaf-to-root (TimeEntry → Task → Project → Customer)
    because each table holds a lookup field referencing the next.  Dataverse may
    also return transient SQL-deadlock errors during metadata operations, so we
    retry failed deletions until all tables are gone or no further progress is
    made.
    """
    print("\n" + "-" * 60)
    print("CLEANUP: Removing demo tables")
    print("-" * 60)

    remaining = [TABLE_TIMEENTRY, TABLE_TASK, TABLE_PROJECT, TABLE_CUSTOMER]
    while remaining:
        failed = []
        for table in remaining:
            try:
                await client.tables.delete(table)
                print(f"[OK] Deleted table: {table}")
            except Exception as e:
                failed.append((table, e))

        if len(failed) == len(remaining):
            # No progress — report and stop to avoid an infinite loop.
            for table, e in failed:
                print(f"[WARN] Could not delete {table}: {e}")
            break

        remaining = [t for t, _ in failed]

    print("[OK] Cleanup complete")


if __name__ == "__main__":
    asyncio.run(main())
