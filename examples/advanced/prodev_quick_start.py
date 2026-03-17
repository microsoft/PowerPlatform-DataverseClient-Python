# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
PowerPlatform Dataverse Client - Pro-Dev Quick Start

A developer-focused example that demonstrates the full SDK lifecycle:
install, authenticate, create a system with 4 related tables, populate
data, query it, and clean up -- all in a single script.

What this example covers:
    1) SDK installation and authentication
    2) Create 4 custom tables (Customer, Project, Task, TimeEntry)
    3) Create columns and relationships between tables
    4) Populate with sample data using DataFrame CRUD
    5) Query and join data across tables
    6) Clean up (delete tables)

    Note: The last step (cleanup) automatically deletes all demo tables.
    Comment out the cleanup() call in run_demo() if you want to keep the
    tables in your environment for inspection.

Why pandas DataFrames?
    This example uses client.dataframe (pandas) instead of raw dict/list CRUD
    because DataFrames provide significant advantages for multi-record operations:

    - Batch operations are natural: create 100 records from a DataFrame in one
      call vs. looping over 100 dicts
    - Column operations (broadcast a value, compute derived fields) are one-liners
      instead of for-loops
    - Joins and aggregations across tables use pandas merge/groupby -- far more
      readable than manual dict matching
    - NaN/None handling is built in (clear_nulls flag controls whether missing
      values clear server fields or are skipped)
    - NumPy type normalization is automatic (int64, float64, Timestamps all
      serialize to JSON correctly without manual conversion)

    The SDK also supports plain dict/list CRUD via client.records for single-record
    operations or when pandas is not needed. Both approaches use the same underlying
    Dataverse Web API calls.

Prerequisites:
    pip install PowerPlatform-Dataverse-Client
    pip install azure-identity
"""

import sys
import uuid
import warnings
from pathlib import Path

# Suppress MSAL advisory about response_mode (third-party library, not actionable here)
warnings.filterwarnings("ignore", message="response_mode=.*form_post", category=UserWarning)

import pandas as pd
from azure.identity import InteractiveBrowserCredential

from PowerPlatform.Dataverse.client import DataverseClient

# -- Table schema names --
# Uses the standard 'new_' publisher prefix (default Dataverse publisher).
# A unique suffix avoids collisions with existing tables.
SUFFIX = uuid.uuid4().hex[:6]
TABLE_CUSTOMER = f"new_DemoCustomer{SUFFIX}"
TABLE_PROJECT = f"new_DemoProject{SUFFIX}"
TABLE_TASK = f"new_DemoTask{SUFFIX}"
TABLE_TIMEENTRY = f"new_DemoTimeEntry{SUFFIX}"

# -- Output folder for exported data (relative to this script) --
_SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = _SCRIPT_DIR / "prodev_output"


def main():
    """Entry point."""
    print("=" * 60)
    print("  DATAVERSE PYTHON SDK -- PRO-DEV QUICK START")
    print("=" * 60)
    print()
    print("  Step 0: pip install PowerPlatform-Dataverse-Client")
    print("  (already done if you're running this script)")
    print()

    base_url = input("Enter Dataverse org URL (e.g. https://yourorg.crm.dynamics.com): ").strip()
    if not base_url:
        print("[ERR] No URL entered; exiting.")
        sys.exit(1)
    base_url = base_url.rstrip("/")

    print("[INFO] Authenticating via browser (Azure Identity)...")
    credential = InteractiveBrowserCredential()

    with DataverseClient(base_url, credential) as client:
        try:
            run_demo(client)
        except Exception as e:
            print(f"\n[ERR] {e}")
            print("[INFO] Attempting cleanup...")
            cleanup(client)
            raise


def run_demo(client):
    """Run the full pro-dev demo pipeline."""
    OUTPUT_DIR.mkdir(exist_ok=True)
    print(f"[INFO] Output folder: {OUTPUT_DIR.resolve()}")

    # -- Step 1: Create 4 tables --
    primary_name_col, primary_id_col = step1_create_tables(client)

    # -- Step 2: Create relationships --
    step2_create_relationships(client)

    # -- Step 3: Populate with sample data --
    customer_ids, project_ids, task_ids = step3_populate_data(client, primary_name_col)

    # -- Step 4: Query and analyze --
    step4_query_and_analyze(client, customer_ids, primary_name_col)

    # -- Step 5: Update and delete --
    step5_update_and_delete(client, task_ids, primary_name_col, primary_id_col)

    # -- Step 6: Cleanup --
    cleanup(client)

    print("\n" + "=" * 60)
    print("[OK] Pro-dev quick start demo complete!")
    print("=" * 60)


# ================================================================
# Step 1: Create tables
# ================================================================


def step1_create_tables(client):
    """Create 4 custom tables."""
    print("\n" + "-" * 60)
    print("STEP 1: Create 4 custom tables")
    print("-" * 60)

    # Customer table
    result = client.tables.create(
        TABLE_CUSTOMER,
        {
            f"{TABLE_CUSTOMER}_Email": "string",
            f"{TABLE_CUSTOMER}_Industry": "string",
            f"{TABLE_CUSTOMER}_Revenue": "money",
        },
    )
    # The primary column logical names are returned by tables.create()
    # so we know exactly what keys to use in payloads and queries.
    primary_name_col = result.primary_name_attribute
    primary_id_col = result.primary_id_attribute
    print(f"[OK] Created table: {TABLE_CUSTOMER} (name: {primary_name_col}, id: {primary_id_col})")

    # Project table
    client.tables.create(
        TABLE_PROJECT,
        {
            f"{TABLE_PROJECT}_Budget": "money",
            f"{TABLE_PROJECT}_Status": "string",
            f"{TABLE_PROJECT}_StartDate": "datetime",
        },
    )
    print(f"[OK] Created table: {TABLE_PROJECT}")

    # Task table
    client.tables.create(
        TABLE_TASK,
        {
            f"{TABLE_TASK}_Priority": "integer",
            f"{TABLE_TASK}_Status": "string",
            f"{TABLE_TASK}_EstimatedHours": "decimal",
        },
    )
    print(f"[OK] Created table: {TABLE_TASK}")

    # TimeEntry table
    client.tables.create(
        TABLE_TIMEENTRY,
        {
            f"{TABLE_TIMEENTRY}_Hours": "decimal",
            f"{TABLE_TIMEENTRY}_Date": "datetime",
            f"{TABLE_TIMEENTRY}_Description": "string",
        },
    )
    print(f"[OK] Created table: {TABLE_TIMEENTRY}")
    print(f"[OK] All 4 tables created (suffix: {SUFFIX})")
    print(f"[INFO] Primary name column: '{primary_name_col}', ID column: '{primary_id_col}'")

    return primary_name_col, primary_id_col


# ================================================================
# Step 2: Create relationships
# ================================================================


def step2_create_relationships(client):
    """Create relationships between the 4 tables using lookup fields."""
    print("\n" + "-" * 60)
    print("STEP 2: Create relationships (lookup fields)")
    print("-" * 60)

    # Customer 1:N Project (lookup on Project pointing to Customer)
    client.tables.create_lookup_field(
        referencing_table=TABLE_PROJECT.lower(),
        lookup_field_name=f"{TABLE_PROJECT}_CustomerId",
        referenced_table=TABLE_CUSTOMER.lower(),
        display_name="Customer",
    )
    print(f"[OK] {TABLE_CUSTOMER} 1:N {TABLE_PROJECT}")

    # Project 1:N Task (lookup on Task pointing to Project)
    client.tables.create_lookup_field(
        referencing_table=TABLE_TASK.lower(),
        lookup_field_name=f"{TABLE_TASK}_ProjectId",
        referenced_table=TABLE_PROJECT.lower(),
        display_name="Project",
    )
    print(f"[OK] {TABLE_PROJECT} 1:N {TABLE_TASK}")

    # Task 1:N TimeEntry (lookup on TimeEntry pointing to Task)
    client.tables.create_lookup_field(
        referencing_table=TABLE_TIMEENTRY.lower(),
        lookup_field_name=f"{TABLE_TIMEENTRY}_TaskId",
        referenced_table=TABLE_TASK.lower(),
        display_name="Task",
    )
    print(f"[OK] {TABLE_TASK} 1:N {TABLE_TIMEENTRY}")

    print("[OK] 3 lookup relationships created (Customer -> Project -> Task -> TimeEntry)")


# ================================================================
# Step 3: Populate with sample data using DataFrame CRUD
# ================================================================


def step3_populate_data(client, primary_name_col):
    """Create sample records using client.dataframe.create().

    Why DataFrames here instead of client.records.create()?

    With client.records (dict/list):
        ids = client.records.create("Customer", [
            {"name": "Contoso", "Email": "info@contoso.com", ...},
            {"name": "Fabrikam", "Email": "contact@fabrikam.com", ...},
        ])
        # ids is a plain list -- manual index tracking needed

    With client.dataframe (pandas):
        df = pd.DataFrame([{"name": "Contoso", ...}, {"name": "Fabrikam", ...}])
        df["id"] = client.dataframe.create("Customer", df)
        # IDs auto-aligned to rows -- use df["id"].iloc[0] to reference later

    The DataFrame approach is more natural when you need to:
    - Reference created IDs for relationship binding (as we do here)
    - Compute derived columns before writing
    - Join/merge data across multiple tables for analysis
    """
    print("\n" + "-" * 60)
    print("STEP 3: Populate with sample data (DataFrame CRUD)")
    print("-" * 60)

    # -- Customers --
    # Use the primary name column returned by tables.create()
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
    customer_ids = client.dataframe.create(TABLE_CUSTOMER, customers_df)
    customers_df["id"] = customer_ids
    print(f"[OK] Created {len(customers_df)} customers")

    # -- Projects (linked to customers via lookup) --
    # @odata.bind keys use the navigation property logical name (lowercase)
    # and the entity set name (also lowercase) in the value.
    customer_lookup = f"{TABLE_PROJECT}_CustomerId".lower() + "@odata.bind"
    customer_set = TABLE_CUSTOMER.lower() + "s"
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
    project_ids = client.dataframe.create(TABLE_PROJECT, projects_df)
    projects_df["id"] = project_ids
    print(f"[OK] Created {len(projects_df)} projects across 3 customers")

    # -- Tasks (linked to projects) --
    tasks_data = []
    task_names = [
        ("Infrastructure Setup", 1, "In Progress", 40),
        ("Data Assessment", 2, "Not Started", 20),
        ("Testing & QA", 1, "Not Started", 60),
        ("Requirements Gathering", 1, "Complete", 30),
        ("Development Sprint 1", 1, "In Progress", 80),
        ("User Training", 3, "Not Started", 16),
    ]
    project_assignment = [0, 0, 0, 1, 1, 2]  # which project each task belongs to

    for i, (task_name, priority, status, hours) in enumerate(task_names):
        proj_idx = project_assignment[i]
        project_lookup = f"{TABLE_TASK}_ProjectId".lower() + "@odata.bind"
        project_set = TABLE_PROJECT.lower() + "s"
        tasks_data.append(
            {
                name_col: task_name,
                f"{TABLE_TASK}_Priority": priority,
                f"{TABLE_TASK}_Status": status,
                f"{TABLE_TASK}_EstimatedHours": hours,
                project_lookup: f"/{project_set}({project_ids.iloc[proj_idx]})",
            }
        )

    tasks_df = pd.DataFrame(tasks_data)
    task_ids = client.dataframe.create(TABLE_TASK, tasks_df)
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


def step4_query_and_analyze(client, customer_ids, primary_name_col):
    """Query data and demonstrate DataFrame analysis."""
    print("\n" + "-" * 60)
    print("STEP 4: Query and analyze data")
    print("-" * 60)

    # Query all projects as a DataFrame
    # Note: The SDK lowercases $select values automatically, so schema-name
    # casing (e.g., new_DemoProject_Budget) works -- it becomes the logical name.
    name_attr = primary_name_col
    projects = client.dataframe.get(
        TABLE_PROJECT,
        select=[
            name_attr,
            f"{TABLE_PROJECT}_Budget",
            f"{TABLE_PROJECT}_Status",
        ],
    )
    print(f"\n  All projects ({len(projects)} rows):")
    print(f"{projects.to_string(index=False)}")

    # Query tasks and analyze
    tasks = client.dataframe.get(
        TABLE_TASK,
        select=[
            name_attr,
            f"{TABLE_TASK}_Priority",
            f"{TABLE_TASK}_Status",
            f"{TABLE_TASK}_EstimatedHours",
        ],
    )
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
        status_counts = tasks[status_col].value_counts()
        for status, count in status_counts.items():
            print(f"    {status}: {count}")

    if budget_col in projects.columns:
        print(f"\n  Project budget summary:")
        print(f"    Total budget:   ${projects[budget_col].sum():,.0f}")
        print(f"    Average budget: ${projects[budget_col].mean():,.0f}")

    # Fetch single record by ID
    first_id = customer_ids.iloc[0]
    single = client.dataframe.get(TABLE_CUSTOMER, record_id=first_id)
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


def step5_update_and_delete(client, task_ids, primary_name_col, primary_id_col):
    """Demonstrate update and delete with DataFrames."""
    print("\n" + "-" * 60)
    print("STEP 5: Update and delete records")
    print("-" * 60)

    status_col = f"{TABLE_TASK}_Status"

    # Update: mark first two tasks as "Complete"
    # Use primary_id_col (from tables.create metadata) as the ID column name
    update_df = pd.DataFrame(
        {
            primary_id_col: [task_ids.iloc[0], task_ids.iloc[1]],
            status_col: ["Complete", "Complete"],
        }
    )
    client.dataframe.update(TABLE_TASK, update_df, id_column=primary_id_col)
    print(f"[OK] Updated 2 tasks to 'Complete'")

    # Delete: remove the last task
    delete_ids = pd.Series([task_ids.iloc[-1]])
    client.dataframe.delete(TABLE_TASK, delete_ids)
    print(f"[OK] Deleted 1 task")

    # Verify
    remaining = client.dataframe.get(
        TABLE_TASK,
        select=[primary_name_col, status_col],
    )
    print(f"\n  Remaining tasks ({len(remaining)}):")
    print(f"{remaining.to_string(index=False)}")


# ================================================================
# Cleanup
# ================================================================


def cleanup(client):
    """Delete all demo tables."""
    print("\n" + "-" * 60)
    print("CLEANUP: Removing demo tables")
    print("-" * 60)

    for table in [TABLE_TIMEENTRY, TABLE_TASK, TABLE_PROJECT, TABLE_CUSTOMER]:
        try:
            client.tables.delete(table)
            print(f"[OK] Deleted table: {table}")
        except Exception as e:
            print(f"[WARN] Could not delete {table}: {e}")

    print("[OK] Cleanup complete")


if __name__ == "__main__":
    main()
