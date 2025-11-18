# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Walkthrough demonstrating core Dataverse SDK operations.

This example shows:
- Table creation with various column types including enums
- Single and multiple record CRUD operations
- Querying with filtering, paging, and SQL
- Picklist label-to-value conversion
- Column management
- Cleanup

Prerequisites:
- pip install PowerPlatform-Dataverse-Client
- pip install azure-identity
"""

import sys
import json
from enum import IntEnum
from azure.identity import InteractiveBrowserCredential
from PowerPlatform.Dataverse.client import DataverseClient


# Simple logging helper
def log_call(description):
    print(f"\n→ {description}")


# Define enum for priority picklist
class Priority(IntEnum):
    LOW = 1
    MEDIUM = 2
    HIGH = 3


def main():
    print("=" * 80)
    print("Dataverse SDK Walkthrough")
    print("=" * 80)

    # ============================================================================
    # 1. SETUP & AUTHENTICATION
    # ============================================================================
    print("\n" + "=" * 80)
    print("1. Setup & Authentication")
    print("=" * 80)

    base_url = input("Enter Dataverse org URL (e.g. https://yourorg.crm.dynamics.com): ").strip()
    if not base_url:
        print("No URL entered; exiting.")
        sys.exit(1)

    base_url = base_url.rstrip("/")

    log_call("InteractiveBrowserCredential()")
    credential = InteractiveBrowserCredential()

    log_call(f"DataverseClient(base_url='{base_url}', credential=...)")
    client = DataverseClient(base_url=base_url, credential=credential)
    print(f"✓ Connected to: {base_url}")

    # ============================================================================
    # 2. TABLE CREATION (METADATA)
    # ============================================================================
    print("\n" + "=" * 80)
    print("2. Table Creation (Metadata)")
    print("=" * 80)

    table_name = "new_WalkthroughDemo"

    log_call(f"client.get_table_info('{table_name}')")
    table_info = client.get_table_info(table_name)

    if table_info:
        print(f"✓ Table already exists: {table_info.get('table_schema_name')}")
        print(f"  Logical Name: {table_info.get('table_logical_name')}")
        print(f"  Entity Set: {table_info.get('entity_set_name')}")
    else:
        log_call(f"client.create_table('{table_name}', columns={{...}})")
        columns = {
            "new_Title": "string",
            "new_Quantity": "int",
            "new_Amount": "decimal",
            "new_Completed": "bool",
            "new_Priority": Priority,
        }
        table_info = client.create_table(table_name, columns)
        print(f"✓ Created table: {table_info.get('table_schema_name')}")
        print(f"  Columns created: {', '.join(table_info.get('columns_created', []))}")

    # ============================================================================
    # 3. CREATE OPERATIONS
    # ============================================================================
    print("\n" + "=" * 80)
    print("3. Create Operations")
    print("=" * 80)

    # Single create
    log_call(f"client.create('{table_name}', {{...}})")
    single_record = {
        "new_Title": "Complete project documentation",
        "new_Quantity": 5,
        "new_Amount": 1250.50,
        "new_Completed": False,
        "new_Priority": Priority.MEDIUM,
    }
    id1 = client.create(table_name, single_record)[0]
    print(f"✓ Created single record: {id1}")

    # Multiple create
    log_call(f"client.create('{table_name}', [{{...}}, {{...}}, {{...}}])")
    multiple_records = [
        {
            "new_Title": "Review code changes",
            "new_Quantity": 10,
            "new_Amount": 500.00,
            "new_Completed": True,
            "new_Priority": Priority.HIGH,
        },
        {
            "new_Title": "Update test cases",
            "new_Quantity": 8,
            "new_Amount": 750.25,
            "new_Completed": False,
            "new_Priority": Priority.LOW,
        },
        {
            "new_Title": "Deploy to staging",
            "new_Quantity": 3,
            "new_Amount": 2000.00,
            "new_Completed": False,
            "new_Priority": Priority.HIGH,
        },
    ]
    ids = client.create(table_name, multiple_records)
    print(f"✓ Created {len(ids)} records: {ids}")

    # ============================================================================
    # 4. READ OPERATIONS
    # ============================================================================
    print("\n" + "=" * 80)
    print("4. Read Operations")
    print("=" * 80)

    # Single read by ID
    log_call(f"client.get('{table_name}', '{id1}')")
    record = client.get(table_name, id1)
    print("✓ Retrieved single record:")
    print(
        json.dumps(
            {
                "new_walkthroughdemoid": record.get("new_walkthroughdemoid"),
                "new_title": record.get("new_title"),
                "new_quantity": record.get("new_quantity"),
                "new_amount": record.get("new_amount"),
                "new_completed": record.get("new_completed"),
                "new_priority": record.get("new_priority"),
                "new_priority@FormattedValue": record.get("new_priority@OData.Community.Display.V1.FormattedValue"),
            },
            indent=2,
        )
    )

    # Multiple read with filter
    log_call(f"client.get('{table_name}', filter='new_quantity gt 5')")
    all_records = []
    for page in client.get(table_name, filter="new_quantity gt 5"):
        all_records.extend(page)
    print(f"✓ Found {len(all_records)} records with new_quantity > 5")
    for rec in all_records:
        print(f"  - new_Title='{rec.get('new_title')}', new_Quantity={rec.get('new_quantity')}")

    # ============================================================================
    # 5. UPDATE OPERATIONS
    # ============================================================================
    print("\n" + "=" * 80)
    print("5. Update Operations")
    print("=" * 80)

    # Single update
    log_call(f"client.update('{table_name}', '{id1}', {{...}})")
    client.update(table_name, id1, {"new_Quantity": 100})
    updated = client.get(table_name, id1)
    print(f"✓ Updated single record new_Quantity: {updated.get('new_quantity')}")

    # Multiple update (broadcast same change)
    log_call(f"client.update('{table_name}', [{len(ids)} IDs], {{...}})")
    client.update(table_name, ids, {"new_Completed": True})
    print(f"✓ Updated {len(ids)} records to new_Completed=True")

    # ============================================================================
    # 6. PAGING DEMO
    # ============================================================================
    print("\n" + "=" * 80)
    print("6. Paging Demo")
    print("=" * 80)

    # Create 20 records for paging
    log_call(f"client.create('{table_name}', [20 records])")
    paging_records = [
        {
            "new_Title": f"Paging test item {i}",
            "new_Quantity": i,
            "new_Amount": i * 10.0,
            "new_Completed": False,
            "new_Priority": Priority.LOW,
        }
        for i in range(1, 21)
    ]
    paging_ids = client.create(table_name, paging_records)
    print(f"✓ Created {len(paging_ids)} records for paging demo")

    # Query with paging
    log_call(f"client.get('{table_name}', page_size=5)")
    print("Fetching records with page_size=5...")
    for page_num, page in enumerate(client.get(table_name, orderby=["new_Quantity"], page_size=5), start=1):
        record_ids = [r.get("new_walkthroughdemoid")[:8] + "..." for r in page]
        print(f"  Page {page_num}: {len(page)} records - IDs: {record_ids}")

    # ============================================================================
    # 7. SQL QUERY
    # ============================================================================
    print("\n" + "=" * 80)
    print("7. SQL Query")
    print("=" * 80)

    log_call(f"client.query_sql('SELECT new_title, new_quantity FROM {table_name} WHERE new_completed = 1')")
    sql = f"SELECT new_title, new_quantity FROM new_walkthroughdemo WHERE new_completed = 1"
    try:
        results = client.query_sql(sql)
        print(f"✓ SQL query returned {len(results)} completed records:")
        for result in results[:5]:  # Show first 5
            print(f"  - new_Title='{result.get('new_title')}', new_Quantity={result.get('new_quantity')}")
    except Exception as e:
        print(f"⚠ SQL query failed (known server-side bug): {str(e)}")

    # ============================================================================
    # 8. PICKLIST LABEL CONVERSION
    # ============================================================================
    print("\n" + "=" * 80)
    print("8. Picklist Label Conversion")
    print("=" * 80)

    log_call(f"client.create('{table_name}', {{'new_Priority': 'High'}})")
    label_record = {
        "new_Title": "Test label conversion",
        "new_Quantity": 1,
        "new_Amount": 99.99,
        "new_Completed": False,
        "new_Priority": "High",  # String label instead of int
    }
    label_id = client.create(table_name, label_record)[0]
    retrieved = client.get(table_name, label_id)
    print(f"✓ Created record with string label 'High' for new_Priority")
    print(f"  new_Priority stored as integer: {retrieved.get('new_priority')}")
    print(f"  new_Priority@FormattedValue: {retrieved.get('new_priority@OData.Community.Display.V1.FormattedValue')}")

    # ============================================================================
    # 9. COLUMN MANAGEMENT
    # ============================================================================
    print("\n" + "=" * 80)
    print("9. Column Management")
    print("=" * 80)

    log_call(f"client.create_columns('{table_name}', {{'new_Notes': 'string'}})")
    created_cols = client.create_columns(table_name, {"new_Notes": "string"})
    print(f"✓ Added column: {created_cols[0]}")

    # Delete the column we just added
    log_call(f"client.delete_columns('{table_name}', ['new_Notes'])")
    client.delete_columns(table_name, ["new_Notes"])
    print(f"✓ Deleted column: new_Notes")

    # ============================================================================
    # 10. DELETE OPERATIONS
    # ============================================================================
    print("\n" + "=" * 80)
    print("10. Delete Operations")
    print("=" * 80)

    # Single delete
    log_call(f"client.delete('{table_name}', '{id1}')")
    client.delete(table_name, id1)
    print(f"✓ Deleted single record: {id1}")

    # Multiple delete (delete the paging demo records)
    log_call(f"client.delete('{table_name}', [{len(paging_ids)} IDs])")
    job_id = client.delete(table_name, paging_ids)
    print(f"✓ Bulk delete job started: {job_id}")
    print(f"  (Deleting {len(paging_ids)} paging demo records)")

    # ============================================================================
    # 11. CLEANUP
    # ============================================================================
    print("\n" + "=" * 80)
    print("11. Cleanup")
    print("=" * 80)

    log_call(f"client.delete_table('{table_name}')")
    client.delete_table(table_name)
    print(f"✓ Deleted table: {table_name}")

    # ============================================================================
    # SUMMARY
    # ============================================================================
    print("\n" + "=" * 80)
    print("Walkthrough Complete!")
    print("=" * 80)
    print("\nDemonstrated operations:")
    print("  ✓ Table creation with multiple column types")
    print("  ✓ Single and multiple record creation")
    print("  ✓ Reading records by ID and with filters")
    print("  ✓ Single and multiple record updates")
    print("  ✓ Paging through large result sets")
    print("  ✓ SQL queries")
    print("  ✓ Picklist label-to-value conversion")
    print("  ✓ Column management")
    print("  ✓ Single and bulk delete operations")
    print("  ✓ Table cleanup")
    print("=" * 80)


if __name__ == "__main__":
    main()
