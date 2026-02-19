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
import time
from enum import IntEnum
from azure.identity import InteractiveBrowserCredential
from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.core.errors import MetadataError
import requests


# Simple logging helper
def log_call(description):
    print(f"\n-> {description}")


# Define enum for priority picklist
class Priority(IntEnum):
    LOW = 1
    MEDIUM = 2
    HIGH = 3


def backoff(op, *, delays=(0, 2, 5, 10, 20, 20)):
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
                retry_count = attempts - 1
                print(f"   [INFO] Backoff succeeded after {retry_count} retry(s); waited {total_delay}s total.")
            return result
        except Exception as ex:  # noqa: BLE001
            last = ex
            continue
    if last:
        if attempts:
            retry_count = max(attempts - 1, 0)
            print(f"   [WARN] Backoff exhausted after {retry_count} retry(s); waited {total_delay}s total.")
        raise last


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
    print(f"[OK] Connected to: {base_url}")

    # ============================================================================
    # 2. TABLE CREATION (METADATA)
    # ============================================================================
    print("\n" + "=" * 80)
    print("2. Table Creation (Metadata)")
    print("=" * 80)

    table_name = "new_WalkthroughDemo"

    log_call(f"client.tables.get('{table_name}')")
    table_info = backoff(lambda: client.tables.get(table_name))

    if table_info:
        print(f"[OK] Table already exists: {table_info.get('table_schema_name')}")
        print(f"  Logical Name: {table_info.get('table_logical_name')}")
        print(f"  Entity Set: {table_info.get('entity_set_name')}")
    else:
        log_call(f"client.tables.create('{table_name}', columns={{...}})")
        columns = {
            "new_Title": "string",
            "new_Quantity": "int",
            "new_Amount": "decimal",
            "new_Completed": "bool",
            "new_Priority": Priority,
        }
        table_info = backoff(lambda: client.tables.create(table_name, columns))
        print(f"[OK] Created table: {table_info.get('table_schema_name')}")
        print(f"  Columns created: {', '.join(table_info.get('columns_created', []))}")

    # ============================================================================
    # 3. CREATE OPERATIONS
    # ============================================================================
    print("\n" + "=" * 80)
    print("3. Create Operations")
    print("=" * 80)

    # Single create
    log_call(f"client.records.create('{table_name}', {{...}})")
    single_record = {
        "new_Title": "Complete project documentation",
        "new_Quantity": 5,
        "new_Amount": 1250.50,
        "new_Completed": False,
        "new_Priority": Priority.MEDIUM,
    }
    id1 = backoff(lambda: client.records.create(table_name, single_record))
    print(f"[OK] Created single record: {id1}")

    # Multiple create
    log_call(f"client.records.create('{table_name}', [{{...}}, {{...}}, {{...}}])")
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
    ids = backoff(lambda: client.records.create(table_name, multiple_records))
    print(f"[OK] Created {len(ids)} records: {ids}")

    # ============================================================================
    # 4. READ OPERATIONS
    # ============================================================================
    print("\n" + "=" * 80)
    print("4. Read Operations")
    print("=" * 80)

    # Single read by ID
    log_call(f"client.records.get('{table_name}', '{id1}')")
    record = backoff(lambda: client.records.get(table_name, id1))
    print("[OK] Retrieved single record:")
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
    log_call(f"client.query.get('{table_name}', filter='new_quantity gt 5')")
    all_records = []
    records_iterator = backoff(lambda: client.query.get(table_name, filter="new_quantity gt 5"))
    for page in records_iterator:
        all_records.extend(page)
    print(f"[OK] Found {len(all_records)} records with new_quantity > 5")
    for rec in all_records:
        print(f"  - new_Title='{rec.get('new_title')}', new_Quantity={rec.get('new_quantity')}")

    # ============================================================================
    # 5. UPDATE OPERATIONS
    # ============================================================================
    print("\n" + "=" * 80)
    print("5. Update Operations")
    print("=" * 80)

    # Single update
    log_call(f"client.records.update('{table_name}', '{id1}', {{...}})")
    backoff(lambda: client.records.update(table_name, id1, {"new_Quantity": 100}))
    updated = backoff(lambda: client.records.get(table_name, id1))
    print(f"[OK] Updated single record new_Quantity: {updated.get('new_quantity')}")

    # Multiple update (broadcast same change)
    log_call(f"client.records.update('{table_name}', [{len(ids)} IDs], {{...}})")
    backoff(lambda: client.records.update(table_name, ids, {"new_Completed": True}))
    print(f"[OK] Updated {len(ids)} records to new_Completed=True")

    # ============================================================================
    # 6. PAGING DEMO
    # ============================================================================
    print("\n" + "=" * 80)
    print("6. Paging Demo")
    print("=" * 80)

    # Create 20 records for paging
    log_call(f"client.records.create('{table_name}', [20 records])")
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
    paging_ids = backoff(lambda: client.records.create(table_name, paging_records))
    print(f"[OK] Created {len(paging_ids)} records for paging demo")

    # Query with paging
    log_call(f"client.query.get('{table_name}', page_size=5)")
    print("Fetching records with page_size=5...")
    paging_iterator = backoff(lambda: client.query.get(table_name, orderby=["new_Quantity"], page_size=5))
    for page_num, page in enumerate(paging_iterator, start=1):
        record_ids = [r.get("new_walkthroughdemoid")[:8] + "..." for r in page]
        print(f"  Page {page_num}: {len(page)} records - IDs: {record_ids}")

    # ============================================================================
    # 7. SQL QUERY
    # ============================================================================
    print("\n" + "=" * 80)
    print("7. SQL Query")
    print("=" * 80)

    log_call(f"client.query.sql('SELECT new_title, new_quantity FROM {table_name} WHERE new_completed = 1')")
    sql = f"SELECT new_title, new_quantity FROM new_walkthroughdemo WHERE new_completed = 1"
    try:
        results = backoff(lambda: client.query.sql(sql))
        print(f"[OK] SQL query returned {len(results)} completed records:")
        for result in results[:5]:  # Show first 5
            print(f"  - new_Title='{result.get('new_title')}', new_Quantity={result.get('new_quantity')}")
    except Exception as e:
        print(f"[WARN] SQL query failed (known server-side bug): {str(e)}")

    # ============================================================================
    # 8. PICKLIST LABEL CONVERSION
    # ============================================================================
    print("\n" + "=" * 80)
    print("8. Picklist Label Conversion")
    print("=" * 80)

    log_call(f"client.records.create('{table_name}', {{'new_Priority': 'High'}})")
    label_record = {
        "new_Title": "Test label conversion",
        "new_Quantity": 1,
        "new_Amount": 99.99,
        "new_Completed": False,
        "new_Priority": "High",  # String label instead of int
    }
    label_id = backoff(lambda: client.records.create(table_name, label_record))
    retrieved = backoff(lambda: client.records.get(table_name, label_id))
    print(f"[OK] Created record with string label 'High' for new_Priority")
    print(f"  new_Priority stored as integer: {retrieved.get('new_priority')}")
    print(f"  new_Priority@FormattedValue: {retrieved.get('new_priority@OData.Community.Display.V1.FormattedValue')}")

    # ============================================================================
    # 9. COLUMN MANAGEMENT
    # ============================================================================
    print("\n" + "=" * 80)
    print("9. Column Management")
    print("=" * 80)

    log_call(f"client.tables.add_columns('{table_name}', {{'new_Notes': 'string'}})")
    created_cols = backoff(lambda: client.tables.add_columns(table_name, {"new_Notes": "string"}))
    print(f"[OK] Added column: {created_cols[0]}")

    # Delete the column we just added
    log_call(f"client.tables.remove_columns('{table_name}', ['new_Notes'])")
    backoff(lambda: client.tables.remove_columns(table_name, ["new_Notes"]))
    print(f"[OK] Deleted column: new_Notes")

    # ============================================================================
    # 10. DELETE OPERATIONS
    # ============================================================================
    print("\n" + "=" * 80)
    print("10. Delete Operations")
    print("=" * 80)

    # Single delete
    log_call(f"client.records.delete('{table_name}', '{id1}')")
    backoff(lambda: client.records.delete(table_name, id1))
    print(f"[OK] Deleted single record: {id1}")

    # Multiple delete (delete the paging demo records)
    log_call(f"client.records.delete('{table_name}', [{len(paging_ids)} IDs])")
    job_id = backoff(lambda: client.records.delete(table_name, paging_ids))
    print(f"[OK] Bulk delete job started: {job_id}")
    print(f"  (Deleting {len(paging_ids)} paging demo records)")

    # ============================================================================
    # 11. CLEANUP
    # ============================================================================
    print("\n" + "=" * 80)
    print("11. Cleanup")
    print("=" * 80)

    log_call(f"client.tables.delete('{table_name}')")
    try:
        backoff(lambda: client.tables.delete(table_name))
        print(f"[OK] Deleted table: {table_name}")
    except Exception as ex:  # noqa: BLE001
        code = getattr(getattr(ex, "response", None), "status_code", None)
        if isinstance(ex, (requests.exceptions.HTTPError, MetadataError)) and code == 404:
            print(f"[OK] Table removed: {table_name}")
        else:
            raise

    # ============================================================================
    # SUMMARY
    # ============================================================================
    print("\n" + "=" * 80)
    print("Walkthrough Complete!")
    print("=" * 80)
    print("\nDemonstrated operations:")
    print("  [OK] Table creation with multiple column types")
    print("  [OK] Single and multiple record creation")
    print("  [OK] Reading records by ID and with filters")
    print("  [OK] Single and multiple record updates")
    print("  [OK] Paging through large result sets")
    print("  [OK] SQL queries")
    print("  [OK] Picklist label-to-value conversion")
    print("  [OK] Column management")
    print("  [OK] Single and bulk delete operations")
    print("  [OK] Table cleanup")
    print("=" * 80)


if __name__ == "__main__":
    main()
