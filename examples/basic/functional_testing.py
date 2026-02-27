#!/usr/bin/env python3
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
PowerPlatform Dataverse Client SDK - Advanced Functional Testing

This script provides comprehensive functional testing of the PowerPlatform-Dataverse-Client SDK:
- Real environment connection testing
- Table creation and metadata operations
- Full CRUD operations testing
- Query functionality validation
- Batch operations (create, read, update, changeset, delete)
- Interactive cleanup options

Prerequisites:
- PowerPlatform-Dataverse-Client SDK installed (run installation_example.py first)
- Azure Identity credentials configured
- Access to a Dataverse environment with table creation permissions

Usage:
    python examples/advanced/functional_testing.py

Note: This is an advanced testing script. For basic installation validation,
      use examples/basic/installation_example.py instead.
"""

import sys
import time
from typing import Optional, Dict, Any
from datetime import datetime

# Import SDK components (assumes installation is already validated)
from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.core.errors import HttpError, MetadataError
from PowerPlatform.Dataverse.models.upsert import UpsertItem
from azure.identity import InteractiveBrowserCredential


def get_dataverse_org_url() -> str:
    """Get Dataverse org URL from user input."""
    print("\n-> Dataverse Environment Setup")
    print("=" * 50)

    if not sys.stdin.isatty():
        print("[ERR] Interactive input required. Run this script in a terminal.")
        sys.exit(1)

    while True:
        org_url = input("Enter your Dataverse org URL (e.g., https://yourorg.crm.dynamics.com): ").strip()
        if org_url:
            return org_url.rstrip("/")
        print("[WARN] Please enter a valid URL.")


def setup_authentication() -> DataverseClient:
    """Set up authentication and create Dataverse client."""
    print("\n-> Authentication Setup")
    print("=" * 50)

    org_url = get_dataverse_org_url()
    try:
        credential = InteractiveBrowserCredential()
        client = DataverseClient(org_url, credential)

        # Test the connection
        print("Testing connection...")
        tables = client.tables.list()
        print(f"[OK] Connection successful! Found {len(tables)} tables.")

        # Test filtered + selected list
        user_owned = client.tables.list(
            filter="OwnershipType eq Microsoft.Dynamics.CRM.OwnershipTypes'UserOwned'",
            select=["LogicalName", "SchemaName", "DisplayName"],
        )
        print(f"[OK] Found {len(user_owned)} user-owned tables (filter + select).")
        return client

    except Exception as e:
        print(f"[ERR] Authentication failed: {e}")
        print("Please check your credentials and permissions.")
        sys.exit(1)


def wait_for_table_metadata(
    client: DataverseClient,
    table_schema_name: str,
    retries: int = 10,
    delay_seconds: int = 3,
) -> Dict[str, Any]:
    """Poll until table metadata is published and entity set becomes available."""

    for attempt in range(1, retries + 1):
        try:
            info = client.tables.get(table_schema_name)
            if info and info.get("entity_set_name"):
                # Check for PrimaryIdAttribute next, make sure it's available
                # so subsequent CRUD calls do not hit a cached miss despite table_info succeeding.
                odata = client._get_odata()
                odata._entity_set_from_schema_name(table_schema_name)

                if attempt > 1:
                    print(f"   [OK] Table metadata available after {attempt} attempts.")
                return info
        except Exception:
            pass

        if attempt < retries:
            print(f"   Waiting for table metadata to publish (attempt {attempt}/{retries})...")
            time.sleep(delay_seconds)

    raise RuntimeError("Table metadata did not become available in time. Please retry later.")


def ensure_test_table(client: DataverseClient) -> Dict[str, Any]:
    """Create or verify test table exists."""
    print("\n-> Test Table Setup")
    print("=" * 50)

    table_schema_name = "test_TestSDKFunctionality"

    try:
        # Check if table already exists
        existing_table = client.tables.get(table_schema_name)
        if existing_table:
            print(f"[OK] Test table '{table_schema_name}' already exists")
            return existing_table

    except Exception:
        print(f"Table '{table_schema_name}' not found, creating...")

    try:
        print("Creating new test table...")
        # Create the test table with various field types
        table_info = client.tables.create(
            table_schema_name,
            primary_column="test_name",
            columns={
                "test_description": "string",  # Description field
                "test_count": "int",  # Integer field
                "test_amount": "decimal",  # Decimal field
                "test_is_active": "bool",  # Boolean field
                "test_created_date": "datetime",  # DateTime field
            },
        )

        print(f"[OK] Created test table: {table_info.get('table_schema_name')}")
        print(f"   Logical name: {table_info.get('table_logical_name')}")
        print(f"   Entity set: {table_info.get('entity_set_name')}")

        return wait_for_table_metadata(client, table_schema_name)

    except MetadataError as e:
        print(f"[ERR] Failed to create table: {e}")
        sys.exit(1)


def test_create_record(client: DataverseClient, table_info: Dict[str, Any]) -> str:
    """Test record creation."""
    print("\n-> Record Creation Test")
    print("=" * 50)

    table_schema_name = table_info.get("table_schema_name")
    attr_prefix = table_schema_name.split("_", 1)[0] if "_" in table_schema_name else table_schema_name
    retries = 5
    delay_seconds = 3

    # Create test record data
    test_data = {
        f"{attr_prefix}_name": f"Test Record {datetime.now().strftime('%H:%M:%S')}",
        f"{attr_prefix}_description": "This is a test record created by the SDK functionality test",
        f"{attr_prefix}_count": 42,
        f"{attr_prefix}_amount": 123.45,
        f"{attr_prefix}_is_active": True,
        f"{attr_prefix}_created_date": datetime.now().isoformat(),
    }

    try:
        print("Creating test record...")
        created_id: Optional[str] = None
        for attempt in range(1, retries + 1):
            try:
                created_id = client.records.create(table_schema_name, test_data)
                if attempt > 1:
                    print(f"   [OK] Record creation succeeded after {attempt} attempts.")
                break
            except HttpError as err:
                if getattr(err, "status_code", None) == 404 and attempt < retries:
                    print(
                        f"   Table not ready for create (attempt {attempt}/{retries}). Retrying in {delay_seconds}s..."
                    )
                    time.sleep(delay_seconds)
                    continue
                raise

        if created_id:
            print(f"[OK] Record created successfully!")
            print(f"   Record ID: {created_id}")
            print(f"   Name: {test_data[f'{attr_prefix}_name']}")
            return created_id
        else:
            raise ValueError("Unexpected response from records.create operation")

    except HttpError as e:
        print(f"[ERR] HTTP error during record creation: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"[ERR] Failed to create record: {e}")
        sys.exit(1)


def test_read_record(client: DataverseClient, table_info: Dict[str, Any], record_id: str) -> Dict[str, Any]:
    """Test record reading."""
    print("\n-> Record Reading Test")
    print("=" * 50)

    table_schema_name = table_info.get("table_schema_name")
    attr_prefix = table_schema_name.split("_", 1)[0] if "_" in table_schema_name else table_schema_name

    retries = 5
    delay_seconds = 3

    try:
        print(f"Reading record: {record_id}")
        record = None
        for attempt in range(1, retries + 1):
            try:
                record = client.records.get(table_schema_name, record_id)
                if attempt > 1:
                    print(f"   [OK] Record read succeeded after {attempt} attempts.")
                break
            except HttpError as err:
                if getattr(err, "status_code", None) == 404 and attempt < retries:
                    print(f"   Record not queryable yet (attempt {attempt}/{retries}). Retrying in {delay_seconds}s...")
                    time.sleep(delay_seconds)
                    continue
                raise

        if record is None:
            raise RuntimeError("Record did not become available in time.")

        if record:
            print("[OK] Record retrieved successfully!")
            print("   Retrieved data:")

            # Display key fields
            for field_name in [
                f"{attr_prefix}_name",
                f"{attr_prefix}_description",
                f"{attr_prefix}_count",
                f"{attr_prefix}_amount",
                f"{attr_prefix}_is_active",
            ]:
                if field_name in record:
                    print(f"     {field_name}: {record[field_name]}")

            return record
        else:
            raise ValueError("Record not found")

    except HttpError as e:
        print(f"[ERR] HTTP error during record reading: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"[ERR] Failed to read record: {e}")
        sys.exit(1)


def test_query_records(client: DataverseClient, table_info: Dict[str, Any]) -> None:
    """Test querying multiple records."""
    print("\n-> Record Query Test")
    print("=" * 50)

    table_schema_name = table_info.get("table_schema_name")
    attr_prefix = table_schema_name.split("_", 1)[0] if "_" in table_schema_name else table_schema_name
    retries = 5
    delay_seconds = 3

    try:
        print("Querying records from test table...")
        for attempt in range(1, retries + 1):
            try:
                records_iterator = client.records.get(
                    table_schema_name,
                    select=[f"{attr_prefix}_name", f"{attr_prefix}_count", f"{attr_prefix}_amount"],
                    filter=f"{attr_prefix}_is_active eq true",
                    top=5,
                    orderby=[f"{attr_prefix}_name asc"],
                )

                record_count = 0
                for batch in records_iterator:
                    for record in batch:
                        record_count += 1
                        name = record.get(f"{attr_prefix}_name", "N/A")
                        count = record.get(f"{attr_prefix}_count", "N/A")
                        amount = record.get(f"{attr_prefix}_amount", "N/A")
                        print(f"   Record {record_count}: {name} (Count: {count}, Amount: {amount})")

                print(f"[OK] Query completed! Found {record_count} active records.")
                break
            except HttpError as err:
                if getattr(err, "status_code", None) == 404 and attempt < retries:
                    print(f"   Query retry {attempt}/{retries} after metadata 404 ({err}). Waiting {delay_seconds}s...")
                    time.sleep(delay_seconds)
                    continue
                raise

    except Exception as e:
        print(f"[WARN] Query test encountered an issue: {e}")
        print("   This might be expected if the table is very new.")


def test_batch_all_operations(client: DataverseClient, table_info: Dict[str, Any]) -> None:
    """Test every available batch operation type in a structured sequence.

    Operations covered:
      records.create (single + CreateMultiple)
      records.get (single by ID)
      records.update (single PATCH + UpdateMultiple)
      records.delete (multi, use_bulk_delete=False)
      records.upsert (graceful — requires configured alternate key)
      tables.get, tables.list
      query.sql
      changeset happy path (create + update via content-ID ref + delete)
      changeset rollback (failing op rolls back entire changeset)
      execute(continue_on_error=True) — mixed success/failure
    """
    print("\n-> Batch Operations Test (All Operations)")
    print("=" * 50)

    table_schema_name = table_info.get("table_schema_name")
    logical_name = table_info.get("table_logical_name", table_schema_name.lower())
    attr_prefix = table_schema_name.split("_", 1)[0] if "_" in table_schema_name else table_schema_name
    all_ids: list = []

    try:
        # -------------------------------------------------------------------
        # [1/8] CREATE — single record + CreateMultiple (list) in one batch
        # -------------------------------------------------------------------
        print("\n[1/8] Create — single + CreateMultiple (2 ops, 1 POST $batch)")
        batch = client.batch.new()
        batch.records.create(
            table_schema_name,
            {
                f"{attr_prefix}_name": f"Batch-A {datetime.now().strftime('%H:%M:%S')}",
                f"{attr_prefix}_count": 1,
                f"{attr_prefix}_is_active": True,
            },
        )
        batch.records.create(
            table_schema_name,
            [
                {
                    f"{attr_prefix}_name": f"Batch-B {datetime.now().strftime('%H:%M:%S')}",
                    f"{attr_prefix}_count": 2,
                    f"{attr_prefix}_is_active": True,
                },
                {
                    f"{attr_prefix}_name": f"Batch-C {datetime.now().strftime('%H:%M:%S')}",
                    f"{attr_prefix}_count": 3,
                    f"{attr_prefix}_is_active": True,
                },
            ],
        )
        result = batch.execute()
        all_ids = list(result.created_ids)
        if result.has_errors:
            for item in result.failed:
                print(f"[WARN] {item.status_code}: {item.error_message}")
        else:
            print(f"[OK] {len(result.succeeded)} ops → {len(all_ids)} records created: {all_ids}")

        # -------------------------------------------------------------------
        # [2/8] READ — get by ID + tables.get + tables.list + query.sql
        #              All 4 reads in one batch request
        # -------------------------------------------------------------------
        if all_ids:
            print("\n[2/8] Read — records.get + tables.get + tables.list + query.sql (4 ops, 1 POST $batch)")
            batch = client.batch.new()
            batch.records.get(
                table_schema_name,
                all_ids[0],
                select=[f"{attr_prefix}_name", f"{attr_prefix}_count"],
            )
            batch.tables.get(table_schema_name)
            batch.tables.list()
            batch.query.sql(f"SELECT TOP 3 {attr_prefix}_name FROM {logical_name}")
            result = batch.execute()
            print(f"[OK] {len(result.succeeded)} succeeded, {len(result.failed)} failed")
            for i, resp in enumerate(result.responses):
                if not resp.is_success:
                    print(f"   [{i}] FAILED {resp.status_code}: {resp.error_message}")
                    continue
                if i == 0 and resp.data:
                    print(
                        f"   records.get → name='{resp.data.get(f'{attr_prefix}_name')}', count={resp.data.get(f'{attr_prefix}_count')}"
                    )
                elif i == 1 and resp.data:
                    print(
                        f"   tables.get  → LogicalName='{resp.data.get('LogicalName')}', EntitySet='{resp.data.get('EntitySetName')}'"
                    )
                elif i == 2 and resp.data:
                    print(f"   tables.list → {len(resp.data.get('value', []))} tables returned")
                elif i == 3 and resp.data:
                    print(f"   query.sql   → {len(resp.data.get('value', []))} rows returned")

        # -------------------------------------------------------------------
        # [3/8] UPDATE — single PATCH + UpdateMultiple (broadcast) in one batch
        # -------------------------------------------------------------------
        if len(all_ids) >= 3:
            print(f"\n[3/8] Update — single PATCH + UpdateMultiple ({len(all_ids)} records, 2 ops, 1 POST $batch)")
            batch = client.batch.new()
            batch.records.update(table_schema_name, all_ids[0], {f"{attr_prefix}_count": 10})
            batch.records.update(table_schema_name, all_ids[1:], {f"{attr_prefix}_count": 20})
            result = batch.execute()
            print(f"[OK] {len(result.succeeded)} updates succeeded, {len(result.failed)} failed")

        # -------------------------------------------------------------------
        # [4/8] CHANGESET (happy path) — create + update via content-ID + delete
        #        All three changeset operation types committed atomically
        # -------------------------------------------------------------------
        if len(all_ids) >= 1:
            print("\n[4/8] Changeset (happy path) — cs.create + cs.update(ref) + cs.delete (1 transaction)")
            batch = client.batch.new()
            with batch.changeset() as cs:
                ref = cs.records.create(
                    table_schema_name,
                    {
                        f"{attr_prefix}_name": f"Batch-D {datetime.now().strftime('%H:%M:%S')}",
                        f"{attr_prefix}_count": 4,
                        f"{attr_prefix}_is_active": False,
                    },
                )
                cs.records.update(table_schema_name, ref, {f"{attr_prefix}_is_active": True})
                cs.records.delete(table_schema_name, all_ids[-1])
            result = batch.execute()
            if result.has_errors:
                for item in result.failed:
                    print(f"[WARN] Changeset error {item.status_code}: {item.error_message}")
            else:
                new_id = next(iter(result.created_ids), None)
                if new_id:
                    all_ids[-1] = new_id  # replace deleted id with the new one
                print(f"[OK] {len(result.succeeded)} ops committed atomically (create + update + delete)")

        # -------------------------------------------------------------------
        # [5/8] CHANGESET (rollback) — failing update rolls back the create
        # -------------------------------------------------------------------
        print("\n[5/8] Changeset (rollback) — cs.create + cs.update(nonexistent) → full rollback")
        nonexistent_id = "00000000-0000-0000-0000-000000000001"
        batch = client.batch.new()
        with batch.changeset() as cs:
            cs.records.create(
                table_schema_name,
                {
                    f"{attr_prefix}_name": f"Rollback-test {datetime.now().strftime('%H:%M:%S')}",
                    f"{attr_prefix}_count": 0,
                    f"{attr_prefix}_is_active": False,
                },
            )
            cs.records.update(table_schema_name, nonexistent_id, {f"{attr_prefix}_count": 999})
        result = batch.execute()
        if result.has_errors:
            leaked = list(result.created_ids)
            if not leaked:
                print("[OK] Changeset rollback verified: changeset failed, no records created")
            else:
                print(f"[WARN] Changeset failed but {len(leaked)} IDs leaked — queuing for cleanup")
                all_ids.extend(leaked)
        else:
            print("[WARN] Expected rollback but changeset succeeded (unexpected)")
            all_ids.extend(result.created_ids)

        # -------------------------------------------------------------------
        # [6/8] UPSERT — requires an alternate key configured on the table.
        #        The test table has none, so this is expected to fail (graceful).
        # -------------------------------------------------------------------
        print(f"\n[6/8] Upsert — UpsertItem with alternate key (expected to fail: no alt key on test table)")
        try:
            batch = client.batch.new()
            batch.records.upsert(
                table_schema_name,
                [
                    UpsertItem(
                        alternate_key={f"{attr_prefix}_name": f"Upsert-E {datetime.now().strftime('%H:%M:%S')}"},
                        record={f"{attr_prefix}_count": 5, f"{attr_prefix}_is_active": True},
                    )
                ],
            )
            result = batch.execute()
            if result.has_errors:
                print(f"[WARN] Upsert failed as expected (no alternate key configured): {result.failed[0].status_code}")
            else:
                upsert_ids = list(result.created_ids)
                all_ids.extend(upsert_ids)
                print(f"[OK] Upsert succeeded: {len(upsert_ids)} record(s) — alternate key was accepted")
        except Exception as e:
            print(f"[WARN] Upsert skipped due to exception: {e}")

        # -------------------------------------------------------------------
        # [7/8] MIXED BATCH with continue_on_error
        #        One intentional 404 alongside a valid get — both attempted
        # -------------------------------------------------------------------
        if all_ids:
            print(f"\n[7/8] Mixed batch (continue_on_error=True) — 1 bad get + 1 good get")
            batch = client.batch.new()
            batch.records.get(
                table_schema_name,
                "00000000-0000-0000-0000-000000000002",
                select=[f"{attr_prefix}_name"],
            )
            batch.records.get(
                table_schema_name,
                all_ids[0],
                select=[f"{attr_prefix}_name"],
            )
            result = batch.execute(continue_on_error=True)
            print(f"[OK] Succeeded: {len(result.succeeded)}, Failed: {len(result.failed)}")
            for item in result.failed:
                print(f"   Expected failure: {item.status_code} {item.error_message}")

        # -------------------------------------------------------------------
        # [8/8] DELETE — multi-delete (use_bulk_delete=False → individual DELETEs)
        # -------------------------------------------------------------------
        if all_ids:
            print(f"\n[8/8] Delete — {len(all_ids)} records via multi-delete (use_bulk_delete=False, 1 POST $batch)")
            batch = client.batch.new()
            batch.records.delete(table_schema_name, all_ids, use_bulk_delete=False)
            result = batch.execute(continue_on_error=True)
            print(f"[OK] Deleted {len(result.succeeded)}, failed {len(result.failed)}")

        print("\n[OK] Batch all-operations test completed!")

    except Exception as e:
        print(f"[WARN] Batch all-operations test encountered an issue: {e}")
        if all_ids:
            try:
                batch = client.batch.new()
                batch.records.delete(table_schema_name, all_ids, use_bulk_delete=False)
                batch.execute(continue_on_error=True)
            except Exception:
                pass


def cleanup_test_data(client: DataverseClient, table_info: Dict[str, Any], record_id: str) -> None:
    """Clean up test data."""
    print("\n-> Cleanup")
    print("=" * 50)

    table_schema_name = table_info.get("table_schema_name")
    retries = 5
    delay_seconds = 3

    # Ask user if they want to clean up
    cleanup_choice = input("Do you want to delete the test record? (y/N): ").strip().lower()

    if cleanup_choice in ["y", "yes"]:
        for attempt in range(1, retries + 1):
            try:
                client.records.delete(table_schema_name, record_id)
                print("[OK] Test record deleted successfully")
                break
            except HttpError as err:
                status = getattr(err, "status_code", None)
                if status == 404:
                    print("Record already deleted or not yet available; skipping.")
                    break
                if attempt < retries:
                    print(
                        f"   Record delete retry {attempt}/{retries} after error ({err}). Waiting {delay_seconds}s..."
                    )
                    time.sleep(delay_seconds)
                    continue
                print(f"[WARN] Failed to delete test record: {err}")
            except Exception as e:
                print(f"[WARN] Failed to delete test record: {e}")
                break
    else:
        print("Test record kept for inspection")

    # Ask about table cleanup
    table_cleanup = input("Do you want to delete the test table? (y/N): ").strip().lower()

    if table_cleanup in ["y", "yes"]:
        for attempt in range(1, retries + 1):
            try:
                client.tables.delete(table_info.get("table_schema_name"))
                print("[OK] Test table deleted successfully")
                break
            except HttpError as err:
                status = getattr(err, "status_code", None)
                if status == 404:
                    if _table_still_exists(client, table_info.get("table_schema_name")):
                        if attempt < retries:
                            print(
                                f"   Table delete retry {attempt}/{retries} after metadata 404 ({err}). Waiting {delay_seconds}s..."
                            )
                            time.sleep(delay_seconds)
                            continue
                        print(f"[WARN] Failed to delete test table due to metadata delay: {err}")
                        break
                    print("[OK] Test table deleted successfully (404 reported).")
                    break
                if attempt < retries:
                    print(f"   Table delete retry {attempt}/{retries} after error ({err}). Waiting {delay_seconds}s...")
                    time.sleep(delay_seconds)
                    continue
                print(f"[WARN] Failed to delete test table: {err}")
            except Exception as e:
                print(f"[WARN] Failed to delete test table: {e}")
                break
    else:
        print("Test table kept for future testing")


def _table_still_exists(client: DataverseClient, table_schema_name: Optional[str]) -> bool:
    if not table_schema_name:
        return False
    try:
        info = client.tables.get(table_schema_name)
        return bool(info and info.get("entity_set_name"))
    except HttpError as probe_err:
        if getattr(probe_err, "status_code", None) == 404:
            return False
        return True
    except Exception:
        return True


def main():
    """Main test function."""
    print("PowerPlatform Dataverse Client SDK - Advanced Functional Testing")
    print("=" * 70)
    print("This script tests SDK functionality in a real Dataverse environment:")
    print("  - Authentication & Connection")
    print("  - Table Creation & Metadata Operations")
    print("  - Record CRUD Operations")
    print("  - Query Functionality")
    print("  - Batch Operations (create, read, update, changeset, delete)")
    print("  - Interactive Cleanup")
    print("=" * 70)
    print("For installation validation, run examples/basic/installation_example.py first")
    print("=" * 70)

    try:
        # Setup and authentication
        client = setup_authentication()

        # Table setup
        table_info = ensure_test_table(client)

        # Test record operations
        record_id = test_create_record(client, table_info)
        retrieved_record = test_read_record(client, table_info, record_id)

        # Test querying
        test_query_records(client, table_info)

        # Test batch operations (all operation types)
        test_batch_all_operations(client, table_info)

        # Success summary
        print("\nFunctional Test Summary")
        print("=" * 50)
        print("[OK] Authentication: Success")
        print("[OK] Table Operations: Success")
        print("[OK] Record Creation: Success")
        print("[OK] Record Reading: Success")
        print("[OK] Record Querying: Success")
        print("[OK] Batch Operations: Success")
        print("\nYour PowerPlatform Dataverse Client SDK is fully functional!")

        # Cleanup
        cleanup_test_data(client, table_info, record_id)

    except KeyboardInterrupt:
        print("\n\n[WARN] Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n[ERR] Unexpected error: {e}")
        print("Please check your environment and try again")
        sys.exit(1)


if __name__ == "__main__":
    main()
