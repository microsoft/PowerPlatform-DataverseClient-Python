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
from typing import Optional, Dict, Any, List
from datetime import datetime

# Import SDK components (assumes installation is already validated)
from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.core.errors import HttpError, MetadataError
from azure.identity import InteractiveBrowserCredential


def get_dataverse_org_url() -> str:
    """Get Dataverse org URL from user input."""
    print("\n🌐 Dataverse Environment Setup")
    print("=" * 50)

    if not sys.stdin.isatty():
        print("❌ Interactive input required. Run this script in a terminal.")
        sys.exit(1)

    while True:
        org_url = input("Enter your Dataverse org URL (e.g., https://yourorg.crm.dynamics.com): ").strip()
        if org_url:
            return org_url.rstrip("/")
        print("⚠️  Please enter a valid URL.")


def setup_authentication() -> DataverseClient:
    """Set up authentication and create Dataverse client."""
    print("\n🔐 Authentication Setup")
    print("=" * 50)

    org_url = get_dataverse_org_url()
    try:
        credential = InteractiveBrowserCredential()
        client = DataverseClient(org_url, credential)

        # Test the connection
        print("🧪 Testing connection...")
        tables = client.list_tables()
        print(f"✅ Connection successful! Found {len(tables)} tables.")
        return client

    except Exception as e:
        print(f"❌ Authentication failed: {e}")
        print("💡 Please check your credentials and permissions.")
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
            info = client.get_table_info(table_schema_name)
            if info and info.get("entity_set_name"):
                # Check for PrimaryIdAttribute next, make sure it's available
                # so subsequent CRUD calls do not hit a cached miss despite table_info succeeding.
                odata = client._get_odata()
                odata._entity_set_from_schema_name(table_schema_name)

                if attempt > 1:
                    print(
                        f"   ✅ Table metadata available after {attempt} attempts."
                    )
                return info
        except Exception:
            pass

        if attempt < retries:
            print(
                f"   ⏳ Waiting for table metadata to publish (attempt {attempt}/{retries})..."
            )
            time.sleep(delay_seconds)

    raise RuntimeError(
        "Table metadata did not become available in time. Please retry later."
    )


def ensure_test_table(client: DataverseClient) -> Dict[str, Any]:
    """Create or verify test table exists."""
    print("\n📋 Test Table Setup")
    print("=" * 50)

    table_schema_name = "test_TestSDKFunctionality"

    try:
        # Check if table already exists
        existing_table = client.get_table_info(table_schema_name)
        if existing_table:
            print(f"✅ Test table '{table_schema_name}' already exists")
            return existing_table

    except Exception:
        print(f"📝 Table '{table_schema_name}' not found, creating...")

    try:
        print("🔨 Creating new test table...")
        # Create the test table with various field types
        table_info = client.create_table(
            table_schema_name,
            primary_column_schema_name="test_name",
            columns={
                "test_description": "string",  # Description field
                "test_count": "int",  # Integer field
                "test_amount": "decimal",  # Decimal field
                "test_is_active": "bool",  # Boolean field
                "test_created_date": "datetime",  # DateTime field
            },
        )

        print(f"✅ Created test table: {table_info.get('table_schema_name')}")
        print(f"   Logical name: {table_info.get('table_logical_name')}")
        print(f"   Entity set: {table_info.get('entity_set_name')}")

        return wait_for_table_metadata(client, table_schema_name)

    except MetadataError as e:
        print(f"❌ Failed to create table: {e}")
        sys.exit(1)


def test_create_record(client: DataverseClient, table_info: Dict[str, Any]) -> str:
    """Test record creation."""
    print("\n📝 Record Creation Test")
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
        print("🚀 Creating test record...")
        created_ids: Optional[List[str]] = None
        for attempt in range(1, retries + 1):
            try:
                created_ids = client.create(table_schema_name, test_data)
                if attempt > 1:
                    print(f"   ✅ Record creation succeeded after {attempt} attempts.")
                break
            except HttpError as err:
                if getattr(err, "status_code", None) == 404 and attempt < retries:
                    print(
                        f"   ⏳ Table not ready for create (attempt {attempt}/{retries}). Retrying in {delay_seconds}s..."
                    )
                    time.sleep(delay_seconds)
                    continue
                raise

        if isinstance(created_ids, list) and created_ids:
            record_id = created_ids[0]
            print(f"✅ Record created successfully!")
            print(f"   Record ID: {record_id}")
            print(f"   Name: {test_data[f'{attr_prefix}_name']}")
            return record_id
        else:
            raise ValueError("Unexpected response from create operation")

    except HttpError as e:
        print(f"❌ HTTP error during record creation: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Failed to create record: {e}")
        sys.exit(1)


def test_read_record(client: DataverseClient, table_info: Dict[str, Any], record_id: str) -> Dict[str, Any]:
    """Test record reading."""
    print("\n📖 Record Reading Test")
    print("=" * 50)

    table_schema_name = table_info.get("table_schema_name")
    attr_prefix = table_schema_name.split("_", 1)[0] if "_" in table_schema_name else table_schema_name

    retries = 5
    delay_seconds = 3

    try:
        print(f"🔍 Reading record: {record_id}")
        record = None
        for attempt in range(1, retries + 1):
            try:
                record = client.get(table_schema_name, record_id)
                if attempt > 1:
                    print(f"   ✅ Record read succeeded after {attempt} attempts.")
                break
            except HttpError as err:
                if getattr(err, "status_code", None) == 404 and attempt < retries:
                    print(
                        f"   ⏳ Record not queryable yet (attempt {attempt}/{retries}). Retrying in {delay_seconds}s..."
                    )
                    time.sleep(delay_seconds)
                    continue
                raise

        if record is None:
            raise RuntimeError("Record did not become available in time.")

        if record:
            print("✅ Record retrieved successfully!")
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
        print(f"❌ HTTP error during record reading: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Failed to read record: {e}")
        sys.exit(1)


def test_query_records(client: DataverseClient, table_info: Dict[str, Any]) -> None:
    """Test querying multiple records."""
    print("\n🔍 Record Query Test")
    print("=" * 50)

    table_schema_name = table_info.get("table_schema_name")
    attr_prefix = table_schema_name.split("_", 1)[0] if "_" in table_schema_name else table_schema_name
    retries = 5
    delay_seconds = 3

    try:
        print("🔍 Querying records from test table...")
        for attempt in range(1, retries + 1):
            try:
                records_iterator = client.get(
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

                print(f"✅ Query completed! Found {record_count} active records.")
                break
            except HttpError as err:
                if getattr(err, "status_code", None) == 404 and attempt < retries:
                    print(
                        f"   ⏳ Query retry {attempt}/{retries} after metadata 404 ({err}). Waiting {delay_seconds}s..."
                    )
                    time.sleep(delay_seconds)
                    continue
                raise

    except Exception as e:
        print(f"⚠️  Query test encountered an issue: {e}")
        print("   This might be expected if the table is very new.")


def cleanup_test_data(client: DataverseClient, table_info: Dict[str, Any], record_id: str) -> None:
    """Clean up test data."""
    print("\n🧹 Cleanup")
    print("=" * 50)

    table_schema_name = table_info.get("table_schema_name")
    retries = 5
    delay_seconds = 3

    # Ask user if they want to clean up
    cleanup_choice = input("Do you want to delete the test record? (y/N): ").strip().lower()

    if cleanup_choice in ["y", "yes"]:
        for attempt in range(1, retries + 1):
            try:
                client.delete(table_schema_name, record_id)
                print("✅ Test record deleted successfully")
                break
            except HttpError as err:
                status = getattr(err, "status_code", None)
                if status == 404:
                    print("ℹ️  Record already deleted or not yet available; skipping.")
                    break
                if attempt < retries:
                    print(
                        f"   ⏳ Record delete retry {attempt}/{retries} after error ({err}). Waiting {delay_seconds}s..."
                    )
                    time.sleep(delay_seconds)
                    continue
                print(f"⚠️  Failed to delete test record: {err}")
            except Exception as e:
                print(f"⚠️  Failed to delete test record: {e}")
                break
    else:
        print("ℹ️  Test record kept for inspection")

    # Ask about table cleanup
    table_cleanup = input("Do you want to delete the test table? (y/N): ").strip().lower()

    if table_cleanup in ["y", "yes"]:
        for attempt in range(1, retries + 1):
            try:
                client.delete_table(table_info.get("table_schema_name"))
                print("✅ Test table deleted successfully")
                break
            except HttpError as err:
                status = getattr(err, "status_code", None)
                if status == 404:
                    if _table_still_exists(client, table_info.get("table_schema_name")):
                        if attempt < retries:
                            print(
                                f"   ⏳ Table delete retry {attempt}/{retries} after metadata 404 ({err}). Waiting {delay_seconds}s..."
                            )
                            time.sleep(delay_seconds)
                            continue
                        print(f"⚠️  Failed to delete test table due to metadata delay: {err}")
                        break
                    print("✅ Test table deleted successfully (404 reported).")
                    break
                if attempt < retries:
                    print(
                        f"   ⏳ Table delete retry {attempt}/{retries} after error ({err}). Waiting {delay_seconds}s..."
                    )
                    time.sleep(delay_seconds)
                    continue
                print(f"⚠️  Failed to delete test table: {err}")
            except Exception as e:
                print(f"⚠️  Failed to delete test table: {e}")
                break
    else:
        print("ℹ️  Test table kept for future testing")


def _table_still_exists(client: DataverseClient, table_schema_name: Optional[str]) -> bool:
    if not table_schema_name:
        return False
    try:
        info = client.get_table_info(table_schema_name)
        return bool(info and info.get("entity_set_name"))
    except HttpError as probe_err:
        if getattr(probe_err, "status_code", None) == 404:
            return False
        return True
    except Exception:
        return True


def main():
    """Main test function."""
    print("🚀 PowerPlatform Dataverse Client SDK - Advanced Functional Testing")
    print("=" * 70)
    print("This script tests SDK functionality in a real Dataverse environment:")
    print("  • Authentication & Connection")
    print("  • Table Creation & Metadata Operations")
    print("  • Record CRUD Operations")
    print("  • Query Functionality")
    print("  • Interactive Cleanup")
    print("=" * 70)
    print("💡 For installation validation, run examples/basic/installation_example.py first")
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

        # Success summary
        print("\n🎉 Functional Test Summary")
        print("=" * 50)
        print("✅ Authentication: Success")
        print("✅ Table Operations: Success")
        print("✅ Record Creation: Success")
        print("✅ Record Reading: Success")
        print("✅ Record Querying: Success")
        print("\n💡 Your PowerPlatform Dataverse Client SDK is fully functional!")

        # Cleanup
        cleanup_test_data(client, table_info, record_id)

    except KeyboardInterrupt:
        print("\n\n⚠️  Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        print("💡 Please check your environment and try again")
        sys.exit(1)


if __name__ == "__main__":
    main()
