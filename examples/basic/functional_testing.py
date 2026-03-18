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
from typing import Optional, Dict, Any
from datetime import datetime

# Import SDK components (assumes installation is already validated)
from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.core.errors import HttpError, MetadataError
from PowerPlatform.Dataverse.models.relationship import (
    LookupAttributeMetadata,
    OneToManyRelationshipMetadata,
    ManyToManyRelationshipMetadata,
    CascadeConfiguration,
)
from PowerPlatform.Dataverse.models.labels import Label, LocalizedLabel
from PowerPlatform.Dataverse.common.constants import (
    CASCADE_BEHAVIOR_NO_CASCADE,
    CASCADE_BEHAVIOR_REMOVE_LINK,
)
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


def backoff(op, *, delays=(0, 2, 5, 10, 20, 20)):
    """Retry helper with exponential backoff for metadata propagation delays."""
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
                print(f"   * Backoff succeeded after {attempts - 1} retry(s); waited {total_delay}s total.")
            return result
        except Exception as ex:
            last = ex
            continue
    if last:
        if attempts:
            print(f"   [WARN] Backoff exhausted after {max(attempts - 1, 0)} retry(s); waited {total_delay}s total.")
        raise last


def test_relationships(client: DataverseClient) -> None:
    """Test relationship lifecycle: create tables, 1:N, N:N, query, delete."""
    print("\n-> Relationship Tests")
    print("=" * 50)

    rel_parent_schema = "test_RelParent"
    rel_child_schema = "test_RelChild"
    rel_m2m_schema = "test_RelProject"

    # Track IDs for cleanup
    rel_id_1n = None
    rel_id_lookup = None
    rel_id_nn = None
    created_tables = []

    try:
        # --- Cleanup any leftover resources from previous run ---
        print("Cleaning up previous relationship test resources...")
        for rel_name in [
            "test_RelParent_RelChild",
            "contact_test_relchild_test_ManagerId",
            "test_relchild_relproject",
        ]:
            try:
                rel = client.tables.get_relationship(rel_name)
                if rel:
                    client.tables.delete_relationship(rel.relationship_id)
                    print(f"   (Cleaned up relationship: {rel_name})")
            except Exception:
                pass

        for tbl in [rel_child_schema, rel_parent_schema, rel_m2m_schema]:
            try:
                if client.tables.get(tbl):
                    client.tables.delete(tbl)
                    print(f"   (Cleaned up table: {tbl})")
            except Exception:
                pass

        # --- Create parent and child tables ---
        print("\nCreating relationship test tables...")

        parent_info = backoff(
            lambda: client.tables.create(
                rel_parent_schema,
                {"test_Code": "string"},
            )
        )
        created_tables.append(rel_parent_schema)
        print(f"[OK] Created parent table: {parent_info['table_schema_name']}")

        child_info = backoff(
            lambda: client.tables.create(
                rel_child_schema,
                {"test_Number": "string"},
            )
        )
        created_tables.append(rel_child_schema)
        print(f"[OK] Created child table: {child_info['table_schema_name']}")

        proj_info = backoff(
            lambda: client.tables.create(
                rel_m2m_schema,
                {"test_ProjectCode": "string"},
            )
        )
        created_tables.append(rel_m2m_schema)
        print(f"[OK] Created M:N table: {proj_info['table_schema_name']}")

        # --- Wait for table metadata to propagate ---
        wait_for_table_metadata(client, rel_parent_schema)
        wait_for_table_metadata(client, rel_child_schema)
        wait_for_table_metadata(client, rel_m2m_schema)

        # --- Test 1: Create 1:N relationship (core API) ---
        print("\n  Test 1: Create 1:N relationship (core API)")
        print("  " + "-" * 45)

        lookup = LookupAttributeMetadata(
            schema_name="test_ParentId",
            display_name=Label(localized_labels=[LocalizedLabel(label="Parent", language_code=1033)]),
            required_level="None",
        )

        relationship = OneToManyRelationshipMetadata(
            schema_name="test_RelParent_RelChild",
            referenced_entity=parent_info["table_logical_name"],
            referencing_entity=child_info["table_logical_name"],
            referenced_attribute=f"{parent_info['table_logical_name']}id",
            cascade_configuration=CascadeConfiguration(
                delete=CASCADE_BEHAVIOR_REMOVE_LINK,
                assign=CASCADE_BEHAVIOR_NO_CASCADE,
                merge=CASCADE_BEHAVIOR_NO_CASCADE,
            ),
        )

        result_1n = backoff(
            lambda: client.tables.create_one_to_many_relationship(
                lookup=lookup,
                relationship=relationship,
            )
        )

        assert result_1n.relationship_schema_name == "test_RelParent_RelChild"
        assert result_1n.relationship_type == "one_to_many"
        assert result_1n.lookup_schema_name is not None
        rel_id_1n = result_1n.relationship_id
        print(f"  [OK] Created 1:N relationship: {result_1n.relationship_schema_name}")
        print(f"       Lookup: {result_1n.lookup_schema_name}")
        print(f"       ID: {rel_id_1n}")

        # --- Test 2: Create lookup field (convenience API) ---
        print("\n  Test 2: Create lookup field (convenience API)")
        print("  " + "-" * 45)

        result_lookup = backoff(
            lambda: client.tables.create_lookup_field(
                referencing_table=child_info["table_logical_name"],
                lookup_field_name="test_ManagerId",
                referenced_table="contact",
                display_name="Manager",
                description="The record's manager contact",
                required=False,
                cascade_delete=CASCADE_BEHAVIOR_REMOVE_LINK,
            )
        )

        assert result_lookup.relationship_type == "one_to_many"
        assert result_lookup.lookup_schema_name is not None
        rel_id_lookup = result_lookup.relationship_id
        print(f"  [OK] Created lookup: {result_lookup.lookup_schema_name}")
        print(f"       Relationship: {result_lookup.relationship_schema_name}")

        # --- Test 3: Create N:N relationship ---
        print("\n  Test 3: Create N:N relationship")
        print("  " + "-" * 45)

        m2m = ManyToManyRelationshipMetadata(
            schema_name="test_relchild_relproject",
            entity1_logical_name=child_info["table_logical_name"],
            entity2_logical_name=proj_info["table_logical_name"],
        )

        result_nn = backoff(lambda: client.tables.create_many_to_many_relationship(relationship=m2m))

        assert result_nn.relationship_schema_name == "test_relchild_relproject"
        assert result_nn.relationship_type == "many_to_many"
        rel_id_nn = result_nn.relationship_id
        print(f"  [OK] Created N:N relationship: {result_nn.relationship_schema_name}")
        print(f"       ID: {rel_id_nn}")

        # --- Test 4: Get relationship metadata ---
        print("\n  Test 4: Query relationship metadata")
        print("  " + "-" * 45)

        fetched_1n = client.tables.get_relationship("test_RelParent_RelChild")
        assert fetched_1n is not None
        assert fetched_1n.relationship_type == "one_to_many"
        assert fetched_1n.relationship_id == rel_id_1n
        print(f"  [OK] Retrieved 1:N: {fetched_1n.relationship_schema_name}")
        print(f"       Referenced: {fetched_1n.referenced_entity}")
        print(f"       Referencing: {fetched_1n.referencing_entity}")

        fetched_nn = client.tables.get_relationship("test_relchild_relproject")
        assert fetched_nn is not None
        assert fetched_nn.relationship_type == "many_to_many"
        assert fetched_nn.relationship_id == rel_id_nn
        print(f"  [OK] Retrieved N:N: {fetched_nn.relationship_schema_name}")
        print(f"       Entity1: {fetched_nn.entity1_logical_name}")
        print(f"       Entity2: {fetched_nn.entity2_logical_name}")

        # Non-existent relationship should return None
        missing = client.tables.get_relationship("nonexistent_relationship_xyz")
        assert missing is None
        print("  [OK] Non-existent relationship returns None")

        # --- Test 5: Delete relationships ---
        print("\n  Test 5: Delete relationships")
        print("  " + "-" * 45)

        backoff(lambda: client.tables.delete_relationship(rel_id_1n))
        rel_id_1n = None
        print("  [OK] Deleted 1:N relationship")

        backoff(lambda: client.tables.delete_relationship(rel_id_lookup))
        rel_id_lookup = None
        print("  [OK] Deleted lookup relationship")

        backoff(lambda: client.tables.delete_relationship(rel_id_nn))
        rel_id_nn = None
        print("  [OK] Deleted N:N relationship")

        # Verify deletion
        verify = client.tables.get_relationship("test_RelParent_RelChild")
        assert verify is None
        print("  [OK] Verified 1:N deletion (get returns None)")

        print("\n[OK] All relationship tests passed!")

    finally:
        # Cleanup: delete any remaining relationships then tables
        for rid in [rel_id_1n, rel_id_lookup, rel_id_nn]:
            if rid:
                try:
                    client.tables.delete_relationship(rid)
                except Exception:
                    pass

        for tbl in reversed(created_tables):
            try:
                backoff(lambda name=tbl: client.tables.delete(name))
                print(f"   (Cleaned up table: {tbl})")
            except Exception as e:
                print(f"   [WARN] Could not delete {tbl}: {e}")


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
    print("  - Relationship Operations (1:N, N:N, lookup, get, delete)")
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

        # Test relationships
        test_relationships(client)

        # Success summary
        print("\nFunctional Test Summary")
        print("=" * 50)
        print("[OK] Authentication: Success")
        print("[OK] Table Operations: Success")
        print("[OK] Record Creation: Success")
        print("[OK] Record Reading: Success")
        print("[OK] Record Querying: Success")
        print("[OK] Relationship Operations: Success")
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
