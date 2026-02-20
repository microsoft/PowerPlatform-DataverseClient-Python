# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Relationship Management Example for Dataverse SDK.

This example demonstrates:
- Creating one-to-many relationships using the core SDK API
- Creating lookup fields using the convenience method
- Creating many-to-many relationships
- Querying and deleting relationships
- Working with relationship metadata types

Prerequisites:
- pip install PowerPlatform-Dataverse-Client
- pip install azure-identity
"""

import sys
import time
from azure.identity import InteractiveBrowserCredential
from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.models.metadata import (
    LookupAttributeMetadata,
    OneToManyRelationshipMetadata,
    ManyToManyRelationshipMetadata,
    Label,
    LocalizedLabel,
    CascadeConfiguration,
)
from PowerPlatform.Dataverse.common.constants import (
    CASCADE_BEHAVIOR_NO_CASCADE,
    CASCADE_BEHAVIOR_REMOVE_LINK,
)


# Simple logging helper
def log_call(description):
    print(f"\n-> {description}")


def delete_relationship_if_exists(client, schema_name):
    """Delete a relationship by schema name if it exists."""
    rel = client.tables.get_relationship(schema_name)
    if rel:
        rel_id = rel.get("MetadataId")
        if rel_id:
            client.tables.delete_relationship(rel_id)
            print(f"   (Cleaned up existing relationship: {schema_name})")
            return True
    return False


def cleanup_previous_run(client):
    """Clean up any resources from a previous run to make the example idempotent."""
    print("\n-> Checking for resources from previous runs...")

    # Known relationship names created by this example
    relationships = [
        "new_Department_Employee",
        "contact_new_employee_new_ManagerId",
        "new_employee_project",
    ]

    # Known table names created by this example
    tables = ["new_Employee", "new_Department", "new_Project"]

    # Delete relationships first (required before tables can be deleted)
    for rel_name in relationships:
        try:
            delete_relationship_if_exists(client, rel_name)
        except Exception as e:
            print(f"   [WARN] Could not delete relationship {rel_name}: {e}")

    # Delete tables
    for table_name in tables:
        try:
            if client.tables.get(table_name):
                client.tables.delete(table_name)
                print(f"   (Cleaned up existing table: {table_name})")
        except Exception as e:
            print(f"   [WARN] Could not delete table {table_name}: {e}")


def backoff(op, *, delays=(0, 2, 5, 10, 20, 20)):
    """Retry helper with exponential backoff."""
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
                print(f"   * Backoff succeeded after {retry_count} retry(s); waited {total_delay}s total.")
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
    # Initialize relationship IDs to None for cleanup safety
    rel_id_1 = None
    rel_id_2 = None
    rel_id_3 = None

    print("=" * 80)
    print("Dataverse SDK - Relationship Management Example")
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
    # 2. CLEANUP PREVIOUS RUN (Idempotency)
    # ============================================================================
    print("\n" + "=" * 80)
    print("2. Cleanup Previous Run (Idempotency)")
    print("=" * 80)

    cleanup_previous_run(client)

    # ============================================================================
    # 3. CREATE SAMPLE TABLES
    # ============================================================================
    print("\n" + "=" * 80)
    print("3. Create Sample Tables")
    print("=" * 80)

    # Create a parent table (Department)
    log_call("Creating 'new_Department' table")

    dept_table = backoff(
        lambda: client.tables.create(
            "new_Department",
            {
                "new_DepartmentCode": "string",
                "new_Budget": "decimal",
            },
        )
    )
    print(f"[OK] Created table: {dept_table['table_schema_name']}")

    # Create a child table (Employee)
    log_call("Creating 'new_Employee' table")

    emp_table = backoff(
        lambda: client.tables.create(
            "new_Employee",
            {
                "new_EmployeeNumber": "string",
                "new_Salary": "decimal",
            },
        )
    )
    print(f"[OK] Created table: {emp_table['table_schema_name']}")

    # Create a project table for many-to-many example
    log_call("Creating 'new_Project' table")

    proj_table = backoff(
        lambda: client.tables.create(
            "new_Project",
            {
                "new_ProjectCode": "string",
                "new_StartDate": "datetime",
            },
        )
    )
    print(f"[OK] Created table: {proj_table['table_schema_name']}")

    # ============================================================================
    # 4. CREATE ONE-TO-MANY RELATIONSHIP (Core SDK API)
    # ============================================================================
    print("\n" + "=" * 80)
    print("4. Create One-to-Many Relationship (Core API)")
    print("=" * 80)

    log_call("Creating lookup field on Employee referencing Department")

    # Define the lookup attribute metadata
    lookup = LookupAttributeMetadata(
        schema_name="new_DepartmentId",
        display_name=Label(localized_labels=[LocalizedLabel(label="Department", language_code=1033)]),
        required_level="None",
    )

    # Define the relationship metadata
    relationship = OneToManyRelationshipMetadata(
        schema_name="new_Department_Employee",
        referenced_entity=dept_table["table_logical_name"],
        referencing_entity=emp_table["table_logical_name"],
        referenced_attribute=f"{dept_table['table_logical_name']}id",
        cascade_configuration=CascadeConfiguration(
            delete=CASCADE_BEHAVIOR_REMOVE_LINK,
            assign=CASCADE_BEHAVIOR_NO_CASCADE,
            merge=CASCADE_BEHAVIOR_NO_CASCADE,
        ),
    )

    # Create the relationship
    result = backoff(
        lambda: client.tables.create_one_to_many_relationship(
            lookup=lookup,
            relationship=relationship,
        )
    )

    print(f"[OK] Created relationship: {result['relationship_schema_name']}")
    print(f"  Lookup field: {result['lookup_schema_name']}")
    print(f"  Relationship ID: {result['relationship_id']}")

    rel_id_1 = result["relationship_id"]

    # ============================================================================
    # 5. CREATE LOOKUP FIELD (Convenience Method)
    # ============================================================================
    print("\n" + "=" * 80)
    print("5. Create Lookup Field (Convenience Method)")
    print("=" * 80)

    log_call("Creating lookup field on Employee referencing Contact as Manager")

    # Use the convenience method for simpler scenarios
    # An Employee has a Manager (who is a Contact in the system)
    result2 = backoff(
        lambda: client.tables.create_lookup_field(
            referencing_table=emp_table["table_logical_name"],
            lookup_field_name="new_ManagerId",
            referenced_table="contact",
            display_name="Manager",
            description="The employee's direct manager",
            required=False,
            cascade_delete=CASCADE_BEHAVIOR_REMOVE_LINK,
        )
    )

    print(f"[OK] Created lookup using convenience method: {result2['lookup_schema_name']}")
    print(f"  Relationship: {result2['relationship_schema_name']}")

    rel_id_2 = result2["relationship_id"]

    # ============================================================================
    # 6. CREATE MANY-TO-MANY RELATIONSHIP
    # ============================================================================
    print("\n" + "=" * 80)
    print("6. Create Many-to-Many Relationship")
    print("=" * 80)

    log_call("Creating M:N relationship between Employee and Project")

    # Define many-to-many relationship
    m2m_relationship = ManyToManyRelationshipMetadata(
        schema_name="new_employee_project",
        entity1_logical_name=emp_table["table_logical_name"],
        entity2_logical_name=proj_table["table_logical_name"],
    )

    result3 = backoff(
        lambda: client.tables.create_many_to_many_relationship(
            relationship=m2m_relationship,
        )
    )

    print(f"[OK] Created M:N relationship: {result3['relationship_schema_name']}")
    print(f"  Relationship ID: {result3['relationship_id']}")

    rel_id_3 = result3["relationship_id"]

    # ============================================================================
    # 7. QUERY RELATIONSHIP METADATA
    # ============================================================================
    print("\n" + "=" * 80)
    print("7. Query Relationship Metadata")
    print("=" * 80)

    log_call("Retrieving 1:N relationship by schema name")

    rel_metadata = client.tables.get_relationship("new_Department_Employee")
    if rel_metadata:
        print(f"[OK] Found relationship: {rel_metadata.get('SchemaName')}")
        print(f"  Type: {rel_metadata.get('@odata.type')}")
        print(f"  Referenced Entity: {rel_metadata.get('ReferencedEntity')}")
        print(f"  Referencing Entity: {rel_metadata.get('ReferencingEntity')}")
    else:
        print("  Relationship not found")

    log_call("Retrieving M:N relationship by schema name")

    m2m_metadata = client.tables.get_relationship("new_employee_project")
    if m2m_metadata:
        print(f"[OK] Found relationship: {m2m_metadata.get('SchemaName')}")
        print(f"  Type: {m2m_metadata.get('@odata.type')}")
        print(f"  Entity 1: {m2m_metadata.get('Entity1LogicalName')}")
        print(f"  Entity 2: {m2m_metadata.get('Entity2LogicalName')}")
    else:
        print("  Relationship not found")

    # ============================================================================
    # 8. CLEANUP
    # ============================================================================
    print("\n" + "=" * 80)
    print("8. Cleanup")
    print("=" * 80)

    cleanup = input("\nDelete created relationships and tables? (y/n): ").strip().lower()

    if cleanup == "y":
        # Delete relationships first (required before deleting tables)
        log_call("Deleting relationships")
        try:
            if rel_id_1:
                backoff(lambda: client.tables.delete_relationship(rel_id_1))
                print(f"  [OK] Deleted relationship: new_Department_Employee")
        except Exception as e:
            print(f"  [WARN] Error deleting relationship 1: {e}")

        try:
            if rel_id_2:
                backoff(lambda: client.tables.delete_relationship(rel_id_2))
                print(f"  [OK] Deleted relationship: contact->employee (Manager)")
        except Exception as e:
            print(f"  [WARN] Error deleting relationship 2: {e}")

        try:
            if rel_id_3:
                backoff(lambda: client.tables.delete_relationship(rel_id_3))
                print(f"  [OK] Deleted relationship: new_employee_project")
        except Exception as e:
            print(f"  [WARN] Error deleting relationship 3: {e}")

        # Delete tables
        log_call("Deleting tables")
        for table_name in ["new_Employee", "new_Department", "new_Project"]:
            try:
                backoff(lambda name=table_name: client.tables.delete(name))
                print(f"  [OK] Deleted table: {table_name}")
            except Exception as e:
                print(f"  [WARN] Error deleting {table_name}: {e}")

        print("\n[OK] Cleanup complete")
    else:
        print("\nSkipping cleanup. Remember to manually delete:")
        print("  - Relationships: new_Department_Employee, contact->employee (Manager), new_employee_project")
        print("  - Tables: new_Employee, new_Department, new_Project")

    print("\n" + "=" * 80)
    print("Example Complete!")
    print("=" * 80)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nExample interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nError: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
